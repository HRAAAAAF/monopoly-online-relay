"""Atomic local relay snapshots. This file contains private reconnect tokens."""
import json,os,time
from pathlib import Path

ROOM_FIELDS=('host_pid','started','finished','winner_pid','active_seats','turn_pid',
             'doubles_count','rolls','seq','event_seq','native_mode','native_setup',
             'ready_checkpoints','pending_event','pending_kind','native_acks',
             'native_fault','event_history','checkpoints','native_decision',
             'decision_reports','decisions')
INTEGER_MAPS=('ready_checkpoints','native_acks','event_history','checkpoints','decision_reports','decisions')

def save(server,path):
    rooms=[]
    now=time.time()
    for room in server.rooms.values():
        if not room.native_mode or not room.started:continue
        rooms.append({'code':room.code,'saved_activity':now-(time.monotonic()-room.last_activity),
                      'state':{key:getattr(room,key) for key in ROOM_FIELDS},
                      'players':[{'pid':p.pid,'name':p.name,'token':p.token} for p in room.players.values()]})
    target=Path(path);target.parent.mkdir(parents=True,exist_ok=True)
    temporary=target.with_name(target.name+'.tmp')
    fd=os.open(temporary,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
    try:
        with os.fdopen(fd,'w',encoding='utf-8') as output:
            json.dump({'schema':1,'rooms':rooms},output,separators=(',',':'))
            output.flush();os.fsync(output.fileno())
        os.replace(temporary,target)
    finally:
        temporary.unlink(missing_ok=True)

def restore(server,path,ttl):
    from .server import Player,Room
    target=Path(path)
    if not target.exists():return
    if target.stat().st_size>64*1024*1024:raise ValueError('Relay recovery snapshot is too large')
    data=json.loads(target.read_text(encoding='utf-8'))
    if data.get('schema')!=1 or not isinstance(data.get('rooms'),list):raise ValueError('Unsupported relay recovery snapshot')
    for saved in data['rooms']:
        age=max(0,time.time()-saved['saved_activity'])
        if age>=ttl:continue
        records=saved['players']
        if not 2<=len(records)<=4:raise ValueError('Invalid saved relay seats')
        players=[Player(p['pid'],p['name'],p['token']) for p in records]
        room=Room(saved['code'],players[0])
        for player in players[1:]:room.add(player)
        state=saved['state']
        if set(state)!=set(ROOM_FIELDS):raise ValueError('Invalid relay recovery fields')
        for key in ROOM_FIELDS:
            value=state[key]
            if key in INTEGER_MAPS:value={int(k):v for k,v in value.items()}
            setattr(room,key,value)
        room.last_activity=time.monotonic()-age
        for player in players:
            if player.pid in server.players or player.token in server.tokens:raise ValueError('Duplicate saved relay identity')
            player.recovering=True
            server.players[player.pid]=player;server.tokens[player.token]=player
            server._next_pid=max(server._next_pid,player.pid+1)
        server.rooms[room.code]=room
