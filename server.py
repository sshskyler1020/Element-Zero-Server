"""Element Zero v1.1 8-fighter relay. Render: python server/server.py"""
import asyncio, json, os, secrets
from http import HTTPStatus
from websockets.asyncio.server import serve
rooms={}; clients={}
async def send(ws,obj):
    try: await ws.send(json.dumps(obj,separators=(',',':')))
    except Exception: pass
async def broadcast(room,obj,exclude=None):
    await asyncio.gather(*(send(w,obj) for w in list(room['players']) if w!=exclude))
def count(room): return sum(clients[w]['local_count'] for w in room['players'])
async def announce(code,msg=''):
    room=rooms.get(code)
    if room:
        await broadcast(room,{'type':'room_update','room':code,'max_fighters':room['max_fighters'],'fighter_count':count(room),'members':[{'name':clients[w]['name'],'ready':clients[w]['ready'],'local_count':clients[w]['local_count'],'slots':clients[w].get('slots',[])} for w in room['players']],'message':msg})
async def leave(ws):
    u=clients.get(ws)
    if not u:return
    code=u['room'];room=rooms.get(code)
    if room and ws in room['players']:
        room['players'].remove(ws)
        if not room['players']:rooms.pop(code,None)
        else:
            room['started']=False
            for w in room['players']:clients[w]['ready']=False
            await announce(code,'A player left; ready up again.')
            await broadcast(room,{'type':'opponent_left'})
    u.update(room='',ready=False,slots=[])
async def handler(ws):
    clients[ws]={'room':'','name':'Player','ready':False,'local_count':1,'slots':[]}
    try:
        async for raw in ws:
            if not isinstance(raw,str) or len(raw)>32768:continue
            try:
                p=json.loads(raw)
                if not isinstance(p,dict):continue
            except (ValueError,TypeError):continue
            kind=p.get('type');u=clients[ws]
            if kind=='create_room':
                await leave(ws)
                code=secrets.token_hex(3).upper()
                while code in rooms:code=secrets.token_hex(3).upper()
                u.update(name=str(p.get('name') or 'Player')[:24],room=code,local_count=max(1,min(2,int(p.get('local_count',1)))))
                rooms[code]={'players':[ws],'started':False,'max_fighters':max(2,min(8,int(p.get('max_fighters',8))))}
                await announce(code,'Room created. Share the code.')
            elif kind=='join_room':
                code=str(p.get('room') or p.get('code') or '').strip().upper();r=rooms.get(code)
                n=max(1,min(2,int(p.get('local_count',1))))
                if not r or r['started'] or count(r)+n>r['max_fighters']:
                    await send(ws,{'type':'error','message':'Room missing, full, or already started.'});continue
                await leave(ws);u.update(name=str(p.get('name') or 'Player')[:24],room=code,local_count=n);r['players'].append(ws)
                for w in r['players']:clients[w]['ready']=False
                await announce(code,'Player joined. Everyone must ready up.')
            elif kind=='leave_room':
                await leave(ws);await send(ws,{'type':'room_update','room':'','members':[]})
            elif kind=='ready':
                r=rooms.get(u['room'])
                if not r:await send(ws,{'type':'error','message':'Join a room first.'});continue
                u['ready']=bool(p.get('ready'));await announce(u['room'])
                if len(r['players'])>=2 and count(r)>=2 and all(clients[w]['ready'] for w in r['players']) and not r['started']:
                    r['started']=True;slot=1
                    for w in r['players']:
                        n=clients[w]['local_count'];clients[w]['slots']=list(range(slot,slot+n));slot+=n
                    for w in r['players']:
                        await send(w,{'type':'match_ready','slot':clients[w]['slots'][0],'slots':clients[w]['slots'],'host':w==r['players'][0],'total':slot-1,'room':u['room'],'fighters':['fire','water'],'stage':'zero_lab'})
            elif kind=='gameplay':
                r=rooms.get(u['room']);data=p.get('data');sub=p.get('kind')
                if not r or not r['started'] or not isinstance(data,dict) or len(json.dumps(data))>24000:continue
                host=r['players'][0]
                if sub=='snapshot' and ws==host:await broadcast(r,{'type':'gameplay','kind':'snapshot','data':data},exclude=ws)
                elif sub=='input' and ws!=host:
                    clean=[]
                    for item in data.get('inputs',[]):
                        if isinstance(item,dict) and item.get('slot') in u['slots'] and isinstance(item.get('ctrl'),dict):
                            clean.append({'slot':item['slot'],'ctrl':{k:bool(v) for k,v in item['ctrl'].items() if k in ('left','right','jump','light','heavy','element','dodge','zero')}})
                    if clean:await send(host,{'type':'gameplay','kind':'input','data':{'inputs':clean}})
    finally:
        await leave(ws);clients.pop(ws,None)
def health(conn,request):
    if request.path in ('/','/health'):
        from websockets.http11 import Response
        return Response(HTTPStatus.OK,'OK',__import__('websockets').Headers([('Content-Type','text/plain'),('Access-Control-Allow-Origin','*')]),b'Element Zero lobby server OK\n')
async def main():
    port=int(os.environ.get('PORT','10000'))
    async with serve(handler,'0.0.0.0',port,process_request=health,max_size=32768):
        print('Element Zero 8-fighter server listening on',port,flush=True)
        await asyncio.Future()
if __name__=='__main__':asyncio.run(main())
