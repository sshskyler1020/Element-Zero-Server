"""Element Zero v1.3 8-fighter relay. Render: python server.py"""
import asyncio, json, os, secrets
from aiohttp import web
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
async def health(request):
    return web.Response(text='Element Zero lobby server OK\n')

async def websocket_endpoint(request):
    ws = web.WebSocketResponse(heartbeat=25, max_msg_size=32768)
    if not ws.can_prepare(request).ok:
        return web.Response(text='WebSocket upgrade required\n', status=426)
    await ws.prepare(request)
    class SocketAdapter:
        def __init__(self, socket): self.socket=socket
        async def send(self, message): await self.socket.send_str(message)
        def __aiter__(self): return self
        async def __anext__(self):
            async for msg in self.socket:
                if msg.type == web.WSMsgType.TEXT: return msg.data
                if msg.type in (web.WSMsgType.CLOSE,web.WSMsgType.CLOSED,web.WSMsgType.ERROR): break
            raise StopAsyncIteration
    await handler(SocketAdapter(ws))
    return ws

async def root(request):
    if request.method == 'HEAD': return web.Response(text='')
    if request.headers.get('Upgrade','').lower() == 'websocket':
        return await websocket_endpoint(request)
    return await health(request)

app = web.Application()
app.router.add_route('GET', '/', root)
app.router.add_route('HEAD', '/', root)
app.router.add_route('GET', '/health', health)
app.router.add_route('HEAD', '/health', health)
app.router.add_route('GET', '/ws', websocket_endpoint)

if __name__ == '__main__':
    web.run_app(app, host='0.0.0.0', port=int(os.environ.get('PORT', '10000')))
