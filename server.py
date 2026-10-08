"""Element Zero v0.12 lightweight WebSocket lobby relay.
Run: pip install -r requirements.txt && python server.py
Host this behind TLS (wss://) for Godot browser clients served over HTTPS.
This server manages rooms and ready states; it does not relay gameplay.
"""
import asyncio
import json
import secrets
from websockets.asyncio.server import serve

rooms = {}
clients = {}

async def send(ws, payload):
    await ws.send(json.dumps(payload))

async def broadcast(code, payload):
    for ws in list(rooms.get(code, {}).get('players', [])):
        try:
            await send(ws, payload)
        except Exception:
            pass

async def announce(code, message=''):
    room = rooms.get(code)
    if room:
        await broadcast(code, {'type':'room_update','room':code,
                               'members':[{'name':clients[w]['name'],'ready':clients[w]['ready']} for w in room['players']],
                               'message':message})

async def leave(ws):
    data = clients.get(ws)
    if not data:
        return
    code = data['room']
    if code in rooms:
        players = rooms[code]['players']
        if ws in players:
            players.remove(ws)
        if not players:
            del rooms[code]
        else:
            for w in players:
                clients[w]['ready'] = False
            await announce(code, 'A player left the room.')
    data['room'] = ''
    data['ready'] = False

async def handler(ws):
    clients[ws] = {'room':'','name':'Player','ready':False}
    try:
        async for raw in ws:
            try:
                packet = json.loads(raw)
                if not isinstance(packet, dict):
                    continue
            except (ValueError, TypeError):
                await send(ws, {'type':'error','message':'Invalid JSON'})
                continue
            kind = packet.get('type')
            if kind == 'leave_room':
                await leave(ws)
                await send(ws, {'type':'room_update','room':'','members':[],'message':'Left room.'})
            elif kind == 'create_room':
                await leave(ws)
                code = secrets.token_hex(3).upper()
                while code in rooms:
                    code = secrets.token_hex(3).upper()
                clients[ws].update(name=str(packet.get('name','Player'))[:24], room=code)
                rooms[code] = {'players':[ws]}
                await announce(code, 'Room created. Share the code with a friend.')
            elif kind == 'join_room':
                code = str(packet.get('room','')).strip().upper()
                if code not in rooms or len(rooms[code]['players']) >= 2:
                    await send(ws, {'type':'error','message':'Room not found or full.'})
                    continue
                await leave(ws)
                clients[ws].update(name=str(packet.get('name','Player'))[:24],room=code)
                rooms[code]['players'].append(ws)
                await announce(code, 'Two players connected. Mark ready.')
            elif kind == 'ready':
                code = clients[ws]['room']
                if not code or code not in rooms:
                    await send(ws, {'type':'error','message':'Join a room first.'})
                    continue
                clients[ws]['ready'] = bool(packet.get('ready',False))
                await announce(code)
                players = rooms[code]['players']
                if len(players) == 2 and all(clients[p]['ready'] for p in players):
                    await broadcast(code, {'type':'match_ready','message':'Both players ready. Combat networking not implemented.'})
            else:
                await send(ws, {'type':'error','message':'Unknown command.'})
    finally:
        await leave(ws)
        clients.pop(ws,None)

async def main():
    async with serve(handler, '0.0.0.0', 8765, max_size=16384):
        print('Element Zero lobby relay listening on port 8765')
        await asyncio.Future()

if __name__ == '__main__':
    asyncio.run(main())
