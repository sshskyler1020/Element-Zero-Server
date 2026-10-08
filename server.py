"""Element Zero v0.12 lightweight WebSocket lobby relay.
Run: pip install -r requirements.txt && python server.py
Host this behind TLS (wss://) for Godot browser clients served over HTTPS.
This server manages rooms and ready states; it does not relay gameplay.
"""
import asyncio
import json
import os
import secrets
from websockets.asyncio.server import serve
from websockets.http11 import Response
from websockets.datastructures import Headers

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
                await send(ws, {'type':'slot','slot':1})
                await announce(code, 'Room created. Share the code with a friend.')
            elif kind == 'join_room':
                code = str(packet.get('room','')).strip().upper()
                if code not in rooms or len(rooms[code]['players']) >= 2:
                    await send(ws, {'type':'error','message':'Room not found or full.'})
                    continue
                await leave(ws)
                clients[ws].update(name=str(packet.get('name','Player'))[:24],room=code)
                rooms[code]['players'].append(ws)
                await send(ws, {'type':'slot','slot':2})
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
                    for idx, player in enumerate(players):
                        await send(player, {'type':'match_ready','slot':idx+1,'message':'Match starting!'})
            elif kind == 'gameplay':
                code = clients[ws]['room']
                if code in rooms and len(rooms[code]['players']) == 2:
                    players = rooms[code]['players']
                    slot = players.index(ws) + 1
                    if (packet.get('kind') == 'input' and slot == 2) or (packet.get('kind') == 'snapshot' and slot == 1):
                        payload = dict(packet)
                        payload['slot'] = slot
                        for other in players:
                            if other != ws:
                                await send(other, payload)
            else:
                await send(ws, {'type':'error','message':'Unknown command.'})
    finally:
        await leave(ws)
        clients.pop(ws,None)

def health_check(connection, request):
    if request.path in ('/', '/health') and request.headers.get('Upgrade', '').lower() != 'websocket':
        body = b'Element Zero lobby server OK\n'
        return Response(200, 'OK', Headers([('Content-Type', 'text/plain; charset=utf-8'), ('Content-Length', str(len(body)))]), body)
    return None

async def main():
    port = int(os.environ.get('PORT', '8765'))
    async with serve(handler, '0.0.0.0', port, max_size=16384, process_request=health_check):
        print(f'Element Zero lobby relay listening on port {port}', flush=True)
        await asyncio.Future()

if __name__ == '__main__':
    asyncio.run(main())
