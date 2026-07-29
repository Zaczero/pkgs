"""Minimal WebSocket echo used to measure the text send path."""


async def app(scope, receive, send):
    if scope['type'] == 'lifespan':
        message = await receive()
        await send({'type': f'{message["type"]}.complete'})
        return
    await receive()
    await send({'type': 'websocket.accept'})
    while True:
        message = await receive()
        if message['type'] != 'websocket.receive':
            return
        await send({'type': 'websocket.send', 'text': message['text']})
