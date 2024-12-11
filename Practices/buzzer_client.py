import asyncio
import websockets
import keyboard

async def start_client():
    uri = "ws://localhost:8765"
    async with websockets.connect(uri) as websocket:
        done = False
        while not done:
            if keyboard.is_pressed("space"):
                await websocket.send("buzz")
                message = await websocket.recv()
                print(message)
                done = True
# run the client
asyncio.run(start_client())