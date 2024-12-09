import asyncio
import websockets

async def hello():
    uri = "ws://localhost:8765" # to the client server
    async with websockets.connect(uri) as websocket: #connecting to an existing websocket server
        name = input("what is your name?")
        await websocket.send(name) #send name to client side , Post method
        print(f'Client sent: {name}')
        yo = await websocket.recv()#received data from a server
        print(f'Client received: {yo}')
if __name__ == '__main__':
    asyncio.run(hello())


