import asyncio
import websockets
async def hello(websocket):
    name = await websocket.recv() # receive a data from clients
    print(f'Server Received: {name}')
    greeting = f'Hello, {name}'

    await websocket.send(greeting)# sending a data to a client
    print(f'Server sent: {greeting}')
    await websocket.close()
async def main():
    async with websockets.serve(hello, 'localhost', 8765): #creating a server
        await asyncio.Future() #runs forever
if __name__ == '__main__':
    asyncio.run(main())