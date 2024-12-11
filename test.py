async def hyperliquid_websocket_handler(ws_url, symbol, stream_type): # return  [level1, level2] such that levels = [px(price), sz(size), n(number of trades)] , levels1 = bid, levels2 = ask
    hyperliquid_message = {
        "method": "subscribe",
        "subscription": {"type": "l2Book", "coin": symbol}
    }
    try:
        # Connect to the WebSocket server
        async with websockets.connect(ws_url) as websocket:
            logging.info("Connected to Hyperliquid WebSocket")

            # Send subscription message
            await websocket.send(json.dumps(hyperliquid_message))
            logging.info("Sent subscription message: %s", json.dumps(hyperliquid_message))

            # Receive data from WebSocket
            while True:
                data = await websocket.recv()
                # logging.info("Received data: %s", data)
                handle_message(data)


    except Exception as e:
        logging.error("Error: %s", str(e))