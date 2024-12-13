import sys
import ccxt.async_support as ccxt
import asyncio
import time
import dummy_config as config
from datetime import datetime
"""
API Hedge incorporating all three exchanges
Make sure the data is pulled correctly before hedging. If data isn't pulled properly send out an error.
"""
logger = config.setup_logger('APIHedge')

if sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

class APIHedge:
    def __init__(self, symbols):
        self.okx_contract_sz = config.OKX_CONTRACT_SZ
        self.hf_traded_coins = symbols
        # self.binance_client = ccxt.binanceusdm({
        #     'apiKey': config.BINANCE_API_KEY,
        #     'secret': config.BINANCE_SECRET_KEY,
        #     'enableRateLimit': True,
        # })
        self.okx_client = ccxt.okx({
            'apiKey': config.OKX_API_KEY,
            'secret': config.OKX_SECRET_KEY,
            'password': config.OKX_PASSPHRASE,
            'enableRateLimit': True,
            'options': {'defaultType': 'swap'}
        })
        # self.gate_client = ccxt.gateio({
        #     'apiKey': config.GATE_API_KEY,
        #     'secret': config.GATE_SECRET_KEY,
        #     'enableRateLimit': True,
        #     'options': {'defaultType': 'swap'}
        # })
        self.bybit_client = ccxt.bybit({
            'apiKey': config.BYBIT_API_KEY,
            'secret': config.BYBIT_SECRET_KEY,
            'enableRateLimit': True,
            'options': {'defaultType': 'swap'}
        })

    async def get_positions(self, exchange, model):
        try:
            if model == 'okx':
                positions = await exchange.fetch_positions()
                print('okx positions:', positions)
                return {p['symbol']: p['contracts'] * (-1 if p['side'] == 'short' else 1) for p in positions if p['contracts'] != 0}
            elif model == 'binance':
                positions = await exchange.fetch_positions()
                print('binance positions:', positions)
                return {p['symbol']: p['contracts'] * (-1 if p['side'] == 'short' else 1) for p in positions if p['contracts'] != 0}
            elif model == 'gate':
                positions = await exchange.fetch_positions()
                print('gate positions:', positions)
                return {p['symbol']: p['contracts'] * p['contractSize'] * (-1 if p['side'] == 'short' else 1) for p in
                        positions if p['contracts'] != 0}
            elif model == 'bybit':
                positions = await exchange.fetch_positions()
                print('bybit positions:', positions)
                return {p['symbol']: p['contracts'] * (-1 if p['side'] == 'short' else 1) for p in positions if p['contracts'] != 0}
            
        except Exception as e:
            logger.error(f"Error fetching positions from {exchange.id}: {e}")
            return {}

    async def close_clients(self):
        # if self.binance_client:
        #     await self.binance_client.close()
        if self.okx_client:
            await self.okx_client.close()
        # if self.gate_client:
        #     await self.gate_client.close()
        if self.bybit_client:
            await self.bybit_client.close()

    # async def adjust_binance_position(self, symbol, amount):
    #     try:
    #         side = 'buy' if amount > 0 else 'sell'
    #         await self.binance_client.create_market_order(symbol, side, abs(amount))
    #         logger.info(f"Adjusted position on Binance for {symbol}: {amount}")
    #         config.send_telegram_message(
    #             f'Successfully adjusted position on Binance for {symbol}: {amount}',
    #             config.bot_token2, config.chat_id2)
    #     except Exception as e:
    #         if 'notional must be no smaller than' in str(e):
    #             logger.info(f"Notional difference for {symbol} is below the minimum notional threshold, hedging skipped")
    #         elif 'Timestamp for this request is outside of the recvWindow' in str(e):
    #             logger.info(f"Timestamp outside of Recvwindow for {symbol} hedge skipped for this iteration")
    #             config.send_telegram_message(f'API Hedge Error, unable to adjust notionals due to timestamp error error. Please check bot if this error continues to repeat. {e}', config.bot_token2, config.chat_id2)
    #         elif 'minimum amount precision' in str(e):
    #             logger.info(f"Precision for {symbol} is below the minimum precision threshold, hedging skipped")
    #         # TODO: Add in stop trading signal when notional flag is triggered
    #         # elif 'Exceeded the maximum allowable position at current leverage.' in str(e):
    #         #     logger.info(f"Notional for {symbol} is above the maximum allowable position, shutting down all bots")
    #         else:
    #             # TODO: Add in stop trading signal when unknown error is triggered
    #             config.send_telegram_message(f'API Hedge Error, unable to adjust notionals due to an unexpected error. Please check the bot. {e}', config.bot_token2, config.chat_id2)
    #             logger.error(f"Error adjusting position on Binance for {symbol}: {e}")
        
    async def adjust_okx_position(self, symbol, amount):
        try:
            side = 'buy' if amount > 0 else 'sell'
            await self.okx_client.create_market_order(symbol, side, abs(amount))
            logger.info(f"Adjusted position on OKX for {symbol}: {amount}")
            config.send_telegram_message(
                f'Successfully adjusted position on OKX for {symbol}: {amount}',
                config.bot_token2, config.chat_id2)
        except Exception as e:
            if 'notional must be no smaller than' in str(e):
                logger.info(f"Notional difference for {symbol} is below the minimum notional threshold, hedging skipped")
            elif 'Timestamp for this request is outside of the recvWindow' in str(e):
                logger.info(f"Timestamp outside of Recvwindow for {symbol} hedge skipped for this iteration")
                config.send_telegram_message(f'API Hedge Error, unable to adjust notionals due to timestamp error error. Please check bot if this error continues to repeat. {e}', config.bot_token2, config.chat_id2)
            elif 'minimum amount precision' in str(e):
                logger.info(f"Precision for {symbol} is below the minimum precision threshold, hedging skipped")
            else:
                config.send_telegram_message(f'API Hedge Error, unable to adjust notionals due to an unexpected error. Please check the bot. {e}', config.bot_token2, config.chat_id2)
                logger.error(f"Error adjusting position on OKX for {symbol}: {e}")

    # async def hedge_check(self, okx_positions, binance_positions, gate_positions):
    #     if okx_positions and binance_positions: # and gate_positions:
    #         for coin in self.hf_traded_coins:
    #             okx_symbol = f"{coin}/USDT:USDT"
    #             binance_symbol = f"{coin}/USDT:USDT"
    #             gate_symbol = f"{coin}/USDT:USDT"

    #             okx_amount = round(okx_positions.get(okx_symbol, 0) * self.okx_contract_sz.get(f"{coin}-USDT-SWAP", 1), 5)
    #             binance_amount = round(binance_positions.get(binance_symbol, 0), 5)
    #             gate_amount = round(gate_positions.get(gate_symbol, 0), 5)

    #             if abs(okx_amount + binance_amount + gate_amount) > 0.001:
    #                 logger.info(f"Mismatch detected for {coin}: OKX={okx_amount}, Binance={binance_amount}, Gate={gate_amount}")
    #                 adjustment = - round((gate_amount + okx_amount) + binance_amount, 4)
    #                 if abs(adjustment) > 0.0001:
    #                     await self.adjust_binance_position(binance_symbol, adjustment)
    #     else:
    #         logger.info(f"Error fetching position data from exchange - API limit reached")
    #     logger.info("Hedge check completed")

    async def hedge_check(self, okx_positions, bybit_positions):
        if okx_positions and bybit_positions:
            for coin in self.hf_traded_coins:
                okx_symbol = f"{coin}/USDT:USDT"
                bybit_symbol = f"{coin}/USDT:USDT"

                okx_amount = round(okx_positions.get(okx_symbol, 0) * self.okx_contract_sz.get(f"{coin}-USDT-SWAP", 1), 5)
                bybit_amount = round(bybit_positions.get(bybit_symbol, 0), 5)

                if abs(okx_amount + bybit_amount) > 0.001:
                    logger.info(f"Mismatch detected for {coin}: OKX={okx_amount}, Bybit={bybit_amount}")
                    adjustment = - round((bybit_amount + okx_amount), 4)
                    adjustment = adjustment / self.okx_contract_sz.get(f"{coin}-USDT-SWAP", 1)
                    adjustment = round(adjustment, 5)
                    if abs(adjustment) > 0.0001:
                        await self.adjust_okx_position(okx_symbol, adjustment)
        else:
            logger.info(f"Error fetching position data from exchange - API limit reached")
        logger.info("Hedge check completed")

    async def perform_hedge_check(self):
        start_time = time.time()
        try:
            # binance_positions, okx_positions, gate_positions = await asyncio.gather(
            #     self.get_positions(self.binance_client, 'binance'),
            #     self.get_positions(self.okx_client, 'okx'),
            #     self.get_positions(self.gate_client, 'gate')
            # )
            okx_positions, bybit_positions = await asyncio.gather(
                self.get_positions(self.okx_client, 'okx'),
                self.get_positions(self.bybit_client, 'bybit')
            )
            # logger.info("Binance positions:")
            # logger.info(binance_positions)
            logger.info("OKX positions:")
            logger.info(okx_positions)
            # logger.info("Gate positions:")
            # logger.info(gate_positions)
            logger.info("Bybit positions:")
            logger.info(bybit_positions)
            await self.hedge_check(okx_positions, bybit_positions)

        except Exception as e:
            logger.error(f"An unexpected error occurred during hedge check: {e}")
            end_time = time.time()
            total_duration = end_time - start_time
            logger.error(f"Error occurred after {total_duration:.4f} seconds")
        finally:
            await self.close_clients()

    async def run(self):
        while True:
            await self.perform_hedge_check()
            await asyncio.sleep(300)  # 5 minutes interval


if __name__ == "__main__":
    error_hedge = APIHedge(config.IN_TRADE)
    try:
        asyncio.run(error_hedge.run())
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt: Shutting down API Hedge")
        exit(0)
