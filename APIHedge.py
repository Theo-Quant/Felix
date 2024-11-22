import ccxt.async_support as ccxt
import asyncio
import time
import config
from datetime import datetime

logger = config.setup_logger('APIHedge')

class APIHedge:
    def __init__(self, symbols):
        self.okx_contract_sz = config.OKX_CONTRACT_SZ
        self.hf_traded_coins = symbols
        self.binance_client = ccxt.binanceusdm({
            'apiKey': config.BINANCE_API_KEY,
            'secret': config.BINANCE_SECRET_KEY,
            'enableRateLimit': True,
        })
        self.okx_client = ccxt.okx({
            'apiKey': config.OKX_API_KEY,
            'secret': config.OKX_SECRET_KEY,
            'password': config.OKX_PASSPHRASE,
            'enableRateLimit': True,
            'options': {'defaultType': 'swap'}
        })

    async def get_positions(self, exchange):
        try:
            positions = await exchange.fetch_positions()
            return {p['symbol']: p['contracts'] * (-1 if p['side'] == 'short' else 1) for p in positions if p['contracts'] != 0}
        except Exception as e:
            logger.error(f"Error fetching positions from {exchange.id}: {e}")
            return {}

    async def close_clients(self):
        if self.binance_client:
            await self.binance_client.close()
        if self.okx_client:
            await self.okx_client.close()

    async def adjust_binance_position(self, symbol, amount):
        try:
            side = 'buy' if amount > 0 else 'sell'
            await self.binance_client.create_market_order(symbol, side, abs(amount))
            logger.info(f"Adjusted position on Binance for {symbol}: {amount}")
            config.send_telegram_message(
                f'Successfully adjusted position on Binance for {symbol}: {amount}',
                config.bot_token2, config.chat_id2)
        except Exception as e:
            if 'notional must be no smaller than' in str(e):
                logger.info(f"Notional difference for {symbol} is below the minimum notional threshold, hedging skipped")
            elif 'Timestamp for this request is outside of the recvWindow' in str(e):
                logger.info(f"Timestamp outside of Recvwindow for {symbol} hedge skipped for this iteration")
                config.send_telegram_message(f'API Hedge Error, unable to adjust notionals due to timestamp error error. Please check bot if this error continues to repeat. {e}', config.bot_token2, config.chat_id2)
            elif 'minimum amount precision' in str(e):
                logger.info(f"Precision for {symbol} is below the minimum precision threshold, hedging skipped")
            # TODO: Add in stop trading signal when notional flag is triggered
            # elif 'Exceeded the maximum allowable position at current leverage.' in str(e):
            #     logger.info(f"Notional for {symbol} is above the maximum allowable position, shutting down all bots")
            else:
                # TODO: Add in stop trading signal when unknown error is triggered
                config.send_telegram_message(f'API Hedge Error, unable to adjust notionals due to an unexpected error. Please check the bot. {e}', config.bot_token2, config.chat_id2)
                logger.error(f"Error adjusting position on Binance for {symbol}: {e}")

    async def hedge_check(self, okx_positions, binance_positions):
        if okx_positions and binance_positions:
            for coin in self.hf_traded_coins:
                okx_symbol = f"{coin}/USDT:USDT"
                binance_symbol = f"{coin}/USDT:USDT"

                okx_amount = round(okx_positions.get(okx_symbol, 0) * self.okx_contract_sz.get(f"{coin}-USDT-SWAP", 1), 5)
                binance_amount = round(binance_positions.get(binance_symbol, 0), 5)

                if abs(okx_amount + binance_amount) > 0.001:
                    logger.info(f"Mismatch detected for {coin}: OKX={okx_amount}, Binance={binance_amount}")
                    adjustment = round(-okx_amount - binance_amount, 4)
                    if abs(adjustment) > 0.0001:
                        await self.adjust_binance_position(binance_symbol, adjustment)
        else:
            logger.info(f"Error fetching position data from exchange - API limit reached")
        logger.info("Hedge check completed")

    async def perform_hedge_check(self):
        start_time = time.time()
        try:
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            print(f"Starting hedge check at: {current_time}")

            # Fetch positions concurrently
            binance_start = time.time()
            okx_start = time.time()
            binance_positions, okx_positions = await asyncio.gather(
                self.get_positions(self.binance_client),
                self.get_positions(self.okx_client)
            )
            binance_end = time.time()
            okx_end = time.time()

            binance_duration = binance_end - binance_start
            okx_duration = okx_end - okx_start

            logger.info("Binance positions:")
            logger.info(binance_positions)
            logger.info("OKX positions:")
            logger.info(okx_positions)

            hedge_start = time.time()
            await self.hedge_check(okx_positions, binance_positions)
            # hedge_end = time.time()
            # hedge_duration = hedge_end - hedge_start
            #
            # end_time = time.time()
            # total_duration = end_time - start_time
            #
            # # logger.info(f"Timing information:")
            # # logger.info(f"  Binance position retrieval: {binance_duration:.4f} seconds")
            # # logger.info(f"  OKX position retrieval: {okx_duration:.4f} seconds")
            # # logger.info(f"  Hedge check execution: {hedge_duration:.4f} seconds")
            # # logger.info(f"  Total execution time: {total_duration:.4f} seconds")

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
    error_hedge = APIHedge([])
    asyncio.run(error_hedge.run())
