import asyncio
import websockets
import json
import time
import redis
import logging
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, timezone


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s',
    handlers=[
        logging.FileHandler("websocket.log"),
        logging.StreamHandler()
    ]
)


# Initialize Redis client
redis_client = redis.Redis(host='localhost', port=6379, db=0)

okx_contract_sz = {'BTC-USDT-SWAP': 0.01, 'ETH-USDT-SWAP': 0.1, 'MATIC-USDT-SWAP': 10.0, 'XRP-USDT-SWAP': 100.0, 'SOL-USDT-SWAP': 1.0, 'DOGE-USDT-SWAP': 1000.0, 'NOT-USDT-SWAP': 100.0, 'PEOPLE-USDT-SWAP': 100.0, 'PEPE-USDT-SWAP': 10000000.0, 'TURBO-USDT-SWAP': 10000.0, '1INCH-USDT-SWAP': 1.0, 'AAVE-USDT-SWAP': 0.1, 'ACE-USDT-SWAP': 1.0, 'ACH-USDT-SWAP': 100.0, 'ADA-USDT-SWAP': 100.0, 'AEVO-USDT-SWAP': 1.0, 'AGIX-USDT-SWAP': 10.0, 'AGLD-USDT-SWAP': 1.0, 'AIDOGE-USDT-SWAP': 10000000000.0, 'ALGO-USDT-SWAP': 10.0, 'ALPHA-USDT-SWAP': 1.0, 'APE-USDT-SWAP': 0.1, 'API3-USDT-SWAP': 1.0, 'APT-USDT-SWAP': 1.0, 'AR-USDT-SWAP': 0.1, 'ARB-USDT-SWAP': 10.0, 'ATOM-USDT-SWAP': 1.0, 'AUCTION-USDT-SWAP': 0.1, 'AVAX-USDT-SWAP': 1.0, 'AXS-USDT-SWAP': 0.1, 'BADGER-USDT-SWAP': 0.1, 'BAL-USDT-SWAP': 0.1, 'BAND-USDT-SWAP': 1.0, 'BAT-USDT-SWAP': 10.0, 'BCH-USDT-SWAP': 0.1, 'BICO-USDT-SWAP': 1.0, 'BIGTIME-USDT-SWAP': 10.0, 'BLOCK-USDT-SWAP': 10.0, 'BLUR-USDT-SWAP': 10.0, 'BNB-USDT-SWAP': 0.01, 'BNT-USDT-SWAP': 10.0, 'BONK-USDT-SWAP': 100000.0, 'BONE-USDT-SWAP': 1.0, 'BSV-USDT-SWAP': 1.0, 'CELO-USDT-SWAP': 1.0, 'CEL-USDT-SWAP': 10.0, 'CETUS-USDT-SWAP': 10.0, 'CFX-USDT-SWAP': 10.0, 'CHZ-USDT-SWAP': 10.0, 'COMP-USDT-SWAP': 0.1, 'CORE-USDT-SWAP': 1.0, 'CRO-USDT-SWAP': 10.0, 'CRV-USDT-SWAP': 1.0, 'CSPR-USDT-SWAP': 1.0, 'CTC-USDT-SWAP': 10.0, 'CVC-USDT-SWAP': 100.0, 'DGB-USDT-SWAP': 100.0, 'DMAIL-USDT-SWAP': 10.0, 'DOT-USDT-SWAP': 1.0, 'DYDX-USDT-SWAP': 1.0, 'EGLD-USDT-SWAP': 0.1, 'ENJ-USDT-SWAP': 10.0, 'ENS-USDT-SWAP': 0.1, 'EOS-USDT-SWAP': 10.0, 'ETC-USDT-SWAP': 10.0, 'ETHW-USDT-SWAP': 0.1, 'ETHFI-USDT-SWAP': 1.0, 'FET-USDT-SWAP': 10.0, 'FIL-USDT-SWAP': 0.1, 'FITFI-USDT-SWAP': 10.0, 'FLM-USDT-SWAP': 10.0, 'FLOKI-USDT-SWAP': 100000.0, 'FLOW-USDT-SWAP': 10.0, 'FLR-USDT-SWAP': 100.0, 'FOXY-USDT-SWAP': 100.0, 'FRONT-USDT-SWAP': 10.0, 'FTM-USDT-SWAP': 10.0, 'FXS-USDT-SWAP': 1.0, 'GALA-USDT-SWAP': 10.0, 'GAL-USDT-SWAP': 1.0, 'GAS-USDT-SWAP': 1.0, 'GFT-USDT-SWAP': 100.0, 'GLM-USDT-SWAP': 10.0, 'GMT-USDT-SWAP': 1.0, 'GMX-USDT-SWAP': 0.1, 'GODS-USDT-SWAP': 1.0, 'GPT-USDT-SWAP': 10.0, 'GRT-USDT-SWAP': 10.0, 'HBAR-USDT-SWAP': 100.0, 'ICP-USDT-SWAP': 0.01, 'ICX-USDT-SWAP': 10.0, 'ID-USDT-SWAP': 10.0, 'IMX-USDT-SWAP': 1.0, 'INJ-USDT-SWAP': 0.1, 'IOST-USDT-SWAP': 1000.0, 'IOTA-USDT-SWAP': 10.0, 'JOE-USDT-SWAP': 10.0, 'JST-USDT-SWAP': 100.0, 'JTO-USDT-SWAP': 1.0, 'JUP-USDT-SWAP': 10.0, 'KISHU-USDT-SWAP': 1000000000.0, 'KLAY-USDT-SWAP': 10.0, 'KNC-USDT-SWAP': 1.0, 'KSM-USDT-SWAP': 0.1, 'LDO-USDT-SWAP': 1.0, 'LINK-USDT-SWAP': 1.0, 'LOOKS-USDT-SWAP': 1.0, 'LPT-USDT-SWAP': 0.1, 'LQTY-USDT-SWAP': 1.0, 'LRC-USDT-SWAP': 10.0, 'LSK-USDT-SWAP': 1.0, 'LTC-USDT-SWAP': 1.0, 'LUNA-USDT-SWAP': 1.0, 'LUNC-USDT-SWAP': 10000.0, 'MAGIC-USDT-SWAP': 1.0, 'MANA-USDT-SWAP': 10.0, 'MASK-USDT-SWAP': 1.0, 'MEME-USDT-SWAP': 100.0, 'MERL-USDT-SWAP': 1.0, 'METIS-USDT-SWAP': 0.1, 'MEW-USDT-SWAP': 1000.0, 'MINA-USDT-SWAP': 1.0, 'MKR-USDT-SWAP': 0.01, 'MOVR-USDT-SWAP': 0.1, 'MSN-USDT-SWAP': 1.0, 'NEAR-USDT-SWAP': 10.0, 'NEO-USDT-SWAP': 1.0, 'NFT-USDT-SWAP': 1000000.0, 'NMR-USDT-SWAP': 0.1, 'OM-USDT-SWAP': 10.0, 'OMG-USDT-SWAP': 1.0, 'ONE-USDT-SWAP': 100.0, 'ONT-USDT-SWAP': 10.0, 'OP-USDT-SWAP': 1.0, 'ORBS-USDT-SWAP': 100.0, 'ORDI-USDT-SWAP': 0.1, 'PERP-USDT-SWAP': 1.0, 'PRCL-USDT-SWAP': 1.0, 'PYTH-USDT-SWAP': 10.0, 'QTUM-USDT-SWAP': 1.0, 'RACA-USDT-SWAP': 10000.0, 'RAY-USDT-SWAP': 1.0, 'RDNT-USDT-SWAP': 10.0, 'REN-USDT-SWAP': 10.0, 'RNDR-USDT-SWAP': 1.0, 'RON-USDT-SWAP': 1.0, 'RSR-USDT-SWAP': 100.0, 'RVN-USDT-SWAP': 10.0, 'SAND-USDT-SWAP': 10.0, 'SATS-USDT-SWAP': 10000000.0, 'SHIB-USDT-SWAP': 1000000.0, 'SLP-USDT-SWAP': 10.0, 'SNX-USDT-SWAP': 1.0, 'SPELL-USDT-SWAP': 10000.0, 'SSV-USDT-SWAP': 0.1, 'STORJ-USDT-SWAP': 10.0, 'STRK-USDT-SWAP': 1.0, 'STX-USDT-SWAP': 10.0, 'SUI-USDT-SWAP': 1.0, 'SUSHI-USDT-SWAP': 1.0, 'SWEAT-USDT-SWAP': 100.0, 'T-USDT-SWAP': 100.0, 'THETA-USDT-SWAP': 10.0, 'TIA-USDT-SWAP': 1.0, 'TNSR-USDT-SWAP': 1.0, 'TON-USDT-SWAP': 1.0, 'TRB-USDT-SWAP': 0.1, 'TRX-USDT-SWAP': 1000.0, 'UMA-USDT-SWAP': 0.1, 'UNI-USDT-SWAP': 1.0, 'USDC-USDT-SWAP': 10.0, 'USTC-USDT-SWAP': 100.0, 'VELO-USDT-SWAP': 1000.0, 'VENOM-USDT-SWAP': 10.0, 'VRA-USDT-SWAP': 1000.0, 'W-USDT-SWAP': 1.0, 'WAXP-USDT-SWAP': 100.0, 'WIF-USDT-SWAP': 1.0, 'WLD-USDT-SWAP': 1.0, 'WOO-USDT-SWAP': 10.0, 'XCH-USDT-SWAP': 0.01, 'XLM-USDT-SWAP': 100.0, 'XTZ-USDT-SWAP': 1.0, 'YFI-USDT-SWAP': 0.0001, 'YGG-USDT-SWAP': 1.0, 'ZENT-USDT-SWAP': 100.0, 'ZERO-USDT-SWAP': 1000.0, 'ZETA-USDT-SWAP': 10.0, 'ZEUS-USDT-SWAP': 10.0, 'ZIL-USDT-SWAP': 100.0, 'ZK-USDT-SWAP': 1.0, 'ZRX-USDT-SWAP': 10.0}

symbols = ['NOT', 'TIA', 'OM', 'MKR', 'AAVE', 'JTO', 'MEW', 'ORDI', 'STORJ', 'TURBO', 'AR', 'TRB', 'ORBS', 'ALPHA', 'AUCTION', 'SOL', 'SUI', 'ZETA', 'TRX', 'ENJ', 'DOGS', 'TON', 'LQTY', 'RDNT', 'BNT', 'AGLD', 'LSK', 'TNSR', 'BIGTIME']

# Global dictionary to store the latest data for all coins
latest_data = {symbol: {'binance': {'bids': [], 'asks': [], 'time': None}, 'okx': {'bids': [], 'asks': [], 'time': None}} for symbol in symbols}


class RateLimiter:
    def __init__(self, interval):
        self.interval = interval
        self.last_check = {}

    def should_process(self, symbol):
        current_time = time.time()
        if symbol not in self.last_check or current_time - self.last_check[symbol] >= self.interval:
            self.last_check[symbol] = current_time
            return True
        return False


# Function to get current UTC time with milliseconds
def get_current_utc_time_with_ms():
    now = datetime.now(timezone.utc)
    return now.isoformat(timespec='milliseconds')


def round_significant_digits(value, significant_digits):
    if value == 0:
        return 0
    d = Decimal(value)
    rounded_value = d.scaleb(-d.adjusted()).quantize(Decimal(10) ** -significant_digits, rounding=ROUND_HALF_UP).scaleb(d.adjusted())
    return float(rounded_value)


# Function to get current time in milliseconds since epoch
def get_current_time_ms():
    return int(time.time() * 1000)


async def websocket_handler(uri, exchange, symbol, subscribe_message=None):
    async with websockets.connect(uri) as websocket:
        if subscribe_message:
            await websocket.send(json.dumps(subscribe_message))
        async for message in websocket:
            process_message(exchange, symbol, message)


def calculate_impact_price(order_book, imn):
    accumulated_notional = 0
    accumulated_quantity = 0

    for price, quantity in order_book:
        notional = price * quantity
        accumulated_notional += notional
        accumulated_quantity += quantity

        if accumulated_notional >= imn:
            remaining_notional = imn - (accumulated_notional - notional)
            remaining_quantity = remaining_notional / price
            impact_price = imn / (accumulated_quantity - quantity + remaining_quantity)
            return impact_price

    return None


rate_limiter = RateLimiter(interval=0.05)


def process_message(exchange, symbol, message):
    data = json.loads(message)
    if exchange == 'okx' and 'data' in data:
        book_data = data['data'][0]
        instId = book_data['instId']
        contract_size = okx_contract_sz.get(instId, 1)

        latest_data[symbol][exchange] = {
            'time': int(book_data['ts']),
            'bids': [(float(bid[0]), float(bid[1]) * contract_size) for bid in book_data['bids']],
            'asks': [(float(ask[0]), float(ask[1]) * contract_size) for ask in book_data['asks']]
        }

    elif exchange == 'binance' and 'E' in data:
        latest_data[symbol][exchange] = {
            'time': int(data['E']),
            'bids': [(float(bid[0]), float(bid[1])) for bid in data['b']],
            'asks': [(float(ask[0]), float(ask[1])) for ask in data['a']]
        }

    if latest_data[symbol]['binance']['time'] is not None and latest_data[symbol]['okx']['time'] is not None:
        if rate_limiter.should_process(symbol):
            current_time = get_current_time_ms()
            combined_data = {
                'timestamp': get_current_utc_time_with_ms(),
                'binance': latest_data[symbol]['binance'],
                'okx': latest_data[symbol]['okx'],
                'timelag': current_time - min(latest_data[symbol]['binance']['time'], latest_data[symbol]['okx']['time'])
            }
            impact_bid_okx = calculate_impact_price(latest_data[symbol]['okx']['bids'], 100)
            impact_ask_okx = calculate_impact_price(latest_data[symbol]['okx']['asks'], 100)
            impact_bid_binance = calculate_impact_price(latest_data[symbol]['binance']['bids'], 100)
            impact_ask_binance = calculate_impact_price(latest_data[symbol]['binance']['asks'], 100)

            if all(x is not None for x in [impact_bid_okx, impact_ask_okx, impact_bid_binance, impact_ask_binance]):
                combined_data_impact = {
                    'timestamp': get_current_utc_time_with_ms(),
                    'entry_spread': round(100*(impact_bid_okx - impact_ask_binance)/impact_ask_binance, 4),
                    'exit_spread': round(100*(impact_ask_okx - impact_bid_binance)/impact_bid_binance, 4),
                    'best_bid_price_okx': latest_data[symbol]['okx']['bids'][0][0],
                    'best_ask_price_okx': latest_data[symbol]['okx']['asks'][0][0],
                    'best_bid_price_binance': latest_data[symbol]['binance']['bids'][0][0],
                    'best_ask_price_binance': latest_data[symbol]['binance']['asks'][0][0],
                    'impact_bid_price_okx': round_significant_digits(impact_bid_okx, 7),
                    'impact_ask_price_okx': round_significant_digits(impact_ask_okx, 7),
                    'impact_bid_price_binance': round_significant_digits(impact_bid_binance, 7),
                    'impact_ask_price_binance': round_significant_digits(impact_ask_binance, 7),
                    'okx_orderbook': latest_data[symbol]['okx'],
                    'binance_orderbook': latest_data[symbol]['binance'],
                    'timelag': current_time - min(latest_data[symbol]['binance']['time'], latest_data[symbol]['okx']['time']),
                    'impact_price_reached': True
                }
            else:
                combined_data_impact = {
                    'timestamp': get_current_utc_time_with_ms(),
                    'entry_spread': None,
                    'exit_spread': None,
                    'best_bid_price_okx': latest_data[symbol]['okx']['bids'][0][0],
                    'best_ask_price_okx': latest_data[symbol]['okx']['asks'][0][0],
                    'best_bid_price_binance': latest_data[symbol]['binance']['bids'][0][0],
                    'best_ask_price_binance': latest_data[symbol]['binance']['asks'][0][0],
                    'impact_bid_price_okx': None,
                    'impact_ask_price_okx': None,
                    'impact_bid_price_binance': None,
                    'impact_ask_price_binance': None,
                    'okx_orderbook': latest_data[symbol]['okx'],
                    'binance_orderbook': latest_data[symbol]['binance'],
                    'timelag': current_time - min(latest_data[symbol]['binance']['time'],
                                                  latest_data[symbol]['okx']['time']),
                    'impact_price_flag': False
                }
            redis_client.rpush(f'combined_data_{symbol}', json.dumps(combined_data_impact))
            redis_client.ltrim(f'combined_data_{symbol}', -500, -1)
            print(f'{symbol} - {combined_data_impact}')


async def main():

    tasks = []
    for symbol in symbols:
        okx_uri = "wss://ws.okx.com:8443/ws/v5/public"
        binance_uri = f"wss://fstream.binance.com/ws/{symbol.lower()}usdt@depth20@100ms"
        okx_subscribe_message = {
            "op": "subscribe",
            "args": [{"channel": "books5", "instId": f"{symbol}-USDT-SWAP"}]
        }
        tasks.append(websocket_handler(okx_uri, 'okx', symbol, okx_subscribe_message))
        tasks.append(websocket_handler(binance_uri, 'binance', symbol))
    await asyncio.gather(*tasks)


async def run():
    while True:
        try:
            await main()
        except Exception as e:
            logging.error(f"Error occurred: {e}")
            logging.info("Restarting the script...")
            time.sleep(5)

if __name__ == "__main__":
    asyncio.run(run())