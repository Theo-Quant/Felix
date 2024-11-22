import ccxt
import time
import config
import requests
import schedule
from datetime import datetime

class MarginMonitor:
    def __init__(self):
        self.okx = ccxt.okx({
            'apiKey': config.OKX_API_KEY,
            'secret': config.OKX_SECRET_KEY,
            'password': config.OKX_PASSPHRASE,
            'enableRateLimit': True,
            'options': {'defaultType': 'swap'}
        })
        self.binance = ccxt.binance({
            'apiKey': config.BINANCE_API_KEY,
            'secret': config.BINANCE_SECRET_KEY,
            'enableRateLimit': True,
            'options': {'defaultType': 'future'}
        })
        self.gate = ccxt.gate({
            'apiKey': config.GATE_API_KEY,
            'secret': config.GATE_SECRET_KEY,
            'enableRateLimit': True
        })
        self.bot_token = config.bot_tokenHFT
        self.chat_id = config.chat_idHFT
        self.bot_tokenE = config.bot_token2
        self.chat_idE = config.chat_id2

    def normalize_symbol(self, symbol):
        return symbol.replace('USDT', '').replace('/', '').replace(':', '').replace('_', '')

    def format_positions(self, okx_positions, binance_positions, gate_positions):
        positions = {}

        def add_position(pos, exchange):
            symbol = self.normalize_symbol(pos['symbol'])
            if exchange == 'okx':
                notional = float(pos['notional'])
                if pos['side'] == 'short':
                    notional = -notional
            elif exchange == 'binance':
                notional = float(pos['positionAmt']) * float(pos['markPrice'])
            else:  # Gate.io
                notional = float(pos['info']['value'])
                if pos['side'] == 'short':
                    notional = -notional

            if abs(notional) > 0:
                if symbol not in positions:
                    positions[symbol] = {'okx': 0, 'binance': 0, 'gate': 0}
                positions[symbol][exchange] = notional

        for pos in okx_positions:
            add_position(pos, 'okx')

        for pos in binance_positions:
            add_position(pos, 'binance')

        for pos in gate_positions:
            add_position(pos, 'gate')

        formatted_positions = []
        for symbol, values in sorted(positions.items()):
            formatted_positions.append(f"{symbol} ({values['okx']:.0f}/{values['binance']:.0f}/{values['gate']:.0f}/{round(values['binance'] + values['okx'] + values['gate'],0):.0f})")

        return formatted_positions

    def get_gate_margin_info(self):
        try:
            account_info = self.gate.fetch_balance({'type': 'swap'})
            positions = self.gate.fetch_positions()
            print(account_info)

            # Extract relevant information from account_info
            info = account_info['info'][0]
            total_equity = float(info['total']) + float(info['unrealised_pnl'])
            margin_balance = float(info['available'])
            unrealized_pnl = float(info['unrealised_pnl'])
            order_margin = float(info['order_margin'])
            position_margin = float(info['position_margin'])

            # Calculate total position size and net position size
            total_position_size = 0
            net_position_size = 0
            maintenance_margin = 0

            for pos in positions:
                if float(pos['info']['size']) != 0:
                    pos_size = abs(float(pos['info']['value']))
                    total_position_size += pos_size

                    if pos['side'] == 'short':
                        net_position_size -= pos_size
                    else:
                        net_position_size += pos_size

                    maintenance_margin += float(pos['info']['maintenance_rate']) * pos_size

            # Calculate margin ratio
            margin_ratio = (maintenance_margin / margin_balance * 100) if margin_balance != 0 else 0

            # Calculate margin level (similar to OKX calculation)
            margin_level = (1 / (maintenance_margin / margin_balance) * 100) if maintenance_margin != 0 else float(
                'inf')

            margin_info = {
                'totalEquity': total_equity,
                'marginBalance': margin_balance,
                'marginLevel': margin_level,
                'marginRatio': margin_ratio,
                'maintenanceMargin': maintenance_margin,
                'unrealizedPnL': unrealized_pnl,
                'totalPositionSize': total_position_size,
                'netPositionSize': net_position_size,
                'orderMargin': order_margin,
                'positionMargin': position_margin
            }
            print(margin_info)
            return margin_info
        except Exception as e:
            print(f"Error fetching Gate.io margin info: {e}")
            return None

    def get_okx_margin_info(self, max_retries=3, retry_delay=5):
        for attempt in range(max_retries):
            try:
                # Fetch futures account balance
                account = self.okx.fetch_balance({'type': 'trading'})
                spot_assets = sum(float(balance['total']) * float(self.okx.fetch_ticker(f"{currency}/USDT")['last'])
                                  for currency, balance in spot_info['total'].items() if currency != 'USDT')
                spot_assets += float(spot_info['total'].get('USDT', 0))

                positions = self.okx.fetch_positions()

                usdt_details = next(
                    (item for item in futures_info['info']['data'][0]['details'] if item['ccy'] == 'USDT'), None)

                if usdt_details:
                    total_equity = float(usdt_details['cashBal']) + round(float(usdt_details['upl']), 2)
                    maintenance_margin = float(usdt_details['mmr'])
                    margin_balance = float(usdt_details['availEq'])
                    margin_level = (1 / maintenance_margin * 100) if maintenance_margin != 0 else float('inf')
                    margin_ratio = 1 / float(usdt_details['mgnRatio']) * 100 if float(
                        usdt_details['mgnRatio']) != 0 else 0
                    unrealized_pnl = round(float(usdt_details['upl']), 2)
                    total_position_size = sum(
                        abs(float(pos['info']['notionalUsd']) * (-1 if pos['side'] == 'short' else 1)) for pos in
                        positions)
                    net_position_size = sum(
                        float(pos['info']['notionalUsd']) * (-1 if pos['side'] == 'short' else 1) for pos in positions)

                    margin_info = {
                        'totalEquity': total_equity,
                        'marginBalance': margin_balance,
                        'marginLevel': margin_level,
                        'marginRatio': margin_ratio,
                        'maintenanceMargin': maintenance_margin,
                        'unrealizedPnL': unrealized_pnl,
                        'totalPositionSize': total_position_size,
                        'netPositionSize': net_position_size,
                        'spotAssets': spot_assets
                    }
                    return margin_info
                else:
                    print("USDT details not found in OKX account info")
                    return None

            except Exception as e:
                error_message = str(e)
                if "Request header OK-ACCESS-KEY can not be empty" in error_message:
                    print(f"OKX API key error (attempt {attempt + 1}): {error_message}")
                    print("Reinitializing OKX exchange object...")
                    self.okx = ccxt.okx({
                        'apiKey': config.OKX_API_KEY,
                        'secret': config.OKX_SECRET_KEY,
                        'password': config.OKX_PASSPHRASE,
                        'enableRateLimit': True,
                        'options': {'defaultType': 'swap'}
                    })
                else:
                    print(f"Error fetching OKX margin info (attempt {attempt + 1}): {error_message}")

                if attempt < max_retries - 1:
                    print(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    print(f"Failed to fetch OKX margin info after {max_retries} attempts")
                    return None

        return None

    def get_binance_margin_info(self):
        try:
            account_info = self.binance.fapiPrivateV2GetAccount()
            positions = self.binance.fapiPrivateV2GetPositionRisk()

            margin_balance = float(account_info['totalMarginBalance'])
            bnb_balance = 0
            for asset in account_info['assets']:
                if asset['asset'] == 'BNB':
                    bnb_balance = float(asset['walletBalance'])
                    break
            # Output the BNB wallet balance
            if bnb_balance is not None:
                print(f"BNB Wallet Balance: {bnb_balance}")
            else:
                print("BNB asset not found.")

            # Fetch the last BNB price using the Binance API
            bnb_price = None
            try:
                response = requests.get('https://api.binance.com/api/v3/ticker/price', params={'symbol': 'BNBUSDT'})
                data = response.json()
                bnb_price = float(data['price'])
                print(f"BNB Price: {bnb_price} USDT")
            except Exception as e:
                print(f"Error fetching BNB price: {e}")

            bnb_notional = round(bnb_price*bnb_balance, 2)
            print(bnb_notional)

            maintenance_margin = float(account_info['totalMaintMargin'])
            margin_ratio = (maintenance_margin / margin_balance * 100) if margin_balance != 0 else 0
            unrealized_pnl = float(account_info['totalUnrealizedProfit'])

            # Calculate total position size using positionAmt * markPrice
            total_position_size = sum(abs(float(pos['positionAmt']) * float(pos['markPrice'])) for pos in positions if
                                      float(pos['positionAmt']) != 0)

            net_position_size = sum(float(pos['positionAmt']) * float(pos['markPrice']) for pos in positions if
                                      float(pos['positionAmt']) != 0)

            margin_info = {
                'totalEquity': float(account_info['totalWalletBalance']) + unrealized_pnl,
                'marginBalance': float(account_info['availableBalance']),
                'marginRatio': margin_ratio,
                'maintenanceMargin': maintenance_margin,
                'unrealizedPnL': unrealized_pnl,
                'totalPositionSize': total_position_size,
                'netPositionSize': net_position_size,
                'bnb_notional': bnb_notional
            }
            return margin_info
        except Exception as e:
            print(f"Error fetching Binance margin info: {e}")
            return None

    def get_alert_emoji(self, okx_ratio, binance_ratio, gate_ratio):
        if okx_ratio < 25 and binance_ratio < 25 and gate_ratio < 25:
            return "🟢"
        elif okx_ratio < 50 and binance_ratio < 50 and gate_ratio < 50:
            return "🟠"
        else:
            return "🔴"

    def send_telegram_message(self, message, chat_id, bot_token):
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown"
        }
        response = requests.post(url, json=payload)
        return response.json()

    def check_and_send_margins(self):
        print(f"Checking margins at {datetime.now()}")
        okx_margin = self.get_okx_margin_info()
        binance_margin = self.get_binance_margin_info()
        gate_margin = self.get_gate_margin_info()

        if okx_margin and binance_margin and gate_margin:

            if okx_margin['marginBalance'] < 15000:
                self.send_telegram_message(f'Margin balance for OKX is {okx_margin['totalEquity']:.2f} - Please transfer funds', self.chat_idE, self.bot_tokenE)
            if binance_margin['marginBalance'] < 15000:
                self.send_telegram_message(f'Margin balance for Binance is {binance_margin['totalEquity']:.2f} - Please transfer funds', self.chat_idE, self.bot_tokenE)
            if gate_margin['marginBalance'] < 5000:
                self.send_telegram_message(f'Margin balance for Gate is {gate_margin['totalEquity']:.2f} - Please transfer funds', self.chat_idE, self.bot_tokenE)

            alert_emoji = self.get_alert_emoji(okx_margin['marginRatio'], binance_margin['marginRatio'], gate_margin['marginRatio'])

            message = f"{alert_emoji} *Margin Monitor Alert* {alert_emoji}\n\n"
            message += "*OKX Margin Info:*\n"
            message += f"Total Equity: {okx_margin['totalEquity']:.2f}\n"
            message += f"Margin Balance: {okx_margin['marginBalance']:.2f}\n"
            message += f"Margin Ratio: {okx_margin['marginRatio']:.2f}%\n"
            message += f"Maintenance Margin: {okx_margin['maintenanceMargin']:.2f}\n"
            message += f"Unrealized PnL: {okx_margin['unrealizedPnL']:.2f}\n"
            message += f"Total Position Size: {okx_margin['totalPositionSize']:.2f}\n"
            message += f"Net Position Size: {okx_margin['netPositionSize']:.2f}\n\n"

            message += "*Binance Margin Info:*\n"
            message += f"Total Equity: {binance_margin['totalEquity']:.2f}\n"
            message += f"Margin Balance: {binance_margin['marginBalance']:.2f}\n"
            message += f"Margin Ratio: {binance_margin['marginRatio']:.2f}%\n"
            message += f"Maintenance Margin: {binance_margin['maintenanceMargin']:.2f}\n"
            message += f"Unrealized PnL: {binance_margin['unrealizedPnL']:.2f}\n"
            message += f"Total Position Size: {binance_margin['totalPositionSize']:.2f}\n"
            message += f"Net Position Size: {binance_margin['netPositionSize']:.2f}\n\n"

            message += "*Gate Margin Info:*\n"
            message += f"Total Equity: {gate_margin['totalEquity']:.2f}\n"
            message += f"Margin Balance: {gate_margin['marginBalance']:.2f}\n"
            message += f"Margin Ratio: {gate_margin['marginRatio']:.2f}%\n"
            message += f"Maintenance Margin: {gate_margin['maintenanceMargin']:.2f}\n"
            message += f"Unrealized PnL: {gate_margin['unrealizedPnL']:.2f}\n"
            message += f"Total Position Size: {gate_margin['totalPositionSize']:.2f}\n"
            message += f"Net Position Size: {gate_margin['netPositionSize']:.2f}\n\n"


            message += f"Total Unrealized PnL: $ {round(binance_margin['unrealizedPnL'] + okx_margin['unrealizedPnL'] + gate_margin['unrealizedPnL'], 2)}\n"
            message += f"Total Balance : $ {round(binance_margin['totalEquity'] + okx_margin['totalEquity'] + gate_margin['totalEquity'] + binance_margin['bnb_notional'] + okx_margin['okx_spot'], 2)}\n"
            message += f"BNB Notional : $ {round(binance_margin['bnb_notional'], 2)}\n"
            message += f"OKX Spot : $ {round(okx_margin['okx_spot'], 2)}\n\n"


            okx_positions = self.okx.fetch_positions()
            gate_positions = self.gate.fetch_positions()
            binance_positions = self.binance.fapiPrivateV2GetPositionRisk()
            formatted_positions = self.format_positions(okx_positions, binance_positions, gate_positions)

            if formatted_positions:
                message += "Current Positions (OKX/BIN/GATE/NET)\n"
                message += "\n".join(formatted_positions)

            self.send_telegram_message(message, self.chat_id, self.bot_token)
            print("Margin info sent to Telegram")

    def run(self):
        self.check_and_send_margins()
        schedule.every(10).minutes.do(self.check_and_send_margins)
        print("Margin Monitor started. Will check margins at the top of every hour.")
        while True:
            schedule.run_pending()
            time.sleep(1)

if __name__ == "__main__":
    monitor = MarginMonitor()
    monitor.run()