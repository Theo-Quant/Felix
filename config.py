import logging
from logging.handlers import RotatingFileHandler
import os
from datetime import datetime
import inspect
from telegram import Bot
import requests
from collections import deque
import time

def load_env(env_path='.env'):
    """
    Load environment variables from a .env file
    """
    try:
        with open(env_path, 'r') as file:
            for line in file:
                # Skip empty lines and comments
                line = line.strip()
                if not line or line.startswith('#'):
                    continue

                # Split on first occurrence of '='
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip()

                # Remove quotes if present
                if value and value[0] == value[-1] and value[0] in ['"', "'"]:
                    value = value[1:-1]

                # Set environment variable
                os.environ[key] = value

    except FileNotFoundError:
        print(f"Warning: {env_path} file not found!")
        return False

    return True


# Exchange API Credentials
BINANCE_API_KEY = os.environ.get('BINANCE_API_KEY', '')
BINANCE_SECRET_KEY = os.environ.get('BINANCE_SECRET_KEY', '')

OKX_API_KEY = os.environ.get('OKX_API_KEY', '')
OKX_SECRET_KEY = os.environ.get('OKX_SECRET_KEY', '')
OKX_PASSPHRASE = os.environ.get('OKX_PASSPHRASE', '')

GATE_API_KEY = os.environ.get('GATE_API_KEY', '')
GATE_SECRET_KEY = os.environ.get('GATE_SECRET_KEY', '')

# Azure Credentials
AZURE_SQL_SERVER = os.environ.get('AZURE_SQL_SERVER', '')
AZURE_SQL_DATABASE = os.environ.get('AZURE_SQL_DATABASE', '')
AZURE_SQL_USERNAME = os.environ.get('AZURE_SQL_USERNAME', '')
AZURE_SQL_PASSWORD = os.environ.get('AZURE_SQL_PASSWORD', '')
AZURE_SQL_CONNECTION_STRING = f"Driver={{ODBC Driver 18 for SQL Server}};Server=tcp:{AZURE_SQL_SERVER},1433;Database={AZURE_SQL_DATABASE};Uid={AZURE_SQL_USERNAME};Pwd={AZURE_SQL_PASSWORD};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"


# Redis Configuration
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0

# Static Variables
OKX_CONTRACT_SZ = {
    'ACT-USDT-SWAP': 1,
    'PNUT-USDT-SWAP': 10,
    'NEIRO-USDT-SWAP': 1000,
    'HMSTR-USDT-SWAP': 100,
    'POPCAT-USDT-SWAP': 10,
    'CAT-USDT-SWAP': 100000,
    'BTC-USDT-SWAP': 0.01,
    'ETH-USDT-SWAP': 0.1,
    'MATIC-USDT-SWAP': 10.0,
    'XRP-USDT-SWAP': 100.0,
    'SOL-USDT-SWAP': 1.0,
    'DOGE-USDT-SWAP': 1000.0,
    'DOGS-USDT-SWAP': 1000.0,
    'TON-USDT-SWAP': 1.0,
    'NOT-USDT-SWAP': 100.0,
    'SATS-USDT-SWAP': 10000000.0,
    '1INCH-USDT-SWAP': 1.0,
    'AAVE-USDT-SWAP': 0.1,
    'ACE-USDT-SWAP': 1.0,
    'ACH-USDT-SWAP': 100.0,
    'ADA-USDT-SWAP': 100.0,
    'AEVO-USDT-SWAP': 1.0,
    'AGLD-USDT-SWAP': 1.0,
    'AIDOGE-USDT-SWAP': 10000000000.0,
    'ALGO-USDT-SWAP': 10.0,
    'ALPHA-USDT-SWAP': 1.0,
    'APE-USDT-SWAP': 0.1,
    'API3-USDT-SWAP': 1.0,
    'APT-USDT-SWAP': 1.0,
    'AR-USDT-SWAP': 0.1,
    'ARB-USDT-SWAP': 10.0,
    'ATH-USDT-SWAP': 100.0,
    'ATOM-USDT-SWAP': 1.0,
    'AUCTION-USDT-SWAP': 0.1,
    'AVAX-USDT-SWAP': 1.0,
    'AXS-USDT-SWAP': 0.1,
    'BADGER-USDT-SWAP': 0.1,
    'BAL-USDT-SWAP': 0.1,
    'BAND-USDT-SWAP': 1.0,
    'BAT-USDT-SWAP': 10.0,
    'BCH-USDT-SWAP': 0.1,
    'BICO-USDT-SWAP': 1.0,
    'BIGTIME-USDT-SWAP': 10.0,
    'BLUR-USDT-SWAP': 10.0,
    'BNB-USDT-SWAP': 0.01,
    'BNT-USDT-SWAP': 10.0,
    'BOME-USDT-SWAP': 1000.0,
    'BONK-USDT-SWAP': 100000.0,
    'BONE-USDT-SWAP': 1.0,
    'BSV-USDT-SWAP': 1.0,
    'CELO-USDT-SWAP': 1.0,
    'CETUS-USDT-SWAP': 10.0,
    'CFX-USDT-SWAP': 10.0,
    'CHZ-USDT-SWAP': 10.0,
    'COMP-USDT-SWAP': 0.1,
    'CORE-USDT-SWAP': 1.0,
    'CRO-USDT-SWAP': 10.0,
    'CRV-USDT-SWAP': 1.0,
    'CSPR-USDT-SWAP': 1.0,
    'CTC-USDT-SWAP': 10.0,
    'CVC-USDT-SWAP': 100.0,
    'CVX-USDT-SWAP': 1.0,
    'DGB-USDT-SWAP': 100.0,
    'DMAIL-USDT-SWAP': 10.0,
    'DOT-USDT-SWAP': 1.0,
    'DYDX-USDT-SWAP': 1.0,
    'EGLD-USDT-SWAP': 0.1,
    'ENJ-USDT-SWAP': 10.0,
    'ENS-USDT-SWAP': 0.1,
    'EOS-USDT-SWAP': 10.0,
    'ETC-USDT-SWAP': 10.0,
    'ETHW-USDT-SWAP': 0.1,
    'ETHFI-USDT-SWAP': 1.0,
    'FIL-USDT-SWAP': 0.1,
    'FLM-USDT-SWAP': 10.0,
    'FLOKI-USDT-SWAP': 100000.0,
    'FLOW-USDT-SWAP': 10.0,
    'FLR-USDT-SWAP': 100.0,
    'FOXY-USDT-SWAP': 100.0,
    'FRONT-USDT-SWAP': 10.0,
    'FTM-USDT-SWAP': 10.0,
    'FXS-USDT-SWAP': 1.0,
    'GALA-USDT-SWAP': 10.0,
    'GAS-USDT-SWAP': 1.0,
    'GFT-USDT-SWAP': 100.0,
    'GLM-USDT-SWAP': 10.0,
    'GMT-USDT-SWAP': 1.0,
    'GMX-USDT-SWAP': 0.1,
    'GODS-USDT-SWAP': 1.0,
    'GPT-USDT-SWAP': 10.0,
    'GRT-USDT-SWAP': 10.0,
    'HBAR-USDT-SWAP': 100.0,
    'ICP-USDT-SWAP': 0.01,
    'ICX-USDT-SWAP': 10.0,
    'ID-USDT-SWAP': 10.0,
    'IMX-USDT-SWAP': 1.0,
    'INJ-USDT-SWAP': 0.1,
    'IOST-USDT-SWAP': 1000.0,
    'IOTA-USDT-SWAP': 10.0,
    'JOE-USDT-SWAP': 10.0,
    'JST-USDT-SWAP': 100.0,
    'JTO-USDT-SWAP': 1.0,
    'JUP-USDT-SWAP': 10.0,
    'KISHU-USDT-SWAP': 1000000000.0,
    'KLAY-USDT-SWAP': 10.0,
    'KNC-USDT-SWAP': 1.0,
    'KSM-USDT-SWAP': 0.1,
    'LDO-USDT-SWAP': 1.0,
    'LINK-USDT-SWAP': 1.0,
    'LOOKS-USDT-SWAP': 1.0,
    'LPT-USDT-SWAP': 0.1,
    'LQTY-USDT-SWAP': 1.0,
    'LRC-USDT-SWAP': 10.0,
    'LSK-USDT-SWAP': 1.0,
    'LTC-USDT-SWAP': 1.0,
    'LUNA-USDT-SWAP': 1.0,
    'LUNC-USDT-SWAP': 10000.0,
    'MAGIC-USDT-SWAP': 1.0,
    'MANA-USDT-SWAP': 10.0,
    'MASK-USDT-SWAP': 1.0,
    'MAX-USDT-SWAP': 10.0,
    'MEME-USDT-SWAP': 100.0,
    'MERL-USDT-SWAP': 1.0,
    'METIS-USDT-SWAP': 0.1,
    'MEW-USDT-SWAP': 1000.0,
    'MINA-USDT-SWAP': 1.0,
    'MKR-USDT-SWAP': 0.01,
    'MOVR-USDT-SWAP': 0.1,
    'NEAR-USDT-SWAP': 10.0,
    'NEO-USDT-SWAP': 1.0,
    'NFT-USDT-SWAP': 1000000.0,
    'NMR-USDT-SWAP': 0.1,
    'OM-USDT-SWAP': 10.0,
    'ONDO-USDT-SWAP': 10.0,
    'ONE-USDT-SWAP': 100.0,
    'ONT-USDT-SWAP': 10.0,
    'OP-USDT-SWAP': 1.0,
    'ORBS-USDT-SWAP': 100.0,
    'ORDI-USDT-SWAP': 0.1,
    'PEOPLE-USDT-SWAP': 100.0,
    'PEPE-USDT-SWAP': 10000000.0,
    'PERP-USDT-SWAP': 1.0,
    'PRCL-USDT-SWAP': 1.0,
    'PYTH-USDT-SWAP': 10.0,
    'QTUM-USDT-SWAP': 1.0,
    'RACA-USDT-SWAP': 10000.0,
    'RAY-USDT-SWAP': 1.0,
    'RDNT-USDT-SWAP': 10.0,
    'REN-USDT-SWAP': 10.0,
    'RENDER-USDT-SWAP': 1.0,
    'RON-USDT-SWAP': 1.0,
    'RSR-USDT-SWAP': 100.0,
    'RVN-USDT-SWAP': 10.0,
    'SAND-USDT-SWAP': 10.0,
    'SHIB-USDT-SWAP': 1000000.0,
    'SLP-USDT-SWAP': 10.0,
    'SNX-USDT-SWAP': 1.0,
    'SSV-USDT-SWAP': 0.1,
    'STORJ-USDT-SWAP': 10.0,
    'STRK-USDT-SWAP': 1.0,
    'STX-USDT-SWAP': 10.0,
    'SUI-USDT-SWAP': 1.0,
    'SUSHI-USDT-SWAP': 1.0,
    'SWEAT-USDT-SWAP': 100.0,
    'T-USDT-SWAP': 100.0,
    'THETA-USDT-SWAP': 10.0,
    'TIA-USDT-SWAP': 1.0,
    'TNSR-USDT-SWAP': 1.0,
    'TRB-USDT-SWAP': 0.1,
    'TRX-USDT-SWAP': 1000.0,
    'TURBO-USDT-SWAP': 10000.0,
    'ULTI-USDT-SWAP': 100.0,
    'UMA-USDT-SWAP': 0.1,
    'UNI-USDT-SWAP': 1.0,
    'USDC-USDT-SWAP': 10.0,
    'USTC-USDT-SWAP': 100.0,
    'UXLINK-USDT-SWAP': 10.0,
    'VELO-USDT-SWAP': 1000.0,
    'VENOM-USDT-SWAP': 10.0,
    'VRA-USDT-SWAP': 1000.0,
    'W-USDT-SWAP': 1.0,
    'WAXP-USDT-SWAP': 100.0,
    'WIF-USDT-SWAP': 1.0,
    'WLD-USDT-SWAP': 1.0,
    'WOO-USDT-SWAP': 10.0,
    'XCH-USDT-SWAP': 0.01,
    'XLM-USDT-SWAP': 100.0,
    'XTZ-USDT-SWAP': 1.0,
    'YFI-USDT-SWAP': 0.0001,
    'YGG-USDT-SWAP': 1.0,
    'ZENT-USDT-SWAP': 100.0,
    'ZERO-USDT-SWAP': 1000.0,
    'ZETA-USDT-SWAP': 10.0,
    'ZEUS-USDT-SWAP': 10.0,
    'ZIL-USDT-SWAP': 100.0,
    'ZK-USDT-SWAP': 10.0,
    'ZRO-USDT-SWAP': 1.0,
    'ZRX-USDT-SWAP': 10.0,
}
BINANCE_STEP_SZ = {
    "1000BONKUSDT": 1.0,
    "1000FLOKIUSDT": 1.0,
    "1000LUNCUSDT": 1.0,
    "1000PEPEUSDT": 1.0,
    "1000RATSUSDT": 1.0,
    "1000SATSUSDT": 1.0,
    "1000SHIBUSDT": 1.0,
    "1000XECUSDT": 1.0,
    "1INCHUSDT": 1.0,
    "AAVEUSDT": 0.1,
    "ACEUSDT": 0.01,
    "ACHUSDT": 1.0,
    "ADAUSDT": 1.0,
    "AEVOUSDT": 0.1,
    "AGIXUSDT": 1.0,
    "AGLDUSDT": 1.0,
    "AIUSDT": 1.0,
    "ALGOUSDT": 0.1,
    "ALICEUSDT": 0.1,
    "ALPACAUSDT": 1.0,
    "ALPHAUSDT": 1.0,
    "ALTUSDT": 1.0,
    "AMBUSDT": 1.0,
    "ANKRUSDT": 1.0,
    "APEUSDT": 1.0,
    "API3USDT": 0.1,
    "APTUSDT": 0.1,
    "ARBUSDT": 0.1,
    "ARKMUSDT": 1.0,
    "ARKUSDT": 1.0,
    "ARPAUSDT": 1.0,
    "ARUSDT": 0.1,
    "ASTRUSDT": 1.0,
    "ATAUSDT": 1.0,
    "ATOMUSDT": 0.01,
    "AUCTIONUSDT": 0.01,
    "AVAXUSDT": 1.0,
    "AXLUSDT": 0.1,
    "AXSUSDT": 1.0,
    "BADGERUSDT": 1.0,
    "BAKEUSDT": 1.0,
    "BALUSDT": 0.1,
    "BANANAUSDT": 0.1,
    "BANDUSDT": 0.1,
    "BATUSDT": 0.1,
    "BBUSDT": 1.0,
    "BCHUSDT": 0.001,
    "BEAMXUSDT": 1.0,
    "BELUSDT": 1.0,
    "BICOUSDT": 1.0,
    "BIGTIMEUSDT": 1.0,
    "BLURUSDT": 1.0,
    "BLZUSDT": 1.0,
    "BNBUSDT": 0.01,
    "BNTUSDT": 1.0,
    "BNXUSDT": 0.1,
    "BOMEUSDT": 1.0,
    "BONDUSDT": 0.1,
    "BRETTUSDT": 1.0,
    "BSVUSDT": 0.1,
    "BTCDOMUSDT": 0.001,
    "BTCSTUSDT": 0.1,
    "BTCUSDT": 0.001,
    "C98USDT": 1.0,
    "CAKEUSDT": 1.0,
    "CELOUSDT": 0.1,
    "CELRUSDT": 1.0,
    "CFXUSDT": 1.0,
    "CHRUSDT": 1.0,
    "CHZUSDT": 1.0,
    "CKBUSDT": 1.0,
    "COMBOUSDT": 0.1,
    "COMPUSDT": 0.001,
    "COTIUSDT": 1.0,
    "CRVUSDT": 0.1,
    "CTKUSDT": 1.0,
    "CTSIUSDT": 1.0,
    "CVCUSDT": 1.0,
    "CVXUSDT": 1.0,
    "CYBERUSDT": 0.1,
    "DARUSDT": 0.1,
    "DASHUSDT": 0.001,
    "DEFIUSDT": 0.001,
    "DENTUSDT": 1.0,
    "DGBUSDT": 1.0,
    "DODOXUSDT": 1.0,
    "DOGEUSDT": 1.0,
    "DOGSUSDT": 1.0,
    "DOTUSDT": 0.1,
    "DUSKUSDT": 1.0,
    "DYDXUSDT": 0.1,
    "DYMUSDT": 0.1,
    "EDUUSDT": 1.0,
    "EGLDUSDT": 0.1,
    "ENAUSDT": 1.0,
    "ENJUSDT": 1.0,
    "ENSUSDT": 0.1,
    "EOSUSDT": 0.1,
    "ETCUSDT": 0.01,
    "ETHFIUSDT": 0.1,
    "ETHUSDT": 0.001,
    "ETHWUSDT": 1.0,
    "FETUSDT": 1.0,
    "FILUSDT": 0.1,
    "FLMUSDT": 1.0,
    "FLOWUSDT": 0.1,
    "FRONTUSDT": 1.0,
    "FTMUSDT": 1.0,
    "FTTUSDT": 0.1,
    "FXSUSDT": 0.1,
    "GALAUSDT": 1.0,
    "GASUSDT": 0.1,
    "GLMRUSDT": 1.0,
    "GLMUSDT": 1.0,
    "GMTUSDT": 1.0,
    "GMXUSDT": 0.01,
    "GRTUSDT": 1.0,
    "GTCUSDT": 0.1,
    "GUSDT": 1.0,
    "HBARUSDT": 1.0,
    "HFTUSDT": 1.0,
    "HIFIUSDT": 1.0,
    "HIGHUSDT": 0.1,
    "HOOKUSDT": 0.1,
    "HOTUSDT": 1.0,
    "ICPUSDT": 1.0,
    "ICXUSDT": 1.0,
    "IDEXUSDT": 1.0,
    "IDUSDT": 1.0,
    "ILVUSDT": 0.1,
    "IMXUSDT": 1.0,
    "INJUSDT": 0.1,
    "IOSTUSDT": 1.0,
    "IOTAUSDT": 0.1,
    "IOTXUSDT": 1.0,
    "IOUSDT": 0.1,
    "JASMYUSDT": 1.0,
    "JOEUSDT": 1.0,
    "JTOUSDT": 1.0,
    "JUPUSDT": 1.0,
    "KASUSDT": 1.0,
    "KAVAUSDT": 0.1,
    "KEYUSDT": 1.0,
    "KLAYUSDT": 0.1,
    "KNCUSDT": 1.0,
    "KSMUSDT": 0.1,
    "LDOUSDT": 1.0,
    "LEVERUSDT": 1.0,
    "LINAUSDT": 1.0,
    "LINKUSDT": 0.01,
    "LISTAUSDT": 1.0,
    "LITUSDT": 0.1,
    "LOOMUSDT": 1.0,
    "LPTUSDT": 0.1,
    "LQTYUSDT": 0.1,
    "LRCUSDT": 1.0,
    "LSKUSDT": 1.0,
    "LTCUSDT": 0.001,
    "LUNA2USDT": 1.0,
    "MAGICUSDT": 0.1,
    "MANAUSDT": 1.0,
    "MANTAUSDT": 0.1,
    "MASKUSDT": 1.0,
    "MATICUSDT": 1.0,
    "MAVIAUSDT": 0.1,
    "MAVUSDT": 1.0,
    "MBOXUSDT": 1.0,
    "MDTUSDT": 1.0,
    "MEMEUSDT": 1.0,
    "METISUSDT": 0.01,
    "MEWUSDT": 1.0,
    "MINAUSDT": 1.0,
    "MKRUSDT": 0.001,
    "MOVRUSDT": 0.01,
    "MTLUSDT": 1.0,
    "MYROUSDT": 1.0,
    "NEARUSDT": 1.0,
    "NEOUSDT": 0.01,
    "NFPUSDT": 0.1,
    "NKNUSDT": 1.0,
    "NMRUSDT": 0.1,
    "NOTUSDT": 1.0,
    "NTRNUSDT": 1.0,
    "NULSUSDT": 1.0,
    "OCEANUSDT": 1.0,
    "OGNUSDT": 1.0,
    "OMGUSDT": 0.1,
    "OMNIUSDT": 0.01,
    "OMUSDT": 0.1,
    "ONDOUSDT": 0.1,
    "ONEUSDT": 1.0,
    "ONGUSDT": 1.0,
    "ONTUSDT": 0.1,
    "OPUSDT": 0.1,
    "ORBSUSDT": 1.0,
    "ORDIUSDT": 0.1,
    "OXTUSDT": 1.0,
    "PENDLEUSDT": 1.0,
    "PEOPLEUSDT": 1.0,
    "PERPUSDT": 0.1,
    "PHBUSDT": 1.0,
    "PIXELUSDT": 1.0,
    "POLYXUSDT": 1.0,
    "POPCATUSDT": 1.0,
    "PORTALUSDT": 0.1,
    "POWRUSDT": 1.0,
    "PYTHUSDT": 1.0,
    "QNTUSDT": 0.1,
    "QTUMUSDT": 0.1,
    "RADUSDT": 1.0,
    "RAREUSDT": 1.0,
    "RAYUSDT": 0.1,
    "RDNTUSDT": 1.0,
    "REEFUSDT": 1.0,
    "RENDERUSDT": 0.1,
    "RENUSDT": 1.0,
    "REZUSDT": 1.0,
    "RIFUSDT": 1.0,
    "RLCUSDT": 0.1,
    "RONINUSDT": 0.1,
    "ROSEUSDT": 1.0,
    "RSRUSDT": 1.0,
    "RUNEUSDT": 1.0,
    "RVNUSDT": 1.0,
    "SAGAUSDT": 0.1,
    "SANDUSDT": 1.0,
    "SCUSDT": 1.0,
    "SEIUSDT": 1.0,
    "SFPUSDT": 1.0,
    "SKLUSDT": 1.0,
    "SLPUSDT": 1.0,
    "SNTUSDT": 1.0,
    "SNXUSDT": 0.1,
    "SOLUSDT": 1.0,
    "SPELLUSDT": 1.0,
    "SSVUSDT": 0.01,
    "STEEMUSDT": 1.0,
    "STGUSDT": 1.0,
    "STMXUSDT": 1.0,
    "STORJUSDT": 1.0,
    "STPTUSDT": 1.0,
    "STRAXUSDT": 1.0,
    "STRKUSDT": 0.1,
    "STXUSDT": 1.0,
    "SUIUSDT": 0.1,
    "SUNUSDT": 1.0,
    "SUPERUSDT": 1.0,
    "SUSHIUSDT": 1.0,
    "SXPUSDT": 0.1,
    "SYNUSDT": 1.0,
    "SYSUSDT": 1.0,
    "TAOUSDT": 0.001,
    "THETAUSDT": 0.1,
    "TIAUSDT": 1.0,
    "TLMUSDT": 1.0,
    "TNSRUSDT": 0.1,
    "TOKENUSDT": 1.0,
    "TONUSDT": 0.1,
    "TRBUSDT": 0.1,
    "TRUUSDT": 1.0,
    "TRXUSDT": 1.0,
    "TURBOUSDT": 1.0,
    "TUSDT": 1.0,
    "TWTUSDT": 1.0,
    "UMAUSDT": 1.0,
    "UNFIUSDT": 0.1,
    "UNIUSDT": 1.0,
    "USDCUSDT": 1.0,
    "USTCUSDT": 1.0,
    "VANRYUSDT": 1.0,
    "VETUSDT": 1.0,
    "VIDTUSDT": 1.0,
    "VOXELUSDT": 1.0,
    "WAVESUSDT": 0.1,
    "WAXPUSDT": 1.0,
    "WIFUSDT": 0.1,
    "WLDUSDT": 1.0,
    "WOOUSDT": 1.0,
    "WUSDT": 0.1,
    "XAIUSDT": 1.0,
    "XEMUSDT": 1.0,
    "XLMUSDT": 1.0,
    "XMRUSDT": 0.001,
    "XRPUSDT": 0.1,
    "XTZUSDT": 0.1,
    "XVGUSDT": 1.0,
    "XVSUSDT": 0.1,
    "YFIUSDT": 0.001,
    "YGGUSDT": 1.0,
    "ZECUSDT": 0.001,
    "ZENUSDT": 0.1,
    "ZETAUSDT": 1.0,
    "ZILUSDT": 1.0,
    "ZKUSDT": 1.0,
    "ZROUSDT": 0.1,
    "ZRXUSDT": 0.1
}
COINS = ['ACT/USDT', 'PNUT/USDT', 'MOODENG/USDT', '1INCH/USDT', 'AAVE/USDT', 'ACE/USDT', 'ACH/USDT', 'ADA/USDT', 'AERGO/USDT', 'AEVO/USDT', 'AGLD/USDT', 'ALGO/USDT', 'ALPHA/USDT', 'APE/USDT', 'API3/USDT', 'APT/USDT', 'AR/USDT', 'ARB/USDT', 'ARKM/USDT', 'ASTR/USDT', 'ATOM/USDT', 'AUCTION/USDT', 'AVAX/USDT', 'AXS/USDT', 'BADGER/USDT', 'BAL/USDT', 'BAND/USDT', 'BAT/USDT', 'BCH/USDT', 'BICO/USDT', 'BIGTIME/USDT', 'BLUR/USDT', 'BNB/USDT', 'BNT/USDT', 'BOME/USDT', 'BRETT/USDT', 'BSV/USDT', 'BTC/USDT', 'CATI/USDT', 'CELO/USDT', 'CELR/USDT', 'CFX/USDT', 'CHZ/USDT', 'COMP/USDT', 'CRV/USDT', 'CVC/USDT', 'CVX/USDT', 'DGB/USDT', 'DIA/USDT', 'DOGE/USDT', 'DOGS/USDT', 'DOT/USDT', 'DYDX/USDT', 'EGLD/USDT', 'EIGEN/USDT', 'ENJ/USDT', 'ENS/USDT', 'EOS/USDT', 'ETC/USDT', 'ETH/USDT', 'ETHFI/USDT', 'ETHW/USDT', 'FET/USDT', 'FIL/USDT', 'FLM/USDT', 'FLOW/USDT', 'FTM/USDT', 'FXS/USDT', 'G/USDT', 'GALA/USDT', 'GAS/USDT', 'GHST/USDT', 'GLM/USDT', 'GLMR/USDT', 'GMT/USDT', 'GMX/USDT', 'GRT/USDT', 'HBAR/USDT', 'HMSTR/USDT', 'ICP/USDT', 'ICX/USDT', 'ID/USDT', 'ILV/USDT', 'IMX/USDT', 'INJ/USDT', 'IOST/USDT', 'IOTA/USDT', 'JOE/USDT', 'JTO/USDT', 'JUP/USDT', 'KDA/USDT', 'KLAY/USDT', 'KNC/USDT', 'KSM/USDT', 'LDO/USDT', 'LINK/USDT', 'LPT/USDT', 'LQTY/USDT', 'LRC/USDT', 'LSK/USDT', 'LTC/USDT', 'MAGIC/USDT', 'MANA/USDT', 'MASK/USDT', 'MDT/USDT', 'MEME/USDT', 'METIS/USDT', 'MEW/USDT', 'MINA/USDT', 'MKR/USDT', 'MOVR/USDT', 'NEAR/USDT', 'NEIRO/USDT', 'NEIROETH/USDT', 'NEO/USDT', 'NMR/USDT', 'NOT/USDT', 'NULS/USDT', 'OM/USDT', 'ONDO/USDT', 'ONE/USDT', 'ONT/USDT', 'OP/USDT', 'ORBS/USDT', 'ORDI/USDT', 'OXT/USDT', 'PENDLE/USDT', 'PEOPLE/USDT', 'PERP/USDT', 'PIXEL/USDT', 'POL/USDT', 'POPCAT/USDT', 'PYTH/USDT', 'QTUM/USDT', 'RAY/USDT', 'RDNT/USDT', 'REN/USDT', 'RENDER/USDT', 'RPL/USDT', 'RSR/USDT', 'RVN/USDT', 'SAND/USDT', 'SC/USDT', 'SKL/USDT', 'SLP/USDT', 'SNT/USDT', 'SNX/USDT', 'SOL/USDT', 'SSV/USDT', 'STORJ/USDT', 'STRK/USDT', 'STX/USDT', 'SUI/USDT', 'SUSHI/USDT', 'T/USDT', 'TAO/USDT', 'THETA/USDT', 'TIA/USDT', 'TNSR/USDT', 'TON/USDT', 'TRB/USDT', 'TRX/USDT', 'TURBO/USDT', 'UMA/USDT', 'UNI/USDT', 'USDC/USDT', 'USTC/USDT', 'UXLINK/USDT', 'W/USDT', 'WAXP/USDT', 'WIF/USDT', 'WLD/USDT', 'WOO/USDT', 'XLM/USDT', 'XRP/USDT', 'XTZ/USDT', 'YFI/USDT', 'YGG/USDT', 'ZETA/USDT', 'ZIL/USDT', 'ZK/USDT', 'ZRO/USDT', 'ZRX/USDT']

ALL_COINS = ['HMSTR/USDT', '1000BONK/USDT', '1000FLOKI/USDT', '1000LUNC/USDT', '1000PEPE/USDT', '1000RATS/USDT', '1000SATS/USDT', '1000SHIB/USDT', '1000XEC/USDT', '1CAT/USDT', '1INCH/USDT', 'AAVE/USDT', 'ACA/USDT', 'ACE/USDT', 'ACH/USDT', 'ADA/USDT', 'AERGO/USDT', 'AERO/USDT', 'AEVO/USDT', 'AGI/USDT', 'AGIX/USDT', 'AGLD/USDT', 'AI/USDT', 'AIOZ/USDT', 'AKRO/USDT', 'AKT/USDT', 'ALCX/USDT', 'ALGO/USDT', 'ALICE/USDT', 'ALPACA/USDT', 'ALPHA/USDT', 'ALT/USDT', 'AMB/USDT', 'AMP/USDT', 'ANKR/USDT', 'APE/USDT', 'API3/USDT', 'APT/USDT', 'AR/USDT', 'ARB/USDT', 'ARK/USDT', 'ARKM/USDT', 'ARPA/USDT', 'ASTR/USDT', 'ATA/USDT', 'ATH/USDT', 'ATOM/USDT', 'AUCTION/USDT', 'AUDIO/USDT', 'AURORA/USDT', 'AVAIL/USDT', 'AVAX/USDT', 'AXL/USDT', 'AXS/USDT', 'BADGER/USDT', 'BAIDOGE/USDT', 'BAKE/USDT', 'BAL/USDT', 'BANANA/USDT', 'BAND/USDT', 'BAT/USDT', 'BB/USDT', 'BBL/USDT', 'BCH/USDT', 'BEAMX/USDT', 'BEER/USDT', 'BEL/USDT', 'BENDOG/USDT', 'BENQI/USDT', 'BICO/USDT', 'BIGTIME/USDT', 'BLAST/USDT', 'BLOCK/USDT', 'BLUR/USDT', 'BLZ/USDT', 'BNB/USDT', 'BNT/USDT', 'BNX/USDT', 'BOBA/USDT', 'BOME/USDT', 'BOND/USDT', 'BONE/USDT', 'BONK/USDT', 'BRETT/USDT', 'BSV/USDT', 'BSW/USDT', 'BTC/USDT', 'BTCDOM/USDT', 'BTT/USDT', 'C98/USDT', 'CAKE/USDT', 'CAT/USDT', 'CEL/USDT', 'CELO/USDT', 'CELR/USDT', 'CETUS/USDT', 'CFX/USDT', 'CHESS/USDT', 'CHR/USDT', 'CHZ/USDT', 'CKB/USDT', 'CLOUD/USDT', 'CLV/USDT', 'COMBO/USDT', 'COMP/USDT', 'CORE/USDT', 'COTI/USDT', 'CREAM/USDT', 'CRO/USDT', 'CRV/USDT', 'CSPR/USDT', 'CTC/USDT', 'CTK/USDT', 'CTSI/USDT', 'CVC/USDT', 'CVX/USDT', 'CYBER/USDT', 'DAR/USDT', 'DASH/USDT', 'DEFI/USDT', 'DEGEN/USDT', 'DEGO/USDT', 'DENT/USDT', 'DGB/USDT', 'DHX/USDT', 'DMAIL/USDT', 'DODO/USDT', 'DODOX/USDT', 'DOG/USDT', 'DOGE/USDT', 'DOGS/USDT', 'DOP/USDT', 'DOT/USDT', 'DRIFT/USDT', 'DUSK/USDT', 'DYDX/USDT', 'DYM/USDT', 'EDU/USDT', 'EGLD/USDT', 'ENA/USDT', 'ENJ/USDT', 'ENS/USDT', 'EOS/USDT', 'ESE/USDT', 'ETC/USDT', 'ETH/USDT', 'ETHFI/USDT', 'ETHW/USDT', 'FARM/USDT', 'FET/USDT', 'FIDA/USDT', 'FIL/USDT', 'FIO/USDT', 'FIS/USDT', 'FITFI/USDT', 'FLIP/USDT', 'FLM/USDT', 'FLOKI/USDT', 'FLOW/USDT', 'FLR/USDT', 'FLT/USDT', 'FLUX/USDT', 'FORT/USDT', 'FORTH/USDT', 'FOXY/USDT', 'FTM/USDT', 'FTN/USDT', 'FTT/USDT', 'FUN/USDT', 'FXS/USDT', 'G/USDT', 'GALA/USDT', 'GAS/USDT', 'GFI/USDT', 'GFT/USDT', 'GHST/USDT', 'GLM/USDT', 'GLMR/USDT', 'GME/USDT', 'GMT/USDT', 'GMX/USDT', 'GNS/USDT', 'GPT/USDT', 'GROK/USDT', 'GRT/USDT', 'GT/USDT', 'GTAI/USDT', 'GTC/USDT', 'HBAR/USDT', 'HFT/USDT', 'HIFI/USDT', 'HIGH/USDT', 'HNT/USDT', 'HOOK/USDT', 'HOT/USDT', 'ICE/USDT', 'ICP/USDT', 'ICX/USDT', 'ID/USDT', 'IDEX/USDT', 'ILV/USDT', 'IMX/USDT', 'INJ/USDT', 'IO/USDT', 'IOST/USDT', 'IOTA/USDT', 'IOTX/USDT', 'IQ/USDT', 'IQ50/USDT', 'JASMY/USDT', 'JOE/USDT', 'JST/USDT', 'JTO/USDT', 'JUP/USDT', 'KARRAT/USDT', 'KAS/USDT', 'KAVA/USDT', 'KDA/USDT', 'KEY/USDT', 'KLAY/USDT', 'KNC/USDT', 'KSM/USDT', 'L3/USDT', 'LADYS/USDT', 'LAI/USDT', 'LDO/USDT', 'LEVER/USDT', 'LINA/USDT', 'LINK/USDT', 'LISTA/USDT', 'LIT/USDT', 'LOKA/USDT', 'LOOKS/USDT', 'LOOM/USDT', 'LPT/USDT', 'LQTY/USDT', 'LRC/USDT', 'LSK/USDT', 'LTC/USDT', 'LUNA/USDT', 'LUNA2/USDT', 'LUNC/USDT', 'MAGIC/USDT', 'MANA/USDT', 'MANTA/USDT', 'MASA/USDT', 'MASK/USDT', 'MAV/USDT', 'MAVIA/USDT', 'MAX/USDT', 'MBABYDOGE/USDT', 'MBL/USDT', 'MBOX/USDT', 'MDT/USDT', 'MEME/USDT', 'MERL/USDT', 'METIS/USDT', 'MEW/USDT', 'MINA/USDT', 'MKR/USDT', 'MNT/USDT', 'MOBILE/USDT', 'MODE/USDT', 'MOG/USDT', 'MON/USDT', 'MOVR/USDT', 'MPC/USDT', 'MPL/USDT', 'MPLX/USDT', 'MSN/USDT', 'MTL/USDT', 'MUBI/USDT', 'MYRIA/USDT', 'MYRO/USDT', 'NEAR/USDT', 'NEIROETH/USDT', 'NEO/USDT', 'NFP/USDT', 'NIBI/USDT', 'NKN/USDT', 'NMR/USDT', 'NOT/USDT', 'NTRN/USDT', 'NULS/USDT', 'OAX/USDT', 'OCEAN/USDT', 'OG/USDT', 'OGN/USDT', 'OKB/USDT', 'OM/USDT', 'OMG/USDT', 'OMNI/USDT', 'ONDO/USDT', 'ONE/USDT', 'ONG/USDT', 'ONT/USDT', 'OP/USDT', 'ORBS/USDT', 'ORDER/USDT', 'ORDI/USDT', 'ORN/USDT', 'OXT/USDT', 'PATEX/USDT', 'PBUX/USDT', 'PENDLE/USDT', 'PEOPLE/USDT', 'PEPE/USDT', 'PEPE2/USDT', 'PERP/USDT', 'PHA/USDT', 'PHB/USDT', 'PIXEL/USDT', 'PIXFI/USDT', 'PNG/USDT', 'POLYX/USDT', 'POND/USDT', 'PONKE/USDT', 'POPCAT/USDT', 'PORTAL/USDT', 'POWR/USDT', 'PRCL/USDT', 'PRIME/USDT', 'PROM/USDT', 'PRQ/USDT', 'PYR/USDT', 'PYTH/USDT', 'QNT/USDT', 'QTUM/USDT', 'QUICK/USDT', 'RACA/USDT', 'RAD/USDT', 'RARE/USDT', 'RATS/USDT', 'RAY/USDT', 'RDNT/USDT', 'REEF/USDT', 'REI/USDT', 'REN/USDT', 'RENDER/USDT', 'REQ/USDT', 'REZ/USDT', 'RIF/USDT', 'RLC/USDT', 'RON/USDT', 'RONIN/USDT', 'ROOT/USDT', 'ROSE/USDT', 'RPL/USDT', 'RSR/USDT', 'RSS3/USDT', 'RUNE/USDT', 'RVN/USDT', 'SAFE/USDT', 'SAGA/USDT', 'SAND/USDT', 'SATS/USDT', 'SC/USDT', 'SCA/USDT', 'SEI/USDT', 'SFP/USDT', 'SHIB/USDT', 'SIDUS/USDT', 'SKL/USDT', 'SLERF/USDT', 'SLN/USDT', 'SLP/USDT', 'SNT/USDT', 'SNX/USDT', 'SOL/USDT', 'SOLS/USDT', 'SPA/USDT', 'SPELL/USDT', 'SSV/USDT', 'STARL/USDT', 'STEEM/USDT', 'STG/USDT', 'STMX/USDT', 'STORJ/USDT', 'STPT/USDT', 'STRAX/USDT', 'STRK/USDT', 'STX/USDT', 'SUI/USDT', 'SUN/USDT', 'SUNDOG/USDT', 'SUPER/USDT', 'SUSHI/USDT', 'SWEAT/USDT', 'SXP/USDT', 'SYN/USDT', 'SYS/USDT', 'T/USDT', 'TAI/USDT', 'TAIKO/USDT', 'TAO/USDT', 'THETA/USDT', 'TIA/USDT', 'TLM/USDT', 'TNSR/USDT', 'TOKEN/USDT', 'TOMI/USDT', 'TON/USDT', 'TRB/USDT', 'TRU/USDT', 'TRX/USDT', 'TURBO/USDT', 'TWT/USDT', 'ULTI/USDT', 'UMA/USDT', 'UNFI/USDT', 'UNI/USDT', 'USDC/USDT', 'USTC/USDT', 'UXLINK/USDT', 'VANRY/USDT', 'VELO/USDT', 'VELODROME/USDT', 'VENOM/USDT', 'VET/USDT', 'VGX/USDT', 'VIDT/USDT', 'VOXEL/USDT', 'VRA/USDT', 'VTHO/USDT', 'W/USDT', 'WAVES/USDT', 'WAXL/USDT', 'WAXP/USDT', 'WEMIX/USDT', 'WIF/USDT', 'WIN/USDT', 'WLD/USDT', 'WOJAK/USDT', 'WOO/USDT', 'XAI/USDT', 'XCH/USDT', 'XCN/USDT', 'XEC/USDT', 'XEM/USDT', 'XEN/USDT', 'XLM/USDT', 'XMR/USDT', 'XNO/USDT', 'XRD/USDT', 'XRP/USDT', 'XTZ/USDT', 'XVG/USDT', 'XVS/USDT', 'YFI/USDT', 'YGG/USDT', 'ZEC/USDT', 'ZEN/USDT', 'ZEROLEND/USDT', 'ZETA/USDT', 'ZEUS/USDT', 'ZIL/USDT', 'ZK/USDT', 'ZKF/USDT', 'ZKJ/USDT', 'ZKL/USDT', 'ZRO/USDT', 'ZRX/USDT']

# Spread_Steps are in OKX/Binance significant digits
SPREAD_STEPS = {'STORJ': '4/4', 'TURBO': '5/4', 'MEW': '4/4', 'JTO': '4/5', 'ORDI': '5/5', 'MKR': '5/5', 'NOT': '5/5', 'TIA': '4/5', 'AAVE': '5/5', 'OM': '5/5', 'AR': '5/5', 'TRB': '4/5', 'ORBS': '4/4', 'ALPHA': '4/4', 'AUCTION': '5/5', 'SOL': '5/6', 'ZETA': '4/4', 'SUI': '4/4', 'ZRX': '4/5'}


HEDGE1 = ['NOT', 'TIA', 'OM', 'MKR', 'AAVE', 'JTO', 'ORDI', 'STORJ', 'ENJ', 'LQTY', 'MEW']
HEDGE2 = ['TURBO', 'AR', 'TRB', 'ORBS', 'AUCTION', 'SOL', 'ZETA', 'SUI', 'TRX', 'DOGS', 'UMA', 'POPCAT'] # ALPHA goes here when un-paused
HEDGE3 = ['TNSR', 'AGLD', 'RDNT', 'BNT', 'TON', 'UXLINK', 'BIGTIME', 'HMSTR', 'APE', 'NEIRO', 'MASK']
IN_TRADE = HEDGE1 + HEDGE2 + HEDGE3 + ['REEF', 'BANANA']


# Logging configuration
LOG_DIRECTORY = "logs"
os.makedirs(LOG_DIRECTORY, exist_ok=True)
LOG_FILENAME = os.path.join(LOG_DIRECTORY, f"trading_bot2.5_{datetime.now().strftime('%Y%m%d')}.log")

# Initialize Telegram Bot for Errors
bot_token2 = '6745253394:AAGRXVRqImFEfVNJVu6lgdHc-a0nZ1PlP1o'
chat_id2 = '-4191307394'
Error_bot = Bot(token=bot_token2)

bot_tokenFunding = '7144896940:AAHos2I8SpBLKrthSr9LjCZbJp7HOk4edLo'
chat_idFunding = '-4221921856'

bot_token = '7038694108:AAFiJYHpk-KuhnUUWOKywVaVS_e-j8ft9OQ'
chat_id = '-4192148173'

bot_tokenHFT = '7325679597:AAHyiObT5cdVEi3E4iXxUJbKYaI-RaSnmyA'
chat_idHFT = '-4227694824'


def send_telegram_message(msg, bot_token, chat_id):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {'chat_id': chat_id, 'text': msg, 'parse_mode': 'Markdown'}
    response = requests.post(url, data=payload)
    return response.text


class TimeWindowOrderFilledHandler(logging.Handler):
    def __init__(self, threshold=10, time_window=5):
        super().__init__()
        self.threshold = threshold
        self.time_window = time_window
        self.order_fill_times = deque(maxlen=threshold)

    def emit(self, record):
        if "Order filled" in record.getMessage():
            current_time = time.time()
            self.order_fill_times.append(current_time)

            if len(self.order_fill_times) == self.threshold:
                time_diff = current_time - self.order_fill_times[0]
                if time_diff <= self.time_window:
                    alert_message = f"Alert: {self.threshold} or more order fills in {time_diff:.2f} seconds. Spike in HighFrequency order volume, Check with bot"
                    send_telegram_message(alert_message, bot_token2, chat_id2)
                self.order_fill_times.popleft()  # Remove the oldest timestamp


def setup_logger(caller_name=None):
    if caller_name is None:
        # Get the name of the script that called this function
        frame = inspect.stack()[1]
        module = inspect.getmodule(frame[0])
        caller_name = module.__name__

    logger = logging.getLogger(caller_name)
    logger.setLevel(logging.DEBUG)

    # Create handlers
    console_handler = logging.StreamHandler()
    file_handler = RotatingFileHandler(LOG_FILENAME, maxBytes=10*1024*1024*500, backupCount=5)
    time_window_order_handler = TimeWindowOrderFilledHandler(threshold=15, time_window=5)

    # Create formatters and add it to handlers
    log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(log_format)
    file_handler.setFormatter(log_format)

    # Add handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.addHandler(time_window_order_handler)

    return logger