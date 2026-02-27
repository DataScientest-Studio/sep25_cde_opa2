import argparse
import logging
import sys
import os
import asyncio
from binance import AsyncClient, BinanceSocketManager
from binance.enums import *
from datetime import datetime, timedelta
from BinanceDataCollector import BinanceDataCollector

# Ajouter le répertoire src au path pour importer config
script_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.dirname(script_dir)  # src/data
src_dir = os.path.dirname(data_dir)     # src
sys.path.insert(0, src_dir)

try:
    from config import (DB_NAME, DB_BOT_USER, DB_BOT_PASSWORD, MONGO_DB_PORT, MONGO_HOST)
except ImportError as e:
    logging.error(f"Impossible d'importer config: {e}")
    logging.error("Assurez-vous que le fichier src/config.py existe et est accessible")
    sys.exit(1)

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_arguments():
    """Parse les arguments de ligne de commande."""
    parser = argparse.ArgumentParser(
        description='Collecte des données Kline depuis Binance vers MongoDB'
    )
    
    parser.add_argument(
        '--symbol',
        type=str,
        required=True,
        help='Symbole de trading (ex: BTCUSDT, ETHUSDT)'
    )
    
    parser.add_argument(
        '--interval',
        type=str,
        default='1h',
        choices=['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d'],
        help='Intervalle des données (défaut: 1h)'
    )
    
    return parser.parse_args()
    
async def main():
    args = parse_arguments()

    # Configuration MongoDB
    mongodb_config = {
        'username': DB_BOT_USER,
        'password': DB_BOT_PASSWORD,
        'host': MONGO_HOST,
        'port': MONGO_DB_PORT,
        'db_name': DB_NAME
    }       
    
    # Initialisation du collecteur
    collector = BinanceDataCollector(mongodb_config)
    
    # Connexion à MongoDB
    if not collector.connect_to_mongodb():
        logger.error("Impossible de se connecter à MongoDB")
        sys.exit(1)
    
    client = await AsyncClient.create()
    bm = BinanceSocketManager(client)
    # start any sockets here, i.e a trade socket
    ks = bm.kline_socket(args.symbol, interval=args.interval)
    # then start receiving messages
    async with ks as kscm:
        while True:
            res = await kscm.recv()

            formatted_data_collection_name = f"klines_{args.symbol}_{args.interval}_ws"
            formatted_data_point = {
                'open_time': datetime.fromtimestamp(res['k']['t'] / 1000),
                'close_time': datetime.fromtimestamp(res['k']['T'] / 1000),
                'open_price': float(res['k']['o']),
                'high_price': float(res['k']['h']),
                'low_price': float(res['k']['l']),
                'close_price': float(res['k']['c']),
                'volume': float(res['k']['v']),
                'quote_volume': float(res['k']['q']),
                'trades_count': int(res['k']['n']),
                'taker_buy_base_volume': float(res['k']['V']),
                'taker_buy_quote_volume': float(res['k']['Q']),
                'ignore': res['k']['B']  # Champ ignoré, peut être utilisé pour des données futures

            }
            print(formatted_data_collection_name)
            print(formatted_data_point)
            # not_formatted_data_point = {
            #         'open_time': kline[0],
            #         'close_time': kline[6],
            #         'open_price': kline[1],
            #         'high_price': kline[2],
            #         'low_price': kline[3],
            #         'close_price': kline[4],
            #         'volume': kline[5],
            #         'quote_volume': kline[7],
            #         'trades_count': kline[8],
            #         'taker_buy_base_volume': kline[9],
            #         'taker_buy_quote_volume': kline[10],
            #         'ignore': kline[11]  # Champ ignoré, peut être utilisé pour des données futures
            #     }
            success_klines = collector.save_kline_to_mongodb(formatted_data_point, formatted_data_collection_name)

    await client.close_connection()

if __name__ == "__main__":

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())