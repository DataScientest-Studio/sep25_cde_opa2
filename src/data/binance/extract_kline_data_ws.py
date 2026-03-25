import argparse
import sys
import asyncio
from binance import AsyncClient, BinanceSocketManager
from binance.enums import *
from datetime import datetime
from src.data.binance.BinanceDataCollector import BinanceDataCollector

from src.common.custom_logger import logger
from src.config import DB_NAME, MONGO_DB_PORT, DB_BOT_USER, DB_BOT_PASSWORD, MONGO_HOST

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
            logger.info(f"Collection: {formatted_data_collection_name}")
            logger.info(f"Données formatées: {formatted_data_point}")

            success_klines = collector.save_kline_to_mongodb(formatted_data_point, formatted_data_collection_name)

    await client.close_connection()

if __name__ == "__main__":

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())