import argparse
import sys
import asyncio
from binance import AsyncClient, BinanceSocketManager
from binance.enums import *
from datetime import datetime
from src.data.binance.BinanceDataCollector import BinanceDataCollector

from src.common.custom_logger import logger

def parse_arguments():
    """Parse les arguments de ligne de commande."""
    parser = argparse.ArgumentParser(
        description='Collecte des données Kline depuis Binance vers MongoDB'
    )
    
    parser.add_argument(
        '--symbol',
        type=str,
        nargs='+',
        required=True,
        help='Symbole(s) de trading, séparés par des espaces (ex: BTCUSDT ETHUSDT)'
    )
    
    parser.add_argument(
        '--interval',
        type=str,
        nargs='+',
        default=['1m'],
        choices=['1s', '1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '1w', '1M'],
        help='Interval(s) des données, séparés par des espaces (ex: 1m 5m 1h). Défaut: 1m'
    )
    
    return parser.parse_args()
    
async def stream_klines(client, collector, symbol, interval):
    """Écoute le flux WebSocket pour un symbol/interval et persiste en MongoDB."""
    bm = BinanceSocketManager(client)
    ks = bm.kline_socket(symbol, interval=interval)
    async with ks as kscm:
        while True:
            res = await kscm.recv()
            collection_name = f"klines_{symbol}_{interval}_ws"
            data_point = {
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
                'ignore': res['k']['B'],
            }
            logger.info(f"Collection: {collection_name}")
            logger.info(f"Données formatées: {data_point}")
            collector.save_kline_to_mongodb(data_point, collection_name)


async def main():
    args = parse_arguments()

    # Initialisation du collecteur
    collector = BinanceDataCollector()

    # Connexion à MongoDB
    if not collector.connect_to_mongodb():
        logger.error("Impossible de se connecter à MongoDB")
        sys.exit(1)

    client = await AsyncClient.create()
    logger.info(f"Démarrage des flux WebSocket pour {args.symbol} — intervals: {args.interval}")
    try:
        await asyncio.gather(
            *[
                stream_klines(client, collector, symbol, interval)
                for symbol in args.symbol
                for interval in args.interval
            ]
        )
    finally:
        await client.close_connection()

if __name__ == "__main__":

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Arrêt manuel par l'utilisateur.")
        sys.exit(0)