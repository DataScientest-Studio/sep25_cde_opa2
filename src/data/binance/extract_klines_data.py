import argparse
import sys
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
        required=True,
        help='Symbole de trading (ex: BTCUSDT, ETHUSDT)'
    )
    
    parser.add_argument(
        '--start_date',
        type=str,
        required=True,
        help='Date de début au format YYYY-MM-DD'
    )
    
    parser.add_argument(
        '--days',
        type=int,
        default=60,
        help='Nombre de jours à récupérer (défaut: 60)'
    )
    
    parser.add_argument(
        '--interval',
        type=str,
        default='1h',
        choices=['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '1w', '1M'],
        help='Intervalle des données (défaut: 1h)'
    )

    parser.add_argument(
        '--resume_from_last',
        action='store_true',
        default=False,
        help='Si activé, reprend depuis la dernière kline en base MongoDB plutôt que start_date'
    )
    
    return parser.parse_args()


def main():
    args = parse_arguments()
    
    try:
        # Validation de la date
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        
        # Initialisation du collecteur
        collector = BinanceDataCollector()
        
        # Connexion à MongoDB
        if not collector.connect_to_mongodb():
            logger.error("Impossible de se connecter à MongoDB")
            sys.exit(1)
        
        # Récupération des données sur les klines
        logger.info(f"Début de la collecte pour {args.symbol}")
        formatted_data, not_formatted_data = collector.get_klines_data(
            symbol=args.symbol,
            start_date=start_date,
            days=args.days,
            interval=args.interval,
            resume_from_last=args.resume_from_last
        )
        
        if not formatted_data or not not_formatted_data:
            if args.resume_from_last:
                logger.info("Aucune nouvelle donnée à récupérer, la base est déjà à jour.")
                collector.close_connections()
                sys.exit(0)
            else:
                logger.error("Problème lors de la récupération des données")
                collector.close_connections()
                sys.exit(1)

        formatted_data_collection_name = f"klines_{args.symbol}_{args.interval}"
        not_formatted_data_collection_name = f"klines_raw_data_{args.symbol}_{args.interval}"

        # Sauvegarde dans MongoDB
        success_klines = collector.save_klines_to_mongodb(formatted_data, formatted_data_collection_name) & \
                  collector.save_klines_to_mongodb(not_formatted_data, not_formatted_data_collection_name)
        
        # Fermeture des connexions
        collector.close_connections()

        if success_klines:
            logger.info("Collecte terminée avec succès\n")
        else:
            logger.error("Erreur lors de la sauvegarde\n")
            sys.exit(1)
            
    except ValueError as e:
        logger.error(f"Format de date invalide. Utilisez YYYY-MM-DD: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Collecte interrompue par l'utilisateur")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Erreur inattendue: {e}")
        e.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
