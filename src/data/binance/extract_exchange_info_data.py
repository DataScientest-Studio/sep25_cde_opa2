import sys
from datetime import datetime

from src.data.binance.BinanceDataCollector import BinanceDataCollector

from src.common.custom_logger import logger

def main():

    
    try:
        # Initialisation du collecteur
        collector = BinanceDataCollector()
        
        # Connexion à MongoDB
        if not collector.connect_to_mongodb():
            logger.error("Impossible de se connecter à MongoDB")
            sys.exit(1)
        
        exchange_info = collector.get_exchange_info()
        logger.info(f"Nombre de symboles disponibles sur Binance: {len(exchange_info['symbols'])}")
        
        success_exchange_info = collector.save_exchange_info_to_mongodb(exchange_info['symbols'], "exchange_info_symbols")

        # Fermeture des connexions
        collector.close_connections()
        
        if success_exchange_info:
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
        logger.exception(e)
        sys.exit(1)


if __name__ == "__main__":
    main()
