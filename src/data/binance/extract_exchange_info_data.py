import argparse
import logging
import sys
import os
from datetime import datetime

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


def main():

    
    try:
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
