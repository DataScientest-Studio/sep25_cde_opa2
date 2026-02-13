
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta

from binance.client import Client
from pymongo import MongoClient
from pymongo.errors import PyMongoError


# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Mappage des intervalles
interval_mapping = {
    '1m': Client.KLINE_INTERVAL_1MINUTE,
    '3m': Client.KLINE_INTERVAL_3MINUTE,
    '5m': Client.KLINE_INTERVAL_5MINUTE,
    '15m': Client.KLINE_INTERVAL_15MINUTE,
    '30m': Client.KLINE_INTERVAL_30MINUTE,
    '1h': Client.KLINE_INTERVAL_1HOUR,
    '2h': Client.KLINE_INTERVAL_2HOUR,
    '4h': Client.KLINE_INTERVAL_4HOUR,
    '6h': Client.KLINE_INTERVAL_6HOUR,
    '8h': Client.KLINE_INTERVAL_8HOUR,
    '12h': Client.KLINE_INTERVAL_12HOUR,
    '1d': Client.KLINE_INTERVAL_1DAY,
}

class BinanceDataCollector:
    """Classe pour collecter des données depuis l'API Binance et les stocker dans MongoDB."""
    
    def __init__(self, mongodb_config: Dict[str, str]):
        """
        Initialise le collecteur de données.
        
        Args:
            mongodb_config: Configuration MongoDB
        """
        self.binance_client = Client()  # Client en lecture seule (pas besoin d'API keys)
        self.mongo_client = None
        self.db = None
        self.mongodb_config = mongodb_config
        
    def connect_to_mongodb(self) -> bool:
        """
        Établit la connexion à MongoDB.
        
        Returns:
            bool: True si la connexion est réussie, False sinon
        """
        try:
            # Construction de l'URI MongoDB
            username = self.mongodb_config['username']
            password = self.mongodb_config['password']
            host = self.mongodb_config['host']
            port = self.mongodb_config['port']
            db_name = self.mongodb_config['db_name']
            
            if username and password and host and port and db_name:
                connection_string = f"mongodb://{username}:{password}@{host}:{port}/{db_name}"
            else:
                logger.error("Configuration MongoDB incomplète. Veuillez vérifier les variables d'environnement.")
                return False
            
            self.mongo_client = MongoClient(connection_string)
            self.db = self.mongo_client[db_name]
            
            # Test de la connexion
            self.mongo_client.admin.command('ping')
            logger.info(f"Connexion réussie à MongoDB: {host}:{port}")
            return True
            
        except PyMongoError as e:
            logger.error(f"Erreur de connexion MongoDB: {e}")
            return False

    def get_klines_data(
        self, 
        symbol: str, 
        start_date: datetime, 
        days: int = 60,
        interval: str = '1h'
    ) -> List[Dict]:
        """
        Récupère les données Klines depuis l'API Binance.
        
        Args:
            symbol: Symbole de trading (ex: BTCUSDT)
            start_date: Date de début
            days: Nombre de jours à récupérer (défaut: 60 jours = ~2 mois)
            interval: Intervalle des données (défaut: 1h)
            
        Returns:
            List[Dict]: Liste des données Kline
        """
        try:
            end_date = start_date + timedelta(days=days)
            
            logger.info(f"Récupération des données {symbol} du {start_date.date()} au {end_date.date()}")
            
            # Conversion en timestamps
            start_str = start_date.strftime('%Y-%m-%d')
            end_str = end_date.strftime('%Y-%m-%d')
            
            # Récupération des données depuis Binance
            klines = self.binance_client.get_historical_klines(
                symbol=symbol,
                interval=interval_mapping[interval],
                start_str=start_str,
                end_str=end_str
            )
            
            formatted_data = [] # Transformation des données en format plus lisible (à voir si on en a besoin ou pas)
            not_formatted_data = []
            for kline in klines:
                formatted_data_point = {
                    'open_time': datetime.fromtimestamp(kline[0] / 1000),
                    'close_time': datetime.fromtimestamp(kline[6] / 1000),
                    'open_price': float(kline[1]),
                    'high_price': float(kline[2]),
                    'low_price': float(kline[3]),
                    'close_price': float(kline[4]),
                    'volume': float(kline[5]),
                    'quote_volume': float(kline[7]),
                    'trades_count': int(kline[8]),
                    'taker_buy_base_volume': float(kline[9]),
                    'taker_buy_quote_volume': float(kline[10]),
                    'ignore': kline[11]  # Champ ignoré, peut être utilisé pour des données futures

                }
                formatted_data.append(formatted_data_point)
                not_formatted_data_point = {
                    'open_time': kline[0],
                    'close_time': kline[6],
                    'open_price': kline[1],
                    'high_price': kline[2],
                    'low_price': kline[3],
                    'close_price': kline[4],
                    'volume': kline[5],
                    'quote_volume': kline[7],
                    'trades_count': kline[8],
                    'taker_buy_base_volume': kline[9],
                    'taker_buy_quote_volume': kline[10],
                    'ignore': kline[11]  # Champ ignoré, peut être utilisé pour des données futures
                }
                not_formatted_data.append(not_formatted_data_point)
            logger.info(f"Récupération terminée: {len(formatted_data)} points de données")
            return formatted_data, not_formatted_data
            
        except Exception as e:
            logger.error(f"Erreur lors de la récupération des données Binance: {e}")
            return []


    def get_exchange_info(self) -> Optional[Dict]:
        """
        Récupère les informations exchange_info depuis l'API Binance.
        
        Returns:
            Dict: Informations exchange_info ou None en cas d'erreur
        """
        try:
            exchange_info = self.binance_client.get_exchange_info()
            logger.info("Récupération des informations exchange_info réussie")
            return exchange_info
        except Exception as e:
            logger.error(f"Erreur lors de la récupération des informations exchange_info: {e}")
            return None


    def save_klines_to_mongodb(self, data: List[Dict], collection_name: str = 'klines') -> bool:
        """
        Sauvegarde les données de klines dans MongoDB.
        
        Args:
            data: Données klines à sauvegarder 
            collection_name: Nom de la collection MongoDB
            
        Returns:
            bool: True si la sauvegarde est réussie
        """
        try:
            if not data:
                logger.warning("Aucune donnée à sauvegarder")
                return False
            
            collection = self.db[collection_name]
            
            # Création d'un index unique pour éviter les doublons
            collection.create_index([
                ('open_time', 1)
            ], unique=True)
            
            # Insertion des données
            inserted_ids = []
            skipped_count = 0
            
            for document in data:
                try:
                    result = collection.insert_one(document)
                    inserted_ids.append(result.inserted_id)
                except PyMongoError as e:
                    if "duplicate key error" in str(e).lower():
                        skipped_count += 1
                    else:
                        logger.warning(f"Erreur lors de l'insertion: {e}")
            
            logger.info(f"Sauvegarde terminée: {len(inserted_ids)} documents insérés, {skipped_count} doublons ignorés")
            return True
            
        except PyMongoError as e:
            logger.error(f"Erreur lors de la sauvegarde MongoDB: {e}")
            return False

    def save_exchange_info_to_mongodb(self, data: List[Dict], collection_name: str = 'exchange_info_symbols') -> bool:
        """
        Sauvegarde les données sur les symbols (contenu dans exchange_info) dans MongoDB.
        
        Args:
            data: Données symbols à sauvegarder
            collection_name: Nom de la collection MongoDB
            
        Returns:
            bool: True si la sauvegarde est réussie
        """
        try:
            if not data:
                logger.warning("Aucune donnée à sauvegarder")
                return False
            
            collection = self.db[collection_name]
            
            # Création d'un index unique pour éviter les doublons
            collection.create_index([
                ('symbol', 1)
            ], unique=True)
            
            # Insertion des données
            inserted_ids = []
            skipped_count = 0
            
            for document in data:
                try:
                    result = collection.insert_one(document)
                    inserted_ids.append(result.inserted_id)
                except PyMongoError as e:
                    if "duplicate key error" in str(e).lower():
                        skipped_count += 1
                    else:
                        logger.warning(f"Erreur lors de l'insertion: {e}")
            
            logger.info(f"Sauvegarde terminée: {len(inserted_ids)} documents insérés, {skipped_count} doublons ignorés")
            return True
            
        except PyMongoError as e:
            logger.error(f"Erreur lors de la sauvegarde MongoDB: {e}")
            return False
        
    def close_connections(self):
        """Ferme la connexion à MongoDB."""
        if self.mongo_client:
            self.mongo_client.close()
            logger.info("Connexion fermée")
