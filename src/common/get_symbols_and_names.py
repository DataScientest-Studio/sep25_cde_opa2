import time
import json

from pathlib import Path

from typing import cast

from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceRequestException

from src.config import BINANCE_API_KEY, BINANCE_API_SECRET, PROJECT_ROOT
from src.common.custom_logger import logger

# Define a default symbols list in case of api error, or file access errors.
DEFAULT_SYMBOLS = [
    {
        "symbol": "BTC",
        "name": "Bitcoin",
        "aliases": ["btc", "bitcoin"]
    }
]

def get_cryptos_symbols_and_names(force_update=False) -> list[dict[str, str]]:
    file_name = "mapping_cryptos_symbol_name.json"
    path_to_symbols_and_names = Path(f"{PROJECT_ROOT}/data/external/{file_name}")

    # If file exist, return the data, except when force_update is set to True
    if not force_update and path_to_symbols_and_names.exists():
        try:
            with open(path_to_symbols_and_names, "r", encoding="utf-8") as f:
                return cast(list[dict[str, str]], json.load(f))
        except json.JSONDecodeError as e:
            logger.error(f"Erreur lors de la lecture du JSON: {e.msg}")
            logger.warning(f"Le fichier va être remplacé par un nouveau")

    # Call binance api if force_update is set to True, or if file doesn't exist.
    logger.info(f"Appel api afin de générer le fichier de mapping entre un symbole crypto et son nom")
    try:
        mapping_cryptos=list()
        binance_client = Client(api_key=BINANCE_API_KEY, api_secret=BINANCE_API_SECRET)
        server_time = binance_client.get_server_time()
        binance_client.timestamp_offset = server_time['serverTime'] - int(time.time() * 1000)
        coins_list=binance_client._request('get', f'{binance_client.MARGIN_API_URL}/v1/capital/config/getall', signed=True, data={})
        
        # Keep only used attributes
        for coin in coins_list:
            mapping_cryptos.append({
                "symbol": coin['coin'],
                "name": coin['name'],
                "aliases": [str.lower(coin['coin']), str.lower(coin['name'])]
            })
        
        # Save file
        try:
            with open(path_to_symbols_and_names, "w", encoding="utf-8") as f:
                json.dump(mapping_cryptos, f, indent=4, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Erreur lors de l'écriture du fichier: {e}")
            logger.warning(
                """La récupération du mapping entre le symbol d'une crypto et son nom n'a pas été possible.
                Ainsi, seul le Bitcoin sera évalué dans l'identification des symbols présents dans les articles.
                """)
            return DEFAULT_SYMBOLS
            
        return mapping_cryptos

    except BinanceAPIException as e:
        logger.error(f"Erreur API Binance: {e.code} - {e.message}")
        logger.warning(
            """La récupération du mapping entre le symbol d'une crypto et son nom n'a pas été possible.
            Ainsi, seul le Bitcoin sera évalué dans l'identification des symbols présents dans les articles.
            """)
        return DEFAULT_SYMBOLS

    except BinanceRequestException as e:
        logger.error(f"Erreur Request Binance: {e.message}")
        logger.warning(
            """La récupération du mapping entre le symbol d'une crypto et son nom n'a pas été possible.
            Ainsi, seul le Bitcoin sera évalué dans l'identification des symbols présents dans les articles.
            """)
        return DEFAULT_SYMBOLS

    except Exception as e:
        logger.error(f"Erreur lors de la récupération des données Binance: {e}")
        logger.warning(
            """La récupération du mapping entre le symbol d'une crypto et son nom n'a pas été possible.
            Ainsi, seul le Bitcoin sera évalué dans l'identification des symbols présents dans les articles.
            """)
        return DEFAULT_SYMBOLS
