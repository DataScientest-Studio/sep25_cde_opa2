import time
import argparse
from datetime import datetime

from BinanceDataCollector import BinanceDataCollector, RateLimitExceededException

from src.common.custom_logger import logger

def main():
    """Fonction principale du script."""
    parser = argparse.ArgumentParser(description='Extraction de données de marché en temps réel depuis Binance')
    parser.add_argument('--symbol', required=True, help='Symbole de trading (ex: BTCUSDT)')
    parser.add_argument('--duration', type=int, default=120, help='Durée de collecte en secondes (défaut: 120 = 2 minutes)')
    parser.add_argument('--interval', type=int, default=10, help='Intervalle entre collectes en secondes (défaut: 10)')
    
    args = parser.parse_args()
    
    # Configuration
    symbol = args.symbol.upper()
    duration = args.duration
    interval = args.interval
    collection_prefix = "realtime"
    
    # Calcul du nombre d'itérations
    total_iterations = duration // interval
    
    logger.info(f"=== Extraction de données temps réel ===")
    logger.info(f"Symbole: {symbol}")
    logger.info(f"Durée: {duration} secondes")
    logger.info(f"Intervalle: {interval} secondes")
    logger.info(f"Nombre de collectes: {total_iterations}")
    logger.info(f"Collections MongoDB: {collection_prefix}_[order_book|avg_price|ticker_24|agg_trades]_{symbol}")
    logger.info(f"Début: {datetime.now()}")
    logger.info("=" * 50)
    
    # Initialisation du collecteur
    collector = BinanceDataCollector()
    
    # Connexion à MongoDB
    if not collector.connect_to_mongodb():
        logger.error("Échec de la connexion à MongoDB. Arrêt du script.")
        return
    
    successful_collections = 0
    failed_collections = 0
    
    try:
        start_time = time.time()
        
        for iteration in range(1, total_iterations + 1):
            iteration_start = time.time()
            
            logger.info(f"Collecte {iteration}/{total_iterations} - {datetime.now().strftime('%H:%M:%S')}")
            
            try:
                # Collecte des données
                market_data = collector.get_realtime_market_data(symbol)
                
                if market_data:
                    # Sauvegarde en base
                    if collector.save_realtime_data_to_mongodb(market_data, collection_prefix):
                        successful_collections += 1
                    else:
                        failed_collections += 1
                        logger.error("   Échec de la sauvegarde")
                else:
                    failed_collections += 1
                    logger.error("   Échec de la collecte des données")
                    
            except RateLimitExceededException as e:
                logger.error(f"\nARRÊT IMMÉDIAT - RATE LIMIT DÉTECTÉ (429)")
                logger.error(f"Message: {e}")
                logger.error(f"Votre IP risque d'être bloquée si vous continuez !")
                logger.error(f"Attendez quelques minutes avant de relancer")
                return
            
            # Attendre avant la prochaine collecte (sauf à la dernière itération)
            if iteration < total_iterations:
                elapsed_time = time.time() - iteration_start
                sleep_time = max(0, interval - elapsed_time)
                
                if sleep_time > 0:
                    logger.info(f"   Attente {sleep_time:.1f}s avant prochaine collecte...")
                    time.sleep(sleep_time)
                else:
                    logger.warning(f"   Collecte a pris {elapsed_time:.1f}s (> {interval}s)")
        
        # Statistiques finales
        total_time = time.time() - start_time
        logger.info("\n" + "=" * 50)
        logger.info("RÉSUMÉ DE LA COLLECTE")
        logger.info("=" * 50)
        logger.info(f"Durée totale: {total_time:.1f} secondes")
        logger.info(f"Collectes réussies: {successful_collections}/{total_iterations}")
        logger.info(f"Collectes échouées: {failed_collections}/{total_iterations}")
        logger.info(f"Taux de succès: {(successful_collections/total_iterations)*100:.1f}%")
        logger.info(f"Fin: {datetime.now()}")
        
    except RateLimitExceededException:
        # Déjà géré dans la boucle, on sort proprement
        pass
    except KeyboardInterrupt:
        logger.info("\n\nCollecte interrompue par l'utilisateur")
        logger.info(f"Collectes réussies avant interruption: {successful_collections}")
    
    except Exception as e:
        logger.error(f"\nErreur inattendue: {e}")
    
    finally:
        # Fermeture des connexions
        collector.close_connections()
        logger.info("Connexions fermées")

if __name__ == '__main__':
    main()