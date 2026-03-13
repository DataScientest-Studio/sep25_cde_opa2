from concurrent.futures import ProcessPoolExecutor
import signal
import subprocess
import time
import sys

from src.custom_logger import logger
from src.config import SCRAPER_DETECT_DELAY, SCRAPER_DETECT_LIMIT, SCRAPER_ENRICH_DELAY, SCRAPER_ENRICH_LIMIT, SCRAPER_INDEX_DELAY, SCRAPER_INDEX_LIMIT

executor = None

def stop_workers(signum, frame):
    """Interception du signal d'arrêt de docker"""
    global executor
    logger.info(f"Signal {signum} reçu. Fermeture des workers...")
    if executor:
        # On attend que les script en cours s'arrête correctement, puis stop
        executor.shutdown(wait=True, cancel_futures=True)
    logger.info("Workers arrêtés.")
    sys.exit(0)

# On affecte notre fonction stop aux signaux docker
signal.signal(signal.SIGTERM, stop_workers)
signal.signal(signal.SIGINT, stop_workers)

def worker(task_info):
    """
    Chargé de lancer une commande:
    - name: Nom de la commande
    - cmd: Commande a executer
    - delay: Delai en millisecondes avant un redemarrage de la commande
    """

    name, cmd_list, delay = task_info
   
    while True:
        # Ajout d'un léger décalage
        if "Enrichissement" in name:
            time.sleep(10)
        if "Détection" in name:
            time.sleep(20)
        try:
            logger.info(f"[{name}] Lancement du script...")
            # Execution de la commande
            subprocess.run(cmd_list, check=True)
            logger.info(f"[{name}] Succès. Reprise dans {delay}s.")
        except subprocess.CalledProcessError as e:
            logger.error(f"[{name}] ERREUR (Code {e.returncode}). Réessai dans 60s...")
            time.sleep(60)
            continue
            
        time.sleep(delay)

if __name__ == "__main__":
    # Les scripts de scrapping peuvent être lancés en parallèle.
    # Les commandes de lancement sont stocker dans un tableau et envoyé un gestionnaires de tâches.
    # Ici ProcessPoolExecutor

    tasks = [
        ("Récupération des articles", ["python", "-m", "src.data.scraping.index_articles", "--nb_page", str(SCRAPER_INDEX_LIMIT)], SCRAPER_INDEX_DELAY),
        ("Enrichissement des articles", ["python", "-m", "src.data.scraping.enrich_articles", "--limit", str(SCRAPER_ENRICH_LIMIT)], SCRAPER_ENRICH_DELAY),
        ("Détection des symboles", ["python", "-m", "src.data.scraping.detect_symbols", "--limit", str(SCRAPER_DETECT_LIMIT)], SCRAPER_DETECT_DELAY),
    ]

    logger.info("--- Démarrage du scrapping, de l'enrichissement, et de la détection des symbols au sein des articles ---")

    with ProcessPoolExecutor(max_workers=len(tasks)) as executor:
        executor.map(worker, tasks)