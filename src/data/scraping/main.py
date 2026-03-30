from concurrent.futures import ProcessPoolExecutor
import signal
import subprocess
import sys

from src.common.custom_logger import logger

executor = None

def stop_workers(signum, frame):
    """Interception du signal d'arrêt de docker"""
    # global executor
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
    """

    name, cmd_list = task_info
   
    try:
        logger.info(f"[{name}] Lancement du script...")
        # Execution de la commande
        subprocess.run(cmd_list, check=True)
        logger.info(f"[{name}] Succès.")
    except subprocess.CalledProcessError as e:
        logger.error(f"[{name}] ERREUR (Code {e.returncode}).")

if __name__ == "__main__":
    # Les scripts de scrapping sont lancés en parallèle une première fois.
    # Les commandes de lancement sont stockées dans un tableau et envoyé à un gestionnaires de tâches.
    # Ici ProcessPoolExecutor

    tasks = [
        ("Récupération des articles", ["python", "-m", "src.data.scraping.index_articles"]),
        ("Enrichissement des articles", ["python", "-m", "src.data.scraping.enrich_articles"]),
        ("Détection des symboles", ["python", "-m", "src.data.scraping.detect_symbols"]),
    ]

    logger.info("--- Démarrage du scrapping, de l'enrichissement, et de la détection des symbols au sein des articles ---")

    try:
        executor = ProcessPoolExecutor(max_workers=len(tasks))
        with executor:
            executor.map(worker, tasks)
    except KeyboardInterrupt:
        logger.info("Arrêt manuel du scraping.")
        sys.exit(0)