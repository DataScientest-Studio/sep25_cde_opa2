from src.config import API_HOST, API_PORT

def get_api_base_url():
    """Configuration de l'URL de base de l'API"""
    # Utilise API_HOST qui s'adapte automatiquement selon l'environnement
    # localhost pour le développement local, nom du service Docker en production
    return f"http://{API_HOST}:{API_PORT}"