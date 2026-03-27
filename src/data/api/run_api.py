import uvicorn


# Import de la configuration après l'ajout au PYTHONPATH
from src.config import API_PORT, ENV

if __name__ == "__main__":
    # Configuration du serveur
    # Utiliser 0.0.0.0 pour Docker ou localhost pour développement local
    host = "0.0.0.0" if ENV == 'docker' else "localhost"
    port = int(API_PORT)  # Conversion en entier
    
    print(f"Démarrage de l'API sur http://{host}:{port}")
    print(f"Documentation disponible sur http://localhost:{port}/docs")
    print(f"Alternative: http://localhost:{port}/redoc")
    
    # Lancement du serveur
    uvicorn.run(
        "src.data.api.fastapi_data:app",
        host=host,
        port=port,
        reload=False,
        log_level="info"
    )