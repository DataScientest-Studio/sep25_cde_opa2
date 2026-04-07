from fastapi import APIRouter, FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

import pandas as pd
from datetime import datetime
from typing import Optional, List

from src.common.connectors import PostgreSQLConnector
from src.common.custom_logger import logger

router = APIRouter(prefix="/scraping", tags=["scraping"])

@router.get("/")
async def root():
    """Route d'accueil de l'API scraping"""
    return {
        "message": "Service de récupération des données liées au scraping",
        "endpoints": [
            "/sraping/sentiment",
        ]
    }