# Connect to mongo db and save datas
import re
import sys
from pymongo.cursor import Cursor

from textblob import TextBlob
from transformers import pipeline

from src.common.get_symbols_and_names import get_cryptos_symbols_and_names
from src.features.scraping.mongo_client import MongoClient
from src.features.scraping.pg_client import PGClient
from src.common.custom_logger import logger

sentiment_pipe = None
emotion_pipe = None

def get_article_texts_by_symbol(blob: TextBlob, symbols: list, cryptos: dict):
        article_texts_by_symbol=[]

        for symbol in symbols:
            crypto=cryptos.get(symbol)
            if not crypto:
                continue

            regex_pattern = rf"\b({'|'.join(map(re.escape, crypto['aliases']))})\b"
            crypto_sentences = [
                str(s) for s in blob.sentences 
                if re.search(regex_pattern, str(s), flags=re.IGNORECASE)
            ]

            if crypto_sentences:
                context_text = " ".join(crypto_sentences)
                article_texts_by_symbol.append({"symbol": symbol, "text": context_text})   

        return article_texts_by_symbol

def get_article_analyses_by_symbol(article_texts_by_symbol: list):
    # Creation d'un tableau de textes
    texts_to_analyze = [c['text'] for c in article_texts_by_symbol]
    
    # Calcul des sentiments et emotions pour chaque textes
    all_sentiments = sentiment_pipe(texts_to_analyze, truncation=True)
    all_emotions = emotion_pipe(texts_to_analyze, truncation=True)

    # Boucle sur les textes classés par symbol afin former les résultats
    symbol_results = []
    for i, text in enumerate(article_texts_by_symbol):
        # Récupération de l'émotion dominante
        emotions_list = all_emotions[i]
        top_emotion = max(emotions_list, key=lambda x: x['score'])
        
        symbol_results.append({
            "symbol": text['symbol'],
            "sentiment": all_sentiments[i]['label'],
            "confidence": all_sentiments[i]['score'],
            "emotion": top_emotion['label'],
            "intensity": top_emotion['score']
        })     

    return symbol_results

def analyse_articles_sentiment(articles: Cursor, pg_client: PGClient, mongodb_client: MongoClient, collection_name: str):
    # Recupération du mapping symbol -> names et aliases
    symbols_and_names=get_cryptos_symbols_and_names()
    # Convertion en dictionnaire
    cryptos={s['symbol']: s for s in symbols_and_names}

    nb_succeed = 0
    nb_failed = 0

    for article in articles:
        try:
            full_content=f"{article['title']}\n{article['text_content']}"
            article_blob=TextBlob(full_content)

            article_texts_by_symbol=get_article_texts_by_symbol(
                blob=article_blob,
                symbols=article.get('symbols', []),
                cryptos=cryptos
                )
            
            if not article_texts_by_symbol:
                nb_failed+=1
                # Pose d'un flag pour ne pas recalculer les sentiments de l'article lors d'une prochaine execution du script.
                # 2 = article traité mais aucune crypto trouvée pour le calcul des sentiments
                mongodb_client.common.flag_articles(ids=[article['_id']], flag="feature_sentiment_analyzed", value=2, collection_name=collection_name)
                continue 

            article_analyse={}
            symbol_results=get_article_analyses_by_symbol(article_texts_by_symbol)

            article_analyse['article_id']=article['_id']
            article_analyse['published_at_timestamp']=article['published_at_timestamp']
            article_analyse['polarity']=article_blob.sentiment.polarity
            article_analyse['subjectivity']=article_blob.sentiment.subjectivity
            article_analyse['symbols']=symbol_results
            
            # Sauvegarde de l'analyse
            if pg_client.insert_sentiment_analyse(analyse=article_analyse):
                nb_succeed+=1
                # Pose d'un flag pour ne pas recalculer les sentiments de l'article lors d'une prochaine execution du script.
                # 1 = article traité et cryptos trouvées pour le calcul des sentiments
                mongodb_client.common.flag_articles(ids=[article['_id']], flag="feature_sentiment_analyzed", value=1, collection_name=collection_name)
                logger.info(f"Article {article['_id']} traité et flaggé.")
            else:
                nb_failed+=1

        except Exception as e:
            logger.error(f"Erreur sur l'article {article['_id']}: {e}")
            continue
    
    return {
            'nb_succeed': nb_succeed,
            'nb_failed': nb_failed
        }


def main():
    mongodb_client = None
    pg_client = None
    global sentiment_pipe, emotion_pipe

    try:
        mongodb_client=MongoClient()
        pg_client=PGClient()

        if sentiment_pipe is None:
            sentiment_pipe = pipeline("sentiment-analysis", model="ProsusAI/finbert")

        if emotion_pipe is None:
            emotion_pipe = pipeline("text-classification", model="j-hartmann/emotion-english-distilroberta-base", top_k=None)

        collection_name='investing_articles_enriched'

        mongodb_client.db[collection_name].create_index([("feature_sentiment_analyzed", 1)])        

        articles=mongodb_client.get_articles(collection_name)
        
        if not articles:
             logger.warning('Aucun article trouvé.')
        else:
            result=analyse_articles_sentiment(articles, pg_client, mongodb_client, collection_name)
            logger.info(f"Analyse de sentiments terminée, {result['nb_succeed']} articles avec sentiments traités, {result['nb_failed']} articles sans résultat.")

    except Exception as e:
            logger.error(f"Erreur critique dans le main : {e}")
            sys.exit(1)
    finally:
        # Close connexions
        if mongodb_client is not None: 
            mongodb_client.close()
        if pg_client is not None: 
            pg_client.close()

if __name__ == "__main__":
    main()