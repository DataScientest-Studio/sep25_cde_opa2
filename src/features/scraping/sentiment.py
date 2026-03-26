# Connect to mongo db and save datas
import re
import sys
from pymongo.cursor import Cursor

from textblob import TextBlob
from transformers import pipeline

from src.common.get_symbols_and_names import get_cryptos_symbols_and_names
from src.features.scraping.mongo_client import MongoClient
from src.common.custom_logger import logger

sentiment_pipe = pipeline("sentiment-analysis", model="ProsusAI/finbert")
emotion_pipe = pipeline("text-classification", model="j-hartmann/emotion-english-distilroberta-base", top_k=None)


def analyse_articles_sentiment(articles: Cursor):
    # Recupération du mapping symbol -> names et aliases
    symbols_and_names=get_cryptos_symbols_and_names()
    # Convertion en dictionnaire
    cryptos={s['symbol']: s for s in symbols_and_names}

    for article in articles:
        full_content=f"{article['title']}\n{article['text_content']}"
        article_blob=TextBlob(full_content)

        article_texts_by_symbol=[]

        for symbol in article.get('symbols', []):
            crypto=cryptos.get(symbol)
            if not crypto:
                continue

            regex_pattern = rf"\b({'|'.join(map(re.escape, crypto['aliases']))})\b"
            crypto_sentences = [
                str(s) for s in article_blob.sentences 
                if re.search(regex_pattern, str(s), flags=re.IGNORECASE)
            ]

            if crypto_sentences:
                context_text = " ".join(crypto_sentences)
                article_texts_by_symbol.append({"symbol": symbol, "text": context_text})

        if not article_texts_by_symbol:
            continue 


        # Creation d'un tableau de textes
        texts_to_analyze = [c['text'] for c in article_texts_by_symbol]
        
        # Calcul des sentiments et emotions pour chaque textes
        all_sentiments = sentiment_pipe(texts_to_analyze, truncation=True)
        all_emotions = emotion_pipe(texts_to_analyze, truncation=True)

        # Boucle sur les textes classés par symbol afin former les résultats
        article_analyse={}
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
        
        article_analyse['polarity']=article_blob.sentiment.polarity
        article_analyse['subjectivity']=article_blob.sentiment.subjectivity
        article_analyse['symbols']=symbol_results
           
        print(article_analyse)

        # @TODO
        # Set flag articles with calculated features

def main():
    try:
        mongodb_client=MongoClient().connect()
        collection_name='investing_articles_enriched'

        articles=mongodb_client.get_articles(collection_name)
        
        if not articles:
             logger.warning('Aucun article trouvé.')
        else:
            analyse_articles_sentiment(articles)

    except Exception as e:
            logger.error(f"Erreur critique dans le main : {e}")
            sys.exit(1)
    finally:
        # Close connexions
        if mongodb_client: 
            mongodb_client.close()

if __name__ == "__main__":
    main()