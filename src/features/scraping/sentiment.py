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
     
    symbols_and_names=get_cryptos_symbols_and_names()

    for article in articles:
        full_content=f"{article['title']}\n{article['text_content']}"
        article_blob=TextBlob(full_content)
        article_analyse={}
        symbols=[]
        symbol_results=[]

        for symbol in article['symbols']:
            for san in symbols_and_names:
                if san['symbol'] == symbol:
                    symbols.append(san)
                
        for symbol in symbols:
            sentences_by_symbol=[]
            for alias in symbol['aliases']:
                for sentence in article_blob.sentences:
                    sentence_lower=str(sentence).lower()
                    if re.search(rf"\b{re.escape(alias)}\b", sentence_lower, flags=re.IGNORECASE):
                        sentences_by_symbol.append(str(sentence))
            
            
            text_by_symbol=" ".join(sentences_by_symbol)

            # Sentiment
            sent=sentiment_pipe(text_by_symbol, truncation=True, max_length=512)[0]
            
            # Emotions
            emotions=emotion_pipe(text_by_symbol, truncation=True, max_length=512)[0]

            # On prend l'émotion dominante
            top_emotion = max(emotions, key=lambda x: x['score'])

            symbol_results.append({
                "symbol": symbol['symbol'],
                "sentiment": sent['label'],
                "confidence": sent['score'],
                "emotion": top_emotion['label'],
                "intensity": top_emotion['score']
            })
            
            article_analyse['polarity']=article_blob.sentiment.polarity
            article_analyse['subjectivity']=article_blob.sentiment.subjectivity
            article_analyse['symbols']=symbol_results

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