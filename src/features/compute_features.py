import time
import argparse
import pandas as pd
import ta

from src.common.connectors import PostgreSQLConnector
from src.common.custom_logger import logger


def get_symbol_id(pg_conn, symbol_name):
    # On cherche l'id du symbol en base, on en a besoin pour toutes les requêtes
    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT id FROM symbols WHERE symbol = %s", (symbol_name,))
            row = cur.fetchone()
            return row[0] if row else None
    except Exception as e:
        logger.error(f"Erreur lors de la récupération de l'id du symbole {symbol_name}: {e}")
        return None


def get_candles(pg_conn, id_symbol, interval):
    # On récupère toutes les candles du symbol depuis PostgreSQL
    # et on les met dans un DataFrame pour pouvoir calculer les indicateurs
    try:
        with pg_conn.cursor() as cur:
            cur.execute("""
                SELECT id, open_time, open, high, low, close, volume
                FROM candles
                WHERE id_symbol = %s AND interval = %s
                ORDER BY open_time ASC;
            """, (id_symbol, interval))
            rows = cur.fetchall()

        if not rows:
            logger.info("Aucune candle trouvée.")
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=['id_candle', 'open_time', 'open', 'high', 'low', 'close', 'volume'])

        # la librairie ta a besoin de floats, pas de Decimal
        df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
        logger.info(f"{len(df)} candles chargées depuis PostgreSQL.")
        return df

    except Exception as e:
        logger.error(f"Erreur lors du chargement des candles: {e}")
        return pd.DataFrame()


def compute_indicators(df):
    # RSI sur 14 périodes — mesure si le marché est suracheté ou survendu
    df['rsi_14'] = ta.momentum.RSIIndicator(close=df['close'], window=14).rsi()

    # MACD — mesure la convergence/divergence de deux moyennes mobiles
    df['macd'] = ta.trend.MACD(close=df['close']).macd()

    # EMA 20, 50, 100 — moyennes mobiles exponentielles sur différentes périodes
    # Plus la période est longue, plus la tendance est lissée
    df['ema_20']  = ta.trend.EMAIndicator(close=df['close'], window=20).ema_indicator()
    df['ema_50']  = ta.trend.EMAIndicator(close=df['close'], window=50).ema_indicator()
    df['ema_100'] = ta.trend.EMAIndicator(close=df['close'], window=100).ema_indicator()

    logger.info("Indicateurs calculés : RSI(14), MACD, EMA(20/50/100).")
    return df


def load_features(pg_conn, df, id_symbol):
    # On ignore les premières lignes où les indicateurs sont NaN
    # C'est normal : EMA(100) a besoin de 100 candles avant de pouvoir calculer quelque chose
    df_valid = df.dropna(subset=['rsi_14', 'macd', 'ema_20', 'ema_50', 'ema_100'])

    if df_valid.empty:
        logger.info("Pas assez de données pour calculer les indicateurs (trop peu de candles).")
        return 0

    inserted = 0
    try:
        with pg_conn.cursor() as cur:
            for _, row in df_valid.iterrows():

                # features_candles n'a pas de contrainte UNIQUE, donc on vérifie
                # manuellement avant d'insérer pour ne pas créer de doublons
                cur.execute("""
                    SELECT id FROM features_candles
                    WHERE id_symbol = %s AND id_candle = %s;
                """, (id_symbol, int(row['id_candle'])))

                existing = cur.fetchone()

                if existing:
                    # La ligne existe déjà, on met juste à jour les valeurs
                    cur.execute("""
                        UPDATE features_candles
                        SET rsi_14 = %s, macd = %s, ema_20 = %s, ema_50 = %s, ema_100 = %s
                        WHERE id_symbol = %s AND id_candle = %s;
                    """, (
                        float(row['rsi_14']), float(row['macd']),
                        float(row['ema_20']), float(row['ema_50']), float(row['ema_100']),
                        id_symbol, int(row['id_candle'])
                    ))
                else:
                    # Nouvelle candle, on insère
                    cur.execute("""
                        INSERT INTO features_candles (
                            id_symbol, id_candle, timestamp_candle,
                            rsi_14, macd, ema_20, ema_50, ema_100
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                    """, (
                        id_symbol, int(row['id_candle']), row['open_time'],
                        float(row['rsi_14']), float(row['macd']),
                        float(row['ema_20']), float(row['ema_50']), float(row['ema_100'])
                    ))
                    inserted += 1

        pg_conn.commit()
        logger.info(f"{inserted} nouvelles lignes insérées dans features_candles.")
        return inserted

    except Exception as e:
        logger.error(f"Erreur lors de l'insertion dans features_candles: {e}")
        pg_conn.rollback()
        return 0


def compute_and_load_features(symbol, interval):
    pg_connector = PostgreSQLConnector().connect()
    pg_conn = pg_connector.conn

    try:
        id_symbol = get_symbol_id(pg_conn, symbol)
        if id_symbol is None:
            logger.error(f"Symbol '{symbol}' introuvable dans la table symbols.")
            return

        df = get_candles(pg_conn, id_symbol, interval)
        if df.empty:
            return

        df = compute_indicators(df)
        load_features(pg_conn, df, id_symbol)

    except Exception as e:
        logger.error(f"Erreur inattendue: {e}")
    finally:
        pg_connector.close()


if __name__ == "__main__":
    # Récupération des arguments passés en ligne de commande
    # Exemple : python -m src.features.compute_features --symbol BTCUSDT --interval 1m
    parser = argparse.ArgumentParser(description="Calcul des indicateurs techniques depuis les candles PostgreSQL.")
    parser.add_argument("--symbol",   type=str, default="BTCUSDT", help="Symbol à traiter (ex: BTCUSDT, ETHUSDT)")
    parser.add_argument("--interval", type=str, default="1m",      help="Intervalle des candles (ex: 1m, 5m, 1h)")
    args = parser.parse_args()

    delay_seconds = 60
    logger.info(f"Démarrage du calcul des indicateurs techniques pour {args.symbol} ({args.interval})...")
    logger.info(f"Exécution toutes les {delay_seconds} secondes.")

    try:
        while True:
            logger.info("Début du calcul des features...")
            start_time = time.time()

            compute_and_load_features(symbol=args.symbol, interval=args.interval)

            duration = round(time.time() - start_time, 2)
            logger.info(f"Calcul terminé en {duration} secondes.")
            logger.info(f"Attente de {delay_seconds} secondes...")
            time.sleep(delay_seconds)

    except KeyboardInterrupt:
        logger.info("Arrêt demandé par l'utilisateur.")
    except Exception as e:
        logger.error(f"Erreur inattendue dans la boucle principale: {e}")
    finally:
        logger.info("Processus de calcul des features arrêté.")