import time
import argparse
import pandas as pd
import pandas_ta as ta

from src.common.connectors import PostgreSQLConnector
from src.common.custom_logger import logger


def get_symbol_id(pg_conn, symbol_name):
    # On cherche l'id du symbol en base vu qu'on en a besoin pour toutes les requêtes
    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT id FROM symbols WHERE symbol = %s", (symbol_name,))
            row = cur.fetchone()
            return row[0] if row else None
    except Exception as e:
        logger.error(f"Erreur lors de la récupération de l'id du symbole {symbol_name}: {e}")
        return None


def get_candles(pg_conn, id_symbol, interval, limit=None):
    # On récupère toutes les candles du symbol depuis PostgreSQL
    # on les met dans un DataFrame pour pouvoir calculer les indicateurs
    try:
        with pg_conn.cursor() as cur:
            if limit:
                # En mode test on ne prend que les candles pas encore dans features_candles
                # pour avoir de nouvelles données à chaque itération du while
                cur.execute("""
                    SELECT c.id, c.open_time, c.open, c.high, c.low, c.close, c.volume
                    FROM candles c
                    WHERE c.id_symbol = %s AND c.interval = %s
                      AND NOT EXISTS (
                          SELECT 1 FROM features_candles fc
                          WHERE fc.id_candle = c.id
                            AND fc.id_symbol = c.id_symbol
                            AND fc.interval  = c.interval
                      )
                    ORDER BY c.open_time ASC
                    LIMIT %s;
                """, (id_symbol, interval, limit))
            else:
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

        # la librairie ta a besoin de floats pour calculer les indicateurs, alors on convertit les colonnes concernées 
        df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
        logger.info(f"{len(df)} candles chargées depuis PostgreSQL.")
        return df

    except Exception as e:
        logger.error(f"Erreur lors du chargement des candles: {e}")
        return pd.DataFrame()


def compute_indicators(df):
    # RSI sur 14 périodes — mesure si le marché est suracheté ou survendu
    df['rsi_14'] = ta.rsi(close=df['close'], length=14)

    # MACD — mesure la convergence/divergence de deux moyennes mobiles
    df_macd = ta.macd(close=df['close'], fast=12, slow=26, signal=9)
    df['macd']        = df_macd['MACD_12_26_9']   # La ligne MACD
    df['macd_signal'] = df_macd['MACDs_12_26_9']  # La ligne Signal

    # EMA 20, 50, 100 — moyennes mobiles exponentielles sur différentes périodes
    # Plus la période est longue plus la tendance est lissée
    df['ema_20']  = ta.ema(close=df['close'], length=20)
    df['ema_50']  = ta.ema(close=df['close'], length=50)
    df['ema_100'] = ta.ema(close=df['close'], length=100)    

    logger.info("Indicateurs calculés : RSI(14), MACD, EMA(20/50/100).")
    return df


def load_features(pg_conn, df, id_symbol, interval):
    # On ignore les premières lignes où les indicateurs sont NaN
    # EMA(100) a besoin de 100 candles avant de pouvoir calculer quelque chose
    df_valid = df.dropna(subset=['rsi_14', 'macd', 'macd_signal', 'ema_20', 'ema_50', 'ema_100'])

    if df_valid.empty:
        logger.info("Pas assez de données pour calculer les indicateurs (trop peu de candles).")
        return 0

    try:
        # Construction du batch en une seule passe — un seul aller-retour réseau
        records = [
            (
                id_symbol, int(row['id_candle']), interval, row['open_time'],
                float(row['rsi_14']), float(row['macd']), float(row['macd_signal']),
                float(row['ema_20']), float(row['ema_50']), float(row['ema_100']),
            )
            for _, row in df_valid.iterrows()
        ]

        with pg_conn.cursor() as cur:
            # Upsert batch : ON CONFLICT s'appuie sur la contrainte UNIQUE(id_symbol, id_candle, interval)
            cur.executemany("""
                INSERT INTO features_candles (
                    id_symbol, id_candle, interval, timestamp_candle,
                    rsi_14, macd, macd_signal, ema_20, ema_50, ema_100
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id_symbol, id_candle, interval)
                DO UPDATE SET
                    rsi_14       = EXCLUDED.rsi_14,
                    macd         = EXCLUDED.macd,
                    macd_signal  = EXCLUDED.macd_signal,
                    ema_20       = EXCLUDED.ema_20,
                    ema_50       = EXCLUDED.ema_50,
                    ema_100      = EXCLUDED.ema_100;
            """, records)

        pg_conn.commit()
        logger.info(f"{len(records)} lignes insérées/mises à jour dans features_candles (batch upsert).")
        return len(records)

    except Exception as e:
        logger.error(f"Erreur lors de l'insertion dans features_candles: {e}")
        pg_conn.rollback()
        return 0


def compute_and_load_features(symbol, interval, limit=None):
    pg_connector = PostgreSQLConnector().connect()
    pg_conn = pg_connector.conn

    try:
        id_symbol = get_symbol_id(pg_conn, symbol)
        if id_symbol is None:
            logger.error(f"Symbol '{symbol}' introuvable dans la table symbols.")
            return

        df = get_candles(pg_conn, id_symbol, interval, limit=limit)
        if df.empty:
            return

        df = compute_indicators(df)
        load_features(pg_conn, df, id_symbol, interval)

    except Exception as e:
        logger.error(f"Erreur inattendue: {e}")
    finally:
        pg_connector.close()


if __name__ == "__main__":
    # Exemple : python -m src.features.compute_features --symbol BTCUSDT --interval 1m
    parser = argparse.ArgumentParser(description="Calcul des indicateurs techniques depuis les candles PostgreSQL.")
    parser.add_argument("--symbol",   type=str, default="BTCUSDT", help="Symbol à traiter (ex: BTCUSDT, ETHUSDT)")
    parser.add_argument("--interval", type=str, default="1m",      help="Intervalle des candles (ex: 1m, 5m, 1h)")
    parser.add_argument("--limit",    type=int, default=None,       help="Limite le nombre de candles chargées (ex: 500 pour tester)")
    parser.add_argument("--loop",     action="store_true",          help="Exécuter en boucle toutes les 60 secondes (mode production)")
    args = parser.parse_args()

    if args.loop:
        delay_seconds = 60
        logger.info(f"Démarrage du calcul des indicateurs techniques pour {args.symbol} ({args.interval})...")
        logger.info(f"Exécution toutes les {delay_seconds} secondes.")
        try:
            while True:
                logger.info("Début du calcul des features...")
                start_time = time.time()
                compute_and_load_features(symbol=args.symbol, interval=args.interval, limit=args.limit)
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
    else:
        logger.info(f"Calcul des indicateurs techniques pour {args.symbol} ({args.interval})...")
        start_time = time.time()
        compute_and_load_features(symbol=args.symbol, interval=args.interval, limit=args.limit)
        duration = round(time.time() - start_time, 2)
        logger.info(f"Calcul terminé en {duration} secondes.")