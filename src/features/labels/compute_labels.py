import time
import argparse
import pandas as pd

from src.common.connectors import PostgreSQLConnector
from src.common.custom_logger import logger


def get_symbol_id(pg_conn, symbol_name):
    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT id FROM symbols WHERE symbol = %s", (symbol_name,))
            row = cur.fetchone()
            return row[0] if row else None
    except Exception as e:
        logger.error(f"Erreur lors de la récupération de l'id du symbole {symbol_name}: {e}")
        return None


def get_candles(pg_conn, id_symbol, interval, horizon, threshold):
    # Récupère uniquement les candles postérieures au dernier label calculé
    # pour cet (id_symbol, interval, horizon, threshold).
    # Si aucun label n'existe encore, toutes les candles sont chargées.
    try:
        with pg_conn.cursor() as cur:
            cur.execute("""
                SELECT id, open_time, close
                FROM candles
                WHERE id_symbol = %s AND interval = %s
                  AND open_time > (
                      SELECT COALESCE(MAX(timestamp), '-infinity'::timestamp)
                      FROM labels
                      WHERE id_symbol = %s AND interval = %s
                        AND horizon = %s AND threshold = %s
                  )
                ORDER BY open_time ASC;
            """, (id_symbol, interval, id_symbol, interval, horizon, float(threshold)))
            rows = cur.fetchall()

        if not rows:
            logger.info("Aucune candle trouvée.")
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=['id_candle', 'open_time', 'close'])
        df['close'] = df['close'].astype(float)
        logger.info(f"{len(df)} candles chargées depuis PostgreSQL.")
        return df

    except Exception as e:
        logger.error(f"Erreur lors du chargement des candles: {e}")
        return pd.DataFrame()


def compute_labels(df, horizon, threshold):
    # Calcul du rendement futur à horizon fixe : r(t) = (close(t+N) - close(t)) / close(t)
    # Les N dernières lignes n'ont pas de future close → leur label restera NaN et sera ignoré
    df['label_return'] = (df['close'].shift(-horizon) - df['close']) / df['close']

    # Discrétisation en 3 classes : +1 (BUY), -1 (SELL), 0 (HOLD)
    df['label_up_down'] = 0
    df.loc[df['label_return'] >  threshold, 'label_up_down'] =  1
    df.loc[df['label_return'] < -threshold, 'label_up_down'] = -1

    # Suppression des N dernières lignes sans label
    df_valid = df.dropna(subset=['label_return']).copy()

    # Distribution des classes
    counts = df_valid['label_up_down'].value_counts().sort_index()
    total = len(df_valid)
    if(total > 0) :
        logger.info(
            f"Labels calculés (horizon={horizon}, seuil={threshold*100:.1f}%) — "
            f"SELL(-1): {counts.get(-1, 0)} ({counts.get(-1, 0)/total*100:.1f}%), "
            f"HOLD(0): {counts.get(0, 0)} ({counts.get(0, 0)/total*100:.1f}%), "
            f"BUY(+1): {counts.get(1, 0)} ({counts.get(1, 0)/total*100:.1f}%)"
        )
    else :
        logger.info(f"Aucun label à calculer.")

    return df_valid


def load_labels(pg_conn, df, id_symbol, interval, horizon, threshold):
    try:
        # Construction du batch en une seule passe sur le DataFrame
        records = [
            (
                id_symbol,
                interval,
                horizon,
                float(threshold),
                row['open_time'],
                int(row['label_up_down']),
                float(row['label_return']),
            )
            for _, row in df.iterrows()
        ]

        with pg_conn.cursor() as cur:
            # Upsert en batch : un seul aller-retour réseau pour toutes les lignes
            # ON CONFLICT s'appuie sur la contrainte UNIQUE(id_symbol, interval, horizon, threshold, timestamp)
            cur.executemany("""
                INSERT INTO labels (id_symbol, interval, horizon, threshold, timestamp, label_up_down, label_return)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id_symbol, interval, horizon, threshold, timestamp)
                DO UPDATE SET
                    label_up_down = EXCLUDED.label_up_down,
                    label_return  = EXCLUDED.label_return;
            """, records)

        pg_conn.commit()
        logger.info(f"{len(records)} labels insérés/mis à jour dans la table labels (batch upsert).")
        return len(records)

    except Exception as e:
        logger.error(f"Erreur lors de l'insertion dans labels: {e}")
        pg_conn.rollback()
        return 0


def compute_and_load_labels(symbol, interval, horizon, threshold):
    pg_connector = PostgreSQLConnector().connect()
    pg_conn = pg_connector.conn

    try:
        id_symbol = get_symbol_id(pg_conn, symbol)
        if id_symbol is None:
            logger.error(f"Symbol '{symbol}' introuvable dans la table symbols.")
            return

        df = get_candles(pg_conn, id_symbol, interval, horizon, threshold)
        if df.empty:
            logger.info("Aucune nouvelle candle à labelliser.")
            return

        df = compute_labels(df, horizon=horizon, threshold=threshold)
        load_labels(pg_conn, df, id_symbol, interval, horizon, threshold)

    except Exception as e:
        logger.error(f"Erreur inattendue: {e}")
    finally:
        pg_connector.close()


if __name__ == "__main__":
    # Exemple : python -m src.features.labels.compute_labels --symbol BTCUSDT --interval 1h --horizon 12 --threshold 0.01
    parser = argparse.ArgumentParser(description="Calcul des étiquettes (labels) pour l'apprentissage supervisé.")
    parser.add_argument("--symbol",    type=str,   default="BTCUSDT", help="Symbol à traiter (ex: BTCUSDT, ETHUSDT)")
    parser.add_argument("--interval",  type=str,   default="1h",      help="Intervalle de référence des candles (ex: 1h, 1d)")
    parser.add_argument("--horizon",   type=int,   default=12,        help="Nombre de candles en avant pour calculer le rendement (défaut: 12)")
    parser.add_argument("--threshold", type=float, default=0.01,      help="Seuil de rendement pour BUY/SELL (défaut: 0.01 = 1%%)")
    parser.add_argument("--loop",      action="store_true",           help="Exécuter en boucle toutes les 60 secondes (mode production)")
    args = parser.parse_args()

    if args.loop:
        delay_seconds = 60
        logger.info(f"Démarrage du calcul des labels pour {args.symbol} ({args.interval}), horizon={args.horizon}, seuil={args.threshold*100:.1f}%...")
        logger.info(f"Exécution toutes les {delay_seconds} secondes.")
        try:
            while True:
                logger.info("Début du calcul des labels...")
                start_time = time.time()
                compute_and_load_labels(
                    symbol=args.symbol,
                    interval=args.interval,
                    horizon=args.horizon,
                    threshold=args.threshold
                )
                duration = round(time.time() - start_time, 2)
                logger.info(f"Calcul terminé en {duration} secondes.")
                logger.info(f"Attente de {delay_seconds} secondes...")
                time.sleep(delay_seconds)
        except KeyboardInterrupt:
            logger.info("Arrêt demandé par l'utilisateur.")
        except Exception as e:
            logger.error(f"Erreur inattendue dans la boucle principale: {e}")
        finally:
            logger.info("Processus de calcul des labels arrêté.")
    else:
        logger.info(f"Calcul des labels pour {args.symbol} ({args.interval}), horizon={args.horizon}, seuil={args.threshold*100:.1f}%...")
        start_time = time.time()
        compute_and_load_labels(
            symbol=args.symbol,
            interval=args.interval,
            horizon=args.horizon,
            threshold=args.threshold
        )
        duration = round(time.time() - start_time, 2)
        logger.info(f"Calcul terminé en {duration} secondes.")
