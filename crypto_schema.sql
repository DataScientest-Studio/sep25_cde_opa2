-- SCRIPT DB CRYPTO 
-- =====================================================

-- ===============================
-- SYMBOLS
-- ===============================

CREATE TABLE IF NOT EXISTS symbols (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL UNIQUE,
    base_asset VARCHAR(20),
    quote_asset VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ===============================
-- CANDLES
-- ===============================

CREATE TABLE IF NOT EXISTS candles (
    id SERIAL PRIMARY KEY,
    id_symbol INT NOT NULL,
    interval VARCHAR(10) NOT NULL,
    open_time TIMESTAMP NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume NUMERIC,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (id_symbol) REFERENCES symbols(id)
);

-- ===============================
-- FEATURES_CANDLES
-- ===============================

CREATE TABLE IF NOT EXISTS features_candles (
    id SERIAL PRIMARY KEY,
    id_symbol INT NOT NULL,
    id_candle INT,
    timestamp_candle TIMESTAMP,
    rsi_14 NUMERIC,
    macd NUMERIC,
    ema_20 NUMERIC,
    ema_50 NUMERIC,
    ema_100 NUMERIC,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (id_symbol) REFERENCES symbols(id),
    FOREIGN KEY (id_candle) REFERENCES candles(id)
);

-- ===============================
-- FEATURES_ORDERBOOK
-- ===============================

CREATE TABLE IF NOT EXISTS features_orderbook (
    id SERIAL PRIMARY KEY,
    id_symbol INT NOT NULL,
    snapshot_time TIMESTAMP NOT NULL,
    best_bid_price NUMERIC,
    best_ask_price NUMERIC,
    spread NUMERIC,
    mid_price NUMERIC,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (id_symbol) REFERENCES symbols(id)
);

-- ===============================
-- FEATURES_TICKER_24
-- ===============================

CREATE TABLE IF NOT EXISTS features_ticker_24 (
    id SERIAL PRIMARY KEY,
    id_symbol INT NOT NULL,
    snapshot_time TIMESTAMP NOT NULL,
    last_price NUMERIC,
    high_price NUMERIC,
    low_price NUMERIC,
    volume_24h NUMERIC,
    price_change_24h NUMERIC,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (id_symbol) REFERENCES symbols(id)
);

-- ===============================
-- DATASET_XXX
-- ===============================

CREATE TABLE IF NOT EXISTS dataset_XXX (
    id SERIAL PRIMARY KEY,
    id_symbol INT NOT NULL,
    timestamp_candle TIMESTAMP,
    rsi_14 NUMERIC,
    macd NUMERIC,
    ema_20 NUMERIC,
    sentiment_score FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (id_symbol) REFERENCES symbols(id)
);

-- ===============================
-- LABELS
-- ===============================

CREATE TABLE IF NOT EXISTS labels (
    id SERIAL PRIMARY KEY,
    id_symbol INT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    label_up_down INT,
    label_return NUMERIC,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (id_symbol) REFERENCES symbols(id)
);

-- ===============================
-- PREDICTIONS
-- ===============================

CREATE TABLE IF NOT EXISTS predictions (
    id SERIAL PRIMARY KEY,
    id_symbol INT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    predicted_value NUMERIC,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (id_symbol) REFERENCES symbols(id)
);

-- ===============================
-- NEWS_SENTIMENTS
-- ===============================

CREATE TABLE IF NOT EXISTS news_sentiments (
    id SERIAL PRIMARY KEY,
    id_symbol INT NOT NULL,
    article_time TIMESTAMP,
    sentiment_score FLOAT,
    sentiment_label VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (id_symbol) REFERENCES symbols(id)
);

-- ===============================
-- NEWS_TEMPORAL
-- ===============================

CREATE TABLE IF NOT EXISTS news_temporal (
    id SERIAL PRIMARY KEY,
    id_symbol INT NOT NULL,
    published TIMESTAMP,
    news_density_1h INT,
    news_density_24h INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (id_symbol) REFERENCES symbols(id)
);

-- ===============================
-- NEWS_MARKET
-- ===============================

CREATE TABLE IF NOT EXISTS news_market (
    id SERIAL PRIMARY KEY,
    id_symbol INT NOT NULL,
    published TIMESTAMP,
    return_1h FLOAT,
    volatility_1h FLOAT,
    trend_1h VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (id_symbol) REFERENCES symbols(id)
);

-- =====================================================
-- END
-- =====================================================

\echo 'Database created successfully!'

