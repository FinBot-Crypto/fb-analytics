import asyncio
import logging
import json
import ccxt
import psycopg2
import os
from datetime import datetime, timezone, timedelta
import numpy as np
import pandas as pd
import torch
import torch.nn as nn

logger = logging.getLogger("fb-shadow-simulator")

class ShadowSimulator:
    def __init__(self, database_url):
        self.db_url = database_url
        self.exchange = ccxt.binance({
            "enableRateLimit": True,
        })
        self.sl_multipliers = [2, 3, 4, 5, 6, None]
        self.tp_multipliers = [2, 3, 4, 5, 6, 8, 10, None]

    def init_db(self):
        try:
            conn = psycopg2.connect(self.db_url)
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS shadow_metrics (
                    trade_id INT PRIMARY KEY,
                    symbol VARCHAR(20),
                    tier VARCHAR(50),
                    rsi_entry FLOAT,
                    hour_entry INT,
                    simulations JSONB,
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
            # Migração: adicionar colunas caso a tabela já exista sem elas
            for col_def in [
                ("tier", "VARCHAR(50)"),
                ("rsi_entry", "FLOAT"),
                ("hour_entry", "INT"),
            ]:
                try:
                    cur.execute(f"ALTER TABLE shadow_metrics ADD COLUMN IF NOT EXISTS {col_def[0]} {col_def[1]}")
                except Exception:
                    pass
            conn.commit()
            cur.close()
            conn.close()
            logger.info("Tabela shadow_metrics inicializada / migrada.")
        except Exception as e:
            logger.error(f"Erro ao inicializar shadow_metrics: {e}")

    async def fetch_1m_data(self, symbol, start_dt):
        """Busca 720 candles de 1m (12h) a partir do start_dt."""
        since_ms = int(start_dt.timestamp() * 1000)
        try:
            loop = asyncio.get_event_loop()
            ohlcv = await loop.run_in_executor(None, lambda: self.exchange.fetch_ohlcv(symbol, '1m', since=since_ms, limit=720))
            return ohlcv
        except Exception as e:
            logger.error(f"Erro ao buscar OHLCV para {symbol}: {e}")
            return []

    def simulate_trade(self, ohlcv, entry_price, atr):
        results = []
        for sl_m in self.sl_multipliers:
            for tp_m in self.tp_multipliers:
                if sl_m is None and tp_m is None:
                    continue

                sl_price = entry_price - (sl_m * atr) if sl_m else 0
                tp_price = entry_price + (tp_m * atr) if tp_m else float('inf')

                reason = "TIME"
                exit_price = entry_price
                minutes = len(ohlcv)

                for idx, candle in enumerate(ohlcv):
                    high = candle[2]
                    low = candle[3]

                    if sl_price > 0 and low <= sl_price:
                        reason = "SL"
                        exit_price = sl_price
                        minutes = idx + 1
                        break

                    if high >= tp_price:
                        reason = "TP"
                        exit_price = tp_price
                        minutes = idx + 1
                        break

                if reason == "TIME" and len(ohlcv) > 0:
                    exit_price = ohlcv[-1][4]

                pnl_pct = (exit_price / entry_price - 1) * 100

                results.append({
                    "sl": sl_m,
                    "tp": tp_m,
                    "pnl": round(pnl_pct, 4),
                    "reason": reason,
                    "minutes": minutes
                })

        # Cenário time-exit puro (sem SL, sem TP)
        if len(ohlcv) > 0:
            time_exit_price = ohlcv[-1][4]
            results.append({
                "sl": None,
                "tp": None,
                "pnl": round((time_exit_price / entry_price - 1) * 100, 4),
                "reason": "TIME",
                "minutes": len(ohlcv)
            })

        return results

    async def _save_empty_simulation(self, trade_id, symbol, tier, rsi_entry, hour_entry):
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, lambda: self._db_save_empty(trade_id, symbol, tier, rsi_entry, hour_entry))
        except Exception as e:
            logger.error(f"Erro ao salvar simulacao vazia para #{trade_id}: {e}")

    def _db_save_empty(self, trade_id, symbol, tier, rsi_entry, hour_entry):
        conn = psycopg2.connect(self.db_url)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO shadow_metrics (trade_id, symbol, tier, rsi_entry, hour_entry, simulations)
            VALUES (%s, %s, %s, %s, %s, '[]'::jsonb)
            ON CONFLICT (trade_id) DO UPDATE SET
                tier = EXCLUDED.tier,
                rsi_entry = EXCLUDED.rsi_entry,
                hour_entry = EXCLUDED.hour_entry,
                simulations = EXCLUDED.simulations
        """, (trade_id, symbol, tier, rsi_entry, hour_entry))
        conn.commit()
        cur.close()
        conn.close()

    async def run_loop(self):
        logger.info("Shadow Simulator iniciado...")
        self.init_db()

        while True:
            try:
                loop = asyncio.get_event_loop()
                trades = await loop.run_in_executor(None, self._db_fetch_pending_trades)

                if not trades:
                    await asyncio.sleep(600)  # Fila vazia, dorme 10 min
                    continue

                for trade in trades:
                    trade_id, symbol, entry_price, sl_price, created_at, tier, rsi_entry = trade
                    hour_entry = created_at.hour if created_at else None

                    if not entry_price or entry_price <= 0:
                        logger.warning(f"Trade #{trade_id} com preco de entrada invalido. Pulando.")
                        await self._save_empty_simulation(trade_id, symbol, tier, rsi_entry, hour_entry)
                        continue

                    # Se sl_price for invalido ou 0, usamos ATR estimado de 1.5% do entry_price
                    if not sl_price or sl_price <= 0:
                        atr = entry_price * 0.015
                    else:
                        atr = (entry_price - sl_price) / 2.0

                    logger.info(f"Simulando trade #{trade_id} ({symbol}) tier={tier} RSI={rsi_entry} hora={hour_entry}...")
                    ohlcv = await self.fetch_1m_data(symbol, created_at)

                    if not ohlcv:
                        logger.warning(f"Sem dados OHLCV para {symbol}. Salvando simulacao vazia para destravar fila.")
                        await self._save_empty_simulation(trade_id, symbol, tier, rsi_entry, hour_entry)
                        continue

                    simulations = self.simulate_trade(ohlcv, entry_price, atr)

                    await loop.run_in_executor(None, lambda: self._db_save_simulations(
                        trade_id, symbol, tier, rsi_entry, hour_entry, simulations
                    ))

                    logger.info(f"Simulação para #{trade_id} finalizada com {len(simulations)} cenários.")
                    await asyncio.sleep(1)

                # Se processou 20 itens, pode ter mais pendentes. Roda em 5s em vez de 10m.
                if len(trades) == 20:
                    logger.info("Leva de 20 concluida. Processando proxima leva em 5s...")
                    await asyncio.sleep(5)
                else:
                    await asyncio.sleep(600)

            except Exception as e:
                logger.error(f"Erro no loop do shadow simulator: {e}")
                await asyncio.sleep(60)

    def _db_fetch_pending_trades(self):
        conn = psycopg2.connect(self.db_url)
        cur = conn.cursor()
        cur.execute("""
            SELECT id, symbol, entry_price, sl_price, created_at, tier, rsi
            FROM trade_log
            WHERE created_at < NOW() - INTERVAL '12 hours'
              AND id NOT IN (SELECT trade_id FROM shadow_metrics)
            ORDER BY created_at ASC
            LIMIT 20
        """)
        trades = cur.fetchall()
        cur.close()
        conn.close()
        return trades

    def _db_save_simulations(self, trade_id, symbol, tier, rsi_entry, hour_entry, simulations):
        conn = psycopg2.connect(self.db_url)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO shadow_metrics (trade_id, symbol, tier, rsi_entry, hour_entry, simulations)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (trade_id) DO UPDATE SET
                tier = EXCLUDED.tier,
                rsi_entry = EXCLUDED.rsi_entry,
                hour_entry = EXCLUDED.hour_entry,
                simulations = EXCLUDED.simulations
        """, (trade_id, symbol, tier, rsi_entry, hour_entry, json.dumps(simulations)))
        conn.commit()
        cur.close()
        conn.close()


def compute_rsi(closes, period=14):
    if len(closes) < period + 1:
        return [None] * len(closes)
    gains = []
    losses = []
    for i in range(1, len(closes)):
        delta = closes[i] - closes[i - 1]
        gains.append(max(delta, 0))
        losses.append(max(-delta, 0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    rsi = [None] * period
    if avg_loss == 0:
        rsi.append(100.0)
    else:
        rsi.append(100 - 100 / (1 + avg_gain / avg_loss))
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0:
            rsi.append(100.0)
        else:
            rsi.append(100 - 100 / (1 + avg_gain / avg_loss))
    return rsi


SYMBOL_TIERS = {
    "BTC": "Major", "ETH": "Major",
    "BNB": "Strong Alt", "SOL": "Strong Alt", "XRP": "Strong Alt",
    "ADA": "Strong Alt", "AVAX": "Strong Alt", "MATIC": "Strong Alt",
    "DOT": "Strong Alt", "LINK": "Strong Alt", "UNI": "Strong Alt",
    "LTC": "Strong Alt", "ATOM": "Strong Alt", "VET": "Strong Alt",
    "ALGO": "Strong Alt", "NEAR": "Strong Alt", "XLM": "Strong Alt",
    "ZEC": "Strong Alt", "FIL": "Strong Alt", "EOS": "Strong Alt",
    "APT": "Strong Alt", "OP": "Strong Alt", "ARB": "Strong Alt",
    "INJ": "Strong Alt", "TIA": "Strong Alt", "SEI": "Strong Alt",
    "SUI": "Strong Alt", "FET": "Strong Alt", "RNDR": "Strong Alt",
    "ASTER": "Strong Alt", "WLD": "Strong Alt",
    "DOGE": "High Volatility", "SHIB": "High Volatility",
    "PEPE": "High Volatility", "FLOKI": "High Volatility",
    "BONK": "High Volatility", "WIF": "High Volatility",
    "MEME": "High Volatility", "PEOPLE": "High Volatility",
    "TRX": "High Volatility", "PORTAL": "High Volatility",
    "ALLO": "High Volatility", "GENIUS": "High Volatility",
    "U": "High Volatility", "NEO": "High Volatility",
    "NOT": "High Volatility", "TNSR": "High Volatility",
    "BEAMX": "High Volatility", "PIXEL": "High Volatility",
    "ENA": "High Volatility", "REZ": "High Volatility",
    "OMNI": "High Volatility", "BB": "High Volatility",
    "IO": "High Volatility", "ZK": "High Volatility",
    "LAYER": "High Volatility", "ME": "High Volatility",
    "BERA": "High Volatility", "VIRTUAL": "High Volatility",
    "KAITO": "High Volatility",
}


class LSTMShortModel(nn.Module):
    def __init__(self, n_features=3, hidden=128, layers=1):
        super().__init__()
        self.lstm = nn.LSTM(n_features, hidden, layers, batch_first=True)
        self.fc = nn.Linear(hidden, 1)

    def forward(self, x):
        o, _ = self.lstm(x)
        return torch.sigmoid(self.fc(o[:, -1, :]))


class BTCTrendFetcher:
    def __init__(self, exchange, lookback_days=30):
        self.exchange = exchange
        self.lookback_days = lookback_days
        self._trend_map = None
        self._btc_ohlcv_cache = None
        self._period = int(os.getenv("BTC_SMA_PERIOD", "12"))

    async def _fetch_btc_1h(self):
        limit = self.lookback_days * 24 + 50
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: self.exchange.fetch_ohlcv('BTC/USDT', '1h', limit=limit))

    def _sma(self, values, period=50):
        return pd.Series(values).rolling(period).mean().values

    def build_trend_map(self, ohlcv_1h):
        closes = np.array([c[4] for c in ohlcv_1h])
        sma = self._sma(closes, self._period)
        trend_map = {}
        for i, candle in enumerate(ohlcv_1h):
            ts = candle[0] / 1000
            if i < self._period or np.isnan(sma[i]):
                continue
            close = closes[i]
            sma_val = sma[i]
            if close > sma_val * 1.01:
                trend_map[int(ts)] = "bull"
            elif close < sma_val * 0.99:
                trend_map[int(ts)] = "bear"
            else:
                trend_map[int(ts)] = "neutral"
        return trend_map

    async def get_trend_map(self):
        if self._trend_map is not None:
            return self._trend_map
        ohlcv = await self._fetch_btc_1h()
        if ohlcv:
            self._btc_ohlcv_cache = ohlcv
            self._trend_map = self.build_trend_map(ohlcv)
        else:
            self._trend_map = {}
        return self._trend_map

    def lookup(self, entry_ts, default="neutral"):
        if not self._trend_map:
            return default
        ts = int(entry_ts.timestamp())
        closest = max((t for t in self._trend_map if t <= ts), default=None)
        if closest is None:
            return default
        return self._trend_map.get(closest, default)

    def get_btc_sma_ratios(self):
        if self._trend_map is None:
            return np.zeros(5)
        try:
            ohlcv = self._btc_ohlcv_cache
            if ohlcv is None or len(ohlcv) < 50:
                return np.zeros(5)
            closes = np.array([c[4] for c in ohlcv])
            current = closes[-1]
            # BTC RSI(14)
            delta = np.diff(closes, prepend=closes[0])
            g = np.maximum(delta, 0)
            l = -np.minimum(delta, 0)
            ag = pd.Series(g).rolling(14).mean().values
            al = pd.Series(l).rolling(14).mean().values
            btc_rsi = (100 - 100 / (1 + ag / (al + 1e-10)))[-1]
            ratios = [(btc_rsi - 50) / 10]
            for p in [12, 24, 36, 48]:
                if len(closes) >= p:
                    ratios.append(current / max(closes[-p:].mean(), 1))
                else:
                    ratios.append(1.0)
            return np.array(ratios)
        except Exception:
            return np.zeros(5)


class ShortShadowScanner:
    MODELS_DIR = os.getenv("MODELS_DIR", "/app/models")
    SHORT_MODEL_FILES = {
        "Major": "model_short_lstm_Major.pt",
        "Strong Alt": "model_short_lstm_StrongAlt.pt",
        "High Volatility": "model_short_lstm_HighVolatility.pt",
    }

    def __init__(self, database_url):
        self.db_url = database_url
        self.exchange = ccxt.binance({"enableRateLimit": True})
        self.sl_multipliers = [2, 3, 4, 5, 6, None]
        self.tp_multipliers = [2, 3, 4, 5, 6, 8, 10, None]
        self.rsi_threshold = float(os.getenv("SHORT_RSI_THRESHOLD", "65"))
        self.min_short_score = float(os.getenv("SHORT_MIN_SCORE_SHADOW", "0.0"))
        self.lookback_days = int(os.getenv("SHORT_LOOKBACK_DAYS", "30"))
        self.scan_interval_hours = int(os.getenv("SHORT_SCAN_INTERVAL_HOURS", "4"))
        self.atr_pct = 0.015
        self._scanned_ranges = {}
        self._symbols_cache = None
        self._models = {}
        self._model_seqs = {}
        self._load_models()
        self._btc_trend = BTCTrendFetcher(self.exchange, self.lookback_days)

    def _load_models(self):
        for tier, fname in self.SHORT_MODEL_FILES.items():
            path = os.path.join(self.MODELS_DIR, fname)
            if not os.path.exists(path):
                logger.warning(f"ShortShadow: modelo SHORT nao encontrado: {path}")
                continue
            ckpt = torch.load(path, map_location="cpu", weights_only=False)
            cfg = ckpt.get("config", {})
            nf = cfg.get("n_features", 3)
            model = LSTMShortModel(nf, cfg.get("hidden", 128), cfg.get("layers", 1))
            model.load_state_dict(ckpt["model_state_dict"])
            model.eval()
            self._models[tier] = model
            self._model_seqs[tier] = cfg.get("seq_len", 144)
            logger.info(f"ShortShadow: modelo SHORT carregado: {tier} ({fname})")

    def _compute_model_features(self, closes, seq_len=144):
        closes = np.array(closes, dtype=np.float64)
        min_needed = seq_len + 56 + 16
        if len(closes) < min_needed:
            return None, None
        period = 56
        n = len(closes)
        rsi_14 = np.full(n, np.nan)
        gains = np.maximum(np.diff(closes), 0)
        losses = -np.minimum(np.diff(closes), 0)
        ag = gains[:period].mean()
        al = max(losses[:period].mean(), 1e-10)
        rsi_14[period] = 100 - 100 / (1 + ag / al)
        for i in range(period + 1, n):
            ag = (ag * (period - 1) + gains[i - 1]) / period
            al = (al * (period - 1) + losses[i - 1]) / period
            rsi_14[i] = 100 - 100 / (1 + ag / max(al, 1e-10))
        rsi_smooth = pd.Series(rsi_14).ewm(span=2, adjust=False).mean().values
        rsi_4h = pd.Series(rsi_14).rolling(16).mean().values
        feats = np.column_stack([
            (rsi_14 - 50) / 10,
            (rsi_smooth - 50) / 10,
            (rsi_4h - 50) / 10,
        ])
        btc_sma = self._btc_trend.get_btc_sma_ratios()
        btc_feats = np.tile(btc_sma, (len(feats), 1))
        feats = np.hstack([feats, btc_feats])
        funding_oi = np.zeros((len(feats), 4))
        feats = np.hstack([feats, funding_oi])
        feats = np.nan_to_num(feats, nan=0.0)
        return feats[-seq_len:], rsi_14[-1]

    def _predict_short_score(self, tier, features):
        if tier not in self._models:
            return None
        model = self._models[tier]
        seq_len = self._model_seqs.get(tier, 144)
        nf = model.lstm.input_size
        feats = features[-seq_len:, :nf]
        X = torch.from_numpy(feats).unsqueeze(0).float()
        with torch.no_grad():
            proba = model(X).item()
        return round(proba, 4)

    def init_db(self):
        conn = psycopg2.connect(self.db_url)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS shadow_short_metrics (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20),
                tier VARCHAR(50),
                rsi_entry FLOAT,
                hour_entry INT,
                entry_price FLOAT,
                sl FLOAT,
                tp FLOAT,
                pnl FLOAT,
                exit_reason VARCHAR(10),
                minutes INT,
                entry_ts TIMESTAMP,
                model_score FLOAT,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_short_symbol ON shadow_short_metrics(symbol);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_short_entry_ts ON shadow_short_metrics(entry_ts);
        """)
        try:
            cur.execute("ALTER TABLE shadow_short_metrics ADD COLUMN IF NOT EXISTS model_score FLOAT")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE shadow_short_metrics ADD COLUMN IF NOT EXISTS btc_trend VARCHAR(10)")
        except Exception:
            pass
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Tabela shadow_short_metrics inicializada.")

    async def _get_monitored_symbols(self):
        if self._symbols_cache:
            return self._symbols_cache
        try:
            conn = psycopg2.connect(self.db_url)
            cur = conn.cursor()
            cur.execute("SELECT DISTINCT symbol FROM market_selection ORDER BY symbol")
            rows = cur.fetchall()
            cur.close()
            conn.close()
            symbols = [r[0] for r in rows]
            if symbols:
                self._symbols_cache = symbols
                return symbols
        except Exception:
            pass
        self._symbols_cache = list(SYMBOL_TIERS.keys())
        return self._symbols_cache

    async def fetch_15m_data(self, symbol, days, end_dt=None):
        limit = days * 4 * 24 + 20
        try:
            loop = asyncio.get_event_loop()
            kwargs = {"limit": limit}
            if end_dt:
                kwargs["params"] = {"endTime": int(end_dt.timestamp() * 1000)}
            ohlcv = await loop.run_in_executor(
                None, lambda: self.exchange.fetch_ohlcv(symbol + '/USDT', '15m', **kwargs)
            )
            return ohlcv
        except Exception as e:
            logger.error(f"Erro ao buscar OHLCV 15m para {symbol}: {e}")
            return []

    def simulate_short(self, ohlcv_15m, entry_idx, atr_pct):
        entry = ohlcv_15m[entry_idx]
        entry_price = entry[4]
        entry_ts = entry[0]

        future = ohlcv_15m[entry_idx + 1:entry_idx + 1 + 48]
        if not future:
            return []

        results = []
        sl_map = {m: entry_price * (1 + m * atr_pct) if m else None for m in self.sl_multipliers}
        tp_map = {m: entry_price * (1 - m * atr_pct) if m else None for m in self.tp_multipliers}

        for sl_m in self.sl_multipliers:
            for tp_m in self.tp_multipliers:
                if sl_m is None and tp_m is None:
                    continue
                sl_price = sl_map[sl_m]
                tp_price = tp_map[tp_m]

                reason = "TIME"
                exit_price = entry_price
                minutes = 0

                for idx, candle in enumerate(future):
                    high = candle[2]
                    low = candle[3]
                    mn = (idx + 1) * 15

                    if sl_price is not None and high >= sl_price:
                        reason = "SL"
                        exit_price = sl_price
                        minutes = mn
                        break
                    if tp_price is not None and low <= tp_price:
                        reason = "TP"
                        exit_price = tp_price
                        minutes = mn
                        break

                if reason == "TIME" and future:
                    exit_price = future[-1][4]
                    minutes = len(future) * 15

                pnl_pct = (entry_price - exit_price) / entry_price * 100

                results.append({
                    "sl": sl_m,
                    "tp": tp_m,
                    "pnl": round(pnl_pct, 4),
                    "reason": reason,
                    "minutes": minutes,
                    "entry_ts": datetime.fromtimestamp(entry_ts / 1000, tz=timezone.utc)
                })

        return results

    async def scan_symbol(self, symbol, tier):
        await asyncio.sleep(0.5)
        ohlcv = await self.fetch_15m_data(symbol, self.lookback_days)
        if len(ohlcv) < 20:
            return 0

        closes = [c[4] for c in ohlcv]
        rsi_vals = compute_rsi(closes, 14)
        seq_len = self._model_seqs.get(tier, 144)

        entries = []
        for i, rsi in enumerate(rsi_vals):
            if rsi is None:
                continue
            if rsi < self.rsi_threshold:
                continue

            sims = self.simulate_short(ohlcv, i, self.atr_pct)
            if not sims:
                continue

            model_score = None
            if i >= seq_len + 56 + 16:
                feats, _ = self._compute_model_features(closes[:i + 1], seq_len)
                if feats is not None:
                    model_score = self._predict_short_score(tier, feats)

            if self.min_short_score > 0 and (model_score is None or model_score < self.min_short_score):
                continue

            entry_ts = datetime.fromtimestamp(ohlcv[i][0] / 1000, tz=timezone.utc)
            btc_t = self._btc_trend.lookup(entry_ts)
            for s in sims:
                entries.append((
                    symbol, tier, round(rsi, 1), entry_ts.hour,
                    ohlcv[i][4], s["sl"], s["tp"], s["pnl"],
                    s["reason"], s["minutes"], s["entry_ts"], model_score, btc_t
                ))

        if entries:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, lambda: self._db_save_shorts(entries))

        logger.info(f"  {symbol}: {len(entries)} registros de SHORT inseridos (RSI >= {self.rsi_threshold})")
        return len(entries)

    def _db_save_shorts(self, entries):
        conn = psycopg2.connect(self.db_url)
        cur = conn.cursor()
        cur.execute("""
            DELETE FROM shadow_short_metrics
            WHERE symbol = %s AND entry_ts >= %s
        """, (
            entries[0][0],
            entries[0][10] - timedelta(days=self.lookback_days + 1)
        ))
        for e in entries:
            cur.execute("""
                INSERT INTO shadow_short_metrics
                    (symbol, tier, rsi_entry, hour_entry, entry_price, sl, tp, pnl, exit_reason, minutes, entry_ts, model_score, btc_trend)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, e)
        conn.commit()
        cur.close()
        conn.close()

    async def run_scan(self):
        logger.info(f"Iniciando scan SHORT (RSI >= {self.rsi_threshold}, {self.lookback_days}d)...")
        self.init_db()
        symbols = await self._get_monitored_symbols()
        await self._btc_trend.get_trend_map()
        logger.info(f"Escaneando {len(symbols)} símbolos para SHORT...")

        total = 0
        for symbol in symbols:
            if any(char > '\u00ff' for char in symbol):
                continue
            tier = SYMBOL_TIERS.get(symbol, "Unknown")
            n = await self.scan_symbol(symbol, tier)
            total += n

        logger.info(f"Scan SHORT concluído: {total} registros em {len(symbols)} símbolos")

    async def run_loop(self):
        logger.info(f"ShortShadowScanner iniciado (intervalo: {self.scan_interval_hours}h, RSI>={self.rsi_threshold})")
        while True:
            try:
                self._symbols_cache = None
                await self.run_scan()
                await asyncio.sleep(self.scan_interval_hours * 3600)
            except Exception as e:
                logger.error(f"Erro no loop do ShortShadowScanner: {e}")
                await asyncio.sleep(300)


class LongShadowScanner:
    MODELS_DIR = os.getenv("MODELS_DIR", "/app/models")
    LONG_MODEL_FILES = {
        "Major": "model_mean_reversion_v1_lstm_Major.pt",
        "Strong Alt": "model_mean_reversion_v1_lstm_StrongAlt.pt",
        "High Volatility": "model_mean_reversion_v1_lstm_HighVolatility.pt",
    }

    def __init__(self, database_url):
        self.db_url = database_url
        self.exchange = ccxt.binance({"enableRateLimit": True})
        self.sl_multipliers = [2, 3, 4, 5, 6, None]
        self.tp_multipliers = [2, 3, 4, 5, 6, 8, 10, None]
        self.rsi_threshold = float(os.getenv("LONG_RSI_THRESHOLD", "35"))
        self.min_score = float(os.getenv("LONG_MIN_SCORE_SHADOW", "0.0"))
        self.lookback_days = int(os.getenv("LONG_SCAN_LOOKBACK_DAYS", "30"))
        self.scan_interval_hours = int(os.getenv("LONG_SCAN_INTERVAL_HOURS", "6"))
        self.atr_pct = 0.015
        self._symbols_cache = None
        self._models = {}
        self._model_seqs = {}
        self._load_models()
        self._btc_trend = BTCTrendFetcher(self.exchange, self.lookback_days)

    def _load_models(self):
        for tier, fname in self.LONG_MODEL_FILES.items():
            path = os.path.join(self.MODELS_DIR, fname)
            if not os.path.exists(path):
                logger.warning(f"LongShadow: modelo LONG nao encontrado: {path}")
                continue
            ckpt = torch.load(path, map_location="cpu", weights_only=False)
            cfg = ckpt.get("config", {})
            nf = cfg.get("n_features", 3)
            model = LSTMShortModel(nf, cfg.get("hidden", 128), cfg.get("layers", 1))
            model.load_state_dict(ckpt["model_state_dict"])
            model.eval()
            self._models[tier] = model
            self._model_seqs[tier] = cfg.get("seq_len", 144)
            logger.info(f"LongShadow: modelo LONG carregado: {tier} ({fname})")

    def init_db(self):
        conn = psycopg2.connect(self.db_url)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS shadow_long_scan (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20),
                tier VARCHAR(50),
                rsi_entry FLOAT,
                hour_entry INT,
                entry_price FLOAT,
                sl FLOAT,
                tp FLOAT,
                pnl FLOAT,
                exit_reason VARCHAR(10),
                minutes INT,
                entry_ts TIMESTAMP,
                model_score FLOAT,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_long_scan_symbol ON shadow_long_scan(symbol)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_long_scan_entry_ts ON shadow_long_scan(entry_ts)")
        try:
            cur.execute("ALTER TABLE shadow_long_scan ADD COLUMN IF NOT EXISTS btc_trend VARCHAR(10)")
        except Exception:
            pass
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Tabela shadow_long_scan inicializada.")

    async def _get_monitored_symbols(self):
        if self._symbols_cache:
            return self._symbols_cache
        try:
            conn = psycopg2.connect(self.db_url)
            cur = conn.cursor()
            cur.execute("SELECT DISTINCT symbol FROM market_selection ORDER BY symbol")
            rows = cur.fetchall()
            cur.close()
            conn.close()
            symbols = [r[0] for r in rows]
            if symbols:
                self._symbols_cache = symbols
                return symbols
        except Exception:
            pass
        self._symbols_cache = list(SYMBOL_TIERS.keys())
        return self._symbols_cache

    def _compute_features(self, closes, seq_len=144):
        closes = np.array(closes, dtype=np.float64)
        if len(closes) < seq_len + 56 + 16:
            return None, None
        period = 56
        n = len(closes)
        rsi_14 = np.full(n, np.nan)
        gains = np.maximum(np.diff(closes), 0)
        losses = -np.minimum(np.diff(closes), 0)
        ag = gains[:period].mean()
        al = max(losses[:period].mean(), 1e-10)
        rsi_14[period] = 100 - 100 / (1 + ag / al)
        for i in range(period + 1, n):
            ag = (ag * (period - 1) + gains[i - 1]) / period
            al = (al * (period - 1) + losses[i - 1]) / period
            rsi_14[i] = 100 - 100 / (1 + ag / max(al, 1e-10))
        rsi_smooth = pd.Series(rsi_14).ewm(span=2, adjust=False).mean().values
        rsi_4h = pd.Series(rsi_14).rolling(16).mean().values
        feats = np.column_stack([
            (rsi_14 - 50) / 10,
            (rsi_smooth - 50) / 10,
            (rsi_4h - 50) / 10,
        ])
        btc_sma = self._btc_trend.get_btc_sma_ratios()
        btc_feats = np.tile(btc_sma, (len(feats), 1))
        feats = np.hstack([feats, btc_feats])
        funding_oi = np.zeros((len(feats), 4))
        feats = np.hstack([feats, funding_oi])
        feats = np.nan_to_num(feats, nan=0.0)
        return feats[-seq_len:], rsi_14[-1]

    def _predict_long(self, tier, features):
        if tier not in self._models:
            return None
        model = self._models[tier]
        seq_len = self._model_seqs.get(tier, 144)
        nf = model.lstm.input_size
        feats = features[-seq_len:, :nf]
        X = torch.from_numpy(feats).unsqueeze(0).float()
        with torch.no_grad():
            proba = model(X).item()
        return round(proba, 4)

    async def fetch_15m_data(self, symbol, days):
        limit = days * 4 * 24 + 20
        try:
            loop = asyncio.get_event_loop()
            ohlcv = await loop.run_in_executor(
                None, lambda: self.exchange.fetch_ohlcv(symbol + '/USDT', '15m', limit=limit)
            )
            return ohlcv
        except Exception as e:
            logger.error(f"Erro ao buscar OHLCV 15m para {symbol}: {e}")
            return []

    def simulate_long(self, ohlcv_15m, entry_idx, atr_pct):
        entry = ohlcv_15m[entry_idx]
        entry_price = entry[4]
        entry_ts = entry[0]
        future = ohlcv_15m[entry_idx + 1:entry_idx + 1 + 48]
        if not future:
            return []
        results = []
        for sl_m in self.sl_multipliers:
            for tp_m in self.tp_multipliers:
                if sl_m is None and tp_m is None:
                    continue
                sl_price = entry_price * (1 - sl_m * atr_pct) if sl_m else 0
                tp_price = entry_price * (1 + tp_m * atr_pct) if tp_m else float('inf')
                reason = "TIME"
                exit_price = entry_price
                minutes = 0
                for idx, candle in enumerate(future):
                    high = candle[2]
                    low = candle[3]
                    mn = (idx + 1) * 15
                    if sl_price > 0 and low <= sl_price:
                        reason = "SL"
                        exit_price = sl_price
                        minutes = mn
                        break
                    if high >= tp_price:
                        reason = "TP"
                        exit_price = tp_price
                        minutes = mn
                        break
                if reason == "TIME" and future:
                    exit_price = future[-1][4]
                    minutes = len(future) * 15
                pnl_pct = (exit_price / entry_price - 1) * 100
                results.append({
                    "sl": sl_m, "tp": tp_m,
                    "pnl": round(pnl_pct, 4),
                    "reason": reason, "minutes": minutes,
                    "entry_ts": datetime.fromtimestamp(entry_ts / 1000, tz=timezone.utc),
                })
        return results

    async def scan_symbol(self, symbol, tier):
        await asyncio.sleep(0.5)
        ohlcv = await self.fetch_15m_data(symbol, self.lookback_days)
        if len(ohlcv) < 20:
            return 0
        closes = [c[4] for c in ohlcv]
        rsi_vals = compute_rsi(closes, 14)
        seq_len = self._model_seqs.get(tier, 144)
        entries = []
        for i, rsi in enumerate(rsi_vals):
            if rsi is None:
                continue
            if rsi >= self.rsi_threshold:
                continue
            if i < seq_len + 56 + 16:
                continue
            feats, _ = self._compute_features(closes[:i + 1], seq_len)
            if feats is None:
                continue
            score = self._predict_long(tier, feats)
            if score is None or score < self.min_score:
                continue
            sims = self.simulate_long(ohlcv, i, self.atr_pct)
            if not sims:
                continue
            entry_ts = datetime.fromtimestamp(ohlcv[i][0] / 1000, tz=timezone.utc)
            btc_t = self._btc_trend.lookup(entry_ts)
            for s in sims:
                entries.append((
                    symbol, tier, round(rsi, 1), entry_ts.hour,
                    ohlcv[i][4], s["sl"], s["tp"], s["pnl"],
                    s["reason"], s["minutes"], s["entry_ts"], score, btc_t
                ))
        if entries:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, lambda: self._db_save_longs(entries))
        logger.info(f"  {symbol}: {len(entries)} registros LONG scan (RSI < {self.rsi_threshold}, score >= {self.min_score})")
        return len(entries)

    def _db_save_longs(self, entries):
        conn = psycopg2.connect(self.db_url)
        cur = conn.cursor()
        cur.execute("DELETE FROM shadow_long_scan WHERE symbol = %s AND entry_ts >= %s",
                     (entries[0][0], entries[0][10] - timedelta(days=self.lookback_days + 1)))
        for e in entries:
            cur.execute("""
                INSERT INTO shadow_long_scan (symbol, tier, rsi_entry, hour_entry, entry_price, sl, tp, pnl, exit_reason, minutes, entry_ts, model_score, btc_trend)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, e)
        conn.commit()
        cur.close()
        conn.close()

    async def run_scan(self):
        logger.info(f"Iniciando scan LONG (RSI < {self.rsi_threshold}, score >= {self.min_score}, {self.lookback_days}d)...")
        self.init_db()
        symbols = await self._get_monitored_symbols()
        await self._btc_trend.get_trend_map()
        logger.info(f"Escaneando {len(symbols)} simbolos para LONG...")
        total = 0
        for symbol in symbols:
            if any(char > '\u00ff' for char in symbol):
                continue
            tier = SYMBOL_TIERS.get(symbol, "Unknown")
            n = await self.scan_symbol(symbol, tier)
            total += n
        logger.info(f"Scan LONG concluido: {total} registros em {len(symbols)} simbolos")

    async def run_loop(self):
        logger.info(f"LongShadowScanner iniciado (intervalo: {self.scan_interval_hours}h, RSI<{self.rsi_threshold}, score>={self.min_score})")
        while True:
            try:
                self._symbols_cache = None
                await self.run_scan()
                await asyncio.sleep(self.scan_interval_hours * 3600)
            except Exception as e:
                logger.error(f"Erro no loop do LongShadowScanner: {e}")
                await asyncio.sleep(300)

