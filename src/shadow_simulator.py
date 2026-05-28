import asyncio
import logging
import json
import ccxt
import psycopg2
from datetime import datetime, timezone

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

