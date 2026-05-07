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
                    simulations JSONB,
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
            conn.commit()
            cur.close()
            conn.close()
            logger.info("Tabela shadow_metrics inicializada.")
        except Exception as e:
            logger.error(f"Erro ao inicializar shadow_metrics: {e}")

    async def fetch_1m_data(self, symbol, start_dt):
        """Busca 720 candles de 1m (12h) a partir do start_dt."""
        since_ms = int(start_dt.timestamp() * 1000)
        try:
            # Roda assincronamente a chamada sincrona do ccxt ou usa run_in_executor
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
                # Ignora cenario sem SL e sem TP para os multiplicadores numericos
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
                    close = candle[4]
                    
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
                    exit_price = ohlcv[-1][4] # Ultimo close
                
                pnl_pct = (exit_price / entry_price - 1) * 100
                
                results.append({
                    "sl": sl_m,
                    "tp": tp_m,
                    "pnl": round(pnl_pct, 2),
                    "reason": reason,
                    "minutes": minutes
                })
                
        # Adiciona o cenario da Estrategia 3 (Sem TP, Sem SL, Time-exit em 12h)
        if len(ohlcv) > 0:
            time_exit_price = ohlcv[-1][4]
            results.append({
                "sl": None,
                "tp": None,
                "pnl": round((time_exit_price / entry_price - 1) * 100, 2),
                "reason": "TIME",
                "minutes": len(ohlcv)
            })
            
        return results

    async def run_loop(self):
        logger.info("Shadow Simulator iniciado...")
        self.init_db()
        
        while True:
            try:
                conn = psycopg2.connect(self.db_url)
                cur = conn.cursor()
                # Puxa trades com mais de 12 horas que ainda não foram simulados
                cur.execute("""
                    SELECT id, symbol, entry_price, sl_price, created_at
                    FROM trade_log
                    WHERE created_at < NOW() - INTERVAL '12 hours'
                      AND id NOT IN (SELECT trade_id FROM shadow_metrics)
                    LIMIT 20
                """)
                trades = cur.fetchall()
                cur.close()
                conn.close()
                
                for trade in trades:
                    trade_id, symbol, entry_price, sl_price, created_at = trade
                    
                    # Calcula o ATR
                    if not sl_price or sl_price >= entry_price:
                        continue
                        
                    atr = (entry_price - sl_price) / 2.0
                    
                    logger.info(f"Simulando trade #{trade_id} ({symbol}) de {created_at}...")
                    ohlcv = await self.fetch_1m_data(symbol, created_at)
                    
                    if not ohlcv:
                        logger.warning(f"Sem dados OHLCV para {symbol}. Pulando.")
                        continue
                        
                    simulations = self.simulate_trade(ohlcv, entry_price, atr)
                    
                    # Salva no banco
                    conn = psycopg2.connect(self.db_url)
                    cur = conn.cursor()
                    cur.execute("""
                        INSERT INTO shadow_metrics (trade_id, symbol, simulations)
                        VALUES (%s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (trade_id, symbol, json.dumps(simulations)))
                    conn.commit()
                    cur.close()
                    conn.close()
                    
                    logger.info(f"Simulação para #{trade_id} finalizada com {len(simulations)} cenários.")
                    await asyncio.sleep(1) # Rate limit Binance
                    
            except Exception as e:
                logger.error(f"Erro no loop do shadow simulator: {e}")
                
            await asyncio.sleep(600) # Roda a cada 10 min
