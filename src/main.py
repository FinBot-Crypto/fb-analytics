"""
fb-analytics: Processa métricas quantitativas do bot.
Ouve 'trade.closed' via NATS, recalcula métricas (win-rate, PnL, score distribution)
e salva na tabela 'daily_metrics' no PostgreSQL.
"""
import asyncio
import logging
import os
import json
import time
from datetime import datetime
import psycopg2
import nats
from nats.js.api import ConsumerConfig
from shadow_simulator import ShadowSimulator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("fb-analytics")

NATS_URL = os.getenv("NATS_URL", "nats://crypto-nats:4222")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://crypto_admin:ZNG5z43LaSrk7FEmwu6CPtRUB2IVKdvY@crypto-postgres:5432/crypto_bot")

class AnalyticsService:
    def __init__(self):
        self.nc = None
        self.js = None

    async def connect_nats(self):
        self.nc = await nats.connect(NATS_URL)
        self.js = self.nc.jetstream()
        logger.info(f"NATS conectado: {NATS_URL}")

    def init_db(self):
        """Inicializa tabela de métricas analíticas."""
        try:
            conn = psycopg2.connect(DATABASE_URL)
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS daily_metrics (
                    id SERIAL PRIMARY KEY,
                    date DATE UNIQUE,
                    total_trades INT DEFAULT 0,
                    winning_trades INT DEFAULT 0,
                    win_rate NUMERIC DEFAULT 0,
                    total_pnl NUMERIC DEFAULT 0,
                    avg_hold_hours NUMERIC DEFAULT 0,
                    metrics_json JSONB,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit()
            cur.close()
            conn.close()
            logger.info("Tabela daily_metrics inicializada.")
        except Exception as e:
            logger.error(f"Erro ao inicializar DB: {e}")

    def run_analysis(self):
        """
        Calcula as estatísticas a partir do trade_log.
        Analisa Win-Rate global, por estratégia, e por faixas de ML Score.
        """
        try:
            conn = psycopg2.connect(DATABASE_URL)
            cur = conn.cursor()
            
            # 1. Total e Win-Rate (Hoje)
            cur.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) as wins,
                    SUM(pnl_pct) as total_pnl,
                    AVG(hold_hours) as avg_hold
                FROM trade_log
                WHERE status = 'CLOSED' AND DATE(updated_at) = CURRENT_DATE
            """)
            hoje = cur.fetchone()
            total_today = hoje[0] or 0
            wins_today = hoje[1] or 0
            win_rate_today = (wins_today / total_today * 100) if total_today > 0 else 0
            pnl_today = hoje[2] or 0
            avg_hold_today = hoje[3] or 0

            # 2. Performance por Estratégia (All-Time)
            cur.execute("""
                SELECT 
                    strategy,
                    COUNT(*),
                    SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END),
                    AVG(pnl_pct)
                FROM trade_log
                WHERE status = 'CLOSED'
                GROUP BY strategy
            """)
            strat_rows = cur.fetchall()
            strategies_stats = []
            for row in strat_rows:
                st, t, w, avg_p = row
                wr = (w / t * 100) if t > 0 else 0
                strategies_stats.append({
                    "strategy": st,
                    "total": t,
                    "win_rate": round(wr, 2),
                    "avg_pnl": round(avg_p, 2) if avg_p else 0
                })

            # 3. Performance por Faixa de Score ML (All-Time)
            # Ajuda a validar se scores maiores realmente dão mais acerto
            cur.execute("""
                SELECT 
                    ROUND((score * 10)::numeric, 0) / 10 AS score_bucket,
                    COUNT(*),
                    SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END)
                FROM trade_log
                WHERE status = 'CLOSED' AND score IS NOT NULL
                GROUP BY score_bucket
                ORDER BY score_bucket DESC
            """)
            score_rows = cur.fetchall()
            score_stats = []
            for row in score_rows:
                bucket, t, w = row
                wr = (w / t * 100) if t > 0 else 0
                score_stats.append({
                    "bucket": f"{float(bucket):.1f} - {float(bucket)+0.1:.1f}",
                    "total": t,
                    "win_rate": round(wr, 2)
                })

            # Salvar no BD
            metrics_json = json.dumps({
                "strategies": strategies_stats,
                "scores": score_stats
            })

            cur.execute("""
                INSERT INTO daily_metrics (date, total_trades, winning_trades, win_rate, total_pnl, avg_hold_hours, metrics_json, updated_at)
                VALUES (CURRENT_DATE, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (date) DO UPDATE SET
                    total_trades = EXCLUDED.total_trades,
                    winning_trades = EXCLUDED.winning_trades,
                    win_rate = EXCLUDED.win_rate,
                    total_pnl = EXCLUDED.total_pnl,
                    avg_hold_hours = EXCLUDED.avg_hold_hours,
                    metrics_json = EXCLUDED.metrics_json,
                    updated_at = NOW();
            """, (total_today, wins_today, win_rate_today, pnl_today, avg_hold_today, metrics_json))
            
            conn.commit()
            cur.close()
            conn.close()

            logger.info(f"📊 ANALYTICS ATUALIZADO:")
            logger.info(f"  Hoje: {total_today} trades | Win-Rate: {win_rate_today:.1f}% | PnL: {pnl_today:.2f}%")
            logger.info(f"  Score Buckets: {score_stats}")

        except Exception as e:
            logger.error(f"Erro ao processar análise: {e}")

    async def handle_trade_closed(self, msg):
        """Ao fechar um trade, espera uns segundos e roda as analises"""
        try:
            data = json.loads(msg.data.decode())
            logger.info(f"Trade recebido: {data['symbol']} fechou com {data['pnl_pct']}%")
            
            # Roda as estatísticas
            self.run_analysis()
            
            await msg.ack()
        except Exception as e:
            logger.error(f"Erro ao processar evento: {e}")

    async def run(self):
        await self.connect_nats()
        self.init_db()

        # Roda uma vez no inicio para pegar o estado atual
        self.run_analysis()

        await self.js.subscribe("trade.closed", durable="ANALYTICS_WORKER",
                                 cb=self.handle_trade_closed, manual_ack=True,
                                 config=ConsumerConfig(ack_wait=30))
        
        logger.info("fb-analytics online. Aguardando trades serem fechados...")
        
        # Inicia o simulador fantasma em background
        shadow = ShadowSimulator(DATABASE_URL)
        asyncio.create_task(shadow.run_loop())
        
        while True:
            if self.nc.is_closed:
                await self.connect_nats()
            await asyncio.sleep(60)

if __name__ == "__main__":
    service = AnalyticsService()
    asyncio.run(service.run())
