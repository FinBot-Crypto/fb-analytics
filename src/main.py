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
from shadow_simulator import ShadowSimulator, ShortShadowScanner, LongShadowScanner

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

    def apply_leme_disable(self, direction, tier, reason):
        group_name = f"{direction}_{tier}"
        allowed_key = f"{direction}_{tier}_allowed"
        try:
            conn = psycopg2.connect(DATABASE_URL)
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO bot_settings (key, value, updated_at)
                VALUES (%s, 'false'::jsonb, CURRENT_TIMESTAMP)
                ON CONFLICT (key) DO UPDATE SET value = 'false'::jsonb, updated_at = CURRENT_TIMESTAMP
            """, (allowed_key,))
            cur.execute("""
                INSERT INTO leme_decisions (group_name, action, reason, details)
                VALUES (%s, 'DISABLE', %s, %s)
            """, (group_name, reason, json.dumps({"timestamp": datetime.now().isoformat()})))
            conn.commit()
            cur.close()
            conn.close()
            logger.warning(f"☸️ LEME: Grupo {group_name} DESATIVADO. Motivo: {reason}")
        except Exception as e:
            logger.error(f"Erro ao desativar grupo {group_name} no Leme: {e}")

    def apply_leme_enable(self, direction, tier, reason):
        group_name = f"{direction}_{tier}"
        allowed_key = f"{direction}_{tier}_allowed"
        try:
            conn = psycopg2.connect(DATABASE_URL)
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO bot_settings (key, value, updated_at)
                VALUES (%s, 'true'::jsonb, CURRENT_TIMESTAMP)
                ON CONFLICT (key) DO UPDATE SET value = 'true'::jsonb, updated_at = CURRENT_TIMESTAMP
            """, (allowed_key,))
            cur.execute("""
                INSERT INTO leme_decisions (group_name, action, reason, details)
                VALUES (%s, 'ENABLE', %s, %s)
            """, (group_name, reason, json.dumps({"timestamp": datetime.now().isoformat()})))
            conn.commit()
            cur.close()
            conn.close()
            logger.info(f"☸️ LEME: Grupo {group_name} REATIVADO. Motivo: {reason}")
        except Exception as e:
            logger.error(f"Erro ao reativar grupo {group_name} no Leme: {e}")

    async def evaluate_leme_rules(self):
        settings = {}
        leme_activation_time = datetime.min
        try:
            conn = psycopg2.connect(DATABASE_URL)
            cur = conn.cursor()
            cur.execute("SELECT key, value, updated_at FROM bot_settings")
            rows = cur.fetchall()
            for r in rows:
                val = r[1]
                if isinstance(val, str):
                    try:
                        val = json.loads(val)
                    except:
                        pass
                settings[r[0]] = val
                if r[0] == "leme_active" and r[2] is not None:
                    leme_activation_time = r[2]
            cur.close()
            conn.close()
        except Exception as e:
            logger.error(f"Leme Guardian: erro ao carregar configs do banco: {e}")
            return

        if not settings.get("leme_active", True):
            return

        tiers = ["Major", "Strong Alt", "High Volatility"]
        directions = ["long", "short"]
        groups = []
        for d in directions:
            for t in tiers:
                groups.append((d, t))

        max_consecutive_sl = int(settings.get("leme_max_consecutive_sl", 3))
        min_win_rate = float(settings.get("leme_min_win_rate", 50.0))
        cooldown_hours = float(settings.get("leme_cooldown_hours", 24.0))
        shadow_min_trades = int(settings.get("leme_shadow_min_trades", 5))
        shadow_min_winrate = float(settings.get("leme_shadow_min_winrate", 60.0))

        try:
            conn = psycopg2.connect(DATABASE_URL)
            cur = conn.cursor()

            # Buscar mapeamento symbol->tier da evaluations_log (sempre atualizada pelo strategy-ml)
            cur.execute("""
                SELECT DISTINCT ON (symbol) symbol, tier 
                FROM evaluations_log 
                WHERE tier IS NOT NULL 
                ORDER BY symbol, created_at DESC
            """)
            rows_ev = cur.fetchall()
            if not rows_ev:
                # Fallback: usar trade_log que também tem coluna tier
                cur.execute("SELECT DISTINCT symbol, tier FROM trade_log WHERE tier IS NOT NULL")
                rows_ev = cur.fetchall()
            symbol_tiers = {r[0]: r[1] for r in rows_ev}
            # Normalizar: remover sufixo /USDT se necessário para compatibilidade
            symbol_tiers_norm = {}
            for sym, tier in symbol_tiers.items():
                symbol_tiers_norm[sym] = tier
                if "/USDT" in sym:
                    symbol_tiers_norm[sym.replace("/USDT", "")] = tier
            symbol_tiers = symbol_tiers_norm


            for direction, tier in groups:
                group_name = f"{direction}_{tier}"
                allowed_key = f"{direction}_{tier}_allowed"
                is_allowed = settings.get(allowed_key, True)

                tier_symbols = [sym for sym, t in symbol_tiers.items() if t == tier]
                if not tier_symbols:
                    continue

                if len(tier_symbols) == 1:
                    symbol_filter = "symbol = %s"
                    symbol_param = tier_symbols[0]
                else:
                    symbol_filter = "symbol IN %s"
                    symbol_param = tuple(tier_symbols)

                if is_allowed:
                    # GRUPO ATIVO: Avaliar se precisa DESATIVAR (DISABLE)
                    # A. Stop Losses consecutivos
                    cur.execute(f"""
                        SELECT pnl_pct 
                        FROM trade_log 
                        WHERE status = 'CLOSED' 
                          AND direction = %s 
                          AND {symbol_filter} 
                          AND updated_at >= %s
                        ORDER BY updated_at DESC 
                        LIMIT %s
                    """, (direction.upper(), symbol_param, leme_activation_time, max_consecutive_sl))
                    recent_trades = cur.fetchall()

                    if len(recent_trades) >= max_consecutive_sl:
                        all_losses = all(r[0] < 0 for r in recent_trades)
                        if all_losses:
                            reason = f"Atingiu o limite de {max_consecutive_sl} Stop Losses consecutivos em operações reais."
                            self.apply_leme_disable(direction, tier, reason)
                            settings[allowed_key] = False
                            continue

                    # B. Win-Rate recente de 10 trades
                    cur.execute(f"""
                        SELECT pnl_pct 
                        FROM trade_log 
                        WHERE status = 'CLOSED' 
                          AND direction = %s 
                          AND {symbol_filter} 
                          AND updated_at >= %s
                        ORDER BY updated_at DESC 
                        LIMIT 10
                    """, (direction.upper(), symbol_param, leme_activation_time))
                    trades_10 = cur.fetchall()
                    if len(trades_10) >= 5:
                        wins = sum(1 for r in trades_10 if r[0] > 0)
                        wr = (wins / len(trades_10)) * 100
                        if wr < min_win_rate:
                            reason = f"Win-Rate real de {wr:.1f}% caiu abaixo do limite de {min_win_rate}% nos últimos {len(trades_10)} trades."
                            self.apply_leme_disable(direction, tier, reason)
                            settings[allowed_key] = False
                            continue
                else:
                    # GRUPO PAUSADO: Avaliar se precisa REATIVAR (ENABLE)
                    # A. Cooldown check
                    cur.execute("""
                        SELECT created_at 
                        FROM leme_decisions 
                        WHERE group_name = %s AND action = 'DISABLE' 
                        ORDER BY created_at DESC 
                        LIMIT 1
                    """, (group_name,))
                    last_disable = cur.fetchone()
                    if last_disable:
                        from datetime import timezone
                        disable_ts = last_disable[0].replace(tzinfo=timezone.utc)
                        elapsed = (datetime.now(timezone.utc) - disable_ts).total_seconds() / 3600.0
                        if elapsed < cooldown_hours:
                            continue

                    # B. Shadow Recovery check
                    if direction == "long":
                        cur.execute("""
                            SELECT pnl FROM shadow_long_scan 
                            WHERE tier = %s AND sl = 3.0 AND tp = 3.0 
                            ORDER BY entry_ts DESC LIMIT %s
                        """, (tier, shadow_min_trades))
                    else:
                        cur.execute("""
                            SELECT pnl FROM shadow_short_metrics 
                            WHERE tier = %s AND sl = 3.0 AND tp = 3.0 
                            ORDER BY entry_ts DESC LIMIT %s
                        """, (tier, shadow_min_trades))

                    shadow_trades = cur.fetchall()
                    if len(shadow_trades) >= shadow_min_trades:
                        wins = sum(1 for r in shadow_trades if r[0] > 0)
                        wr = (wins / len(shadow_trades)) * 100
                        if wr >= shadow_min_winrate:
                            reason = f"Recuperação detectada no Shadow: Win-rate de {wr:.1f}% nos últimos {len(shadow_trades)} trades simulados (mín: {shadow_min_winrate}%)."
                            self.apply_leme_enable(direction, tier, reason)
                            settings[allowed_key] = True

            cur.close()
            conn.close()
        except Exception as e:
            logger.error(f"Erro ao avaliar regras do Leme Guardian: {e}")

    async def run_leme_guardian_loop(self):
        logger.info("Leme Guardian Loop iniciado.")
        while True:
            try:
                await self.evaluate_leme_rules()
            except Exception as e:
                logger.error(f"Erro no ciclo do Leme Guardian: {e}")
            await asyncio.sleep(600)  # Executa a cada 10 minutos

    async def handle_trade_closed(self, msg):
        """Ao fechar um trade, espera uns segundos e roda as analises"""
        try:
            data = json.loads(msg.data.decode())
            logger.info(f"Trade recebido: {data['symbol']} fechou com {data['pnl_pct']}%")
            
            # Roda as estatísticas e avalia o Leme imediatamente
            self.run_analysis()
            await self.evaluate_leme_rules()
            
            await msg.ack()
        except Exception as e:
            logger.error(f"Erro ao processar evento: {e}")

    async def run(self):
        await self.connect_nats()
        self.init_db()

        # Roda uma vez no inicio para pegar o estado atual
        self.run_analysis()
        await self.evaluate_leme_rules()

        await self.js.subscribe("trade.closed", durable="ANALYTICS_WORKER",
                                 cb=self.handle_trade_closed, manual_ack=True,
                                 config=ConsumerConfig(ack_wait=30))
        
        logger.info("fb-analytics online. Aguardando trades serem fechados...")
        
        # Inicia o simulador fantasma LONG em background
        shadow = ShadowSimulator(DATABASE_URL)
        asyncio.create_task(shadow.run_loop())

        # Inicia o simulador fantasma LONG via modelos (scan OHLCV)
        long_shadow = LongShadowScanner(DATABASE_URL)
        asyncio.create_task(long_shadow.run_loop())

        # Inicia o simulador fantasma SHORT em background
        short_shadow = ShortShadowScanner(DATABASE_URL)
        asyncio.create_task(short_shadow.run_loop())

        # Inicia o loop periódico do Leme Guardian
        asyncio.create_task(self.run_leme_guardian_loop())
        
        while True:
            if self.nc.is_closed:
                await self.connect_nats()
            await asyncio.sleep(60)

if __name__ == "__main__":
    service = AnalyticsService()
    asyncio.run(service.run())
