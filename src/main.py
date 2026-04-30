import time
import logging
import os
import json
import redis
import psycopg2

# Configuração de Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("analytics")

# Configurações via Ambiente
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
DATABASE_URL = os.getenv("DATABASE_URL")

class AnalyticsService:
    def __init__(self):
        self.r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        self.pubsub = self.r.pubsub()
        self.init_db()

    def init_db(self):
        """Cria a tabela de trades se não existir."""
        try:
            conn = psycopg2.connect(DATABASE_URL)
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20),
                    strategy VARCHAR(50),
                    entry_price NUMERIC,
                    exit_price NUMERIC,
                    pnl NUMERIC,
                    exit_reason VARCHAR(20),
                    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
            cur.close()
            conn.close()
            logger.info("Banco de dados inicializado.")
        except Exception as e:
            logger.error(f"Erro ao conectar ao Postgres: {e}")

    def save_trade(self, message):
        """Salva o trade encerrado para análise futura."""
        trade = json.loads(message['data'])
        logger.info(f"Salvando trade de {trade['symbol']} no Analytics...")
        
        try:
            conn = psycopg2.connect(DATABASE_URL)
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO trades (symbol, strategy, entry_price, exit_price, pnl, exit_reason) VALUES (%s, %s, %s, %s, %s, %s)",
                (trade['symbol'], trade['strategy'], trade['entry_price'], trade['exit_price'], trade['pnl'], trade['exit_reason'])
            )
            conn.commit()
            cur.close()
            conn.close()
            logger.info(f"Trade de {trade['symbol']} salvo com sucesso.")
        except Exception as e:
            logger.error(f"Erro ao salvar trade: {e}")

    def run(self):
        self.pubsub.subscribe(**{'events:trade_closed': self.save_trade})
        logger.info("Analytics Service aguardando 'events:trade_closed'...")
        
        for message in self.pubsub.listen():
            if message['type'] == 'message':
                pass

if __name__ == "__main__":
    service = AnalyticsService()
    service.run()
