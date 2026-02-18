# producer/producer.py
import os
import json
import logging
import time
import requests
from confluent_kafka import Producer
from datetime import datetime
import backoff
from dotenv import load_dotenv
import schedule

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.getenv("LOG_DIR", os.path.join(BASE_DIR, "logs"))
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'producer.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class StockDataProducer:
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.topic = os.getenv('KAFKA_TOPIC', 'stock_prices')
        self.api_key = os.getenv('RAPIDAPI_KEY')
        self.api_host = os.getenv('RAPIDAPI_HOST', 'alpha-vantage.p.rapidapi.com')
        self.symbols = os.getenv('STOCK_SYMBOLS', 'AAPL,MSFT,GOOGL,AMZN,TSLA').split(',')
        self.producer = self.create_kafka_producer()
        
    def create_kafka_producer(self):
        conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': 'stock-producer',
            'acks': 'all',
            'retries': 3,
            'compression.type': 'snappy',
            'batch.size': 16384,
            'linger.ms': 10,
            'enable.idempotence': True
        }
        return Producer(conf)
    
    def delivery_report(self, err, msg):
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')
    
    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.RequestException,
        max_tries=3
    )
    def fetch_stock_data(self, symbol: str):
        """Fetch real-time stock data for a single symbol"""
        url = "https://alpha-vantage.p.rapidapi.com/query"
        querystring = {
            "function": "GLOBAL_QUOTE",
            "symbol": symbol,
            "datatype": "json"
        }
        headers = {
            "x-rapidapi-key": self.api_key,
            "x-rapidapi-host": self.api_host
        }
        try:
            response = requests.get(url, headers=headers, params=querystring, timeout=10)
            response.raise_for_status()
            data = response.json()
            quote = data.get('Global Quote', {})
            return {
                'symbol': symbol,
                'price': float(quote.get('05. price', 0)),
                'volume': int(quote.get('06. volume', 0)),
                'change': float(quote.get('09. change', 0)),
                'change_percent': float(quote.get('10. change percent', '0%').replace('%', '')),
                'day_high': 0.0,
                'day_low': 0.0,
                'event_time': datetime.now().isoformat(),
                'company_name': symbol,
                'sector': 'Unknown'
            }
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {str(e)}")
            raise
    
    def publish_to_kafka(self, data: dict):
        try:
            value = json.dumps(data).encode('utf-8')
            key = data['symbol'].encode('utf-8')
            self.producer.produce(
                self.topic,
                key=key,
                value=value,
                callback=self.delivery_report
            )
            self.producer.poll(0)
        except Exception as e:
            logger.error(f"Failed to publish to Kafka: {str(e)}")
            raise
    
    def run_once(self):
        """Fetch and publish data with staggered delays to respect rate limits"""
        delay_between_calls = 12  # seconds (5 symbols × 12 = 60 seconds)
        for symbol in self.symbols:
            try:
                data = self.fetch_stock_data(symbol.strip())
                self.publish_to_kafka(data)
            except Exception as e:
                logger.error(f"Failed to process {symbol}: {e}")
            time.sleep(delay_between_calls)  # Crucial: pause between calls
        self.producer.flush()
    
    def run_scheduled(self):
        self.run_once()
        interval = int(os.getenv('POLL_INTERVAL', 60))
        schedule.every(interval).seconds.do(self.run_once)
        logger.info(f"Producer started. Polling every {interval} seconds")
        while True:
            schedule.run_pending()
            time.sleep(1)

if __name__ == "__main__":
    producer = StockDataProducer()
    logger.info("Starting stock data producer...")
    try:
        producer.run_scheduled()
    except KeyboardInterrupt:
        logger.info("Shutting down producer...")
        producer.producer.flush()
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        producer.producer.flush()