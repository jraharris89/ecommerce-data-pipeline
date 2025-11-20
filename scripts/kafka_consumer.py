from kafka import KafkaConsumer
import json
import psycopg2
from psycopg2 import pool
import logging
import signal
import sys
import os
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration from environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'ecommerce-orders')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'ecommerce-consumer-group')
DB_HOST = os.getenv('DB_HOST', 'postgres')
DB_PORT = int(os.getenv('DB_PORT', '5432'))
DB_NAME = os.getenv('DB_NAME', 'ecommerce')
DB_USER = os.getenv('DB_USER', 'airflow')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'airflow')
DB_POOL_MIN = int(os.getenv('DB_POOL_MIN', '1'))
DB_POOL_MAX = int(os.getenv('DB_POOL_MAX', '5'))
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))

class EcommerceConsumer:
    def __init__(self):
        self.shutdown_requested = False

        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        # Connect to Kafka
        try:
            self.consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset='earliest'
            )
            logger.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}, topic: {KAFKA_TOPIC}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise

        # Create PostgreSQL connection pool
        try:
            self.db_pool = pool.SimpleConnectionPool(
                minconn=DB_POOL_MIN,
                maxconn=DB_POOL_MAX,
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            logger.info(f"PostgreSQL connection pool created (min={DB_POOL_MIN}, max={DB_POOL_MAX})")
        except psycopg2.Error as e:
            logger.error(f"Failed to create PostgreSQL connection pool: {e}")
            raise

        # Create raw table if not exists
        self.create_table()

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.shutdown_requested = True
        
    def create_table(self):
        """Create raw orders table in PostgreSQL"""
        conn = None
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()

            create_table_query = """
            CREATE TABLE IF NOT EXISTS raw_orders (
                id SERIAL PRIMARY KEY,
                event_type VARCHAR(50),
                event_timestamp TIMESTAMP,
                order_id VARCHAR(100),
                customer_id VARCHAR(100),
                customer_city VARCHAR(100),
                customer_state VARCHAR(50),
                product_id VARCHAR(100),
                product_category VARCHAR(100),
                price DECIMAL(10, 2),
                freight_value DECIMAL(10, 2),
                payment_type VARCHAR(50),
                payment_value DECIMAL(10, 2),
                order_status VARCHAR(50),
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            cursor.execute(create_table_query)

            # Create indexes for better query performance
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_raw_orders_order_id ON raw_orders(order_id);",
                "CREATE INDEX IF NOT EXISTS idx_raw_orders_customer_id ON raw_orders(customer_id);",
                "CREATE INDEX IF NOT EXISTS idx_raw_orders_event_timestamp ON raw_orders(event_timestamp);",
                "CREATE INDEX IF NOT EXISTS idx_raw_orders_ingested_at ON raw_orders(ingested_at);"
            ]

            for index_query in indexes:
                cursor.execute(index_query)

            conn.commit()
            cursor.close()
            logger.info("Table 'raw_orders' and indexes created/verified")
        except psycopg2.Error as e:
            logger.error(f"Error creating table or indexes: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                self.db_pool.putconn(conn)
        
    def process_messages(self):
        """Consume messages and write to PostgreSQL"""
        logger.info("Starting to consume messages...")
        batch = []
        batch_size = BATCH_SIZE
        
        try:
            for message in self.consumer:
                # Check for shutdown request
                if self.shutdown_requested:
                    logger.info("Shutdown requested, processing remaining batch...")
                    break

                try:
                    event = message.value
                    batch.append(event)

                    # Process in batches for efficiency
                    if len(batch) >= batch_size:
                        self.write_batch(batch)
                        batch = []

                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode message: {e}")
                    continue
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    continue

        except KeyboardInterrupt:
            logger.info("Stopping consumer...")
        except Exception as e:
            logger.error(f"Unexpected error in message consumption: {e}")
            raise
        finally:
            # Process any remaining messages
            if batch:
                logger.info(f"Processing final batch of {len(batch)} messages...")
                self.write_batch(batch)
                
    def write_batch(self, batch):
        """Write batch of events to PostgreSQL using connection pool"""
        if not batch:
            return

        insert_query = """
        INSERT INTO raw_orders (
            event_type, event_timestamp, order_id, customer_id,
            customer_city, customer_state, product_id, product_category,
            price, freight_value, payment_type, payment_value, order_status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        conn = None
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()

            values = []
            for event in batch:
                try:
                    values.append((
                        event['event_type'],
                        datetime.fromisoformat(event['event_timestamp']),
                        event['order_id'],
                        event['customer_id'],
                        event['customer_city'],
                        event['customer_state'],
                        event['product_id'],
                        event.get('product_category', ''),
                        event['price'],
                        event['freight_value'],
                        event['payment_type'],
                        event['payment_value'],
                        event['order_status']
                    ))
                except KeyError as e:
                    logger.error(f"Missing required field in event: {e}")
                    continue
                except ValueError as e:
                    logger.error(f"Invalid value in event: {e}")
                    continue

            if values:
                cursor.executemany(insert_query, values)
                conn.commit()
                logger.info(f"Wrote {len(values)} events to PostgreSQL")

            cursor.close()
        except psycopg2.Error as e:
            logger.error(f"Database error writing batch: {e}")
            if conn:
                conn.rollback()
            raise
        except Exception as e:
            logger.error(f"Unexpected error writing batch: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                self.db_pool.putconn(conn)
        
    def close(self):
        """Close all connections gracefully"""
        logger.info("Closing consumer connections...")
        try:
            self.consumer.close()
            logger.info("Kafka consumer closed")
        except Exception as e:
            logger.error(f"Error closing Kafka consumer: {e}")

        try:
            self.db_pool.closeall()
            logger.info("PostgreSQL connection pool closed")
        except Exception as e:
            logger.error(f"Error closing database pool: {e}")

if __name__ == "__main__":
    consumer = EcommerceConsumer()
    try:
        consumer.process_messages()
    finally:
        consumer.close()