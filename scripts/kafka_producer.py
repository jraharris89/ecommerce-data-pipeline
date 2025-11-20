import pandas as pd
from kafka import KafkaProducer
import json
import time
import logging
import os
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration from environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
DATA_PATH = os.getenv('DATA_PATH', 'data/raw')

class EcommerceProducer:
    def __init__(self, bootstrap_servers=None):
        if bootstrap_servers is None:
            bootstrap_servers = [KAFKA_BOOTSTRAP_SERVERS]

        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info(f"Connected to Kafka at {bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise
        
    def load_data(self):
        """Load Olist datasets"""
        try:
            logger.info("Loading datasets...")
            self.orders = pd.read_csv(f'{DATA_PATH}/olist_orders_dataset.csv')
            self.order_items = pd.read_csv(f'{DATA_PATH}/olist_order_items_dataset.csv')
            self.customers = pd.read_csv(f'{DATA_PATH}/olist_customers_dataset.csv')
            self.products = pd.read_csv(f'{DATA_PATH}/olist_products_dataset.csv')
            self.payments = pd.read_csv(f'{DATA_PATH}/olist_order_payments_dataset.csv')
        except FileNotFoundError as e:
            logger.error(f"Dataset file not found: {e}")
            raise
        except pd.errors.ParserError as e:
            logger.error(f"Error parsing CSV file: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error loading datasets: {e}")
            raise

        try:
            # Merge data for complete order events
            self.merged_data = self.orders.merge(
                self.order_items, on='order_id'
            ).merge(
                self.customers, on='customer_id'
            ).merge(
                self.products, on='product_id'
            ).merge(
                self.payments, on='order_id'
            )

            logger.info(f"Loaded {len(self.merged_data)} order events")
        except KeyError as e:
            logger.error(f"Column not found during merge: {e}")
            raise
        except Exception as e:
            logger.error(f"Error merging datasets: {e}")
            raise
        
    def stream_orders(self, speed_multiplier=100, max_events=1000):
        """
        Stream orders to Kafka
        speed_multiplier: How much faster than real-time (100 = 100x faster)
        max_events: Maximum number of events to stream (None = all)
        """
        logger.info(f"Starting to stream orders (speed: {speed_multiplier}x)...")
        
        events_streamed = 0

        for row in self.merged_data.itertuples(index=False):
            if max_events and events_streamed >= max_events:
                break

            try:
                # Create order event with current timestamp
                order_event = {
                    'event_type': 'order_created',
                    'event_timestamp': datetime.now().isoformat(),
                    'order_id': row.order_id,
                    'customer_id': row.customer_id,
                    'customer_city': row.customer_city,
                    'customer_state': row.customer_state,
                    'product_id': row.product_id,
                    'product_category': row.product_category_name,
                    'price': float(row.price),  # type: ignore
                    'freight_value': float(row.freight_value),  # type: ignore
                    'payment_type': row.payment_type,
                    'payment_value': float(row.payment_value),  # type: ignore
                    'order_status': row.order_status
                }

                # Send to Kafka
                self.producer.send('ecommerce-orders', value=order_event)

                events_streamed += 1

                if events_streamed % 100 == 0:
                    logger.info(f"Streamed {events_streamed} events...")
                    # Flush periodically to prevent data loss
                    self.producer.flush()

                # Sleep to simulate real-time streaming
                time.sleep(1.0 / speed_multiplier)

            except AttributeError as e:
                logger.error(f"Missing attribute in row data: {e}")
                continue
            except Exception as e:
                logger.error(f"Error streaming event {events_streamed + 1}: {e}")
                continue
        
        self.producer.flush()
        logger.info(f"Finished streaming {events_streamed} events")
        
    def close(self):
        self.producer.close()

if __name__ == "__main__":
    producer = EcommerceProducer()
    producer.load_data()
    
    # Stream 1000 orders at 100x speed (takes ~10 seconds)
    producer.stream_orders(speed_multiplier=100, max_events=1000)
    
    producer.close()