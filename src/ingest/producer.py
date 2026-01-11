#!/usr/bin/env python3
"""
Kafka Producer for M-Pesa Transactions

Produces M-Pesa transaction data to Kafka topic 'mpesa-transactions'.
Can read from simulated JSONL files or generate transactions on-the-fly.
"""

import json
import time
import random
import logging
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, Iterator
import numpy as np

from kafka import KafkaProducer, KafkaError
from kafka.errors import KafkaTimeoutError, KafkaConnectionError


class MPesaKafkaProducer:
    """Kafka producer for M-Pesa transactions with error handling and location randomization"""
    
    def __init__(self, 
                 bootstrap_servers: str = 'localhost:9092',
                 topic: str = 'mpesa-transactions',
                 transactions_per_second: int = 1):
        
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.transactions_per_second = transactions_per_second
        
        # Nairobi coordinates for location randomization
        self.base_lat = -1.286389
        self.base_long = 36.817223
        self.geo_radius = 0.1  # ~11km radius around Nairobi
        
        # Setup logging
        self.setup_logging()
        
        # Initialize Kafka producer
        self.producer = None
        self.setup_producer()
        
        # Statistics
        self.stats = {
            'messages_sent': 0,
            'messages_failed': 0,
            'start_time': None
        }
    
    def setup_logging(self):
        """Configure logging for the producer"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('data/simulated/producer.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def setup_producer(self):
        """Initialize Kafka producer with error handling"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',  # Wait for all replicas
                retries=3,
                retry_backoff_ms=100,
                request_timeout_ms=30000,
                compression_type='gzip',
                batch_size=16384,
                linger_ms=10,
                buffer_memory=33554432,
                max_block_ms=60000
            )
            self.logger.info(f"Kafka producer initialized for {self.bootstrap_servers}")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka producer: {e}")
            raise
    
    def randomize_location(self, location: Dict[str, float]) -> Dict[str, float]:
        """Randomize location within 0.1 degrees of Nairobi"""
        # Add random offset within the specified radius
        lat_offset = random.uniform(-self.geo_radius, self.geo_radius)
        long_offset = random.uniform(-self.geo_radius, self.geo_radius)
        
        return {
            'lat': self.base_lat + lat_offset,
            'long': self.base_long + long_offset
        }
    
    def generate_transaction_on_the_fly(self) -> Dict[str, Any]:
        """Generate a single transaction on-the-fly"""
        # Generate realistic amount
        amount_mean = 2740.0
        amount_std = 1000.0
        min_amount = 100.0
        
        amount = max(min_amount, np.random.normal(amount_mean, amount_std))
        amount = round(amount, 2)
        
        # Random user ID
        user_id = f"user_{random.randint(1, 1000)}"
        
        # Randomized location
        location = self.randomize_location({})
        
        # Current timestamp
        timestamp = datetime.now().isoformat()
        
        transaction = {
            'user_id': user_id,
            'amount': amount,
            'timestamp': timestamp,
            'location': location,
            'type': 'transfer'
        }
        
        return transaction
    
    def read_transactions_from_file(self, file_path: str) -> Iterator[Dict[str, Any]]:
        """Read transactions from JSONL file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        transaction = json.loads(line.strip())
                        # Randomize location for each transaction
                        transaction['location'] = self.randomize_location(transaction['location'])
                        yield transaction
                    except json.JSONDecodeError as e:
                        self.logger.warning(f"Failed to parse JSON line: {e}")
                        continue
        except FileNotFoundError:
            self.logger.error(f"Transaction file not found: {file_path}")
            raise
        except Exception as e:
            self.logger.error(f"Error reading transaction file: {e}")
            raise
    
    def send_transaction(self, transaction: Dict[str, Any], key: Optional[str] = None) -> bool:
        """Send a single transaction to Kafka with error handling"""
        try:
            # Use user_id as partition key for better distribution
            message_key = key or transaction.get('user_id')
            
            # Send message
            future = self.producer.send(
                topic=self.topic,
                value=transaction,
                key=message_key,
                timestamp=datetime.now().timestamp() * 1000  # milliseconds
            )
            
            # Block for acknowledgment
            record_metadata = future.get(timeout=10)
            
            self.stats['messages_sent'] += 1
            
            self.logger.debug(
                f"Sent transaction {transaction.get('user_id')} to "
                f"partition {record_metadata.partition}, offset {record_metadata.offset}"
            )
            
            return True
            
        except KafkaTimeoutError:
            self.logger.error("Kafka producer timeout - message may not have been sent")
            self.stats['messages_failed'] += 1
            return False
            
        except KafkaConnectionError:
            self.logger.error("Kafka connection error - cannot send message")
            self.stats['messages_failed'] += 1
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to send transaction: {e}")
            self.stats['messages_failed'] += 1
            return False
    
    def create_topic_if_not_exists(self):
        """Create Kafka topic if it doesn't exist"""
        try:
            from kafka.admin import KafkaAdminClient, NewTopic
            
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers
            )
            
            # Check if topic exists
            existing_topics = admin_client.list_topics()
            
            if self.topic not in existing_topics:
                # Create topic
                topic = NewTopic(
                    name=self.topic,
                    num_partitions=3,
                    replication_factor=1
                )
                
                admin_client.create_topics([topic])
                self.logger.info(f"Created Kafka topic: {self.topic}")
            else:
                self.logger.info(f"Kafka topic already exists: {self.topic}")
                
            admin_client.close()
            
        except ImportError:
            self.logger.warning("kafka-admin not available - cannot create topic automatically")
        except Exception as e:
            self.logger.warning(f"Failed to create topic {self.topic}: {e}")
    
    def run_from_file(self, file_path: str, loop: bool = False):
        """Run producer reading from JSONL file"""
        self.logger.info(f"Starting producer from file: {file_path}")
        
        # Create topic if needed
        self.create_topic_if_not_exists()
        
        self.stats['start_time'] = time.time()
        
        while True:
            try:
                transactions_sent = 0
                
                for transaction in self.read_transactions_from_file(file_path):
                    success = self.send_transaction(transaction)
                    
                    if success:
                        transactions_sent += 1
                    
                    # Rate limiting
                    time.sleep(1.0 / self.transactions_per_second)
                
                self.logger.info(f"Completed file processing. Sent {transactions_sent} transactions.")
                
                if not loop:
                    break
                    
                self.logger.info("Looping back to start of file...")
                time.sleep(5)  # Brief pause before looping
                
            except KeyboardInterrupt:
                self.logger.info("Producer stopped by user")
                break
            except Exception as e:
                self.logger.error(f"Error in producer loop: {e}")
                time.sleep(5)  # Wait before retrying
        
        self.print_stats()
    
    def run_on_the_fly(self, duration_minutes: Optional[int] = None):
        """Run producer generating transactions on-the-fly"""
        self.logger.info(f"Starting on-the-fly producer at {self.transactions_per_second} txn/sec")
        
        # Create topic if needed
        self.create_topic_if_not_exists()
        
        self.stats['start_time'] = time.time()
        end_time = None
        
        if duration_minutes:
            end_time = time.time() + (duration_minutes * 60)
            self.logger.info(f"Will run for {duration_minutes} minutes")
        
        try:
            while True:
                # Check duration limit
                if end_time and time.time() >= end_time:
                    self.logger.info("Duration limit reached, stopping producer")
                    break
                
                # Generate and send transaction
                transaction = self.generate_transaction_on_the_fly()
                self.send_transaction(transaction)
                
                # Rate limiting
                time.sleep(1.0 / self.transactions_per_second)
                
        except KeyboardInterrupt:
            self.logger.info("Producer stopped by user")
        
        self.print_stats()
    
    def print_stats(self):
        """Print producer statistics"""
        if self.stats['start_time']:
            elapsed_time = time.time() - self.stats['start_time']
            rate = self.stats['messages_sent'] / elapsed_time if elapsed_time > 0 else 0
            
            self.logger.info("=== Producer Statistics ===")
            self.logger.info(f"Messages sent: {self.stats['messages_sent']}")
            self.logger.info(f"Messages failed: {self.stats['messages_failed']}")
            self.logger.info(f"Success rate: {(self.stats['messages_sent']/(self.stats['messages_sent']+self.stats['messages_failed'])*100):.1f}%")
            self.logger.info(f"Average rate: {rate:.2f} messages/second")
            self.logger.info(f"Elapsed time: {elapsed_time:.1f} seconds")
    
    def close(self):
        """Close the Kafka producer"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            self.logger.info("Kafka producer closed")


def main():
    """Main function to run the Kafka producer"""
    parser = argparse.ArgumentParser(description='M-Pesa Kafka Transaction Producer')
    parser.add_argument('--mode', choices=['file', 'generate'], default='generate',
                       help='Mode: file (read from JSONL) or generate (on-the-fly)')
    parser.add_argument('--file', type=str, default='data/simulated/transactions.jsonl',
                       help='JSONL file path (for file mode)')
    parser.add_argument('--bootstrap-servers', type=str, default='localhost:9092',
                       help='Kafka bootstrap servers')
    parser.add_argument('--topic', type=str, default='mpesa-transactions',
                       help='Kafka topic name')
    parser.add_argument('--rate', type=int, default=1,
                       help='Transactions per second')
    parser.add_argument('--duration', type=int, 
                       help='Duration in minutes (for generate mode)')
    parser.add_argument('--loop', action='store_true',
                       help='Loop file continuously (for file mode)')
    
    args = parser.parse_args()
    
    # Create producer
    producer = MPesaKafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        transactions_per_second=args.rate
    )
    
    try:
        if args.mode == 'file':
            producer.run_from_file(args.file, loop=args.loop)
        else:
            producer.run_on_the_fly(duration_minutes=args.duration)
    
    except Exception as e:
        logging.error(f"Producer failed: {e}")
        raise
    finally:
        producer.close()


if __name__ == "__main__":
    main()
