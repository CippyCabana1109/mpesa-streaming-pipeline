#!/usr/bin/env python3
"""
M-Pesa Transaction Simulator

Generates realistic M-Pesa-like transactions with patterns and anomalies.
Simulates 1000 transactions per minute with configurable parameters.
"""

import json
import time
import random
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any
import numpy as np
from dataclasses import dataclass, asdict


@dataclass
class Location:
    """GPS location around Nairobi, Kenya"""
    lat: float
    long: float


@dataclass
class Transaction:
    """M-Pesa transaction structure"""
    user_id: str
    amount: float
    timestamp: str
    location: Location
    type: str


class MPesaTransactionSimulator:
    """Simulates realistic M-Pesa transactions with patterns and anomalies"""
    
    def __init__(self, output_path: str = "data/simulated/transactions.jsonl"):
        self.output_path = Path(output_path)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Nairobi coordinates (approximate city center)
        self.base_lat = -1.286389
        self.base_long = 36.817223
        self.geo_radius = 0.1  # ~11km radius around Nairobi
        
        # Transaction parameters
        self.amount_mean = 2740.0
        self.amount_std = 1000.0
        self.min_amount = 100.0
        self.max_users = 1000
        self.transactions_per_minute = 1000
        
        # Anomaly parameters
        self.anomaly_probability = 0.05  # 5% anomalies
        self.high_amount_threshold = 10000.0
        self.cluster_probability = 0.03  # 3% clustering
        
        # End-of-month volume boost
        self.eom_boost_start = 25  # Day of month
        self.eom_boost_factor = 2.5
        
        # Setup logging
        self.setup_logging()
        
        # Track clustering state
        self.cluster_active = False
        self.cluster_user = None
        self.cluster_remaining = 0
        
        # Statistics
        self.stats = {
            'total_transactions': 0,
            'anomalies': 0,
            'high_amount': 0,
            'clustered': 0
        }
    
    def setup_logging(self):
        """Configure logging for the simulator"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('data/simulated/simulator.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def generate_location(self) -> Location:
        """Generate random location around Nairobi"""
        lat_offset = random.uniform(-self.geo_radius, self.geo_radius)
        long_offset = random.uniform(-self.geo_radius, self.geo_radius)
        
        return Location(
            lat=self.base_lat + lat_offset,
            long=self.base_long + long_offset
        )
    
    def generate_amount(self, is_anomaly: bool = False) -> float:
        """Generate transaction amount with normal distribution"""
        if is_anomaly and random.random() < 0.7:
            # High amount anomaly
            amount = random.uniform(self.high_amount_threshold, self.high_amount_threshold * 3)
        else:
            # Normal distribution with minimum constraint
            amount = max(self.min_amount, np.random.normal(self.amount_mean, self.amount_std))
        
        return round(amount, 2)
    
    def is_end_of_month_boost(self, current_time: datetime) -> bool:
        """Check if current time should have end-of-month volume boost"""
        return current_time.day >= self.eom_boost_start
    
    def calculate_volume_multiplier(self, current_time: datetime) -> float:
        """Calculate transaction volume multiplier based on time patterns"""
        base_multiplier = 1.0
        
        # End-of-month boost
        if self.is_end_of_month_boost(current_time):
            boost_factor = 1 + (self.eom_boost_factor - 1) * (
                (current_time.day - self.eom_boost_start) / (31 - self.eom_boost_start)
            )
            base_multiplier *= boost_factor
        
        # Hourly patterns (higher during business hours)
        hour = current_time.hour
        if 8 <= hour <= 18:  # Business hours
            base_multiplier *= 1.5
        elif 19 <= hour <= 22:  # Evening
            base_multiplier *= 1.2
        else:  # Night
            base_multiplier *= 0.3
        
        return base_multiplier
    
    def handle_clustering(self) -> tuple[bool, str]:
        """Handle transaction clustering anomalies"""
        if self.cluster_active and self.cluster_remaining > 0:
            self.cluster_remaining -= 1
            if self.cluster_remaining == 0:
                self.cluster_active = False
            return True, self.cluster_user
        
        # Start new cluster
        if random.random() < self.cluster_probability:
            self.cluster_active = True
            self.cluster_user = f"user_{random.randint(1, self.max_users)}"
            self.cluster_remaining = random.randint(3, 8)  # 3-8 clustered transactions
            self.logger.info(f"Started transaction cluster for user {self.cluster_user}")
            return True, self.cluster_user
        
        return False, None
    
    def generate_transaction(self, current_time: datetime) -> Transaction:
        """Generate a single transaction"""
        # Determine if this is an anomaly
        is_anomaly = random.random() < self.anomaly_probability
        
        # Handle clustering
        is_clustered, cluster_user = self.handle_clustering()
        
        # Generate user ID
        if is_clustered and cluster_user:
            user_id = cluster_user
        else:
            user_id = f"user_{random.randint(1, self.max_users)}"
        
        # Generate amount
        amount = self.generate_amount(is_anomaly)
        
        # Generate location
        location = self.generate_location()
        
        # Create transaction
        transaction = Transaction(
            user_id=user_id,
            amount=amount,
            timestamp=current_time.isoformat(),
            location=location,
            type="transfer"
        )
        
        # Update statistics
        self.stats['total_transactions'] += 1
        if is_anomaly:
            self.stats['anomalies'] += 1
        if amount > self.high_amount_threshold:
            self.stats['high_amount'] += 1
        if is_clustered:
            self.stats['clustered'] += 1
        
        return transaction
    
    def generate_batch(self, duration_minutes: int = 1) -> List[Transaction]:
        """Generate a batch of transactions for specified duration"""
        transactions = []
        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=duration_minutes)
        
        # Calculate volume based on time patterns
        volume_multiplier = self.calculate_volume_multiplier(start_time)
        target_count = int(self.transactions_per_minute * volume_multiplier * duration_minutes)
        
        self.logger.info(f"Generating {target_count} transactions for {duration_minutes} minute(s) "
                        f"(multiplier: {volume_multiplier:.2f})")
        
        current_time = start_time
        for i in range(target_count):
            # Distribute transactions evenly across the time period
            transaction_time = start_time + timedelta(
                seconds=(i / target_count) * duration_minutes * 60
            )
            
            transaction = self.generate_transaction(transaction_time)
            transactions.append(transaction)
            
            # Log progress
            if (i + 1) % 100 == 0:
                self.logger.debug(f"Generated {i + 1}/{target_count} transactions")
        
        return transactions
    
    def save_to_jsonl(self, transactions: List[Transaction]):
        """Save transactions to JSONL file"""
        with open(self.output_path, 'a', encoding='utf-8') as f:
            for transaction in transactions:
                # Convert to dict and handle Location serialization
                transaction_dict = asdict(transaction)
                f.write(json.dumps(transaction_dict) + '\n')
        
        self.logger.info(f"Saved {len(transactions)} transactions to {self.output_path}")
    
    def print_stats(self):
        """Print generation statistics"""
        self.logger.info("=== Transaction Generation Statistics ===")
        self.logger.info(f"Total transactions: {self.stats['total_transactions']}")
        self.logger.info(f"Anomalies: {self.stats['anomalies']} ({self.stats['anomalies']/self.stats['total_transactions']*100:.1f}%)")
        self.logger.info(f"High amount (>10K): {self.stats['high_amount']} ({self.stats['high_amount']/self.stats['total_transactions']*100:.1f}%)")
        self.logger.info(f"Clustered: {self.stats['clustered']} ({self.stats['clustered']/self.stats['total_transactions']*100:.1f}%)")
    
    def run_continuous(self, duration_minutes: int = 60, batch_interval: int = 1):
        """Run continuous simulation for specified duration"""
        self.logger.info(f"Starting continuous simulation for {duration_minutes} minutes")
        
        elapsed_minutes = 0
        while elapsed_minutes < duration_minutes:
            # Generate batch
            transactions = self.generate_batch(batch_interval)
            
            # Save to file
            self.save_to_jsonl(transactions)
            
            # Wait for next batch
            time.sleep(batch_interval * 60)
            elapsed_minutes += batch_interval
        
        self.print_stats()
    
    def run_once(self, duration_minutes: int = 1):
        """Generate transactions once and exit"""
        self.logger.info(f"Generating transactions for {duration_minutes} minute(s)")
        
        transactions = self.generate_batch(duration_minutes)
        self.save_to_jsonl(transactions)
        self.print_stats()
        
        return transactions


def main():
    """Main function to run the simulator"""
    import argparse
    
    parser = argparse.ArgumentParser(description='M-Pesa Transaction Simulator')
    parser.add_argument('--duration', type=int, default=1, 
                       help='Duration in minutes to generate transactions')
    parser.add_argument('--continuous', action='store_true',
                       help='Run continuous simulation')
    parser.add_argument('--batch-interval', type=int, default=1,
                       help='Batch interval in minutes for continuous mode')
    parser.add_argument('--output', type=str, default='data/simulated/transactions.jsonl',
                       help='Output file path')
    
    args = parser.parse_args()
    
    # Create simulator
    simulator = MPesaTransactionSimulator(args.output)
    
    try:
        if args.continuous:
            simulator.run_continuous(args.duration, args.batch_interval)
        else:
            simulator.run_once(args.duration)
    
    except KeyboardInterrupt:
        simulator.logger.info("Simulation interrupted by user")
        simulator.print_stats()
    except Exception as e:
        simulator.logger.error(f"Simulation failed: {e}")
        raise


if __name__ == "__main__":
    main()
