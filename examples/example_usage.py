#!/usr/bin/env python3
"""
Example usage of the M-Pesa Streaming Pipeline

This script demonstrates the complete pipeline workflow:
1. Generate transactions
2. Send to Kafka
3. Process with Spark
4. Store in Elasticsearch
5. Query results
"""

import time
import json
from pathlib import Path
import sys

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from simulate.generate_transactions import MPesaTransactionSimulator
from ingest.producer import MPesaKafkaProducer
from elasticsearch import Elasticsearch
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def example_1_generate_transactions():
    """Example 1: Generate sample transactions"""
    logger.info("=== Example 1: Generating Transactions ===")
    
    simulator = MPesaTransactionSimulator(
        output_path="data/examples/transactions.jsonl"
    )
    
    # Generate 1 minute of transactions
    transactions = simulator.run_once(duration_minutes=1)
    
    logger.info(f"Generated {len(transactions)} transactions")
    simulator.print_stats()
    
    return transactions


def example_2_send_to_kafka():
    """Example 2: Send transactions to Kafka"""
    logger.info("=== Example 2: Sending to Kafka ===")
    
    producer = MPesaKafkaProducer(
        bootstrap_servers='localhost:9092',
        topic='mpesa-transactions',
        transactions_per_second=5
    )
    
    # Send transactions from file
    producer.run_from_file(
        file_path='data/examples/transactions.jsonl',
        loop=False
    )
    
    producer.print_stats()
    producer.close()


def example_3_query_elasticsearch():
    """Example 3: Query Elasticsearch for results"""
    logger.info("=== Example 3: Querying Elasticsearch ===")
    
    es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])
    
    if not es.ping():
        logger.error("Elasticsearch is not available")
        return
    
    # Query flagged transactions
    query = {
        "query": {
            "term": {"document_type": "flagged_transaction"}
        },
        "size": 10
    }
    
    response = es.search(index="mpesa-insights", body=query)
    
    logger.info(f"Found {response['hits']['total']['value']} flagged transactions")
    
    for hit in response['hits']['hits']:
        doc = hit['_source']
        logger.info(
            f"Flagged: User {doc['user_id']}, "
            f"Amount {doc['amount']}, "
            f"High Amount: {doc.get('is_high_amount', False)}, "
            f"Location Outlier: {doc.get('is_location_outlier', False)}"
        )


def example_4_fraud_detection_rules():
    """Example 4: Demonstrate fraud detection rules"""
    logger.info("=== Example 4: Fraud Detection Rules ===")
    
    from process.stream_processor import MPesaStreamProcessor
    
    processor = MPesaStreamProcessor()
    
    # Example transactions
    test_cases = [
        {
            "name": "Normal transaction",
            "amount": 1000.0,
            "lat": -1.286,
            "long": 36.817
        },
        {
            "name": "High amount fraud",
            "amount": 15000.0,
            "lat": -1.286,
            "long": 36.817
        },
        {
            "name": "Location outlier",
            "amount": 1000.0,
            "lat": -4.0435,  # Mombasa
            "long": 39.6682
        },
        {
            "name": "Both fraud types",
            "amount": 20000.0,
            "lat": -4.0435,
            "long": 39.6682
        }
    ]
    
    for case in test_cases:
        distance = processor.calculate_distance_from_nairobi(
            case["lat"], case["long"]
        )
        is_high_amount = case["amount"] > 10000
        is_location_outlier = distance > processor.distance_threshold_km
        is_flagged = is_high_amount or is_location_outlier
        
        logger.info(f"\n{case['name']}:")
        logger.info(f"  Amount: {case['amount']}")
        logger.info(f"  Distance from Nairobi: {distance:.2f} km")
        logger.info(f"  High Amount: {is_high_amount}")
        logger.info(f"  Location Outlier: {is_location_outlier}")
        logger.info(f"  Flagged: {is_flagged}")


def example_5_ml_anomaly_detection():
    """Example 5: ML-based anomaly detection"""
    logger.info("=== Example 5: ML Anomaly Detection ===")
    
    from process.ml_anomaly_detector import MLAnomalyDetector
    
    detector = MLAnomalyDetector(
        model_path="models/anomaly_detector.pkl"
    )
    
    # Try to load existing model
    if detector.load_model():
        logger.info("✅ Loaded pre-trained model")
        
        # Example transaction
        example_transaction = {
            "user_id": "user_123",
            "amount": 15000.0,
            "timestamp": "2024-01-15T10:00:00",
            "location": {"lat": -1.286, "long": 36.817},
            "type": "transfer",
            "transaction_timestamp": "2024-01-15T10:00:00"
        }
        
        # Explain anomaly
        explanation = detector.explain_anomaly(example_transaction)
        
        logger.info(f"Transaction Analysis:")
        logger.info(f"  Is Anomaly: {explanation.get('is_anomaly', False)}")
        logger.info(f"  Anomaly Score: {explanation.get('anomaly_score', 0):.4f}")
        logger.info(f"  Explanations: {explanation.get('explanations', [])}")
    else:
        logger.warning("⚠️  No pre-trained model found. Run train_ml_model.py first.")


def main():
    """Run all examples"""
    logger.info("🚀 M-Pesa Streaming Pipeline - Example Usage\n")
    
    try:
        # Example 1: Generate transactions
        example_1_generate_transactions()
        time.sleep(2)
        
        # Example 2: Send to Kafka (requires Kafka running)
        # Uncomment if Kafka is available
        # example_2_send_to_kafka()
        # time.sleep(2)
        
        # Example 3: Query Elasticsearch (requires Elasticsearch running)
        # Uncomment if Elasticsearch is available
        # example_3_query_elasticsearch()
        # time.sleep(2)
        
        # Example 4: Fraud detection rules
        example_4_fraud_detection_rules()
        time.sleep(2)
        
        # Example 5: ML anomaly detection
        example_5_ml_anomaly_detection()
        
        logger.info("\n✅ All examples completed!")
        
    except Exception as e:
        logger.error(f"Error running examples: {e}", exc_info=True)


if __name__ == "__main__":
    main()

