#!/usr/bin/env python3
"""
Example script for training the ML anomaly detection model

This script demonstrates how to:
1. Load historical transaction data
2. Prepare features for ML model
3. Train Isolation Forest model
4. Evaluate model performance
5. Save the trained model
"""

import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from pyspark.sql import SparkSession
from process.ml_anomaly_detector import MLAnomalyDetector
from simulate.generate_transactions import MPesaTransactionSimulator
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def generate_training_data(num_transactions: int = 10000):
    """Generate synthetic training data"""
    logger.info(f"Generating {num_transactions} transactions for training...")
    
    simulator = MPesaTransactionSimulator(
        output_path="data/training/transactions.jsonl"
    )
    
    # Generate transactions (approximately 10 minutes of data)
    transactions = simulator.generate_batch(duration_minutes=10)
    
    logger.info(f"Generated {len(transactions)} transactions")
    return transactions


def create_spark_dataframe(spark: SparkSession, transactions):
    """Convert transactions to Spark DataFrame"""
    from pyspark.sql import Row
    from datetime import datetime
    
    rows = []
    for txn in transactions:
        rows.append(Row(
            user_id=txn.user_id,
            amount=txn.amount,
            timestamp=txn.timestamp,
            transaction_timestamp=datetime.fromisoformat(txn.timestamp),
            latitude=txn.location.lat,
            longitude=txn.location.long,
            type=txn.type
        ))
    
    # Create DataFrame
    df = spark.createDataFrame(rows)
    return df


def main():
    """Main training function"""
    logger.info("Starting ML model training...")
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("MLModelTraining") \
        .master("local[*]") \
        .getOrCreate()
    
    try:
        # Step 1: Generate or load training data
        logger.info("Step 1: Loading training data...")
        transactions = generate_training_data(num_transactions=10000)
        
        # Step 2: Convert to Spark DataFrame
        logger.info("Step 2: Converting to Spark DataFrame...")
        df = create_spark_dataframe(spark, transactions)
        df.show(5)
        
        # Step 3: Split into training and test sets
        logger.info("Step 3: Splitting data into train/test sets...")
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
        logger.info(f"Training set: {train_df.count()} transactions")
        logger.info(f"Test set: {test_df.count()} transactions")
        
        # Step 4: Initialize ML detector
        logger.info("Step 4: Initializing ML anomaly detector...")
        detector = MLAnomalyDetector(
            model_path="models/anomaly_detector.pkl",
            contamination=0.1,  # Expect 10% anomalies
            random_state=42
        )
        
        # Step 5: Prepare features
        logger.info("Step 5: Preparing features...")
        train_df_features = detector.prepare_features(train_df)
        test_df_features = detector.prepare_features(test_df)
        
        # Step 6: Train model
        logger.info("Step 6: Training Isolation Forest model...")
        detector.train_model(train_df_features, test_data=test_df_features)
        
        # Step 7: Evaluate on test set
        logger.info("Step 7: Evaluating model on test set...")
        predictions = detector.predict_anomaly_scores(test_df_features)
        
        # Show sample predictions
        logger.info("Sample predictions:")
        predictions.select(
            "user_id", "amount", "ml_anomaly_score", "is_ml_anomaly"
        ).show(20)
        
        # Step 8: Print statistics
        logger.info("Step 8: Model statistics...")
        pandas_df = predictions.toPandas()
        anomaly_rate = pandas_df['is_ml_anomaly'].mean()
        logger.info(f"Anomaly detection rate: {anomaly_rate:.2%}")
        logger.info(f"Mean anomaly score: {pandas_df['ml_anomaly_score'].mean():.4f}")
        
        # Step 9: Feature importance
        logger.info("Step 9: Feature importance:")
        importance = detector.get_feature_importance()
        for feature, score in sorted(importance.items(), key=lambda x: x[1], reverse=True):
            logger.info(f"  {feature}: {score:.3f}")
        
        logger.info("✅ Model training completed successfully!")
        logger.info(f"Model saved to: {detector.model_path}")
        
    except Exception as e:
        logger.error(f"Error during training: {e}", exc_info=True)
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

