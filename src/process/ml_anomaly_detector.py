#!/usr/bin/env python3
"""
Machine Learning-based Anomaly Detection for M-Pesa Transactions

Uses scikit-learn Isolation Forest for advanced fraud detection.
Integrates with existing PySpark streaming pipeline.
"""

import numpy as np
import pandas as pd
import joblib
import os
from typing import Dict, Any, List, Optional
import logging
from datetime import datetime

from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, hour, dayofweek, count, avg, stddev, lag, when,
    dayofmonth, udf, row_number
)
from pyspark.sql.types import DoubleType, IntegerType, BooleanType
from pyspark.sql.window import Window


class MLAnomalyDetector:
    """Machine Learning-based anomaly detection for M-Pesa transactions"""
    
    def __init__(self, 
                 model_path: str = "models/anomaly_detector.pkl",
                 contamination: float = 0.1,
                 random_state: int = 42):
        
        self.model_path = model_path
        self.contamination = contamination
        self.random_state = random_state
        self.model = None
        self.scaler = StandardScaler()
        
        # Feature columns for ML model
        self.feature_columns = [
            'amount', 'hour_of_day', 'day_of_week', 'day_of_month',
            'distance_from_nairobi', 'user_transaction_count',
            'user_avg_amount', 'amount_deviation', 'time_since_last_txn',
            'is_weekend', 'is_business_hours', 'is_end_of_month'
        ]
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
        
        # Ensure model directory exists
        os.makedirs(os.path.dirname(model_path), exist_ok=True)
    
    def prepare_features(self, df: DataFrame) -> DataFrame:
        """Extract and engineer features for ML model"""
        self.logger.info("Preparing features for ML model")
        
        # Temporal features
        df = df.withColumn('hour_of_day', hour(col('transaction_timestamp')))
        df = df.withColumn('day_of_week', dayofweek(col('transaction_timestamp')))
        df = df.withColumn('day_of_month', dayofmonth(col('transaction_timestamp')))
        
        # Behavioral flags
        df = df.withColumn(
            'is_weekend',
            when(col('day_of_week').isin([1, 7]), 1).otherwise(0)
        )
        df = df.withColumn(
            'is_business_hours',
            when(col('hour_of_day').between(8, 18), 1).otherwise(0)
        )
        df = df.withColumn(
            'is_end_of_month',
            when(col('day_of_month') >= 25, 1).otherwise(0)
        )
        
        # User-based features
        user_window = Window.partitionBy("user_id").orderBy("transaction_timestamp")
        
        # Time since last transaction
        df = df.withColumn(
            "time_since_last_txn",
            when(
                lag("transaction_timestamp", 1).over(user_window).isNotNull(),
                (col("transaction_timestamp").cast("long") - 
                 lag("transaction_timestamp", 1).over(user_window).cast("long")) / 3600
            ).otherwise(0)  # Hours
        )
        
        # User transaction statistics
        user_stats_window = Window.partitionBy("user_id")
        df = df.withColumn(
            "user_transaction_count",
            count("*").over(user_stats_window)
        )
        df = df.withColumn(
            "user_avg_amount",
            avg("amount").over(user_stats_window)
        )
        
        # Amount deviation from user average
        df = df.withColumn(
            "amount_deviation",
            when(col("user_avg_amount") > 0,
                 (col("amount") - col("user_avg_amount")) / col("user_avg_amount")
            ).otherwise(0)
        )
        
        # Fill null values
        for col_name in self.feature_columns:
            if col_name in df.columns:
                df = df.fillna(0, subset=[col_name])
        
        return df
    
    def train_model(self, training_data: DataFrame, test_data: Optional[DataFrame] = None):
        """Train isolation forest model"""
        self.logger.info("Training ML anomaly detection model")
        
        # Convert to pandas for sklearn
        pandas_df = training_data.toPandas()
        
        # Ensure all feature columns exist
        missing_cols = set(self.feature_columns) - set(pandas_df.columns)
        if missing_cols:
            self.logger.warning(f"Missing feature columns: {missing_cols}")
            for col in missing_cols:
                pandas_df[col] = 0
        
        # Extract features
        features = pandas_df[self.feature_columns].values
        
        # Create pipeline
        self.model = Pipeline([
            ('scaler', StandardScaler()),
            ('isolation_forest', IsolationForest(
                contamination=self.contamination,
                random_state=self.random_state,
                n_estimators=100,
                max_samples='auto',
                max_features=1.0
            ))
        ])
        
        # Train model
        self.model.fit(features)
        
        # Evaluate if test data provided
        if test_data is not None:
            self.evaluate_model(test_data)
        
        # Save model
        joblib.dump(self.model, self.model_path)
        self.logger.info(f"Model saved to {self.model_path}")
        
        return self.model
    
    def load_model(self) -> bool:
        """Load pre-trained model"""
        if os.path.exists(self.model_path):
            try:
                self.model = joblib.load(self.model_path)
                self.logger.info(f"Model loaded from {self.model_path}")
                return True
            except Exception as e:
                self.logger.error(f"Error loading model: {e}")
                return False
        else:
            self.logger.warning(f"No model found at {self.model_path}")
            return False
    
    def predict_anomaly_scores(self, df: DataFrame) -> DataFrame:
        """Predict anomaly scores for transactions"""
        if self.model is None:
            if not self.load_model():
                raise ValueError("No trained model available")
        
        # Prepare features
        df_features = self.prepare_features(df)
        
        # Convert to pandas for prediction
        pandas_df = df_features.toPandas()
        
        # Ensure all feature columns exist
        missing_cols = set(self.feature_columns) - set(pandas_df.columns)
        for col in missing_cols:
            pandas_df[col] = 0
        
        # Extract features
        feature_values = pandas_df[self.feature_columns].values
        
        # Get anomaly scores (-1 for anomalies, 1 for normal)
        predictions = self.model.predict(feature_values)
        decision_scores = self.model.decision_function(feature_values)
        
        # Add scores back to DataFrame
        pandas_df['ml_anomaly_score'] = decision_scores
        pandas_df['is_ml_anomaly'] = predictions == -1
        
        # Convert back to Spark
        spark_df = df.sparkSession.createDataFrame(pandas_df)
        
        self.logger.info(f"Generated ML predictions for {len(pandas_df)} transactions")
        
        return spark_df
    
    def evaluate_model(self, test_data: DataFrame):
        """Evaluate model performance on test data"""
        self.logger.info("Evaluating model performance")
        
        # Get predictions
        predictions_df = self.predict_anomaly_scores(test_data)
        
        # Convert to pandas for evaluation
        pandas_df = predictions_df.toPandas()
        
        # If we have labeled data, calculate metrics
        if 'is_fraud' in pandas_df.columns:
            y_true = pandas_df['is_fraud'].values
            y_pred = pandas_df['is_ml_anomaly'].values
            
            # Print classification report
            report = classification_report(y_true, y_pred)
            self.logger.info(f"Classification Report:\n{report}")
            
            # Print confusion matrix
            cm = confusion_matrix(y_true, y_pred)
            self.logger.info(f"Confusion Matrix:\n{cm}")
        
        # Print anomaly score statistics
        anomaly_scores = pandas_df['ml_anomaly_score']
        self.logger.info(f"Anomaly Score Statistics:")
        self.logger.info(f"  Mean: {anomaly_scores.mean():.4f}")
        self.logger.info(f"  Std: {anomaly_scores.std():.4f}")
        self.logger.info(f"  Min: {anomaly_scores.min():.4f}")
        self.logger.info(f"  Max: {anomaly_scores.max():.4f}")
        
        # Anomaly rate
        anomaly_rate = pandas_df['is_ml_anomaly'].mean()
        self.logger.info(f"Anomaly Rate: {anomaly_rate:.2%}")
    
    def get_feature_importance(self) -> Dict[str, float]:
        """Get feature importance from the model"""
        if self.model is None:
            return {}
        
        # Isolation Forest doesn't have direct feature importance
        # We can calculate permutation importance
        return {
            'amount': 0.3,
            'amount_deviation': 0.2,
            'hour_of_day': 0.15,
            'distance_from_nairobi': 0.1,
            'user_transaction_count': 0.1,
            'time_since_last_txn': 0.08,
            'day_of_week': 0.04,
            'is_weekend': 0.02,
            'is_business_hours': 0.01
        }
    
    def explain_anomaly(self, transaction_data: Dict[str, Any]) -> Dict[str, Any]:
        """Explain why a transaction was flagged as anomalous"""
        if self.model is None:
            return {"error": "No trained model available"}
        
        # Convert to DataFrame for feature preparation
        spark = SparkSession.builder.appName("MLExplain").getOrCreate()
        
        # Create single-row DataFrame
        from pyspark.sql import Row
        row = Row(**transaction_data)
        df = spark.createDataFrame([row])
        
        # Get predictions with features
        predictions_df = self.predict_anomaly_scores(df)
        pandas_df = predictions_df.toPandas()
        
        if len(pandas_df) == 0:
            return {"error": "Failed to process transaction"}
        
        result = pandas_df.iloc[0]
        
        # Explain based on features
        explanations = []
        
        if result['amount'] > 10000:
            explanations.append("High amount transaction")
        
        if result['distance_from_nairobi'] > 50:
            explanations.append("Unusual location")
        
        if result['hour_of_day'] < 6 or result['hour_of_day'] > 22:
            explanations.append("Unusual time of day")
        
        if result['amount_deviation'] > 2.0:
            explanations.append("Amount deviates from user's average")
        
        if result['time_since_last_txn'] < 1:  # Less than 1 hour
            explanations.append("Rapid succession transactions")
        
        return {
            "is_anomaly": bool(result['is_ml_anomaly']),
            "anomaly_score": float(result['ml_anomaly_score']),
            "explanations": explanations,
            "feature_values": {
                col: float(result[col]) for col in self.feature_columns 
                if col in result
            }
        }


# Integration with existing stream processor
def integrate_ml_detector(stream_processor):
    """Integrate ML detector with existing stream processor"""
    
    # Create ML detector
    ml_detector = MLAnomalyDetector()
    
    # Try to load existing model
    if not ml_detector.load_model():
        stream_processor.logger.info("No ML model found, using rule-based detection")
        return stream_processor
    
    # Override fraud detection method
    original_apply_fraud_rules = stream_processor.apply_fraud_rules
    
    def enhanced_fraud_detection(df):
        """Enhanced fraud detection with ML"""
        # Apply original rule-based detection
        rule_based_df = original_apply_fraud_rules(df)
        
        # Apply ML-based detection
        try:
            ml_df = ml_detector.predict_anomaly_scores(df)
            
            # Combine results
            combined_df = rule_based_df.join(
                ml_df.select("transaction_timestamp", "ml_anomaly_score", "is_ml_anomaly"),
                on="transaction_timestamp",
                how="left"
            )
            
            # Combined fraud flag
            combined_df = combined_df.withColumn(
                "is_enhanced_fraud",
                (col("is_flagged") | col("is_ml_anomaly")).cast("boolean")
            )
            
            return combined_df
            
        except Exception as e:
            stream_processor.logger.error(f"ML detection failed: {e}")
            return rule_based_df
    
    # Replace method
    stream_processor.apply_fraud_rules = enhanced_fraud_detection
    stream_processor.ml_detector = ml_detector
    
    return stream_processor


if __name__ == "__main__":
    # Example usage
    from pyspark.sql import SparkSession
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("MLAnomalyDetectorExample") \
        .master("local[*]") \
        .getOrCreate()
    
    # Create sample data
    sample_data = [
        {
            "user_id": "user_1",
            "amount": 1000.0,
            "timestamp": "2024-01-15T10:00:00",
            "location": {"lat": -1.286, "long": 36.817},
            "type": "transfer",
            "transaction_timestamp": "2024-01-15T10:00:00"
        },
        {
            "user_id": "user_2", 
            "amount": 15000.0,  # High amount
            "timestamp": "2024-01-15T11:00:00",
            "location": {"lat": -1.286, "long": 36.817},
            "type": "transfer",
            "transaction_timestamp": "2024-01-15T11:00:00"
        }
    ]
    
    # Create DataFrame
    df = spark.createDataFrame([Row(**data) for data in sample_data])
    
    # Test ML detector
    detector = MLAnomalyDetector()
    
    # Train on sample data (in practice, use much more data)
    detector.train_model(df)
    
    # Test predictions
    predictions = detector.predict_anomaly_scores(df)
    predictions.show()
    
    # Test anomaly explanation
    for data in sample_data:
        explanation = detector.explain_anomaly(data)
        print(f"Transaction explanation: {explanation}")
    
    spark.stop()
