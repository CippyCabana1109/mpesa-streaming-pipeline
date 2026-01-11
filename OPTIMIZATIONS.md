# M-Pesa Streaming Pipeline - Optimization Guide

## Overview

This guide outlines advanced optimizations for the M-Pesa streaming pipeline, including machine learning-based anomaly detection and Kubernetes scaling strategies.

## 🤖 Machine Learning Anomaly Detection

### Current Limitations

The current fraud detection uses rule-based approaches:
- Fixed amount thresholds (>10,000 KES)
- Simple frequency counting (>5 transactions/window)
- Basic geographic distance rules (>50km from Nairobi)

### Enhanced ML-Based Detection

#### 1. Isolation Forest Implementation

```python
# src/process/ml_anomaly_detector.py
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DoubleType
import joblib
import os

class MLAnomalyDetector:
    """Machine Learning-based anomaly detection for M-Pesa transactions"""
    
    def __init__(self, model_path: str = "models/anomaly_detector.pkl"):
        self.model_path = model_path
        self.model = None
        self.scaler = StandardScaler()
        self.feature_columns = [
            'amount', 'hour_of_day', 'day_of_week', 
            'distance_from_nairobi', 'user_transaction_count'
        ]
        
    def prepare_features(self, df):
        """Extract features for ML model"""
        from pyspark.sql.functions import hour, dayofweek, count
        
        # Add temporal features
        df = df.withColumn('hour_of_day', hour(col('transaction_timestamp')))
        df = df.withColumn('day_of_week', dayofweek(col('transaction_timestamp')))
        
        # Add user transaction frequency
        user_counts = df.groupBy('user_id').count()
        df = df.join(user_counts, on='user_id', how='left')
        df = df.withColumnRenamed('count', 'user_transaction_count')
        
        return df
    
    def train_model(self, training_data):
        """Train isolation forest model"""
        # Convert to pandas for sklearn
        features = training_data[self.feature_columns].values
        
        # Create pipeline
        self.model = Pipeline([
            ('scaler', StandardScaler()),
            ('isolation_forest', IsolationForest(
                contamination=0.1,  # Expected anomaly rate
                random_state=42,
                n_estimators=100
            ))
        ])
        
        # Train model
        self.model.fit(features)
        
        # Save model
        os.makedirs(os.path.dirname(self.model_path), exist_ok=True)
        joblib.dump(self.model, self.model_path)
        
        return self.model
    
    def load_model(self):
        """Load pre-trained model"""
        if os.path.exists(self.model_path):
            self.model = joblib.load(self.model_path)
            return True
        return False
    
    def predict_anomaly_score(self, features):
        """Predict anomaly scores"""
        if self.model is None:
            return 0.0
        
        # Predict (-1 for anomalies, 1 for normal)
        predictions = self.model.decision_function(features)
        return predictions
    
    def apply_ml_detection(self, df):
        """Apply ML-based anomaly detection to Spark DataFrame"""
        # Prepare features
        df_features = self.prepare_features(df)
        
        # Convert to pandas for prediction
        pandas_df = df_features.toPandas()
        feature_values = pandas_df[self.feature_columns].values
        
        # Get anomaly scores
        anomaly_scores = self.predict_anomaly_score(feature_values)
        
        # Add scores back to DataFrame
        pandas_df['ml_anomaly_score'] = anomaly_scores
        pandas_df['is_ml_anomaly'] = anomaly_scores < -0.1  # Threshold
        
        # Convert back to Spark
        spark_df = self.spark.createDataFrame(pandas_df)
        
        return spark_df
```

#### 2. Enhanced Feature Engineering

```python
# src/process/feature_engineering.py
from pyspark.sql.functions import col, when, lag, avg, stddev
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

class FeatureEngineer:
    """Advanced feature engineering for fraud detection"""
    
    def add_behavioral_features(self, df):
        """Add user behavioral patterns"""
        # User transaction window
        user_window = Window.partitionBy("user_id").orderBy("transaction_timestamp")
        
        # Time since last transaction
        df = df.withColumn(
            "time_since_last_txn",
            col("transaction_timestamp").cast("long") - 
            lag("transaction_timestamp", 1).over(user_window).cast("long")
        )
        
        # User average transaction amount
        user_avg_window = Window.partitionBy("user_id")
        df = df.withColumn(
            "user_avg_amount",
            avg("amount").over(user_avg_window)
        )
        
        # Amount deviation from user average
        df = df.withColumn(
            "amount_deviation",
            (col("amount") - col("user_avg_amount")) / col("user_avg_amount")
        )
        
        return df
    
    def add_temporal_features(self, df):
        """Add temporal pattern features"""
        # Weekend flag
        df = df.withColumn(
            "is_weekend",
            when(dayofweek(col("transaction_timestamp")).isin([1, 7]), 1).otherwise(0)
        )
        
        # Business hours flag
        df = df.withColumn(
            "is_business_hours",
            when(hour(col("transaction_timestamp")).between(8, 18), 1).otherwise(0)
        )
        
        # End-of-month flag
        df = df.withColumn(
            "is_end_of_month",
            when(dayofmonth(col("transaction_timestamp")) >= 25, 1).otherwise(0)
        )
        
        return df
    
    def add_network_features(self, df):
        """Add network-based features"""
        # Common recipient pattern detection
        recipient_window = Window.partitionBy("user_id", "recipient_id")
        df = df.withColumn(
            "recipient_frequency",
            count("*").over(recipient_window)
        )
        
        # New recipient flag
        df = df.withColumn(
            "is_new_recipient",
            when(col("recipient_frequency") == 1, 1).otherwise(0)
        )
        
        return df
```

#### 3. Real-Time Model Updates

```python
# src/process/model_updater.py
import time
from datetime import datetime, timedelta
from sklearn.metrics import classification_report
import mlflow
import mlflow.sklearn

class ModelUpdater:
    """Real-time model updating and monitoring"""
    
    def __init__(self, update_interval_hours: int = 24):
        self.update_interval = timedelta(hours=update_interval_hours)
        self.last_update = datetime.now()
        self.performance_threshold = 0.85
        
    def should_update_model(self):
        """Check if model should be updated"""
        return datetime.now() - self.last_update > self.update_interval
    
    def collect_training_data(self, spark_df):
        """Collect recent labeled data for retraining"""
        # Get recent transactions with human labels
        recent_data = spark_df.filter(
            col("transaction_timestamp") > 
            (datetime.now() - timedelta(days=7))
        )
        
        return recent_data
    
    def evaluate_model_performance(self, model, test_data):
        """Evaluate current model performance"""
        predictions = model.predict(test_data)
        
        # Calculate metrics
        accuracy = accuracy_score(test_labels, predictions)
        precision = precision_score(test_labels, predictions)
        recall = recall_score(test_labels, predictions)
        
        return {
            'accuracy': accuracy,
            'precision': precision,
            'recall': recall,
            'f1_score': f1_score(test_labels, predictions)
        }
    
    def update_model_if_needed(self, current_model, new_data):
        """Update model if performance degrades"""
        if not self.should_update_model():
            return current_model
        
        # Evaluate current model
        current_performance = self.evaluate_model_performance(
            current_model, new_data
        )
        
        # Retrain if performance below threshold
        if current_performance['accuracy'] < self.performance_threshold:
            new_model = self.retrain_model(new_data)
            
            # Log to MLflow
            with mlflow.start_run():
                mlflow.log_metrics(current_performance)
                mlflow.sklearn.log_model(new_model, "anomaly_detector")
            
            self.last_update = datetime.now()
            return new_model
        
        return current_model
```

## ☸️ Kubernetes Scaling Architecture

### Current Architecture Limitations

- Single-node deployment
- Manual scaling
- Limited fault tolerance
- No auto-scaling capabilities

### Kubernetes Deployment Strategy

#### 1. Container Strategy

```yaml
# k8s/namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: mpesa-pipeline
---
# k8s/configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: mpesa-config
  namespace: mpesa-pipeline
data:
  KAFKA_BOOTSTRAP_SERVERS: "kafka-service:9092"
  ELASTICSEARCH_HOSTS: "elasticsearch-service:9200"
  SPARK_MASTER_URL: "spark://spark-master-service:7077"
```

#### 2. Kafka Cluster Deployment

```yaml
# k8s/kafka-cluster.yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: mpesa-kafka
  namespace: mpesa-pipeline
spec:
  kafka:
    version: 3.5.0
    replicas: 3
    listeners:
      - name: plain
        port: 9092
        type: internal
        tls: false
    storage:
      type: persistent-claim
      size: 100Gi
      class: fast-ssd
    config:
      num.partitions: 12
      num.recovery.threads.per.data.dir: 4
      transaction.state.log.replication.factor: 3
      transaction.state.log.min.isr: 2
  zookeeper:
    replicas: 3
    storage:
      type: persistent-claim
      size: 10Gi
  entityOperator:
    topicOperator: {}
    userOperator: {}
```

#### 3. Spark on Kubernetes

```yaml
# k8s/spark-cluster.yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: mpesa-stream-processor
  namespace: mpesa-pipeline
spec:
  type: Scala
  mode: cluster
  image: "mpesa/stream-processor:latest"
  imagePullPolicy: Always
  mainClass: "org.apache.spark.examples.SparkPi"
  mainApplicationFile: "local:///opt/spark/examples/jars/spark-examples_2.12-3.5.0.jar"
  sparkConf:
    "spark.kubernetes.namespace": "mpesa-pipeline"
    "spark.kubernetes.container.image": "mpesa/stream-processor:latest"
    "spark.kubernetes.driver.pod.name": "mpesa-stream-driver"
    "spark.executor.instances": "4"
    "spark.executor.cores": "2"
    "spark.executor.memory": "4g"
    "spark.driver.cores": "2"
    "spark.driver.memory": "4g"
    "spark.sql.streaming.checkpointLocation": "/data/checkpoints"
  driver:
    cores: 2
    memory: "4g"
    labels:
      version: "3.5.0"
    serviceAccount: spark
  executor:
    cores: 2
    instances: 4
    memory: "4g"
    labels:
      version: "3.5.0"
```

#### 4. Auto-scaling Configuration

```yaml
# k8s/hpa.yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: mpesa-producer-hpa
  namespace: mpesa-pipeline
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: mpesa-producer
  minReplicas: 2
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: mpesa-processor-hpa
  namespace: mpesa-pipeline
spec:
  scaleTargetRef:
    apiVersion: sparkoperator.k8s.io/v1beta2
    kind: SparkApplication
    name: mpesa-stream-processor
  minReplicas: 2
  maxReplicas: 8
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 75
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 60
      policies:
      - type: Percent
        value: 100
        periodSeconds: 15
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
      - type: Percent
        value: 10
        periodSeconds: 60
```

#### 5. Service Mesh Integration

```yaml
# k8s/istio.yaml
apiVersion: networking.istio.io/v1beta1
kind: VirtualService
metadata:
  name: mpesa-gateway
  namespace: mpesa-pipeline
spec:
  hosts:
  - mpesa-api.example.com
  gateways:
  - mpesa-gateway
  http:
  - match:
    - uri:
        prefix: "/api/v1/transactions"
    route:
    - destination:
        host: mpesa-producer-service
        port:
          number: 8080
    timeout: 30s
    retries:
      attempts: 3
      perTryTimeout: 10s
---
apiVersion: security.istio.io/v1beta1
kind: AuthorizationPolicy
metadata:
  name: mpesa-authz
  namespace: mpesa-pipeline
spec:
  selector:
    matchLabels:
      app: mpesa-producer
  rules:
  - from:
    - source:
        principals: ["cluster.local/ns/istio-system/sa/istio-ingressgateway-service-account"]
  - to:
    - operation:
        methods: ["GET", "POST"]
```

## 🚀 Performance Optimizations

### 1. Spark Optimizations

```python
# src/process/optimized_stream_processor.py
from pyspark.sql import SparkSession

class OptimizedStreamProcessor:
    """Optimized Spark streaming processor"""
    
    def create_optimized_session(self):
        """Create optimized Spark session"""
        return SparkSession.builder \
            .appName("OptimizedMPesaProcessor") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.streaming.backpressure.enabled", "true") \
            .config("spark.sql.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .config("spark.sql.streaming.checkpointLocation", "/data/checkpoints") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.sql.autoBroadcastJoinThreshold", "10MB") \
            .getOrCreate()
    
    def optimize_kafka_source(self):
        """Optimized Kafka source configuration"""
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka-service:9092") \
            .option("subscribe", "mpesa-transactions") \
            .option("startingOffsets", "latest") \
            .option("maxOffsetsPerTrigger", 1000) \
            .option("failOnDataLoss", "false") \
            .option("minOffsetsPerTrigger", 100) \
            .load()
```

### 2. Caching Strategy

```python
# src/process/cache_manager.py
from pyspark.sql.functions import broadcast
from functools import lru_cache

class CacheManager:
    """Intelligent caching for frequently accessed data"""
    
    def __init__(self, spark_session):
        self.spark = spark_session
        self.user_profiles_cache = None
        self.fraud_rules_cache = None
    
    @lru_cache(maxsize=1000)
    def get_user_profile(self, user_id: str):
        """Get cached user profile"""
        if self.user_profiles_cache is None:
            # Load user profiles from database
            self.user_profiles_cache = self.spark.read.parquet(
                "s3://mpesa-data/user_profiles/"
            ).cache()
        
        return self.user_profiles_cache.filter(col("user_id") == user_id)
    
    def cache_fraud_rules(self):
        """Cache fraud detection rules"""
        self.fraud_rules_cache = self.spark.read.parquet(
            "s3://mpesa-data/fraud_rules/"
        ).cache()
        
        return broadcast(self.fraud_rules_cache)
```

### 3. Memory Management

```python
# src/process/memory_optimizer.py
import gc
from pyspark.sql import DataFrame

class MemoryOptimizer:
    """Memory optimization for streaming applications"""
    
    def __init__(self, spark_session):
        self.spark = spark_session
    
    def optimize_dataframe(self, df: DataFrame) -> DataFrame:
        """Optimize DataFrame memory usage"""
        # Convert to optimal data types
        df = df.withColumn("amount", col("amount").cast("float"))
        df = df.withColumn("user_id", col("user_id").cast("string"))
        
        # Drop unnecessary columns
        df = df.drop("kafka_timestamp", "partition", "offset")
        
        return df
    
    def cleanup_memory(self):
        """Clean up memory resources"""
        # Clear Spark cache
        self.spark.catalog.clearCache()
        
        # Force garbage collection
        gc.collect()
        
        # Print memory usage
        runtime_memory = self.spark.sparkContext._jvm.Runtime.getRuntime
        used_memory = runtime_memory.totalMemory() - runtime_memory.freeMemory()
        print(f"Memory usage: {used_memory / 1024 / 1024:.2f} MB")
```

## 📊 Monitoring & Observability

### 1. Prometheus Metrics

```python
# src/monitoring/metrics.py
from prometheus_client import Counter, Histogram, Gauge, start_http_server
import time

class PipelineMetrics:
    """Prometheus metrics for the pipeline"""
    
    def __init__(self):
        # Transaction metrics
        self.transactions_processed = Counter(
            'mpesa_transactions_processed_total',
            'Total number of transactions processed'
        )
        self.transaction_processing_time = Histogram(
            'mpesa_transaction_processing_seconds',
            'Time spent processing transactions'
        )
        
        # Fraud detection metrics
        self.fraud_alerts_total = Counter(
            'mpesa_fraud_alerts_total',
            'Total number of fraud alerts generated'
        )
        self.fraud_detection_accuracy = Gauge(
            'mpesa_fraud_detection_accuracy',
            'Accuracy of fraud detection model'
        )
        
        # System metrics
        self.kafka_lag = Gauge(
            'mpesa_kafka_consumer_lag',
            'Kafka consumer lag'
        )
        self.elasticsearch_index_rate = Gauge(
            'mpesa_elasticsearch_index_rate',
            'Rate of documents indexed in Elasticsearch'
        )
    
    def start_metrics_server(self, port: int = 8000):
        """Start Prometheus metrics server"""
        start_http_server(port)
        print(f"Metrics server started on port {port}")
```

### 2. Distributed Tracing

```python
# src/monitoring/tracing.py
from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

class DistributedTracing:
    """Distributed tracing setup"""
    
    def __init__(self, service_name: str = "mpesa-pipeline"):
        self.service_name = service_name
        self.setup_tracing()
    
    def setup_tracing(self):
        """Setup OpenTelemetry tracing"""
        trace.set_tracer_provider(TracerProvider())
        tracer = trace.get_tracer(__name__)
        
        # Setup Jaeger exporter
        jaeger_exporter = JaegerExporter(
            agent_host_name="jaeger-service",
            agent_port=6831,
        )
        
        # Add span processor
        span_processor = BatchSpanProcessor(jaeger_exporter)
        trace.get_tracer_provider().add_span_processor(span_processor)
    
    def trace_transaction_processing(self, transaction_data):
        """Trace transaction processing"""
        tracer = trace.get_tracer(__name__)
        
        with tracer.start_as_current_span("process_transaction") as span:
            span.set_attribute("transaction.user_id", transaction_data.get("user_id"))
            span.set_attribute("transaction.amount", transaction_data.get("amount"))
            
            # Process transaction
            result = self.process_transaction_logic(transaction_data)
            
            span.set_attribute("processing.result", result)
            return result
```

## 🎯 Implementation Roadmap

### Phase 1: ML Enhancement (Weeks 1-2)
1. Implement Isolation Forest model
2. Add feature engineering pipeline
3. Integrate with existing stream processor
4. A/B test against rule-based approach

### Phase 2: Kubernetes Migration (Weeks 3-4)
1. Containerize all components
2. Deploy Kafka cluster on Kubernetes
3. Setup Spark operator
4. Configure auto-scaling

### Phase 3: Monitoring & Optimization (Weeks 5-6)
1. Implement Prometheus metrics
2. Setup distributed tracing
3. Add performance optimizations
4. Configure alerting

### Phase 4: Advanced Features (Weeks 7-8)
1. Real-time model updating
2. Advanced anomaly detection
3. Performance tuning
4. Production hardening

## 📈 Expected Improvements

| Metric | Current | Target | Improvement |
|--------|---------|--------|-------------|
| Fraud Detection Accuracy | 85% | 95% | +10% |
| Processing Latency | 2-5s | <1s | 60-80% |
| Throughput | 1K txns/sec | 10K txns/sec | 10x |
| Availability | 95% | 99.9% | +4.9% |
| Auto-scaling | Manual | Automatic | 100% |

## 🔧 Configuration Files

### Environment Variables
```bash
# ML Configuration
ML_MODEL_PATH=/models/anomaly_detector.pkl
ML_RETRAIN_INTERVAL_HOURS=24
ML_PERFORMANCE_THRESHOLD=0.85

# Kubernetes Configuration
K8S_NAMESPACE=mpesa-pipeline
SPARK_EXECUTOR_INSTANCES=4
KAFKA_REPLICATION_FACTOR=3

# Monitoring Configuration
PROMETHEUS_PORT=8000
JAEGER_ENDPOINT=jaeger-service:6831
```

### Helm Values
```yaml
# helm-values.yaml
ml:
  enabled: true
  modelUpdateInterval: 24h
  performanceThreshold: 0.85

scaling:
  minReplicas: 2
  maxReplicas: 10
  targetCPUUtilization: 70
  targetMemoryUtilization: 80

monitoring:
  prometheus:
    enabled: true
    port: 8000
  jaeger:
    enabled: true
    endpoint: jaeger-service:6831
```

This optimization guide provides a comprehensive roadmap for enhancing the M-Pesa streaming pipeline with advanced ML capabilities and cloud-native scaling.
