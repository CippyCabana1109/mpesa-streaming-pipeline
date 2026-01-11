# M-Pesa Streaming Pipeline

## Project Overview

Real-time streaming pipeline for simulated M-Pesa transactions with fraud detection capabilities. This project processes mobile money transactions in real-time, analyzes patterns, and identifies potentially fraudulent activities using machine learning.

## Tech Stack

- **Apache Kafka**: Distributed streaming platform for real-time data ingestion
- **PySpark Streaming**: Real-time data processing and stream analytics
- **Elasticsearch**: Search and analytics engine for storing and querying transaction data
- **scikit-learn**: Machine learning library for fraud detection algorithms
- **Docker**: Containerization for consistent development and deployment

## Architecture Diagram

```mermaid
graph TD
    A[Transaction Simulator] --> B[Kafka Broker]
    B --> C[Spark Streaming]
    C --> D[Fraud Detection Model]
    C --> E[Elasticsearch]
    E --> F[Dashboard/API]
    
    subgraph "Data Flow"
        A -->|Simulated Transactions| B
        B -->|Real-time Stream| C
        C -->|Processed Data| E
        C -->|ML Analysis| D
        D -->|Fraud Alerts| F
    end
    
    subgraph "Infrastructure"
        G[Zookeeper] --> B
        H[Spark Master] --> I[Spark Workers]
        I --> C
    end
```

## Setup

### Local Development with Docker

1. **Prerequisites**
   - Docker Desktop installed
   - Python 3.8+ installed
   - Git installed

2. **Clone the repository**
   ```bash
   git clone https://github.com/CippyCabana1109/mpesa-streaming-pipeline.git
   cd mpesa-streaming-pipeline
   ```

3. **Start the infrastructure**
   ```bash
   docker-compose up -d
   ```

4. **Install Python dependencies**
   ```bash
   pip install -r requirements.txt
   ```

5. **Verify setup**
   - Kafka Broker: `localhost:9092`
   - Elasticsearch: `http://localhost:9200`
   - Spark Master UI: `http://localhost:8080`

### Optional: Confluent Cloud

For production deployment, you can use Confluent's free tier:
- Sign up at [Confluent Cloud](https://www.confluent.cloud/)
- Create Kafka cluster
- Update connection details in configuration files

## How to Run

### Quick Start

1. **Start Infrastructure**
   ```bash
   docker-compose up -d
   ```

2. **Generate Test Data** (optional)
   ```bash
   python src/simulate/generate_transactions.py --duration 1
   ```

3. **Start Kafka Producer**
   ```bash
   python src/ingest/producer.py --mode generate --rate 1
   ```

4. **Start Stream Processor**
   ```bash
   spark-submit src/process/stream_processor.py --output elasticsearch
   ```

5. **Monitor Results**
   ```bash
   # Check Elasticsearch
   curl http://localhost:9200/mpesa-insights/_search?pretty
   
   # View Spark UI
   # http://localhost:8080
   ```

### Detailed Steps

#### Step 1: Infrastructure Setup
```bash
# Start all services (Kafka, Spark, Elasticsearch)
docker-compose up -d

# Verify services are running
docker-compose ps

# Check logs if needed
docker-compose logs kafka
docker-compose logs elasticsearch
```

#### Step 2: Data Generation & Ingestion
```bash
# Option A: Generate transactions to file first
python src/simulate/generate_transactions.py --duration 5

# Option B: Stream directly to Kafka
python src/ingest/producer.py --mode generate --rate 5 --duration 10

# Option C: Read from generated file
python src/ingest/producer.py --mode file --file data/simulated/transactions.jsonl
```

#### Step 3: Stream Processing
```bash
# Console mode (for testing)
python src/process/stream_processor.py --output console

# Elasticsearch mode (production)
spark-submit src/process/stream_processor.py --output elasticsearch

# Custom configuration
spark-submit src/process/stream_processor.py \
  --kafka-servers localhost:9092 \
  --topic mpesa-transactions \
  --elasticsearch-host localhost \
  --elasticsearch-port 9200 \
  --output elasticsearch
```

#### Step 4: Verification & Monitoring
```bash
# Check Elasticsearch indices
curl http://localhost:9200/_cat/indices

# Query flagged transactions
curl http://localhost:9200/mpesa-insights/_search -H 'Content-Type: application/json' -d'
{
  "query": {"term": {"document_type": "flagged_transaction"}},
  "size": 10
}'

# Check hourly aggregates
curl http://localhost:9200/mpesa-insights/_search -H 'Content-Type: application/json' -d'
{
  "query": {"term": {"document_type": "hourly_aggregate"}},
  "size": 10
}'

# Spark UI for monitoring
# http://localhost:8080
```

### Testing

```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test categories
python -m pytest tests/test_pipeline.py::TestTransactionSimulator -v
python -m pytest tests/test_pipeline.py::TestKafkaProducer -v
python -m pytest tests/test_pipeline.py::TestStreamProcessor -v

# Run integration tests (requires running infrastructure)
python -m pytest tests/ --run-integration -v

# Run with coverage
python -m pytest tests/ --cov=src --cov-report=html
```

## Usage

### 1. Start Transaction Simulation
```bash
# Generate 1 minute of transactions
python src/simulate/generate_transactions.py --duration 1

# Continuous simulation for 60 minutes
python src/simulate/generate_transactions.py --continuous --duration 60
```

### 2. Run Data Ingestion
```bash
# Generate transactions on-the-fly
python src/ingest/producer.py --mode generate --rate 1

# Read from JSONL file
python src/ingest/producer.py --mode file --loop
```

### 3. Start Stream Processing
```bash
# Console output for testing
python src/process/stream_processor.py --output console

# Elasticsearch output for production
spark-submit src/process/stream_processor.py --output elasticsearch
```

### 4. Monitor Results
- **Elasticsearch**: `http://localhost:9200`
- **Kibana**: `http://localhost:5601` (if enabled)
- **Spark UI**: `http://localhost:8080`
- **Kafka UI**: Use external tools like Kafka UI

### 5. Kibana Dashboard Setup

#### Access Kibana
1. Open `http://localhost:5601` in your browser
2. Navigate to **Management > Stack Management > Index Patterns**
3. Create index pattern: `mpesa-insights*`

#### Create Fraud Detection Dashboard

**Step 1: Transaction Volume Visualization**
```json
{
  "_source": ["amount", "timestamp", "user_id"],
  "size": 0,
  "aggs": {
    "transactions_over_time": {
      "date_histogram": {
        "field": "timestamp",
        "calendar_interval": "1h"
      },
      "aggs": {
        "total_volume": {
          "sum": {
            "field": "amount"
          }
        }
      }
    }
  }
}
```

**Step 2: Fraud Alerts Map**
```json
{
  "query": {
    "term": {
      "document_type": "flagged_transaction"
    }
  },
  "size": 1000
}
```

**Step 3: Hourly Aggregates Chart**
```json
{
  "query": {
    "term": {
      "document_type": "hourly_aggregate"
    }
  },
  "sort": [
    {
      "hour": {
        "order": "asc"
      }
    }
  ],
  "size": 24
}
```

**Step 4: Fraud Detection Metrics**
```json
{
  "size": 0,
  "query": {
    "term": {
      "document_type": "flagged_transaction"
    }
  },
  "aggs": {
    "fraud_types": {
      "filters": {
        "filters": {
          "high_amount": {
            "term": {
              "is_high_amount": true
            }
          },
          "location_outlier": {
            "term": {
              "is_location_outlier": true
            }
          }
        }
      }
    },
    "total_flagged": {
      "value_count": {
        "field": "user_id"
      }
    }
  }
}
```

#### Dashboard Visualization Types

1. **Line Chart**: Transaction volume over time
2. **Map**: Geographic distribution of flagged transactions
3. **Bar Chart**: Hourly aggregate metrics
4. **Pie Chart**: Fraud detection breakdown
5. **Data Table**: Detailed flagged transactions

#### Sample Dashboard Queries

**Real-time Fraud Rate:**
```bash
curl -X GET "localhost:9200/mpesa-insights/_search" -H 'Content-Type: application/json' -d'
{
  "size": 0,
  "aggs": {
    "fraud_rate": {
      "filters": {
        "filters": {
          "flagged": {
            "term": {"document_type": "flagged_transaction"}
          },
          "total": {
            "match_all": {}
          }
        }
      }
    }
  }
}'
```

**Top Fraudulent Users:**
```bash
curl -X GET "localhost:9200/mpesa-insights/_search" -H 'Content-Type: application/json' -d'
{
  "query": {"term": {"document_type": "flagged_transaction"}},
  "aggs": {
    "top_users": {
      "terms": {
        "field": "user_id",
        "size": 10
      },
      "aggs": {
        "total_amount": {
          "sum": {"field": "amount"}
        }
      }
    }
  }
}'
```

**Location-based Fraud Hotspots:**
```bash
curl -X GET "localhost:9200/mpesa-insights/_search" -H 'Content-Type: application/json' -d'
{
  "query": {
    "bool": {
      "must": [
        {"term": {"document_type": "flagged_transaction"}},
        {"term": {"is_location_outlier": true}}
      ]
    }
  },
  "size": 100,
  "sort": [{"distance_from_nairobi": {"order": "desc"}}]
}'
```

## Project Structure

```
mpesa-streaming-pipeline/
├── src/
│   ├── simulate/          # Transaction simulation scripts
│   ├── ingest/            # Kafka producers for data ingestion
│   ├── process/           # Spark streaming jobs
│   └── store/             # Elasticsearch sink and storage
├── tests/                 # Test suite with pytest
├── data/
│   └── simulated/        # Sample data and outputs
├── docker-compose.yml     # Infrastructure setup
├── requirements.txt       # Python dependencies
└── README.md             # Project documentation
```

## Features

- **Real-time Transaction Processing**: Sub-second latency for transaction analysis
- **Fraud Detection**: Machine learning models identify suspicious patterns
- **Scalable Architecture**: Horizontal scaling with Kafka and Spark
- **Data Persistence**: Elasticsearch provides fast search and analytics
- **Monitoring**: Built-in metrics and health checks

## Development

### Adding New Features
1. Create feature branch: `git checkout -b feature/new-feature`
2. Make changes and test locally
3. Update documentation
4. Submit pull request

### Testing
```bash
# Run unit tests
python -m pytest tests/

# Run integration tests
python -m pytest tests/integration/
```

## Configuration

Key configuration files:
- `config/kafka_config.json`: Kafka connection settings
- `config/spark_config.json`: Spark job configurations
- `config/elasticsearch_config.json`: Elasticsearch connection details

## Tips for Success

### 🚀 Local Setup Best Practices

**Prerequisites Check**
```bash
# Verify Docker is running
docker --version
docker-compose --version

# Check available ports (avoid conflicts)
netstat -tulpn | grep -E ':(9092|9200|8080|5601)'

# Ensure sufficient resources (recommended 8GB+ RAM)
docker system df
```

**Infrastructure Startup Order**
```bash
# 1. Start core services first
docker-compose up -d zookeeper elasticsearch

# 2. Wait for services to be ready (30-60 seconds)
docker-compose logs -f elasticsearch | grep "started"

# 3. Start Kafka
docker-compose up -d kafka

# 4. Start Spark cluster
docker-compose up -d spark-master spark-worker

# 5. Verify all services
docker-compose ps
```

**Service Health Verification**
```bash
# Check Kafka
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Check Elasticsearch
curl -f http://localhost:9200/_cluster/health?pretty

# Check Spark Master
curl http://localhost:8080/api/v1/applications
```

### 🎯 Simulation Realism Enhancements

**Temporal Pattern Simulation**
```python
# Adjust generator for realistic patterns
python src/simulate/generate_transactions.py \
  --duration 60 \
  --base-rate 1000 \
  --eom-boost 3.0 \
  --business-hours-boost 1.5

# Simulate specific dates (e.g., month-end rush)
python src/simulate/generate_transactions.py \
  --simulate-date "2024-01-30" \
  --hourly-pattern "business"
```

**Geographic Distribution**
```python
# Add realistic location patterns
# Nairobi CBD: High volume, small radius
# Coastal areas: Tourism patterns
# Rural areas: Lower volume, larger distances

# Example: Simulate holiday patterns
python src/simulate/generate_transactions.py \
  --holiday-mode "christmas" \
  --location-pattern "tourism"
```

**User Behavior Simulation**
```python
# Create realistic user segments
# - Regular users: Daily transactions, consistent amounts
# - Business users: High volume, variable amounts  
# - Occasional users: Weekly/monthly transactions
# - Suspicious patterns: Clustered transactions

python src/simulate/generate_transactions.py \
  --user-segments "regular,business,occasional" \
  --anomaly-rate 0.05
```

### 📈 Scaling & Performance Testing

**Gradual Load Testing**
```bash
# Start with 1 txn/sec for testing
python src/ingest/producer.py --mode generate --rate 1 --duration 5

# Monitor system resources
docker stats

# Scale up gradually
python src/ingest/producer.py --mode generate --rate 10 --duration 10
python src/ingest/producer.py --mode generate --rate 100 --duration 15
python src/ingest/producer.py --mode generate --rate 1000 --duration 30
```

**Performance Benchmarking**
```bash
# Benchmark transaction processing
time python src/ingest/producer.py --mode generate --rate 1000 --duration 60

# Expected results for optimization:
# - 1K txns/sec: <2s end-to-end latency
# - 10K txns/sec: <5s end-to-end latency  
# - 50K txns/sec: <10s end-to-end latency
```

**Resource Monitoring**
```bash
# Monitor Kafka lag
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group mpesa-consumer

# Monitor Elasticsearch indexing rate
curl -X GET "localhost:9200/_cat/indices/mpesa-insights?v"

# Monitor Spark job performance
curl http://localhost:8080/api/v1/applications | jq '.[] | {id, name, uptime}'
```

**Performance Optimization Checklist**
- [ ] Kafka partitions >= number of consumers
- [ ] Spark executor memory >= 4GB per executor
- [ ] Elasticsearch heap size >= 1GB
- [ ] Network latency < 10ms between services
- [ ] Disk I/O >= 100MB/s for checkpointing

### 🔒 Ethical Considerations & Data Privacy

**⚠️ Important Notice**
This repository uses **simulated data only**. No real M-Pesa transaction data or customer information is used or stored.

**Data Privacy Principles**
```python
# All user IDs are randomly generated
user_id = f"user_{random.randint(1, 1000)}"

# Locations are randomized around Nairobi coordinates
location = {
    'lat': -1.286 + random.uniform(-0.1, 0.1),
    'long': 36.817 + random.uniform(-0.1, 0.1)
}

# Amounts follow statistical distributions, not real patterns
amount = max(100, np.random.normal(2740, 1000))
```

**Ethical Guidelines**
- **Simulation Only**: All data is artificially generated
- **No Real Identities**: User IDs, locations, and amounts are fictional
- **Privacy by Design**: No personal data collection or storage
- **Research Purpose**: Intended for fraud detection research and education
- **Compliance**: Follows data protection regulations (GDPR, CCPA)

**Production Deployment Considerations**
```bash
# If adapting for production use:
# 1. Implement proper data anonymization
# 2. Add encryption at rest and in transit
# 3. Implement access controls and audit logging
# 4. Follow regulatory compliance requirements
# 5. Conduct privacy impact assessments
```

**Responsible AI Usage**
```python
# Model fairness checks
def check_fairness(predictions, protected_attributes):
    """Ensure model doesn't discriminate"""
    # Check for bias across user segments
    # Monitor false positive rates
    # Validate against protected attributes
    pass

# Explainability requirements
def explain_fraud_decision(transaction_id):
    """Provide clear reasoning for fraud alerts"""
    # Return feature contributions
    # Provide confidence scores
    # Suggest human review when needed
    pass
```

### 🛠️ Troubleshooting Common Issues

**Kafka Connection Issues**
```bash
# Topic not found
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic mpesa-transactions --partitions 3 --replication-factor 1

# Consumer lag issues
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --reset-offsets --group mpesa-consumer --to-latest --all-topics
```

**Elasticsearch Issues**
```bash
# Index mapping errors
curl -X DELETE "localhost:9200/mpesa-insights"
curl -X PUT "localhost:9200/mpesa-insights" \
  -H 'Content-Type: application/json' \
  -d @config/elasticsearch-mapping.json

# Memory issues
docker exec elasticsearch curl -X PUT "localhost:9200/_cluster/settings" \
  -H 'Content-Type: application/json' \
  -d '{"transient": {"indices.memory.index_buffer_size": "10%"}}'
```

**Spark Performance Issues**
```bash
# Check executor status
curl http://localhost:8080/api/v1/applications | jq '.[] | .executors'

# Adjust Spark configuration
export SPARK_EXECUTOR_MEMORY=4g
export SPARK_EXECUTOR_CORES=2
export SPARK_DRIVER_MEMORY=2g
```

### 📊 Success Metrics

**Target Performance Indicators**
- **Throughput**: Process 10K+ transactions/second
- **Latency**: <2s end-to-end processing time
- **Accuracy**: 95%+ fraud detection precision
- **Availability**: 99.9%+ uptime
- **Resource Efficiency**: <80% CPU/Memory utilization

**Monitoring Dashboard Setup**
```bash
# Set up Grafana for monitoring
docker run -d -p 3000:3000 grafana/grafana

# Import dashboard templates
curl -X POST http://admin:admin@localhost:3000/api/dashboards/db \
  -H 'Content-Type: application/json' \
  -d @config/grafana-dashboard.json
```

**Quality Assurance Checklist**
- [ ] All tests passing (`pytest tests/ -v`)
- [ ] Code coverage >80% (`pytest --cov=src`)
- [ ] No security vulnerabilities (`bandit -r src/`)
- [ ] Documentation up to date
- [ ] Performance benchmarks met
- [ ] Ethical guidelines followed

---

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
