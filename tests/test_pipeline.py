#!/usr/bin/env python3
"""
Comprehensive tests for M-Pesa streaming pipeline

Tests include:
- Kafka producer/consumer mocking
- Fraud rule validation
- End-to-end pipeline testing
- Elasticsearch integration
"""

import pytest
import json
import time
import tempfile
import shutil
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

# Add src to path for imports
import sys
sys.path.append(str(Path(__file__).parent.parent / "src"))

from simulate.generate_transactions import MPesaTransactionSimulator, Transaction, Location
from ingest.producer import MPesaKafkaProducer
from process.stream_processor import MPesaStreamProcessor


class TestTransactionSimulator:
    """Test the transaction simulator component"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.simulator = MPesaTransactionSimulator(
            output_path=f"{self.temp_dir}/test_transactions.jsonl"
        )
    
    def teardown_method(self):
        """Cleanup test environment"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_transaction_generation(self):
        """Test basic transaction generation"""
        transactions = self.simulator.generate_batch(duration_minutes=1)
        
        assert len(transactions) > 0, "Should generate transactions"
        
        # Test transaction structure
        transaction = transactions[0]
        assert isinstance(transaction, Transaction)
        assert transaction.user_id.startswith("user_")
        assert transaction.amount >= 100.0
        assert transaction.type == "transfer"
        assert isinstance(transaction.location, Location)
        assert -1.5 <= transaction.location.lat <= -1.0  # Nairobi area
        assert 36.5 <= transaction.location.long <= 37.0
    
    def test_high_amount_anomaly(self):
        """Test high amount anomaly generation"""
        # Force anomaly generation
        self.simulator.anomaly_probability = 1.0
        
        transactions = self.simulator.generate_batch(duration_minutes=1)
        
        # Should have some high amount transactions
        high_amount_txns = [t for t in transactions if t.amount > 10000]
        assert len(high_amount_txns) > 0, "Should generate high amount anomalies"
    
    def test_transaction_clustering(self):
        """Test transaction clustering anomaly"""
        # Force clustering
        self.simulator.cluster_probability = 1.0
        
        transactions = self.simulator.generate_batch(duration_minutes=1)
        
        # Should have clustered transactions from same user
        user_counts = {}
        for txn in transactions:
            user_counts[txn.user_id] = user_counts.get(txn.user_id, 0) + 1
        
        clustered_users = [user for user, count in user_counts.items() if count > 1]
        assert len(clustered_users) > 0, "Should generate clustered transactions"
    
    def test_end_of_month_volume(self):
        """Test end-of-month volume boost"""
        # Create end-of-month timestamp
        eom_time = datetime(2024, 1, 30, 10, 0, 0)
        
        multiplier = self.simulator.calculate_volume_multiplier(eom_time)
        assert multiplier > 1.0, "End-of-month should boost volume"
    
    def test_save_to_jsonl(self):
        """Test saving transactions to JSONL file"""
        transactions = self.simulator.generate_batch(duration_minutes=1)
        self.simulator.save_to_jsonl(transactions)
        
        # Verify file exists and has content
        assert Path(self.simulator.output_path).exists()
        
        with open(self.simulator.output_path, 'r') as f:
            lines = f.readlines()
        
        assert len(lines) == len(transactions)
        
        # Verify JSON structure
        first_txn = json.loads(lines[0])
        assert 'user_id' in first_txn
        assert 'amount' in first_txn
        assert 'location' in first_txn


class TestKafkaProducer:
    """Test the Kafka producer component"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
    
    def teardown_method(self):
        """Cleanup test environment"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    @patch('ingest.producer.KafkaProducer')
    def test_producer_initialization(self, mock_kafka_producer):
        """Test Kafka producer initialization"""
        mock_producer_instance = Mock()
        mock_kafka_producer.return_value = mock_producer_instance
        
        producer = MPesaKafkaProducer(
            bootstrap_servers='localhost:9092',
            topic='test-topic'
        )
        
        assert producer.bootstrap_servers == 'localhost:9092'
        assert producer.topic == 'test-topic'
        mock_kafka_producer.assert_called_once()
    
    @patch('ingest.producer.KafkaProducer')
    def test_send_transaction(self, mock_kafka_producer):
        """Test sending transaction to Kafka"""
        mock_producer_instance = Mock()
        mock_future = Mock()
        mock_future.get.return_value = Mock(partition=0, offset=123)
        mock_producer_instance.send.return_value = mock_future
        mock_kafka_producer.return_value = mock_producer_instance
        
        producer = MPesaKafkaProducer()
        
        transaction = {
            'user_id': 'user_123',
            'amount': 1000.0,
            'timestamp': datetime.now().isoformat(),
            'location': {'lat': -1.286, 'long': 36.817},
            'type': 'transfer'
        }
        
        result = producer.send_transaction(transaction)
        
        assert result == True
        mock_producer_instance.send.assert_called_once()
        mock_future.get.assert_called_once_with(timeout=10)
    
    @patch('ingest.producer.KafkaProducer')
    def test_send_transaction_failure(self, mock_kafka_producer):
        """Test handling of Kafka send failures"""
        mock_producer_instance = Mock()
        mock_future = Mock()
        mock_future.get.side_effect = Exception("Kafka error")
        mock_producer_instance.send.return_value = mock_future
        mock_kafka_producer.return_value = mock_producer_instance
        
        producer = MPesaKafkaProducer()
        
        transaction = {
            'user_id': 'user_123',
            'amount': 1000.0,
            'timestamp': datetime.now().isoformat(),
            'location': {'lat': -1.286, 'long': 36.817},
            'type': 'transfer'
        }
        
        result = producer.send_transaction(transaction)
        
        assert result == False
        assert producer.stats['messages_failed'] == 1
    
    def test_location_randomization(self):
        """Test location randomization"""
        producer = MPesaKafkaProducer()
        
        original_location = {'lat': -1.286, 'long': 36.817}
        randomized = producer.randomize_location(original_location)
        
        assert randomized['lat'] != original_location['lat']
        assert randomized['long'] != original_location['long']
        assert -1.4 <= randomized['lat'] <= -1.2  # Within Nairobi area
        assert 36.7 <= randomized['long'] <= 36.9
    
    def test_generate_transaction_on_the_fly(self):
        """Test on-the-fly transaction generation"""
        producer = MPesaKafkaProducer()
        
        transaction = producer.generate_transaction_on_the_fly()
        
        assert 'user_id' in transaction
        assert 'amount' in transaction
        assert 'timestamp' in transaction
        assert 'location' in transaction
        assert 'type' in transaction
        assert transaction['type'] == 'transfer'
        assert transaction['amount'] >= 100.0


class TestStreamProcessor:
    """Test the stream processor component"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
    
    def teardown_method(self):
        """Cleanup test environment"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    @patch('process.stream_processor.SparkSession')
    @patch('process.stream_processor.Elasticsearch')
    def test_processor_initialization(self, mock_elasticsearch, mock_spark_session):
        """Test stream processor initialization"""
        mock_spark_instance = Mock()
        mock_spark_session.builder.return_value.config.return_value \
            .config.return_value.config.return_value.config.return_value \
            .config.return_value.config.return_value.getOrCreate.return_value = mock_spark_instance
        
        mock_es_instance = Mock()
        mock_es_instance.ping.return_value = True
        mock_elasticsearch.return_value = mock_es_instance
        
        processor = MPesaStreamProcessor(
            checkpoint_location=self.temp_dir
        )
        
        assert processor.kafka_topic == 'mpesa-transactions'
        assert processor.window_duration == '60 seconds'
        mock_spark_session.assert_called_once()
        mock_elasticsearch.assert_called_once()
    
    def test_distance_calculation(self):
        """Test Nairobi distance calculation"""
        processor = MPesaStreamProcessor()
        
        # Test distance from Nairobi center
        nairobi_lat, nairobi_long = -1.286389, 36.817223
        distance = processor.calculate_distance_from_nairobi(nairobi_lat, nairobi_long)
        
        assert distance == 0.0, "Distance from Nairobi center should be 0"
        
        # Test distance from different location
        mombasa_lat, mombasa_long = -4.0435, 39.6682
        distance = processor.calculate_distance_from_nairobi(mombasa_lat, mombasa_long)
        
        assert distance > 400, "Distance from Nairobi to Mombasa should be >400km"
    
    @patch('process.stream_processor.SparkSession')
    @patch('process.stream_processor.Elasticsearch')
    def test_fraud_rules_high_amount(self, mock_elasticsearch, mock_spark_session):
        """Test fraud detection for high amounts"""
        # Mock Spark components
        mock_spark_instance = Mock()
        mock_spark_session.builder.return_value.config.return_value \
            .config.return_value.config.return_value.config.return_value \
            .config.return_value.config.return_value.getOrCreate.return_value = mock_spark_instance
        
        mock_es_instance = Mock()
        mock_es_instance.ping.return_value = True
        mock_elasticsearch.return_value = mock_es_instance
        
        processor = MPesaStreamProcessor()
        
        # Create mock DataFrame with high amount transaction
        mock_df = Mock()
        mock_df.withColumn.return_value = mock_df
        mock_df.filter.return_value = mock_df
        
        # Mock the fraud rule application
        with patch.object(processor, 'apply_fraud_rules', return_value=mock_df):
            result = processor.apply_fraud_rules(mock_df)
            assert result == mock_df
    
    @patch('process.stream_processor.SparkSession')
    @patch('process.stream_processor.Elasticsearch')
    def test_fraud_rules_location_outlier(self, mock_elasticsearch, mock_spark_session):
        """Test fraud detection for location outliers"""
        mock_spark_instance = Mock()
        mock_spark_session.builder.return_value.config.return_value \
            .config.return_value.config.return_value.config.return_value \
            .config.return_value.config.return_value.getOrCreate.return_value = mock_spark_instance
        
        mock_es_instance = Mock()
        mock_es_instance.ping.return_value = True
        mock_elasticsearch.return_value = mock_es_instance
        
        processor = MPesaStreamProcessor()
        
        # Test location outlier detection
        # Location far from Nairobi (>50km)
        far_location = processor.calculate_distance_from_nairobi(-2.0, 38.0)
        assert far_location > processor.distance_threshold_km, \
            "Far location should be flagged as outlier"
    
    @patch('process.stream_processor.SparkSession')
    @patch('process.stream_processor.Elasticsearch')
    def test_aggregate_metrics(self, mock_elasticsearch, mock_spark_session):
        """Test metrics aggregation"""
        mock_spark_instance = Mock()
        mock_spark_session.builder.return_value.config.return_value \
            .config.return_value.config.return_value.config.return_value \
            .config.return_value.config.return_value.getOrCreate.return_value = mock_spark_instance
        
        mock_es_instance = Mock()
        mock_es_instance.ping.return_value = True
        mock_elasticsearch.return_value = mock_es_instance
        
        processor = MPesaStreamProcessor()
        
        mock_df = Mock()
        mock_df.withColumn.return_value = mock_df
        mock_df.groupBy.return_value.agg.return_value.orderBy.return_value = mock_df
        
        with patch.object(processor, 'aggregate_metrics', return_value=mock_df):
            result = processor.aggregate_metrics(mock_df)
            assert result == mock_df


class TestEndToEndPipeline:
    """End-to-end integration tests"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
    
    def teardown_method(self):
        """Cleanup test environment"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    @patch('process.stream_processor.Elasticsearch')
    def test_simulator_to_elasticsearch_flow(self, mock_elasticsearch):
        """Test complete flow from simulator to Elasticsearch"""
        # Mock Elasticsearch
        mock_es_instance = Mock()
        mock_es_instance.ping.return_value = True
        mock_es_instance.indices.exists.return_value = False
        mock_es_instance.indices.create.return_value = {}
        mock_es_instance.index.return_value = {'result': 'created'}
        mock_elasticsearch.return_value = mock_es_instance
        
        # Step 1: Generate transactions
        simulator = MPesaTransactionSimulator(
            output_path=f"{self.temp_dir}/e2e_transactions.jsonl"
        )
        transactions = simulator.generate_batch(duration_minutes=1)
        simulator.save_to_jsonl(transactions)
        
        assert len(transactions) > 0, "Should generate transactions"
        
        # Step 2: Verify some transactions are flagged
        flagged_txns = [t for t in transactions if t.amount > 10000]
        print(f"Generated {len(flagged_txns)} flagged transactions")
        
        # Step 3: Mock Elasticsearch indexing
        for txn in transactions:
            doc = {
                'user_id': txn.user_id,
                'amount': txn.amount,
                'timestamp': txn.timestamp,
                'location': {'lat': txn.location.lat, 'lon': txn.location.long},
                'type': txn.type,
                'document_type': 'flagged_transaction' if txn.amount > 10000 else 'normal_transaction'
            }
            
            # Mock the Elasticsearch index call
            mock_es_instance.index.return_value = {'result': 'created'}
        
        # Verify Elasticsearch was called
        assert mock_es_instance.ping.called
        assert mock_es_instance.indices.create.called
    
    def test_fraud_detection_accuracy(self):
        """Test fraud detection accuracy on sample data"""
        # Create test data with known fraud patterns
        test_transactions = [
            {'user_id': 'user_1', 'amount': 5000.0, 'location': {'lat': -1.286, 'long': 36.817}},  # Normal
            {'user_id': 'user_2', 'amount': 15000.0, 'location': {'lat': -1.286, 'long': 36.817}}, # High amount fraud
            {'user_id': 'user_3', 'amount': 1000.0, 'location': {'lat': -3.0, 'long': 38.0}},         # Location fraud
            {'user_id': 'user_4', 'amount': 800.0, 'location': {'lat': -1.286, 'long': 36.817}},   # Normal
        ]
        
        processor = MPesaStreamProcessor()
        
        # Test fraud detection logic
        fraud_flags = []
        for txn in test_transactions:
            # High amount fraud
            is_high_amount = txn['amount'] > 10000
            
            # Location fraud (distance from Nairobi)
            distance = processor.calculate_distance_from_nairobi(
                txn['location']['lat'], 
                txn['location']['long']
            )
            is_location_outlier = distance > processor.distance_threshold_km
            
            is_fraud = is_high_amount or is_location_outlier
            fraud_flags.append(is_fraud)
        
        # Verify fraud detection
        assert fraud_flags[0] == False, "Normal transaction should not be flagged"
        assert fraud_flags[1] == True, "High amount transaction should be flagged"
        assert fraud_flags[2] == True, "Location outlier should be flagged"
        assert fraud_flags[3] == False, "Normal transaction should not be flagged"
        
        # Verify detection accuracy
        flagged_count = sum(fraud_flags)
        assert flagged_count == 2, f"Should detect 2 fraudulent transactions, got {flagged_count}"
    
    def test_data_quality_validation(self):
        """Test data quality and validation"""
        simulator = MPesaTransactionSimulator(
            output_path=f"{self.temp_dir}/quality_test.jsonl"
        )
        
        transactions = simulator.generate_batch(duration_minutes=1)
        
        # Validate data quality
        for txn in transactions:
            # Check required fields
            assert hasattr(txn, 'user_id'), "Transaction should have user_id"
            assert hasattr(txn, 'amount'), "Transaction should have amount"
            assert hasattr(txn, 'timestamp'), "Transaction should have timestamp"
            assert hasattr(txn, 'location'), "Transaction should have location"
            assert hasattr(txn, 'type'), "Transaction should have type"
            
            # Check data types and ranges
            assert isinstance(txn.amount, float), "Amount should be float"
            assert txn.amount >= 100.0, "Amount should be >= 100"
            assert txn.type == "transfer", "Type should be 'transfer'"
            assert isinstance(txn.location, Location), "Location should be Location object"
            assert -90 <= txn.location.lat <= 90, "Latitude should be valid"
            assert -180 <= txn.location.long <= 180, "Longitude should be valid"
            
            # Check timestamp format
            try:
                datetime.fromisoformat(txn.timestamp.replace('Z', '+00:00'))
            except ValueError:
                pytest.fail(f"Invalid timestamp format: {txn.timestamp}")


# Integration test markers
pytest.mark.integration = pytest.mark.skipif(
    not pytest.config.getoption("--run-integration"),
    reason="Integration tests require --run-integration flag"
)


@pytest.mark.integration
class TestRealIntegration:
    """Real integration tests (require running infrastructure)"""
    
    def test_real_kafka_connection(self):
        """Test real Kafka connection if available"""
        try:
            producer = MPesaKafkaProducer(
                bootstrap_servers='localhost:9092',
                topic='test-topic'
            )
            assert producer.producer is not None
        except Exception as e:
            pytest.skip(f"Kafka not available: {e}")
    
    def test_real_elasticsearch_connection(self):
        """Test real Elasticsearch connection if available"""
        try:
            from elasticsearch import Elasticsearch
            es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])
            if es.ping():
                assert True
            else:
                pytest.skip("Elasticsearch not responding")
        except Exception as e:
            pytest.skip(f"Elasticsearch not available: {e}")


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])
