import runpy
from pathlib import Path

import pytest

from pathlib import Path

from src.anomaly_detector import AnomalyDetector
from src.aiops_pipeline import run_pipeline
from src.aiops_pipeline import load_data
from src.event_consumer import EventConsumer
from src.event_producer import EventProducer
from src.event_topic import EventTopic
from src.calculations import area_of_circle, get_nth_fibonacci


def test_normal_record_is_not_anomaly():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:00:00",
        "service": "payment-service",
        "response_time_ms": 120,
        "cpu_percent": 42,
        "memory_percent": 51,
        "log_level": "INFO",
        "message": "Payment request processed successfully"
    }

    assert detector.detect(record) is None


def test_anomalous_record_is_detected():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:05:00",
        "service": "payment-service",
        "response_time_ms": 610,
        "cpu_percent": 75,
        "memory_percent": 70,
        "log_level": "ERROR",
        "message": "Payment service timeout"
    }

    event = detector.detect(record)

    assert event is not None
    assert event["type"] == "ANOMALY"


def test_producer_publishes_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)

    event = {
        "type": "ANOMALY",
        "service": "payment-service"
    }

    assert producer.publish(event)
    assert len(topic.get_messages()) == 1


def test_consumer_receives_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)
    consumer = EventConsumer(topic)

    event = {
        "type": "ANOMALY",
        "service": "payment-service"
    }

    producer.publish(event)

    messages = consumer.consume()

    assert len(messages) == 1


def test_negative_radius_raises_value_error():
    with pytest.raises(ValueError):
        area_of_circle(-1)


def test_negative_fibonacci_raises_value_error():
    with pytest.raises(ValueError):
        get_nth_fibonacci(-1)


def test_warning_log_is_treated_as_anomaly():
    detector = AnomalyDetector()
    record = {
        "timestamp": "2026-09-20T10:11:00",
        "service": "shipping-service",
        "response_time_ms": 100,
        "cpu_percent": 40,
        "memory_percent": 41,
        "log_level": "WARNING",
        "message": "High latency warning"
    }

    event = detector.detect(record)

    assert event is not None
    assert "Error log detected" in event["reasons"]


def test_pipeline_processes_and_consumes_anomalies():
    repo_root = Path(__file__).resolve().parents[1]
    result = run_pipeline(str(repo_root / "data" / "service_data.json"))

    assert result["records_processed"] == 10
    assert len(result["anomalies_detected"]) == 2
    assert len(result["events_consumed"]) == 2
    assert all(event["type"] == "ANOMALY" for event in result["events_consumed"])


def test_event_topic_clear_removes_messages():
    topic = EventTopic("test-topic")
    topic.publish({"type": "ANOMALY"})

    topic.clear()

    assert topic.get_messages() == []


def test_event_producer_rejects_empty_event():
    topic = EventTopic("test-topic")
    producer = EventProducer(topic)

    assert not producer.publish(None)


def test_main_block_runs_without_error():
    repo_root = Path(__file__).resolve().parents[1]
    script_path = repo_root / "src" / "aiops_pipeline.py"

    runpy.run_path(str(script_path), run_name="__main__")


def test_load_data_reads_json_file():
    repo_root = Path(__file__).resolve().parents[1]

    data = load_data(str(repo_root / "data" / "service_data.json"))

    assert len(data) == 10