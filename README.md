# AIOps Monitoring and Event Processing Assessment

## Scenario
This project simulates a lightweight AIOps workflow for a payment service that emits operational telemetry. The goal is to monitor service health, detect abnormal behavior in metrics and logs, turn those findings into events, and pass them through a simplified event-streaming flow before producing an operational alert.

The service being monitored is a `payment-service`. The operational problem is that performance and resource metrics can spike unexpectedly while error-level log events appear, which may indicate a service degradation or outage. The purpose of AIOps in this assessment is to identify those abnormal signals early, convert them into structured events, and confirm that those events can travel through the producer/topic/consumer pipeline to a final downstream processing stage.

## Operational Data
The repository contains synthetic service telemetry in `data/service_data.json`. Each record contains:

- `timestamp`: the time of the observation
- `service`: the monitored service name
- `response_time_ms`: request latency in milliseconds
- `cpu_percent`: CPU usage
- `memory_percent`: memory usage
- `log_level`: log severity such as `INFO`, `WARNING`, or `ERROR`
- `message`: human-readable operational message

## Observations from the Logs and Metrics
### Metrics fields
The metric fields are:

- `response_time_ms`
- `cpu_percent`
- `memory_percent`

### Log fields
The log-related fields are:

- `log_level`
- `message`

### Timestamp usage
The `timestamp` values are sequential one-minute intervals from `2026-09-20T10:00:00` through `2026-09-20T10:09:00`. They show the progression of service behavior over time and allow us to correlate abnormal spikes with the surrounding operational history.

### Normal observations
The records between `10:00` and `10:04` and again between `10:07` and `10:09` are normal. They show stable response times, moderate CPU and memory usage, and `INFO` log entries indicating successful processing.

### Unusual observations
The records at `10:05` and `10:06` are clearly abnormal:

- `response_time_ms` jumps from around 120-150 ms to 610-640 ms
- `cpu_percent` rises to 75% and then 94%
- `memory_percent` rises to 70% and then 91%
- `log_level` changes to `ERROR`
- `message` content indicates timeout and database connection failures

These signals reflect degraded service behavior and are the main anomalies in the dataset.

## Anomaly Detection Findings
The detection logic in `src/anomaly_detector.py` flags records when one or more threshold conditions are exceeded:

- high response time above 500 ms
- high CPU usage above 80%
- high memory usage above 80%
- warning or error log activity

The detector correctly identifies the abnormal records at `10:05` and `10:06` as anomalies and produces structured events containing a timestamp, service name, event type, reason list, and the original record data.

### Detected anomalies
The final pipeline output identified two anomalies:

1. `2026-09-20T10:05:00` - service timeout, high response time, and error log event
2. `2026-09-20T10:06:00` - service degradation, very high CPU and memory use, plus an error log event

### Missed anomalies or false positives
No expected anomaly was missed in this dataset. The record at `10:06` is correctly flagged, and the normal records around it remain non-anomalous. In this implementation, a warning or error log alone is enough to contribute to the anomaly diagnosis, which is appropriate for this assessment data.

### Limitation and improvement
A limitation of this approach is that it uses hardcoded thresholds and a simple rule-based model. It does not learn from past baselines or account for service-specific normal ranges. A useful improvement would be to introduce dynamic thresholds or statistical anomaly detection based on historical usage patterns.

## Event Processing Flow
The repository simulates an AIOps event pipeline:

1. Operational data is loaded from `data/service_data.json`
2. `AnomalyDetector` inspects each record
3. Matching abnormal records produce `ANOMALY` events
4. `EventProducer` publishes the event to an in-memory topic
5. `EventTopic` stores the published messages
6. `EventConsumer` retrieves the messages from the topic
7. The final pipeline output reports the consumed anomaly events

The key components are:

- Producer: `src/event_producer.py`
- Topic: `src/event_topic.py`
- Consumer: `src/event_consumer.py`
- Event/message: structured anomaly objects passed between components

## Workflow Fixes Applied
The repository initially had several issues that prevented the pipeline from working cleanly:

1. Python import path issue: `pytest` was not resolving the `src` package from the repo root
2. Module imports in the event producer, consumer, and pipeline did not work reliably in a package context
3. The anomaly detector was misclassifying log conditions and the pipeline used the wrong topic wiring for the end-to-end flow

These issues were corrected by:

- adding a project-level `pytest.ini` file to ensure `pythonpath = .`
- using package-safe imports in the source modules
- aligning the anomaly detection logic with the evaluation requirements
- ensuring the event producer and consumer operate on the same topic instance during the pipeline run

## Final Workflow Result
The final end-to-end workflow was verified successfully with:

- `pytest -q` → 8 passed
- `python src/aiops_pipeline.py` → pipeline completed successfully

Pipeline summary:

- Records processed: 10
- Anomalies detected: 2
- Events consumed: 2

The detected anomalies correspond to the payment service timeout and the following database connection timeout event.

## Reproduction Steps
To reproduce the demonstration from a fresh checkout:

1. Open the repository in GitHub Codespaces or a local clone.
2. Navigate to the repo root.
3. Run:
   ```bash
   pytest -q
   ```
4. Run:
   ```bash
   python src/aiops_pipeline.py
   ```
5. Review the output showing records processed, detected anomalies, and consumed events.
6. Confirm the final event flow from anomaly detection to topic consumption is working.

## Validation
The repository validation and workflow were verified using the project’s provided tests and the full pipeline execution. The result confirms that operational data can be processed, anomalies can be detected, events can be produced and consumed, and the AIOps pipeline completes successfully.

---

&copy; 2025 GitHub &bull; [Code of Conduct](https://www.contributor-covenant.org/version/2/1/code_of_conduct/code_of_conduct.md) &bull; [MIT License](https://gh.io/mit)

