# Producers - Kafka Data Streaming

Kafka producers that stream MIMIC-IV data to Confluent Cloud topics.

## Available Producers

### 1. Admission Stream (`stream_admissions.py`)
Streams hospital admission events in chronological order.

**Topic**: `hospital-admissions`

**Usage**:
```bash
python stream_admissions.py --max-events 50 --speed 10
```

**Event Schema**:
```json
{
  "event_type": "ADMISSION",
  "timestamp": "2112-05-28 19:45:00",
  "patient": {
    "subject_id": "10000032",
    "age": 52,
    "gender": "F"
  },
  "admission": {
    "hadm_id": "29079034",
    "type": "URGENT",
    "location": "EMERGENCY ROOM",
    "insurance": "Medicare",
    "language": "ENGLISH",
    "marital_status": "MARRIED"
  },
  "discharge": {
    "time": "2112-06-05 15:45:00",
    "location": "HOME"
  }
}
```

### 2. Lab Results Stream (`stream_labs.py`) - TODO
Streams laboratory test results.

**Topic**: `patient-labs`

### 3. Vital Signs Stream (`stream_vitals.py`) - TODO
Streams ICU vital signs (heart rate, blood pressure, etc.).

**Topic**: `patient-vitals`

## Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Ensure Kafka config exists
ls ../_data/kafka_config.json

# Run producer
python stream_admissions.py
```

## Monitoring

Check Confluent Cloud console:
1. Go to https://confluent.cloud
2. Select cluster: `aorta-cluster-98919aab`
3. Click "Topics" → Select topic
4. View messages in real-time
