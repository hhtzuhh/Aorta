# Aorta

Real-time hospital admission monitoring system using Confluent Cloud + GCP.

Built for Confluent Hackathon 2024 - demonstrates streaming AI application for healthcare.

## Project Overview

**Goal**: Stream hospital admission events in real-time, detect high-priority cases, and alert medical staff.

**Tech Stack**:
- **Data Source**: MIMIC-IV medical database (SQLite)
- **Streaming**: Confluent Cloud (Kafka + Flink)
- **Cloud**: Google Cloud Platform (Vertex AI, BigQuery, Cloud Run)
- **Infrastructure**: Terraform (automated setup/teardown)

## Quick Start

### 1. Set Up Infrastructure (Terraform)

```bash
cd terraform/

# Configure your Confluent Cloud credentials
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your API keys

# Create all infrastructure
terraform init
terraform apply
```

See [terraform/README.md](terraform/README.md) for detailed instructions.

### 2. Stream Admission Data (Python)

```bash
# Install dependencies
pip install confluent-kafka pandas

# Get Kafka config from terraform
cd ..
terraform -chdir=terraform output -json kafka_config > kafka_config.json

# Stream admissions to Kafka
python stream_admissions.py
```

### 3. Process with Flink (SQL)

Create Flink SQL job in Confluent Cloud console to detect emergency admissions.

### 4. Build Dashboard (Optional)

Deploy Cloud Run app to visualize real-time alerts.

## Project Structure

```
Aorta/
├── terraform/              # Infrastructure as Code
│   ├── main.tf            # Confluent Cloud resources
│   ├── variables.tf       # Configuration variables
│   ├── outputs.tf         # Resource outputs
│   └── README.md          # Setup instructions
│
├── src/                   # Application code (to be created)
│   ├── producer.py       # Stream admissions to Kafka
│   ├── consumer.py       # Read alerts from Kafka
│   └── flink_jobs/       # Flink SQL definitions
│
├── data/                  # MIMIC-IV data (link to parent)
│   └── mimic_demo.db     # SQLite database
│
└── README.md             # This file
```

## Development Workflow

### Daily Dev Cycle

**Morning** (Start of work):
```bash
cd terraform/
terraform apply    # Create infrastructure (~5 min)
```

**Evening** (End of work):
```bash
terraform destroy  # Delete everything to save cost (~5 min)
```

**Why?** Confluent Cloud costs ~$50-100/day when running. Destroying resources stops billing.

### Cost Optimization

- **Terraform**: Automates create/destroy cycle
- **Free tier**: Confluent gives $400 credit
- **Development**: Only run infrastructure when actively working
- **Production**: Keep running for demo/presentation

## Architecture

```
MIMIC-IV SQLite DB
    ↓
Python Producer (stream_admissions.py)
    ↓
Kafka Topic: hospital-admissions
    ↓
Flink SQL (detect emergencies)
    ↓
Kafka Topic: admission-alerts
    ↓
Cloud Run Dashboard (visualize)
```

## Next Steps

1. ✅ Set up Terraform infrastructure
2. ⏳ Write Python Kafka producer
3. ⏳ Create Flink SQL job for alerts
4. ⏳ Build simple dashboard
5. ⏳ Record demo video
6. ⏳ Submit to hackathon

## Resources

- [Confluent Cloud Console](https://confluent.cloud)
- [MIMIC-IV Documentation](https://mimic.mit.edu/docs/iv/)
- [Terraform Setup Guide](terraform/README.md)
- [Hackathon Rules](../docs/rule.md)

## License

Educational project for Confluent Hackathon 2024.
