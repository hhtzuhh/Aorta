# Aorta - Terraform Infrastructure

Automated infrastructure setup for Aorta hospital admission monitoring system using Confluent Cloud.

## What This Creates

- **Confluent Cloud Environment** - Isolated environment for all resources
- **Kafka Cluster** - Standard tier, single-zone (upgradable to multi-zone)
- **3 Kafka Topics**:
  - `hospital-admissions` - Source admission events
  - `admission-alerts` - Processed alerts
  - `patient-labs` - Lab result enrichment (optional)
- **Flink Compute Pool** - For stream processing (10 CFU max)
- **Service Account** - With full permissions
- **API Keys** - For Kafka, Schema Registry, and Flink
- **Schema Registry** - For data schemas (Essentials package)

## Prerequisites

1. **Confluent Cloud Account**
   - Sign up at https://confluent.cloud (free trial available)
   - Note: Free trial includes $400 credit

2. **Confluent Cloud API Keys**
   - Go to https://confluent.cloud/settings/api-keys
   - Click "Add API Key"
   - Select "Cloud resource management"
   - Save the key and secret (you won't see the secret again!)

3. **Terraform Installed**
   ```bash
   brew install terraform
   ```

## Setup Instructions

### Step 1: Configure Variables

```bash
cd terraform/
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` and add your Confluent Cloud credentials:

```hcl
confluent_cloud_api_key    = "YOUR_KEY_HERE"
confluent_cloud_api_secret = "YOUR_SECRET_HERE"
gcp_region                 = "us-central1"
gcp_project_id            = "your-gcp-project"
owner_email               = "you@example.com"
```

### Step 2: Initialize Terraform

```bash
terraform init
```

This downloads the Confluent provider and prepares terraform.

### Step 3: Review the Plan

```bash
terraform plan
```

This shows what will be created. Review carefully!

### Step 4: Create Resources

```bash
terraform apply
```

Type `yes` when prompted. This takes ~5-10 minutes.

### Step 5: View Outputs

```bash
# See all outputs
terraform output

# See specific sensitive values
terraform output kafka_api_key
terraform output kafka_api_secret
terraform output kafka_config
```

## Cost Estimation

**Confluent Cloud costs** (approximate):
- Standard Kafka Cluster: ~$1.50/hour (~$36/day)
- Flink Compute Pool (10 CFU): ~$1.50/hour when running
- Schema Registry (Essentials): ~$0.50/hour
- Data transfer: $0.10/GB

**Estimated daily cost**: ~$50-100 (depends on usage)

**To minimize costs**:
1. Use `terraform destroy` when not actively developing
2. Pause Flink compute pool when not in use
3. Use Basic cluster instead of Standard (edit main.tf)

## Daily Workflow

### Start of Day (Create Resources)
```bash
terraform apply
# Takes ~5-10 minutes
# Cost starts accruing
```

### End of Day (Destroy Resources)
```bash
terraform destroy
# Type 'yes' to confirm
# Takes ~5 minutes
# Stops all billing
```

### Quick Commands

```bash
# See what exists
terraform show

# See outputs
terraform output summary

# Refresh state
terraform refresh

# Format terraform files
terraform fmt

# Validate configuration
terraform validate
```

## Using the Outputs in Python

After `terraform apply`, get your Kafka configuration:

```bash
terraform output -json kafka_config > ../kafka_config.json
```

Then in Python:

```python
import json
from confluent_kafka import Producer

# Load config
with open('kafka_config.json') as f:
    config = json.load(f)

# Create producer
producer = Producer({
    'bootstrap.servers': config['bootstrap_servers'],
    'sasl.username': config['sasl_username'],
    'sasl.password': config['sasl_password'],
    'security.protocol': config['security_protocol'],
    'sasl.mechanism': config['sasl_mechanism']
})

# Send message
producer.produce('hospital-admissions', value='{"test": "message"}')
producer.flush()
```

## Troubleshooting

### Error: "Invalid credentials"
- Check your Confluent Cloud API key and secret in `terraform.tfvars`
- Make sure you created a "Cloud resource management" key, not a cluster-specific key

### Error: "Region not supported"
- Change `gcp_region` in `terraform.tfvars` to a supported region
- Supported regions are listed in `variables.tf`

### Resources already exist
```bash
terraform import confluent_environment.aorta_env env-xxxxx
```

### Want to start fresh
```bash
terraform destroy  # Delete everything
rm -rf .terraform terraform.tfstate*  # Clean state
terraform init     # Reinitialize
terraform apply    # Create fresh
```

## File Structure

```
terraform/
├── main.tf                    # Main resource definitions
├── variables.tf               # Input variables
├── providers.tf              # Provider configuration
├── outputs.tf                # Output values
├── terraform.tfvars.example  # Example configuration
├── terraform.tfvars          # Your actual config (git-ignored)
├── .gitignore               # Ignore sensitive files
└── README.md                # This file
```

## Security Notes

- **Never commit `terraform.tfvars`** - Contains API secrets
- **Never commit `terraform.tfstate`** - Contains sensitive data
- Both are in `.gitignore` for safety
- API keys are marked `sensitive = true` in outputs

## Next Steps

After infrastructure is created:

1. **Test connectivity**:
   ```bash
   terraform output kafka_bootstrap_endpoint
   ```

2. **Create Python producer** - Stream admission data to Kafka

3. **Create Flink jobs** - Process streams and generate alerts

4. **Build dashboard** - Visualize alerts (optional)

## Resources

- [Confluent Cloud Console](https://confluent.cloud)
- [Confluent Terraform Provider Docs](https://registry.terraform.io/providers/confluentinc/confluent/latest/docs)
- [Flink SQL Reference](https://docs.confluent.io/cloud/current/flink/reference/overview.html)
