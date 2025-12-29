# ============================================================
# Aorta - Hospital Admission Monitoring
# Confluent Cloud Infrastructure
# ============================================================

# Random suffix for unique resource names
resource "random_id" "suffix" {
  byte_length = 4
}

locals {
  project_name = "aorta"

  # GCP region mapping to Confluent Cloud regions
  confluent_region = var.gcp_region
  cloud_provider   = "GCP"
}

# ============================================================
# CONFLUENT CLOUD ENVIRONMENT
# ============================================================

resource "confluent_environment" "aorta_env" {
  display_name = "${local.project_name}-env-${random_id.suffix.hex}"

  stream_governance {
    package = "ESSENTIALS"  # Use ESSENTIALS for lower cost, upgrade to ADVANCED if needed
  }
}

# ============================================================
# KAFKA CLUSTER
# ============================================================

resource "confluent_kafka_cluster" "aorta_cluster" {
  display_name = "${local.project_name}-cluster-${random_id.suffix.hex}"
  availability = "SINGLE_ZONE"  # Multi-zone for production
  cloud        = local.cloud_provider
  region       = local.confluent_region

  standard {}  # Standard cluster (can change to basic for lower cost)

  environment {
    id = confluent_environment.aorta_env.id
  }
}

# ============================================================
# SCHEMA REGISTRY
# ============================================================

data "confluent_schema_registry_cluster" "essentials" {
  environment {
    id = confluent_environment.aorta_env.id
  }

  depends_on = [
    confluent_kafka_cluster.aorta_cluster
  ]
}

# ============================================================
# SERVICE ACCOUNT & PERMISSIONS
# ============================================================

resource "confluent_service_account" "aorta_app" {
  display_name = "${local.project_name}-app-${random_id.suffix.hex}"
  description  = "Service account for Aorta application"
}

# Grant environment admin role
resource "confluent_role_binding" "aorta_app_env_admin" {
  principal   = "User:${confluent_service_account.aorta_app.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.aorta_env.resource_name
}

# ============================================================
# API KEYS
# ============================================================

# Kafka API Key
resource "confluent_api_key" "aorta_kafka_key" {
  display_name = "${local.project_name}-kafka-key"
  description  = "Kafka API Key for Aorta application"

  owner {
    id          = confluent_service_account.aorta_app.id
    api_version = confluent_service_account.aorta_app.api_version
    kind        = confluent_service_account.aorta_app.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.aorta_cluster.id
    api_version = confluent_kafka_cluster.aorta_cluster.api_version
    kind        = confluent_kafka_cluster.aorta_cluster.kind

    environment {
      id = confluent_environment.aorta_env.id
    }
  }

  depends_on = [
    confluent_role_binding.aorta_app_env_admin
  ]
}

# Schema Registry API Key
resource "confluent_api_key" "aorta_schema_registry_key" {
  display_name = "${local.project_name}-sr-key"
  description  = "Schema Registry API Key for Aorta"

  owner {
    id          = confluent_service_account.aorta_app.id
    api_version = confluent_service_account.aorta_app.api_version
    kind        = confluent_service_account.aorta_app.kind
  }

  managed_resource {
    id          = data.confluent_schema_registry_cluster.essentials.id
    api_version = data.confluent_schema_registry_cluster.essentials.api_version
    kind        = data.confluent_schema_registry_cluster.essentials.kind

    environment {
      id = confluent_environment.aorta_env.id
    }
  }

  depends_on = [
    confluent_role_binding.aorta_app_env_admin
  ]
}

# ============================================================
# FLINK COMPUTE POOL
# ============================================================

data "confluent_flink_region" "aorta_flink_region" {
  cloud  = local.cloud_provider
  region = local.confluent_region
}

resource "confluent_flink_compute_pool" "aorta_flink_pool" {
  display_name = "${local.project_name}-flink-pool-${random_id.suffix.hex}"
  cloud        = local.cloud_provider
  region       = local.confluent_region
  max_cfu      = 10  # Start small, can scale up to 20

  environment {
    id = confluent_environment.aorta_env.id
  }
}

# Flink API Key
resource "confluent_api_key" "aorta_flink_key" {
  display_name = "${local.project_name}-flink-key"
  description  = "Flink API Key for Aorta"

  owner {
    id          = confluent_service_account.aorta_app.id
    api_version = confluent_service_account.aorta_app.api_version
    kind        = confluent_service_account.aorta_app.kind
  }

  managed_resource {
    id          = data.confluent_flink_region.aorta_flink_region.id
    api_version = data.confluent_flink_region.aorta_flink_region.api_version
    kind        = data.confluent_flink_region.aorta_flink_region.kind

    environment {
      id = confluent_environment.aorta_env.id
    }
  }
}

# ============================================================
# KAFKA TOPICS
# ============================================================

# Topic: hospital-admissions (source data)
resource "confluent_kafka_topic" "admissions" {
  kafka_cluster {
    id = confluent_kafka_cluster.aorta_cluster.id
  }

  topic_name         = "hospital-admissions"
  partitions_count   = 3
  rest_endpoint      = confluent_kafka_cluster.aorta_cluster.rest_endpoint

  config = {
    "retention.ms"         = "604800000"  # 7 days
    "cleanup.policy"       = "delete"
  }

  credentials {
    key    = confluent_api_key.aorta_kafka_key.id
    secret = confluent_api_key.aorta_kafka_key.secret
  }
}

# Topic: admission-alerts (processed alerts)
resource "confluent_kafka_topic" "alerts" {
  kafka_cluster {
    id = confluent_kafka_cluster.aorta_cluster.id
  }

  topic_name         = "admission-alerts"
  partitions_count   = 3
  rest_endpoint      = confluent_kafka_cluster.aorta_cluster.rest_endpoint

  config = {
    "retention.ms"         = "604800000"  # 7 days
    "cleanup.policy"       = "delete"
  }

  credentials {
    key    = confluent_api_key.aorta_kafka_key.id
    secret = confluent_api_key.aorta_kafka_key.secret
  }
}

# Topic: patient-labs (optional - for lab enrichment)
resource "confluent_kafka_topic" "labs" {
  kafka_cluster {
    id = confluent_kafka_cluster.aorta_cluster.id
  }

  topic_name         = "patient-labs"
  partitions_count   = 3
  rest_endpoint      = confluent_kafka_cluster.aorta_cluster.rest_endpoint

  config = {
    "retention.ms"         = "604800000"  # 7 days
    "cleanup.policy"       = "delete"
  }

  credentials {
    key    = confluent_api_key.aorta_kafka_key.id
    secret = confluent_api_key.aorta_kafka_key.secret
  }
}

# Topic: patient-vitals (chartevents vital signs)
resource "confluent_kafka_topic" "vitals" {
  kafka_cluster {
    id = confluent_kafka_cluster.aorta_cluster.id
  }

  topic_name         = "patient-vitals"
  partitions_count   = 3
  rest_endpoint      = confluent_kafka_cluster.aorta_cluster.rest_endpoint

  config = {
    "retention.ms"         = "604800000"  # 7 days
    "cleanup.policy"       = "delete"
  }

  credentials {
    key    = confluent_api_key.aorta_kafka_key.id
    secret = confluent_api_key.aorta_kafka_key.secret
  }
}

# Topic: icu-admissions (ICU stay events)
resource "confluent_kafka_topic" "icu_admissions" {
  kafka_cluster {
    id = confluent_kafka_cluster.aorta_cluster.id
  }

  topic_name         = "icu-admissions"
  partitions_count   = 3
  rest_endpoint      = confluent_kafka_cluster.aorta_cluster.rest_endpoint

  config = {
    "retention.ms"         = "604800000"  # 7 days
    "cleanup.policy"       = "delete"
  }

  credentials {
    key    = confluent_api_key.aorta_kafka_key.id
    secret = confluent_api_key.aorta_kafka_key.secret
  }
}

# Topic: sepsis-alerts (ML prediction alerts)
resource "confluent_kafka_topic" "sepsis_alerts" {
  kafka_cluster {
    id = confluent_kafka_cluster.aorta_cluster.id
  }

  topic_name         = "sepsis-alerts"
  partitions_count   = 3
  rest_endpoint      = confluent_kafka_cluster.aorta_cluster.rest_endpoint

  config = {
    "retention.ms"         = "604800000"  # 7 days
    "cleanup.policy"       = "delete"
  }

  credentials {
    key    = confluent_api_key.aorta_kafka_key.id
    secret = confluent_api_key.aorta_kafka_key.secret
  }
}

# Topic: clinical-recommendations (RAG-generated treatment recommendations)
resource "confluent_kafka_topic" "clinical_recommendations" {
  kafka_cluster {
    id = confluent_kafka_cluster.aorta_cluster.id
  }

  topic_name         = "clinical-recommendations"
  partitions_count   = 3
  rest_endpoint      = confluent_kafka_cluster.aorta_cluster.rest_endpoint

  config = {
    "retention.ms"         = "604800000"  # 7 days
    "cleanup.policy"       = "delete"
  }

  credentials {
    key    = confluent_api_key.aorta_kafka_key.id
    secret = confluent_api_key.aorta_kafka_key.secret
  }
}

# ============================================================
# ACLs (Access Control Lists)
# ============================================================

# Allow app to read from all topics
resource "confluent_kafka_acl" "app_read" {
  kafka_cluster {
    id = confluent_kafka_cluster.aorta_cluster.id
  }
  resource_type = "TOPIC"
  resource_name = "*"
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.aorta_app.id}"
  host          = "*"
  operation     = "READ"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.aorta_cluster.rest_endpoint

  credentials {
    key    = confluent_api_key.aorta_kafka_key.id
    secret = confluent_api_key.aorta_kafka_key.secret
  }
}

# Allow app to write to all topics
resource "confluent_kafka_acl" "app_write" {
  kafka_cluster {
    id = confluent_kafka_cluster.aorta_cluster.id
  }
  resource_type = "TOPIC"
  resource_name = "*"
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.aorta_app.id}"
  host          = "*"
  operation     = "WRITE"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.aorta_cluster.rest_endpoint

  credentials {
    key    = confluent_api_key.aorta_kafka_key.id
    secret = confluent_api_key.aorta_kafka_key.secret
  }
}

# Allow app to create topics
resource "confluent_kafka_acl" "app_create" {
  kafka_cluster {
    id = confluent_kafka_cluster.aorta_cluster.id
  }
  resource_type = "TOPIC"
  resource_name = "*"
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.aorta_app.id}"
  host          = "*"
  operation     = "CREATE"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.aorta_cluster.rest_endpoint

  credentials {
    key    = confluent_api_key.aorta_kafka_key.id
    secret = confluent_api_key.aorta_kafka_key.secret
  }
}

# Allow app to describe cluster
resource "confluent_kafka_acl" "app_describe" {
  kafka_cluster {
    id = confluent_kafka_cluster.aorta_cluster.id
  }
  resource_type = "CLUSTER"
  resource_name = "kafka-cluster"
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.aorta_app.id}"
  host          = "*"
  operation     = "DESCRIBE"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.aorta_cluster.rest_endpoint

  credentials {
    key    = confluent_api_key.aorta_kafka_key.id
    secret = confluent_api_key.aorta_kafka_key.secret
  }
}

# Allow app to read consumer groups
resource "confluent_kafka_acl" "app_read_group" {
  kafka_cluster {
    id = confluent_kafka_cluster.aorta_cluster.id
  }
  resource_type = "GROUP"
  resource_name = "*"
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.aorta_app.id}"
  host          = "*"
  operation     = "READ"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.aorta_cluster.rest_endpoint

  credentials {
    key    = confluent_api_key.aorta_kafka_key.id
    secret = confluent_api_key.aorta_kafka_key.secret
  }
}
