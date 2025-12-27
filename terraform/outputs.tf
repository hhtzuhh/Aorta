# ============================================================
# Aorta - Terraform Outputs
# ============================================================

# Environment Info
output "environment_id" {
  description = "Confluent Environment ID"
  value       = confluent_environment.aorta_env.id
}

output "environment_name" {
  description = "Confluent Environment Name"
  value       = confluent_environment.aorta_env.display_name
}

# Kafka Cluster Info
output "kafka_cluster_id" {
  description = "Kafka Cluster ID"
  value       = confluent_kafka_cluster.aorta_cluster.id
}

output "kafka_cluster_name" {
  description = "Kafka Cluster Name"
  value       = confluent_kafka_cluster.aorta_cluster.display_name
}

output "kafka_bootstrap_endpoint" {
  description = "Kafka Bootstrap Server Endpoint"
  value       = confluent_kafka_cluster.aorta_cluster.bootstrap_endpoint
}

output "kafka_rest_endpoint" {
  description = "Kafka REST API Endpoint"
  value       = confluent_kafka_cluster.aorta_cluster.rest_endpoint
}

# Schema Registry Info
output "schema_registry_id" {
  description = "Schema Registry Cluster ID"
  value       = data.confluent_schema_registry_cluster.essentials.id
}

output "schema_registry_endpoint" {
  description = "Schema Registry REST Endpoint"
  value       = data.confluent_schema_registry_cluster.essentials.rest_endpoint
}

# Service Account Info
output "service_account_id" {
  description = "Service Account ID"
  value       = confluent_service_account.aorta_app.id
}

# API Keys (sensitive)
output "kafka_api_key" {
  description = "Kafka API Key"
  value       = confluent_api_key.aorta_kafka_key.id
  sensitive   = true
}

output "kafka_api_secret" {
  description = "Kafka API Secret"
  value       = confluent_api_key.aorta_kafka_key.secret
  sensitive   = true
}

output "schema_registry_api_key" {
  description = "Schema Registry API Key"
  value       = confluent_api_key.aorta_schema_registry_key.id
  sensitive   = true
}

output "schema_registry_api_secret" {
  description = "Schema Registry API Secret"
  value       = confluent_api_key.aorta_schema_registry_key.secret
  sensitive   = true
}

# Flink Info
output "flink_compute_pool_id" {
  description = "Flink Compute Pool ID"
  value       = confluent_flink_compute_pool.aorta_flink_pool.id
}

output "flink_rest_endpoint" {
  description = "Flink REST Endpoint"
  value       = data.confluent_flink_region.aorta_flink_region.rest_endpoint
}

output "flink_api_key" {
  description = "Flink API Key"
  value       = confluent_api_key.aorta_flink_key.id
  sensitive   = true
}

output "flink_api_secret" {
  description = "Flink API Secret"
  value       = confluent_api_key.aorta_flink_key.secret
  sensitive   = true
}

# Topics
output "topics" {
  description = "Created Kafka Topics"
  value = {
    admissions     = confluent_kafka_topic.admissions.topic_name
    alerts         = confluent_kafka_topic.alerts.topic_name
    labs           = confluent_kafka_topic.labs.topic_name
    vitals         = confluent_kafka_topic.vitals.topic_name
    icu_admissions = confluent_kafka_topic.icu_admissions.topic_name
  }
}

# Connection Config (for Python producer)
output "kafka_config" {
  description = "Kafka configuration for Python clients"
  value = {
    bootstrap_servers = confluent_kafka_cluster.aorta_cluster.bootstrap_endpoint
    sasl_username     = confluent_api_key.aorta_kafka_key.id
    sasl_password     = confluent_api_key.aorta_kafka_key.secret
    security_protocol = "SASL_SSL"
    sasl_mechanism    = "PLAIN"
  }
  sensitive = true
}

# Summary for easy reference
output "summary" {
  description = "Quick reference summary"
  value = <<-EOT

    ========================================
    Aorta - Confluent Cloud Resources
    ========================================

    Environment: ${confluent_environment.aorta_env.display_name}
    Cluster:     ${confluent_kafka_cluster.aorta_cluster.display_name}
    Region:      ${var.gcp_region}

    Bootstrap Server:
      ${confluent_kafka_cluster.aorta_cluster.bootstrap_endpoint}

    Topics:
      - ${confluent_kafka_topic.admissions.topic_name}
      - ${confluent_kafka_topic.alerts.topic_name}
      - ${confluent_kafka_topic.labs.topic_name}
      - ${confluent_kafka_topic.vitals.topic_name}
      - ${confluent_kafka_topic.icu_admissions.topic_name}

    Flink Pool: ${confluent_flink_compute_pool.aorta_flink_pool.display_name}

    To view sensitive outputs (API keys):
      terraform output kafka_api_key
      terraform output kafka_api_secret

    ========================================
  EOT
}
