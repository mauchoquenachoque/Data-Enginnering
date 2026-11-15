terraform {
  required_version = ">= 1.5.0"
  required_providers {
    upstash = {
      source  = "upstash/upstash"
      version = "~> 1.4.0"
    }
  }
}

variable "upstash_email" {
  type        = string
  description = "Upstash account email"
}

variable "upstash_api_key" {
  type        = string
  description = "Upstash Management API Key"
  sensitive   = true
}

provider "upstash" {
  email   = var.upstash_email
  api_key = var.upstash_api_key
}

# Provision Serverless Kafka Cluster on public SaaS
resource "upstash_kafka_cluster" "byteflow_cluster" {
  cluster_name = "byteflow-production-cluster"
  region       = "eu-central-1"
  multizone    = false
}

# Provision Orders Event Topic
resource "upstash_kafka_topic" "orders_topic" {
  cluster_id       = upstash_kafka_cluster.byteflow_cluster.cluster_id
  topic_name       = "ecommerce.orders"
  partitions       = 3
  retention_time_ms = 604800000 # 7 days
  cleanup_policy   = "delete"
}

output "kafka_endpoint" {
  value = upstash_kafka_cluster.byteflow_cluster.tcp_endpoint
}

output "kafka_username" {
  value = upstash_kafka_cluster.byteflow_cluster.username
}

output "kafka_password" {
  value     = upstash_kafka_cluster.byteflow_cluster.password
  sensitive = true
}
