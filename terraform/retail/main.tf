terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
}

# Autenticación: usa Application Default Credentials (gcloud auth application-default login)
provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id    = var.retail_dataset_id
  friendly_name = var.retail_dataset_id 
  description   = "Dataset proyecto de retail"
  location      = var.location
  
  

  labels = {
    env     = "prod"
    managed = "terraform"
  }
}

resource "google_bigquery_table" "table" { 
  dataset_id=google_bigquery_dataset.dataset.dataset_id 
  table_id=var.retail_table_id
  deletion_protection=false
  schema= file("${path.module}/${var.schema_path}")
  
  labels= { 
    env="prod"
  }
}
