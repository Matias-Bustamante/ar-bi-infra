variable "project_id" {
  description = "ID del proyecto de GCP"
  type        = string
  default     = "data-engineer-509817"
}

variable "region" {
  description = "Región del datasets"
  type        = string
  default     = "southamerica-west1"
}

variable "location" {
  description = "Ubicación del dataset"
  type        = string
  default     = "southamerica-west1"
}

variable "retail_dataset_id" {
  description = "Nombre del dataset de BigQuery"
  type        = string
  default     = "retail_online"
}

variable "retail_table_id" { 
  description="Nombre de la tabla en bigquery" 
  type=string
  default="stg_retail_online"
}

variable "schema_path" { 
  type=string
  default="../../data/schema/retail/schema.json" 
}
