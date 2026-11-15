output "dataset_id" {
  value = google_bigquery_dataset.dataset.dataset_id
}

output "dataset_self_link" {
  value = google_bigquery_dataset.dataset.self_link
}

output "table_id" { 
  value=google_bigquery_table.table.table_id 
}

