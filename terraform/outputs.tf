output "data_bucket" {
  description = "S3 data-lake bucket. Export as DATA_BUCKET for ingestion + dbt."
  value       = aws_s3_bucket.lake.bucket
}

output "glue_database" {
  description = "Glue/Athena database. Export as GLUE_DATABASE."
  value       = aws_glue_catalog_database.nyc_taxi.name
}

output "glue_crawler" {
  description = "Bronze Glue crawler name. Export as GLUE_CRAWLER."
  value       = aws_glue_crawler.bronze.name
}

output "athena_workgroup" {
  description = "Athena workgroup. Export as ATHENA_WORKGROUP."
  value       = aws_athena_workgroup.nyc_taxi.name
}

output "env_exports" {
  description = "Copy-paste block to configure ingestion + dbt."
  value       = <<-EOT
    export DATA_BUCKET=${aws_s3_bucket.lake.bucket}
    export AWS_REGION=${var.region}
    export GLUE_DATABASE=${aws_glue_catalog_database.nyc_taxi.name}
    export ATHENA_WORKGROUP=${aws_athena_workgroup.nyc_taxi.name}
    export GLUE_CRAWLER=${aws_glue_crawler.bronze.name}
  EOT
}
