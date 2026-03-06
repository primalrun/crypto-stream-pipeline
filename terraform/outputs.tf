output "snowflake_warehouse" {
  description = "Snowflake warehouse name"
  value       = snowflake_warehouse.crypto.name
}

output "snowflake_database" {
  description = "Snowflake database name"
  value       = snowflake_database.crypto.name
}

output "snowflake_role" {
  description = "Snowflake role for streaming writes"
  value       = snowflake_account_role.stream.name
}
