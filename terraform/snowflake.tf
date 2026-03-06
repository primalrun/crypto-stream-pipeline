resource "snowflake_warehouse" "crypto" {
  name           = "CRYPTO_WH"
  warehouse_size = "XSMALL"
  auto_suspend   = 60
  auto_resume    = true
}

resource "snowflake_database" "crypto" {
  name = "CRYPTO"
}

resource "snowflake_schema" "raw" {
  database = snowflake_database.crypto.name
  name     = "RAW"
}

resource "snowflake_account_role" "stream" {
  name = "STREAM_ROLE"
}

resource "snowflake_grant_account_role" "stream_to_user" {
  role_name = snowflake_account_role.stream.name
  user_name = upper(var.snowflake_user)
}

resource "snowflake_grant_privileges_to_account_role" "warehouse_usage" {
  privileges        = ["USAGE"]
  account_role_name = snowflake_account_role.stream.name
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.crypto.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "database_usage" {
  privileges        = ["USAGE"]
  account_role_name = snowflake_account_role.stream.name
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.crypto.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "schema_usage" {
  privileges        = ["USAGE", "CREATE TABLE"]
  account_role_name = snowflake_account_role.stream.name
  on_schema {
    schema_name = "${snowflake_database.crypto.name}.${snowflake_schema.raw.name}"
  }
}

resource "snowflake_grant_privileges_to_account_role" "future_tables" {
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"]
  account_role_name = snowflake_account_role.stream.name
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = "${snowflake_database.crypto.name}.${snowflake_schema.raw.name}"
    }
  }
}
