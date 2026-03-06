provider "snowflake" {
  account_name      = var.snowflake_account
  organization_name = var.snowflake_org
  user              = var.snowflake_user
  password          = var.snowflake_password
  role              = "ACCOUNTADMIN"
}
