variable "snowflake_org" {
  description = "Snowflake organization name"
  type        = string
}

variable "snowflake_account" {
  description = "Snowflake account name (without org prefix)"
  type        = string
}

variable "snowflake_user" {
  description = "Snowflake admin user"
  type        = string
}

variable "snowflake_password" {
  description = "Snowflake admin password"
  type        = string
  sensitive   = true
}
