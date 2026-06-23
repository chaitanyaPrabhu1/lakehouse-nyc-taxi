variable "project" {
  description = "Name prefix for all resources."
  type        = string
  default     = "nyc-taxi"
}

variable "region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "us-east-1"
}

variable "alarm_email" {
  description = "Email to receive the billing alarm (leave empty to skip the alarm)."
  type        = string
  default     = ""
}

variable "monthly_cost_alarm_usd" {
  description = "Estimated-charges threshold (USD) that triggers the billing alarm."
  type        = number
  default     = 5
}
