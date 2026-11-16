variable "project" {
  type        = string
  description = "Google Cloud project ID"
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "bq_location" {
  type    = string
  default = "US"
}

variable "image" {
  type        = string
  description = "Container image built from this repository's Dockerfile, in Artifact Registry"
}

variable "finance_group" {
  type        = string
  default     = ""
  description = "Google group allowed to read the restricted datasets (pay rates, store-day labor cost)"
}

variable "pos_base_url" {
  type = string
}

variable "timekeeping_base_url" {
  type = string
}

variable "audits_base_url" {
  type = string
}

variable "sftp_host" {
  type = string
}

variable "budget_bucket" {
  type        = string
  description = "Bucket finance drops budget workbooks into"
}
