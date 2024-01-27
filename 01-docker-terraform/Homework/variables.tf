variable "credentials" {
    description = "My credentials"
    default = "./keys/my-creds.json"
}

variable "project" {
  description = "Project"
  default     = "global-sign-411702"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "location" {
    description = "Project Location"
    default = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default ="demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  # The bucket name should be unique, based on project name preferred
  default = "global-sign-411702-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}



