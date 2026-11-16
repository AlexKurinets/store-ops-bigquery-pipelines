# Production layout on Google Cloud. Each source contract becomes a Cloud Run job and a Cloud Scheduler trigger,
# with the cron and time zone read from the same contracts/*.yaml the pipeline and the docs use.
# CI runs `terraform validate` on this module; it is not applied to a real project from this repository.

terraform {
  required_version = ">= 1.9"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.10"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
}

locals {
  contracts = {
    for f in fileset("${path.module}/../../contracts", "*.yaml") :
    trimsuffix(f, ".yaml") => yamldecode(file("${path.module}/../../contracts/${f}"))
  }
  datasets            = ["raw", "raw_restricted", "reference", "staged", "conformed", "restricted", "metrics", "ops"]
  restricted_datasets = ["raw_restricted", "restricted"]
  secrets = ["pos-client-id", "pos-client-secret", "timekeeping-user", "timekeeping-password", "audits-api-key",
  "sftp-password", "sftp-host-key-pub", "pseudonym-key"]
  job_env = {
    STOREOPS_WAREHOUSE            = "bigquery"
    STOREOPS_BQ_PROJECT           = var.project
    STOREOPS_BQ_LOCATION          = var.bq_location
    STOREOPS_SECRETS              = "gcp:${var.project}"
    STOREOPS_LANDING              = "gcs"
    STOREOPS_LANDING_BUCKET       = google_storage_bucket.landing.name
    STOREOPS_RESTRICTED_BUCKET    = google_storage_bucket.landing_restricted.name
    STOREOPS_POS_BASE_URL         = var.pos_base_url
    STOREOPS_TIMEKEEPING_BASE_URL = var.timekeeping_base_url
    STOREOPS_AUDITS_BASE_URL      = var.audits_base_url
    STOREOPS_SFTP_HOST            = var.sftp_host
    STOREOPS_BUDGET_BUCKET        = var.budget_bucket
    STOREOPS_TOTAL_ATTEMPTS       = "2"
  }
}

# ------------------------------------------------------------------------------------------- service accounts
resource "google_service_account" "pipeline" {
  account_id   = "storeops-pipeline"
  display_name = "storeops connectors and dbt"
}

resource "google_service_account" "agents" {
  account_id   = "storeops-agents"
  display_name = "MCP server: reads the metrics dataset only"
}

resource "google_service_account" "scheduler" {
  account_id   = "storeops-scheduler"
  display_name = "Cloud Scheduler: starts the Cloud Run jobs"
}

resource "google_project_iam_member" "pipeline_jobs" {
  project = var.project
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_project_iam_member" "agents_jobs" {
  project = var.project
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.agents.email}"
}

# ------------------------------------------------------------------------------------------------- storage
resource "google_storage_bucket" "landing" {
  name                        = "${var.project}-storeops-landing"
  location                    = var.bq_location
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  lifecycle_rule {
    condition { age = 90 }
    action { type = "Delete" }
  }
}

resource "google_storage_bucket" "landing_restricted" {
  name                        = "${var.project}-storeops-landing-restricted"
  location                    = var.bq_location
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  lifecycle_rule {
    condition { age = 30 }
    action { type = "Delete" }
  }
}

resource "google_storage_bucket_iam_member" "landing_writer" {
  for_each = { landing = google_storage_bucket.landing.name, restricted = google_storage_bucket.landing_restricted.name }
  bucket   = each.value
  role     = "roles/storage.objectAdmin"
  member   = "serviceAccount:${google_service_account.pipeline.email}"
}

# ------------------------------------------------------------------------------------------------ BigQuery
resource "google_bigquery_dataset" "layer" {
  for_each                   = toset(local.datasets)
  dataset_id                 = each.key
  location                   = var.bq_location
  delete_contents_on_destroy = false
  labels                     = { system = "storeops", restricted = contains(local.restricted_datasets, each.key) ? "yes" : "no" }
}

resource "google_bigquery_dataset_iam_member" "pipeline_owner" {
  for_each   = google_bigquery_dataset.layer
  dataset_id = each.value.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

# Agents read the metrics dataset and nothing else.
resource "google_bigquery_dataset_iam_member" "agents_metrics" {
  dataset_id = google_bigquery_dataset.layer["metrics"].dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.agents.email}"
}

# Finance can read the restricted datasets; nobody else is granted them.
resource "google_bigquery_dataset_iam_member" "finance_restricted" {
  for_each   = var.finance_group == "" ? toset([]) : toset(local.restricted_datasets)
  dataset_id = google_bigquery_dataset.layer[each.key].dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "group:${var.finance_group}"
}

# ------------------------------------------------------------------------------------------- Secret Manager
resource "google_secret_manager_secret" "secret" {
  for_each  = toset(local.secrets)
  secret_id = each.key
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_iam_member" "pipeline_reads" {
  for_each  = google_secret_manager_secret.secret
  secret_id = each.value.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.pipeline.email}"
}

# --------------------------------------------------------------------------------- Cloud Run jobs + Scheduler
resource "google_cloud_run_v2_job" "source" {
  for_each            = local.contracts
  name                = "storeops-${each.key}"
  location            = var.region
  deletion_protection = false
  template {
    task_count = 1
    template {
      service_account = google_service_account.pipeline.email
      max_retries     = 1 # one automatic retry; the runner alerts only when the last attempt fails
      timeout         = "1800s"
      containers {
        image = var.image
        args  = ["storeops", "run", each.key, "--date", "auto"]
        dynamic "env" {
          for_each = local.job_env
          content {
            name  = env.key
            value = env.value
          }
        }
        env {
          name = "STOREOPS_ALERT_WEBHOOK_URL"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.alert_webhook.secret_id
              version = "latest"
            }
          }
        }
        resources {
          limits = { cpu = "1", memory = "1Gi" }
        }
      }
    }
  }
}

resource "google_secret_manager_secret" "alert_webhook" {
  secret_id = "slack-alert-webhook"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_iam_member" "pipeline_reads_webhook" {
  secret_id = google_secret_manager_secret.alert_webhook.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_cloud_run_v2_job" "transform" {
  name                = "storeops-transform"
  location            = var.region
  deletion_protection = false
  template {
    template {
      service_account = google_service_account.pipeline.email
      max_retries     = 1
      timeout         = "1800s"
      containers {
        image = var.image
        args  = ["sh", "-c", "storeops transform && storeops check pii && storeops check reconciliation; storeops check freshness"]
        dynamic "env" {
          for_each = local.job_env
          content {
            name  = env.key
            value = env.value
          }
        }
      }
    }
  }
}

resource "google_cloud_run_v2_job_iam_member" "scheduler_invokes" {
  for_each = merge({ for k, j in google_cloud_run_v2_job.source : k => j.name }, { transform = google_cloud_run_v2_job.transform.name })
  name     = each.value
  location = var.region
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.scheduler.email}"
}

resource "google_cloud_scheduler_job" "source" {
  for_each  = local.contracts
  name      = "storeops-${each.key}"
  schedule  = each.value.schedule
  time_zone = each.value.timezone
  region    = var.region
  retry_config {
    retry_count = 1
  }
  http_target {
    http_method = "POST"
    uri         = "https://run.googleapis.com/v2/projects/${var.project}/locations/${var.region}/jobs/${google_cloud_run_v2_job.source[each.key].name}:run"
    oauth_token {
      service_account_email = google_service_account.scheduler.email
    }
  }
}

# dbt and the checks run after the morning loads, and again after the distributor's 07:30 job.
resource "google_cloud_scheduler_job" "transform" {
  name      = "storeops-transform"
  schedule  = "15 7,9 * * *"
  time_zone = "America/Chicago"
  region    = var.region
  http_target {
    http_method = "POST"
    uri         = "https://run.googleapis.com/v2/projects/${var.project}/locations/${var.region}/jobs/${google_cloud_run_v2_job.transform.name}:run"
    oauth_token {
      service_account_email = google_service_account.scheduler.email
    }
  }
}

# ------------------------------------------------------------------------------------------------ MCP server
resource "google_cloud_run_v2_service" "mcp" {
  name                = "storeops-mcp"
  location            = var.region
  ingress             = "INGRESS_TRAFFIC_INTERNAL_ONLY"
  deletion_protection = false
  template {
    service_account = google_service_account.agents.email
    containers {
      image = var.image
      args  = ["storeops", "mcp", "--host", "0.0.0.0", "--port", "8080"]
      ports {
        container_port = 8080
      }
      env {
        name  = "STOREOPS_WAREHOUSE"
        value = "bigquery"
      }
      env {
        name  = "STOREOPS_BQ_PROJECT"
        value = var.project
      }
    }
  }
}
