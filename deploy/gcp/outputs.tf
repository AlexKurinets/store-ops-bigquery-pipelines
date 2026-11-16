output "jobs" {
  value = { for k, j in google_cloud_run_v2_job.source : k => j.name }
}

output "schedules" {
  value = { for k, s in google_cloud_scheduler_job.source : k => "${s.schedule} ${s.time_zone}" }
}

output "mcp_url" {
  value = google_cloud_run_v2_service.mcp.uri
}
