module "enable_apis" {
  source  = "terraform-google-modules/project-factory/google//modules/project_services"
  version = "11.1.1"

  for_each = var.project_to_api_mapping

  project_id    = each.key
  activate_apis = each.value
}