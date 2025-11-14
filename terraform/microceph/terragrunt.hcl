terraform {
  source = "."
}

locals {
  env_model_name = trimspace(
    get_env("JUJU_MODEL", "")
  )
  env_model_owner = trimspace(
    get_env("MICROCEPH_MODEL_OWNER", "")
  )
  env_model_uuid = trimspace(
    get_env("MICROCEPH_MODEL_UUID", "")
  )

  default_model_uuid  = local.env_model_uuid != "" ? local.env_model_uuid : null
  default_model_owner = local.env_model_owner != "" ? local.env_model_owner : "admin"
  default_model_name  = local.env_model_name != "" ? local.env_model_name : null
}

inputs = merge(
  local.default_model_uuid != null ? { model_uuid = local.default_model_uuid } : {},
  local.default_model_name != null ? { model_name = local.default_model_name } : {},
  local.default_model_name != null ? { model_owner = local.default_model_owner } : {}
)
