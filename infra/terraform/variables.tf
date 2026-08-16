variable "subscription_id" {
  description = "Azure subscription ID"
  type        = string
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "uksouth"
}

variable "project" {
  description = "Short project slug used in resource names"
  type        = string
  default     = "energylake"
}

variable "github_repo" {
  description = "owner/repo on GitHub, for scoping the OIDC federated identity credential's subject claim so only this exact repo can use it"
  type        = string
  default     = "faddaful/uk-energy-lakehouse"
}