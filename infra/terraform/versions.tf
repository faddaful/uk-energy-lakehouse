terraform {
  required_version = ">= 1.9.0"

  # State was local-only until this line (infra/terraform/terraform.tfstate,
  # gitignored) -- meaning only this machine ever knew what had actually
  # been deployed. CI does a fresh checkout with no state at all, so every
  # `terraform output` there returned empty strings, not an error: from
  # CI's point of view nothing had ever been created. That surfaced as
  # empty-string abfss:// URLs in dbt's bronze() macro, not as an obviously
  # state-related failure.
  #
  # Backend is the same storage account this project already provisions
  # (a separate "tfstate" container from "lakehouse", so state and bronze
  # data never share a container), authenticated with use_azuread_auth
  # rather than a storage account key -- the same "no stored secrets"
  # choice as everywhere else in this project. Both the human identity and
  # CI's identity already have Storage Blob Data Contributor on this
  # account (see main.tf), which is all this needs; no new RBAC grant.
  backend "azurerm" {
    resource_group_name  = "rg-energylake"
    storage_account_name = "stenergylakeka1xq2"
    container_name        = "tfstate"
    key                    = "lakehouse.tfstate"
    use_azuread_auth      = true
  }

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 3.0"
    }
  }
}

# For the GitHub Actions app registration + federated identity credential
# (CI's own identity, no secrets involved -- see main.tf). Authenticates
# the same way the azurerm provider does: the Azure CLI's cached login,
# no separate config needed here.
provider "azuread" {}

provider "azurerm" {
  subscription_id = var.subscription_id
  features {}

  # By default the provider checks the registration status of ~20 "core"
  # resource provider namespaces against the Azure API on every plan/apply,
  # one HTTP call at a time, before it does anything else — including ones
  # unrelated to anything this config uses (e.g. Microsoft.MixedReality).
  # That's what silently ate 30 minutes with zero output: normal `terraform
  # plan` prints nothing during this phase. Microsoft.Storage and
  # Microsoft.Authorization (the two this config actually needs) are
  # already Registered on this subscription — confirmed with
  # `az provider show -n <name> --query registrationState` — so there is
  # nothing for this reconciliation to do here. Skipping it removes the
  # stall entirely.
  resource_provider_registrations = "none"
}