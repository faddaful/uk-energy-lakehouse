terraform {
  required_version = ">= 1.9.0"

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