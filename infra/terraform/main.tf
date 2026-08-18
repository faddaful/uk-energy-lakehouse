data "azurerm_client_config" "current" {}

resource "random_string" "suffix" {
  length  = 6
  lower   = true
  upper   = false
  numeric = true
  special = false
}

resource "azurerm_resource_group" "this" {
  name     = "rg-${var.project}"
  location = var.location
}

resource "azurerm_storage_account" "this" {
  name                     = "st${var.project}${random_string.suffix.result}"
  resource_group_name      = azurerm_resource_group.this.name
  location                 = azurerm_resource_group.this.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  access_tier              = "Hot"

  is_hns_enabled                  = true
  min_tls_version                 = "TLS1_2"
  https_traffic_only_enabled      = true
  allow_nested_items_to_be_public = false

  tags = {
    project = var.project
    owner   = "personal"
  }
}

resource "azurerm_storage_container" "lakehouse" {
  name                  = "lakehouse"
  storage_account_id    = azurerm_storage_account.this.id
  container_access_type = "private"
}

# This is the actual cost tripwire for the whole project. Amount is in
# whatever currency this subscription bills in, assumed GBP here (a UK
# tenant, signed up from the UK), not something Terraform lets you pin
# explicitly on this resource. If Cost Management ever shows a different
# currency, this number means something other than 1.50 GBP and is worth
# revisiting, but everything this project provisions costs low pennies a
# month regardless, so it is not the kind of assumption that bites hard
# if wrong.
resource "azurerm_consumption_budget_subscription" "this" {
  name            = "${var.project}-budget"
  subscription_id = "/subscriptions/${data.azurerm_client_config.current.subscription_id}"
  amount          = 1.5
  time_grain      = "Monthly"

  time_period {
    start_date = "2026-08-01T00:00:00Z"
  }

  notification {
    enabled        = true
    threshold      = 80
    operator       = "GreaterThan"
    contact_emails = ["olayanjubiodun@outlook.com"]
  }

  notification {
    enabled        = true
    threshold      = 100
    operator       = "GreaterThan"
    contact_emails = ["olayanjubiodun@outlook.com"]
  }
}

# Lets your own signed-in identity read and write data with Azure AD,
# so no account keys are needed in your application code. principal_id is
# the pinned var.my_object_id, not data.azurerm_client_config.current.
# See the variable's own comment for why that distinction is load-bearing
# here, not stylistic.
resource "azurerm_role_assignment" "me_blob_contributor" {
  scope                = azurerm_storage_account.this.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = var.my_object_id
}

# CI's own identity. Same "no secrets" principle as the role assignment
# above, extended to GitHub Actions: a federated identity credential lets
# a workflow run authenticate by presenting a GitHub-issued OIDC token
# that this app trusts, instead of a stored client secret. There is
# nothing here for a leaked repo secret to ever expose, because there is
# no long-lived credential to leak.
resource "azuread_application" "github_actions" {
  display_name = "${var.project}-github-actions"
}

resource "azuread_service_principal" "github_actions" {
  client_id = azuread_application.github_actions.client_id
}

# Scoped to push events on main only, deliberately not pull_request: a
# fork's PR runs with that PR's own (possibly modified) workflow file, so
# granting OIDC access on pull_request would let any external contributor
# author a workflow step that authenticates to this subscription. Push to
# main only runs code that has already been merged, i.e. already trusted.
resource "azuread_application_federated_identity_credential" "github_actions_main" {
  application_id = azuread_application.github_actions.id
  display_name   = "github-actions-push-main"
  description    = "GitHub Actions on ${var.github_repo}, push to main only, see README for why not pull_request"
  audiences      = ["api://AzureADTokenExchange"]
  issuer         = "https://token.actions.githubusercontent.com"
  subject        = "repo:${var.github_repo}:ref:refs/heads/main"
}

# Same data-plane role as the human identity above, for the same reason:
# the dbt build --target azure CI step needs to read the bronze Delta
# tables, nothing more.
resource "azurerm_role_assignment" "github_actions_blob_contributor" {
  scope                = azurerm_storage_account.this.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azuread_service_principal.github_actions.object_id
}

# Reader, not Contributor: terraform plan only needs to read the current
# state of what's already there to compute a diff, it does not create or
# change anything. Scoped to the subscription, not the resource group --
# started narrower and had to widen after two separate real CI failures
# (401 on the budget, then 403 reading a role assignment) both turned out
# to be the same root cause: RBAC scope only flows downward, a resource-
# group-scoped grant can never see a subscription-scoped resource no
# matter how much read access it carries. Reader's own permission list is
# just "*/read" (checked directly: `az role definition list --name
# Reader`), so this was never about needing more access, only a wider
# scope for the same access. Subscription-scoped Reader now also covers
# what the separate Cost Management Reader grant below used to cover, so
# that one was removed rather than kept redundant.
resource "azurerm_role_assignment" "github_actions_reader" {
  scope                = "/subscriptions/${data.azurerm_client_config.current.subscription_id}"
  role_definition_name = "Reader"
  principal_id         = azuread_service_principal.github_actions.object_id
}

# azurerm_storage_account has computed attributes (primary_access_key,
# primary_connection_string, etc.) that only exist by calling listKeys,
# and the provider populates them on every refresh regardless of whether
# this config ever reads them. Reader deliberately excludes
# Microsoft.Storage/storageAccounts/listKeys/action (keys are full
# credential material, a read-only role should not be able to mint them),
# so plain `terraform plan` 403'd on this in CI. storage_use_azuread
# on the provider (versions.tf) was tried first, on the theory that it
# would make the provider use Azure AD instead of ever calling listKeys;
# confirmed with TF_LOG=DEBUG that it does not eliminate this specific
# call for azurerm_storage_account's own attribute refresh, only for
# separate data-plane resources. Storage Account Contributor is the
# narrowest built-in role that includes listKeys. It is a
# Microsoft.Storage/storageAccounts/* wildcard, broader than ideal, but
# scoped to this one storage account, not the resource group, and CI
# already fully manages this exact resource via Terraform regardless.
resource "azurerm_role_assignment" "github_actions_storage_account_contributor" {
  scope                = azurerm_storage_account.this.id
  role_definition_name = "Storage Account Contributor"
  principal_id         = azuread_service_principal.github_actions.object_id
}
