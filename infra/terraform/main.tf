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
# whatever currency this subscription bills in -- assumed GBP here (a UK
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
# so no account keys are needed in your application code.
resource "azurerm_role_assignment" "me_blob_contributor" {
  scope                = azurerm_storage_account.this.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_client_config.current.object_id
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
  description    = "GitHub Actions on ${var.github_repo}, push to main only -- see README for why not pull_request"
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
# change anything. If a real CI run of `terraform plan` ever fails on
# permissions with this in place, that is the signal to revisit this
# choice, not a reason to default to Contributor pre-emptively.
resource "azurerm_role_assignment" "github_actions_reader" {
  scope                = azurerm_resource_group.this.id
  role_definition_name = "Reader"
  principal_id         = azuread_service_principal.github_actions.object_id
}