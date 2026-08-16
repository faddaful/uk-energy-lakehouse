output "storage_account_name" {
  value = azurerm_storage_account.this.name
}

output "container_name" {
  value = azurerm_storage_container.lakehouse.name
}

output "abfss_root" {
  value = "abfss://${azurerm_storage_container.lakehouse.name}@${azurerm_storage_account.this.name}.dfs.core.windows.net"
}

# Not secrets -- a client ID, tenant ID, and subscription ID identify an
# application, they don't authenticate as one. The federated identity
# credential is what actually grants access, and that trust relationship
# lives in Azure AD, not in these values. Still go in GitHub as repo
# secrets rather than plain variables, matching the near-universal
# convention for azure/login's inputs, even though the values themselves
# aren't sensitive.
output "github_actions_client_id" {
  value = azuread_application.github_actions.client_id
}

output "azure_tenant_id" {
  value = data.azurerm_client_config.current.tenant_id
}

output "azure_subscription_id" {
  value = data.azurerm_client_config.current.subscription_id
}