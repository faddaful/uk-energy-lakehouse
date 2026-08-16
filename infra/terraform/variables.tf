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
  # This is not plain "owner/repo". GitHub appends the owner's and repo's
  # stable numeric IDs (repo:owner@ownerId/repo@repoId:...) to the OIDC
  # subject instead of the plain names whenever a repo has rename or
  # ownership-transfer history -- a protection against someone later
  # claiming an old, now-free repo name and inheriting its federated
  # credential trust. This repo has that history, confirmed two ways: the
  # AADSTS700213 error from a real failed CI run named the exact subject
  # GitHub presented, and a plain `curl api.github.com/repos/...` lookup
  # independently returned the same repo id (1318837658) and owner id
  # (25750119). Azure AD's federated credential only matches on the exact
  # subject string, there is no wildcard or "match the current name"
  # option, so this has to be the ID-suffixed form or every CI run fails
  # AADSTS700213 the same way. If this repo is ever renamed again, this
  # value does not need to change -- the ids are stable across renames,
  # that is the entire point of them existing.
  description = "GitHub's OIDC subject claim for this repo (owner@ownerId/repo@repoId, not plain owner/repo -- see comment)"
  type        = string
  default     = "faddaful@25750119/uk-energy-lakehouse@1318837658"
}

variable "my_object_id" {
  # Pinned, not read from data.azurerm_client_config.current.object_id --
  # that data source resolves to whoever is CURRENTLY authenticated when
  # terraform runs, not a fixed reference to "me". Using it here meant
  # every CI run (authenticated as the GitHub Actions service principal,
  # not me) saw this role assignment's principal_id as needing to change
  # to CI's object id, and planned to destroy-and-replace it: not a
  # no-op, an actual plan to strip my own blob access and redundantly
  # grant CI's identity a role it already separately has. Never actually
  # applied -- caught in a `terraform plan` in CI before any apply ran --
  # but it would have flip-flopped destructively forever, alternating
  # between "belongs to me" and "belongs to CI" depending on who ran
  # terraform last. data.azurerm_client_config.current is still the right
  # tool for things that should genuinely track the caller (the provider's
  # own subscription_id, for instance); it was never the right tool for a
  # role assignment meant to stay pointed at one specific fixed identity
  # regardless of who is running Terraform at the time.
  description = "Your own Azure AD object id (az ad signed-in-user show --query id -o tsv), pinned so this doesn't silently repoint to whoever is running terraform"
  type        = string
  default     = "4409573e-839d-4003-98e2-b9ee88eabd0c"
}
