"""
Resolve where bronze Delta tables live, and what credentials to read or
write them with, based on the TARGET environment variable. Every extractor
and every dbt staging model goes through this module rather than building
paths or credentials itself, so there is exactly one place that knows the
difference between local and Azure.

TARGET=local (the default): tables live under LOCAL_DATA_ROOT (default
"data") as plain directories on disk, e.g. data/bronze/carbon_intensity/,
each one a real Delta table (parquet files plus a _delta_log/ of JSON
commits). No credentials needed.

TARGET=azure: tables live in the ADLS Gen2 container Terraform created
(infra/terraform/main.tf), addressed as
abfss://<container>@<account>.dfs.core.windows.net/bronze/<name>/.

Loads .env on import so these variables don't need to be exported by hand
every session. load_dotenv() does not override a variable that is already
set in the real environment, so `export TARGET=azure` in a shell still
wins over whatever .env says, which is what you want when testing.
"""

import os

from dotenv import load_dotenv

load_dotenv()


def _target() -> str:
    return os.environ.get("TARGET", "local")


def table_uri(layer: str, name: str) -> str:
    """
    The URI for a single Delta table.

    Args:
        layer (str): The medallion layer, e.g. "bronze".
        name (str): The table name, e.g. "carbon_intensity".
    """
    if _target() == "azure":
        account = os.environ["AZURE_STORAGE_ACCOUNT_NAME"]
        container = os.environ["AZURE_STORAGE_CONTAINER"]
        return f"abfss://{container}@{account}.dfs.core.windows.net/{layer}/{name}"

    root = os.environ.get("LOCAL_DATA_ROOT", "data")
    return os.path.join(root, layer, name)


def storage_options() -> dict[str, str] | None:
    """
    Credentials for deltalake's own Azure backend (not adlfs: deltalake
    talks to Azure through its own Rust client, which only recognises its
    own azure_*-prefixed option keys). None for local, where a filesystem
    path needs no credentials at all.

    Default: azure_use_azure_cli, meaning deltalake shells out to the
    Azure CLI's own cached login (the same one `az account show` uses) for
    a token each time. Nothing secret ever touches disk this way, and it
    is backed by the "Storage Blob Data Contributor" role
    infra/terraform/main.tf assigns to your signed-in identity.

    Fallback: if AZURE_STORAGE_ACCOUNT_KEY is set in .env, it is used
    instead of the CLI token. This works reliably even if the CLI path
    misbehaves, but it is a long-lived secret sitting in a file, so it is
    a stopgap, not the default. Get the key with:
        az storage account keys list --account-name <name> --query "[0].value" -o tsv
    """
    if _target() != "azure":
        return None

    account = os.environ["AZURE_STORAGE_ACCOUNT_NAME"]
    account_key = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")

    if account_key:
        return {
            "azure_storage_account_name": account,
            "azure_storage_account_key": account_key,
        }

    return {
        "azure_storage_account_name": account,
        "azure_use_azure_cli": "true",
    }
