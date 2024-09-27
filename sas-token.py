import os
import datetime
import argparse
from azure.identity import DefaultAzureCredential
from azure.storage.blob import (
    BlobServiceClient,
    ContainerClient,
    ContainerSasPermissions,
    generate_container_sas,
)
from azure.keyvault.secrets import SecretClient


def create_service_sas_container(container_client: ContainerClient, account_key: str):
    # Create a SAS token that is valid for 13 weeks (~quarter) plus 2 weeks to allow for an implementation duration overlap
    # This token should be created and sent 2 weeks before the end of the quarter
    start_time = datetime.datetime.now(datetime.timezone.utc)
    expiry_time = start_time + datetime.timedelta(weeks=13 + 2)

    sas_token = generate_container_sas(
        account_name=str(container_client.account_name),
        container_name=container_client.container_name,
        account_key=account_key,
        permission=ContainerSasPermissions(
            read=False, write=True, list=True, delete=True, delete_previous_version=True
        ),
        start=start_time,
        expiry=expiry_time,
    )
    return sas_token


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="sas-token",
        description="dit script genereert een SAS token voor data leveranciers",
    )
    parser.add_argument("-a", "--account-name", required=True)
    parser.add_argument("-c", "--container-name", required=True)
    parser.add_argument("-k", "--key-vault-name", required=False)
    parser.add_argument("-s", "--secret-name", required=False)
    args = parser.parse_args()

    credential = DefaultAzureCredential()
    account_url = f"https://{args.account_name}.blob.core.windows.net"
    key_vault_name = os.environ["KEY_VAULT_NAME"] or args.key_vault_name
    key_vault_url = f"https://{key_vault_name}.vault.azure.net"

    client = SecretClient(vault_url=key_vault_url, credential=credential)
    secret_name = args.secret_name or "storage-account-key"
    account_key = str(client.get_secret(secret_name).value)

    blob_service_client = BlobServiceClient(account_url, credential=credential)
    blob_service_client_account_key = BlobServiceClient(
        account_url, credential=account_key
    )

    container_client = blob_service_client.get_container_client(
        container=args.container_name
    )
    # Assumes the service client object was created with a shared access key
    sas_token = create_service_sas_container(
        container_client=container_client,
        account_key=account_key,
    )

    # The SAS token string can be appended to the resource URL with a ? delimiter
    sas_url = f"{container_client.url}?{sas_token}"
    print(f"SAS token: {sas_token}")
    print(f"SAS URL: {sas_url}")

    # Create a ContainerClient object with SAS authorization, just for testing the URL
    container_client_sas = ContainerClient.from_container_url(container_url=sas_url)
