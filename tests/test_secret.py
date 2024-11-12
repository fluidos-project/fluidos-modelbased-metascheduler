from base64 import b64decode

import pytest  # type: ignore
from kubernetes import client  # type: ignore
from kubernetes import config  # type: ignore
from kubernetes.client.api_client import ApiClient  # type: ignore


@pytest.mark.skip()
def test_service_credentials() -> None:
    # this should run only if a cluster exits
    # I hope

    conf = client.Configuration()  # type: ignore
    config.load_config(client_configuration=conf)  # type: ignore

    k8s_client = ApiClient(configuration=conf)

    api_client = client.CoreV1Api(api_client=k8s_client)

    secret = api_client.read_namespaced_secret("credentials-contract-provider-fluidos", "default")

    assert secret.data is not None

    decoded = {
        key: b64decode(value).decode() for key, value in secret.data.items()
    }

    assert False, decoded
