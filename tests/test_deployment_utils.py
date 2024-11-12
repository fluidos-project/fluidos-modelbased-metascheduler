import pkg_resources
import yaml
from kubernetes import client  # type: ignore
from kubernetes import config
from kubernetes import utils  # type: ignore
from pytest_kubernetes.providers.base import AClusterManager  # type: ignore


def test_loading_of_objects(k8s: AClusterManager) -> None:
    k8s.create()
    myconfig = client.Configuration()  # type: ignore
    config.kube_config.load_kube_config(client_configuration=myconfig, config_file=str(k8s.kubeconfig))

    k8s_client = client.ApiClient(myconfig)  # type: ignore
    with pkg_resources.resource_stream(__name__, "k8s/pod.yaml") as stream:
        pod_dict = yaml.safe_load(stream)

    assert pod_dict

    pod = utils.create_from_dict(k8s_client=k8s_client, data=pod_dict)

    assert pod is not None
