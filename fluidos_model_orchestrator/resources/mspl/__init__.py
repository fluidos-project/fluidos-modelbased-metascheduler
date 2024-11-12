import logging
from time import sleep

import requests

from fluidos_model_orchestrator.configuration import CONFIGURATION


logger = logging.getLogger(__name__)


def request_application(policy: str, endpoint: str, request_name: str) -> str:
    endpoint = endpoint + "/" + request_name
    try:
        response = requests.post(endpoint, data=policy.strip(), headers={
            "Content-Type": "application/xml"
        })

        get_endpoint: str | None = None

        for _ in range(CONFIGURATION.n_try):
            if response.status_code // 100 == 1:
                sleep(CONFIGURATION.API_SLEEP_TIME)
                if get_endpoint is None:
                    get_endpoint = response.headers.get("Location", endpoint)
                response = requests.get(get_endpoint)
            if response.status_code // 100 == 1:
                sleep(CONFIGURATION.API_SLEEP_TIME)
                if get_endpoint is None:
                    get_endpoint = response.headers.get("Location", endpoint)
                response = requests.get(get_endpoint)

            if response.status_code // 100 == 2:
                return response.text
            if response.status_code // 100 == 4:
                raise RuntimeError("Error on our side")
            if response.status_code // 100 == 5:
                raise RuntimeError("Error on their side")

    except Exception as e:
        logger.error("Unable to perform requeest")
        logger.error(e)

    raise RuntimeError("Unable to receive a response in the required time")
