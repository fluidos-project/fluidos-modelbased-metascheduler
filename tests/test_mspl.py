# import pytest
from requests_mock import Mocker  # type: ignore

from fluidos_model_orchestrator.resources.mspl import request_application


endpoint = "http://www.um.es/mspl/endpoint"


def test_request_no_poll(requests_mock: Mocker) -> None:
    requests_mock.post(endpoint + "/123", status_code=200, text="response")

    text = request_application("<stupid><xml /></stupid>", endpoint, "123")

    assert text == "response"


def test_request_poll(requests_mock: Mocker) -> None:
    requests_mock.post(endpoint, status_code=100, headers={'Location': endpoint + "/123"})
    requests_mock.get(endpoint + "/123", status_code=200, text="response")

    text = request_application("<stupid><xml /></stupid>", endpoint, "123")

    assert text == "response"


# @pytest.mark.skip
def test_interaction_with_bastion() -> None:
    policy = """
    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ITResourceOrchestration xmlns="http://modeliosoft/xsddesigner/a22bd60b-ee3d-425c-8618-beb6a854051a/ITResource.xsd" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:iot="http://www.example.org/IoTHoneynetSchema" xsi:schemaLocation="http://modeliosoft/xsddesigner/a22bd60b-ee3d-425c-8618-beb6a854051a/ITResource.xsd mspl.xsd http://www.example.org/IoTHoneynetSchema iot-honeynet.xsd" id="omspl_43a2ce61f85c4c4faf0acc30a84516a9">
  <ITResource id="mspl_9f1a88b4fc67421b98de270d5a63d35a" orchestrationID="omspl_43a2ce61f85c4c4faf0acc30a84516a9" tenantID="1" sliceID="1548">
    <configuration xsi:type="RuleSetConfiguration">
      <capability>
        <Name>CyberDecepto</Name>
      </capability>
      <configurationRule>
        <configurationRuleAction xsi:type="HoneyNetAction">
          <HoneyNetActionType>DEPLOY</HoneyNetActionType>
          <ioTHoneyNet>
            <iot:name>a1</iot:name>
            <!-- Dataflows -->
            <iot:datapath id="1">
              <iot:name>m1m2</iot:name>
              <iot:source>a1m1</iot:source>
              <iot:destination>a1m2</iot:destination>
            </iot:datapath>
            <iot:datapath id="2">
              <iot:name>m2m3</iot:name>
              <iot:source>a1m2</iot:source>
              <iot:destination>a1m3</iot:destination>
            </iot:datapath>
            <!-- Microservices -->
            <iot:honeyPot id="1">
              <iot:name>a1m1</iot:name>
              <iot:interaction_level>MEDIUM</iot:interaction_level>
              <iot:decoy>false</iot:decoy>
            </iot:honeyPot>
            <iot:honeyPot id="2">
              <iot:name>a1m2</iot:name>
              <iot:interaction_level>MEDIUM</iot:interaction_level>
              <iot:decoy>false</iot:decoy>
            </iot:honeyPot>
            <iot:honeyPot id="3">
              <iot:name>a1m3</iot:name>
              <iot:interaction_level>MEDIUM</iot:interaction_level>
              <iot:decoy>false</iot:decoy>
            </iot:honeyPot>
          </ioTHoneyNet>
        </configurationRuleAction>
        <configurationCondition>
          <isCNF>false</isCNF>
        </configurationCondition>
        <externalData xsi:type="Priority">
          <value>0</value>
        </externalData>
        <Name>Rule0</Name>
        <isCNF>false</isCNF>
      </configurationRule>
      <resolutionStrategy xsi:type="FMR"/>
      <Name>Conf0</Name>
    </configuration>
    <priority>3000</priority>
  </ITResource>
</ITResourceOrchestration>
    """

    response = request_application(policy=policy, endpoint="http://fluidos-mspl.sl.cloud9.ibm.com:8002/meservice", request_name="request-name")

    assert response is not None
