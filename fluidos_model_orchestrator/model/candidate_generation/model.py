import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd
import pkg_resources
import torch  # type: ignore
import torch.nn as nn  # type: ignore
import torch.nn.functional as F  # type: ignore
from huggingface_hub import PyTorchModelHubMixin  # type: ignore
from sentence_transformers import SentenceTransformer  # type: ignore

from ...common import KnownIntent
from ...common import ModelPredictRequest
from ...common import ModelPredictResponse
from ...common import OrchestratorInterface
from ...common import Resource
from fluidos_model_orchestrator.model.candidate_generation.utils import FEEDBACK_STATUS
from fluidos_model_orchestrator.model.candidate_generation.utils import tr2id_from_str_to_list
from fluidos_model_orchestrator.model.utils import D_TYPE
from fluidos_model_orchestrator.model.utils import D_UNITS
from fluidos_model_orchestrator.model.utils import DATA_DEPENDENCY
from fluidos_model_orchestrator.model.utils import FLUIDOS_COL_NAMES
from fluidos_model_orchestrator.model.utils import KNOWN_INTENT_TO_POD_INTENT


logger = logging.getLogger(__name__)


# https://theiconic.tech/implementing-the-youtube-recommendations-paper-in-tensorflow-part-1-d1e1299d5622
# https://storage.googleapis.com/pub-tools-public-publication-data/pdf/45530.pdf
class EmbeddingAggregation(nn.Module):
    def __init__(self, aggregation_mode: str = "mean") -> None:
        super().__init__()
        if aggregation_mode not in ['sum', "mean"]:
            raise NotImplementedError(f"mode {aggregation_mode} not implemented!")
        self.aggregation_mode = aggregation_mode

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        if self.aggregation_mode == 'sum':
            aggregated = torch.sum(x, dim=1)  # axis
        elif self.aggregation_mode == 'mean':
            aggregated = torch.mean(x, dim=1)  # axis

        return aggregated


class BaseOrchestrationModel(nn.Module):
    def __init__(self, config: dict[str, Any]) -> None:
        pass

    def predict(self, x: list[torch.Tensor]) -> torch.Tensor:
        return x[0]


class OrchestrationModelLegacy(nn.Module, PyTorchModelHubMixin):  # Current model in the public version
    def __init__(self, config: dict[str, Any]):
        super().__init__()

        self.device = config['device']
        self.config_embedding = nn.Embedding(num_embeddings=config['num_configs'], embedding_dim=8, device=self.device)
        self.config_embedding_dropout = nn.Dropout(p=0.2)
        self.pod_embedding = nn.Embedding(num_embeddings=119547, embedding_dim=512, device=self.device)  # distiluse-base-multilingual-cased-v2

        self.fc1_size = config['fc1_size']
        self.fc2_size = config['fc2_size']
        self.fc3_size = config['fc3_size']

        self.dropout_val1 = config['dropout1']
        self.dropout_val2 = config['dropout2']
        self.dropout_val3 = config['dropout3']

        self.embedding_aggregator = EmbeddingAggregation(aggregation_mode=config["aggregation_mode"])  # component-wise average
        self.linear1 = nn.Linear(512 + 2 * 8, self.fc1_size)  # pod_embedding + rel_configs_embedding + non-rel_configs_embedding
        self.activation1 = nn.ReLU(inplace=True)
        self.batch_norm1 = nn.BatchNorm1d(self.fc1_size)
        self.dropout1 = nn.Dropout(p=self.dropout_val1)
        self.fc_layer1 = nn.Sequential(self.linear1, self.activation1, self.batch_norm1, self.dropout1)

        self.linear2 = nn.Linear(self.fc1_size, self.fc2_size)
        self.activation2 = nn.ReLU(inplace=True)
        self.batch_norm2 = nn.BatchNorm1d(self.fc2_size)
        self.dropout2 = nn.Dropout(p=self.dropout_val2)
        self.fc_layer2 = nn.Sequential(self.linear2, self.activation2, self.batch_norm2, self.dropout2)

        self.linear3 = nn.Linear(self.fc2_size, self.fc3_size)
        self.activation3 = nn.ReLU(inplace=True)
        self.batch_norm = nn.BatchNorm1d(self.fc3_size)
        self.dropout3 = nn.Dropout(p=self.dropout_val3)
        self.fc_layer3 = nn.Sequential(self.linear3, self.activation3, self.batch_norm, self.dropout3)
        self.head = torch.nn.Linear(in_features=self.fc3_size, out_features=config['num_configs'])

    def forward(self, input: list[torch.Tensor]) -> torch.Tensor:
        """
        Predicts relevant config id
        config = (cpu, memory, location, throughput)

        Args:
            input (List): input features list, 3 items
            input[0] (torch.Tensor): 0..512 for distiluse-base-multilingual-cased-v2 sentence transformer model,
                0:512, pod_embeddings
            input[1] (torch.Tensor): list of relevant configuration ids
            input[2] (torch.Tensor): list of non relevant configuration ids

        Returns:
            torch.Tensor: logits (tensor with maximum index refers to predicted configuration id)
        """
        # Embedding preprocessing
        x_in = input[0]
        x_rel = input[1]
        x_non_rel = input[2]

        pod_embedding = x_in[:, :512]
        pod_embedding = F.normalize(pod_embedding)

        rel_config_embedding = self.config_embedding(x_rel)
        rel_config_embedding = self.config_embedding_dropout(rel_config_embedding)
        rel_config_embedding = F.normalize(rel_config_embedding)
        rel_config_embedding = self.embedding_aggregator(rel_config_embedding)

        non_rel_config_embedding = self.config_embedding.forward(x_non_rel)
        non_rel_config_embedding = F.normalize(non_rel_config_embedding)
        non_rel_config_embedding = self.embedding_aggregator(non_rel_config_embedding)

        x = torch.cat((pod_embedding, rel_config_embedding, non_rel_config_embedding), dim=1)

        x = self.fc_layer1(x)
        x = self.fc_layer2(x)
        x = self.fc_layer3(x)
        x = self.head(x)
        x = F.softmax(x, dim=1)
        return x


class OrchestrationModel(BaseOrchestrationModel, PyTorchModelHubMixin):
    def __init__(self, config: dict[str, Any], target_column: str = FLUIDOS_COL_NAMES.TARGET_MOST_OPTIMAL_TEMPLATE_ID, device: str = "cpu"):
        super(BaseOrchestrationModel, self).__init__()

        self.target_column = target_column
        self.device = device
        self.config_embedding = nn.Embedding(num_embeddings=config['num_configs'], embedding_dim=config["tr_embedding_size"], device=self.device)
        self.config_embedding_dropout = nn.Dropout(p=0.2)
        self.pod_embedding = nn.Embedding(num_embeddings=119547, embedding_dim=config['pod_embedding_size'], device=self.device)  # distiluse-base-multilingual-cased-v2

        self.fc1_size = config['fc1_size']
        self.fc2_size = config['fc2_size']
        self.fc3_size = config['fc3_size']

        self.dropout_val1 = config['dropout1']
        self.dropout_val2 = config['dropout2']
        self.dropout_val3 = config['dropout3']

        self.embedding_aggregator = EmbeddingAggregation(aggregation_mode=config["aggregation_mode"])  # component-wise average
        self.linear1 = nn.Linear(512 + 2 * config["tr_embedding_size"], self.fc1_size)  # pod_embedding + rel_configs_embedding + non-rel_configs_embedding
        self.activation1 = nn.ReLU(inplace=True)
        self.batch_norm1 = nn.BatchNorm1d(self.fc1_size)
        self.dropout1 = nn.Dropout(p=self.dropout_val1)
        self.fc_layer1 = nn.Sequential(self.linear1, self.activation1, self.batch_norm1, self.dropout1)

        self.linear2 = nn.Linear(self.fc1_size, self.fc2_size)
        self.activation2 = nn.ReLU(inplace=True)
        self.batch_norm2 = nn.BatchNorm1d(self.fc2_size)
        self.dropout2 = nn.Dropout(p=self.dropout_val2)
        self.fc_layer2 = nn.Sequential(self.linear2, self.activation2, self.batch_norm2, self.dropout2)

        self.linear3 = nn.Linear(self.fc2_size, self.fc3_size)
        self.activation3 = nn.ReLU(inplace=True)
        self.batch_norm = nn.BatchNorm1d(self.fc3_size)
        self.dropout3 = nn.Dropout(p=self.dropout_val3)
        self.fc_layer3 = nn.Sequential(self.linear3, self.activation3, self.batch_norm, self.dropout3)
        self.head = torch.nn.Linear(in_features=self.fc3_size, out_features=config['num_configs'])

    def get_columns_dependencies(self) -> dict[str, Any]:
        return {
            DATA_DEPENDENCY.DEPENDENCY_INPUTS.name: [FLUIDOS_COL_NAMES.POD_FILE_NAME, FLUIDOS_COL_NAMES.POD_MANIFEST],
            DATA_DEPENDENCY.DEPENDENCY_TARGET.name: self.target_column
        }

    def forward(self, input: list[torch.Tensor]) -> torch.Tensor:
        """
        Predicts relevant config id
        config = (cpu, memory, location, throughput)

        Args:
            input (List): input features list, 3 items
            input[0] (torch.Tensor): 0..512 for distiluse-base-multilingual-cased-v2 sentence transformer model,
                0:512, pod_embeddings
            input[1] (torch.Tensor): list of relevant configuration ids
            input[2] (torch.Tensor): list of non relevant configuration ids

        Returns:
            torch.Tensor: logits (tensor with maximum index refers to predicted configuration id)
        """
        # Embedding preprocessing
        x_in = input[0]
        x_rel = input[1]
        x_non_rel = input[2]

        pod_embedding = x_in[:, :512]
        pod_embedding = F.normalize(pod_embedding)

        rel_config_embedding = self.config_embedding(x_rel)
        rel_config_embedding = self.config_embedding_dropout(rel_config_embedding)
        rel_config_embedding = F.normalize(rel_config_embedding)
        rel_config_embedding = self.embedding_aggregator(rel_config_embedding)

        non_rel_config_embedding = self.config_embedding.forward(x_non_rel)
        non_rel_config_embedding = F.normalize(non_rel_config_embedding)
        non_rel_config_embedding = self.embedding_aggregator(non_rel_config_embedding)

        x = torch.cat((pod_embedding, rel_config_embedding, non_rel_config_embedding), dim=1)

        x = self.fc_layer1(x)
        x = self.fc_layer2(x)
        x = self.fc_layer3(x)
        x = self.head(x)
        x = F.softmax(x, dim=1)
        return x


class Orchestrator(OrchestratorInterface):
    embedding_model_name: str = "distiluse-base-multilingual-cased-v2"  # TODO read from metadata

    def __init__(self, model_name: str = "fluidos/candidate-generation", device: str = "cpu", feedback_db_path: Path = Path("tests/model/feedback/feedback.csv")) -> None:
        self.model_name = model_name
        self.orchestrator: OrchestrationModel | OrchestrationModelLegacy
        metadata_filename = "metadata_cg_v0.0.2.json"
        match self.model_name:
            case "fluidos/candidate-generation":
                metadata_filename = "metadata_cg_v0.0.1.json"
                self.orchestrator = OrchestrationModelLegacy.from_pretrained(self.model_name)  # Will be dropped when v2 works fine enough
            case  "fluidos/candidate-generation-v2":
                metadata_filename = "metadata_cg_v0.0.2.json"
                self.orchestrator = OrchestrationModel.from_pretrained(self.model_name)  # type: ignore

        self.sentence_transformer = SentenceTransformer(self.embedding_model_name)
        self.device = device
        with pkg_resources.resource_stream(__name__, metadata_filename) as metadata_stream:
            logger.info(f"METADATA LOADED: {metadata_stream is not None}")
            self.metadata: dict[str, Any] = json.load(metadata_stream)

        self.feedback_db_path = feedback_db_path

        self.template_resources2id: list[str | dict[str, Any]] = tr2id_from_str_to_list(self.metadata["template_resource2id"])

    @staticmethod
    def create_sample_request() -> ModelPredictRequest:

        #PODS should be identical
        pod_request = {FLUIDOS_COL_NAMES.POD_FILE_NAME: ['pod_mysql.yaml'],
                       FLUIDOS_COL_NAMES.POD_MANIFEST: {'apiVersion': 'v1',
                                                        'kind': 'Pod',
                                                        'metadata': {'name': 'mysql', 'annotations': {'fluidos-intent-location': 'bitbrains_a'}},
                                                        'spec': {'containers':
                                                                 [{'image': 'mysql:latest', 'name': 'mysql', 'ports': [{'containerPort': 80}], 'resources': {'requests': {'cpu': '50m', 'memory': '208Mi'}}}]}}}

        return ModelPredictRequest(
            id="dummyId",
            namespace="namespace",
            pod_request=pod_request,
            intents=[],
            container_image_embeddings=[]
        )

    @staticmethod
    def create_sample_request_legacy() -> ModelPredictRequest:
        #PODS should be identical
        pod_request = {FLUIDOS_COL_NAMES.POD_FILE_NAME: ['pod_mysql.yaml'],
                       FLUIDOS_COL_NAMES.POD_MANIFEST: {'apiVersion': 'v1',
                                                        'kind': 'Pod',
                                                        'metadata': {'name': 'mysql', 'annotations': {'fluidos-intent-location': 'a',
                                                                                                      'fluidos-intent-throughput': '4.47Ks'}},
                                                        'spec': {'containers':
                                                                 [{'image': 'mysql:latest', 'name': 'mysql', 'ports': [{'containerPort': 80}], 'resources': {'requests': {'cpu': '61m', 'memory': '306Mi'}}}]}}}

        return ModelPredictRequest(
            id="dummyId",
            namespace="namespace",
            pod_request=pod_request,
            intents=[],
            container_image_embeddings=[]
        )

    def _check_feedback_for_relevant_candidates(self, image_name: str) -> tuple[torch.Tensor, torch.Tensor]:
        logger.info(f"{self.feedback_db_path.absolute()}")
        try:
            feedback = pd.read_csv(self.feedback_db_path.absolute())
        except:
            logger.warning(f"No feedback was found for {image_name} at {self.feedback_db_path.absolute()}")
            return torch.tensor([], dtype=torch.int32).unsqueeze(0), torch.tensor([], dtype=torch.int32).unsqueeze(0)
        
        image_feedback = feedback[feedback['image_name'] == image_name]
        relevant_candidates_ids = image_feedback[image_feedback['status'] == FEEDBACK_STATUS.OK]['template_resource_id'].tolist()
        non_relevant_candidates_ids = image_feedback[image_feedback['status'] == FEEDBACK_STATUS.FAIL]['template_resource_id'].tolist()
        relevant_configs_tensor = torch.tensor(relevant_candidates_ids, device=self.device, dtype=torch.int32).unsqueeze(0)
        non_relevant_configs_tensor = torch.tensor(non_relevant_candidates_ids, device=self.device, dtype=torch.int32).unsqueeze(0)

        return relevant_configs_tensor, non_relevant_configs_tensor

    def __compute_embedding_for_sentence(self, sentence: str) -> torch.Tensor:
        embeddings = self.sentence_transformer.encode(sentence)
        return torch.tensor(embeddings).unsqueeze(0)

    def predict(self, data: ModelPredictRequest, architecture: str = "amd64") -> ModelPredictResponse:
        logger.info("Pod embedding generation")
        pod_embedding = self.__compute_embedding_for_sentence(str(data.pod_request[FLUIDOS_COL_NAMES.POD_MANIFEST]))
        intents_dict: dict[str, Any] = {}
        for intent in data.intents:
            if intent.name.name not in KNOWN_INTENT_TO_POD_INTENT:
                logger.info(f"Skipping {intent.name}")
                continue  # patch for skipping unmanaged intent
            intent_name = KNOWN_INTENT_TO_POD_INTENT[intent.name.name]
            unit_len = -1 * len(D_UNITS[intent_name][0])
            intent_value = intent.value[:unit_len]
            intents_dict[KNOWN_INTENT_TO_POD_INTENT[intent.name.name]] = D_TYPE[intent_name]["type"](intent_value)

        image_name = data.pod_request[FLUIDOS_COL_NAMES.POD_MANIFEST]['spec']['containers'][0]['image']  # TODO: add loop over all images mentioned (?)
        relevant_candidates, non_relevant_candidates = self._check_feedback_for_relevant_candidates(image_name)

        # model input feature vector
        logger.info("model input feature vector")
        model_input = [pod_embedding, relevant_candidates, non_relevant_candidates]

        self.orchestrator.eval()
        logits = self.orchestrator.forward(model_input)
        predicted_configuration_id = logits.detach().numpy().argmax()

        predicted_config = self.template_resources2id[predicted_configuration_id]

        if predicted_config == "none":
            predicted_config_dict: dict[str, str] = {}
            for item in data.intents:
                predicted_config_dict[str(item.name)] = "-1"
        else:
            predicted_config_dict = predicted_config

        return ModelPredictResponse(
            data.id,
            resource_profile=Resource(
                id=data.id,
                region=_get_region(data),  # predicted_config_dict.get(FLUIDOS_COL_NAMES.TEMPLATE_RESOURCE_LOCATION, "Dublin"),
                cpu=f"{predicted_config_dict.get(FLUIDOS_COL_NAMES.TEMPLATE_RESOURCE_CPU, '1')}{D_UNITS[FLUIDOS_COL_NAMES.TEMPLATE_RESOURCE_CPU][0]}",
                memory=f"{predicted_config_dict.get(FLUIDOS_COL_NAMES.TEMPLATE_RESOURCE_MEMORY, '1')}{D_UNITS[FLUIDOS_COL_NAMES.TEMPLATE_RESOURCE_MEMORY][0]}",
                architecture=architecture)
        )


def _get_region(data: ModelPredictRequest) -> str:
    asked_region = [i for i in data.intents if i.name == KnownIntent.location]

    if len(asked_region):
        return asked_region[0].value
    else:
        return "Dublin"
