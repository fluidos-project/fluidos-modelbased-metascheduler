from __future__ import annotations

import ast
import io
import json
import logging
import random  # TODO remove
from pathlib import Path
from typing import Any

import pkg_resources
import torch  # type: ignore
import torch.nn as nn  # type: ignore
import torch.nn.functional as F  # type: ignore
from sentence_transformers import SentenceTransformer  # type: ignore

from ...common import ModelInterface
from ...common import ModelPredictRequest
from ...common import ModelPredictResponse
from ...common import Resource
from fluidos_model_orchestrator.model.candidate_generation.model_utils import compute_embedding_for_sentence
from fluidos_model_orchestrator.model.candidate_generation.model_utils import find_matching_configs

logger = logging.getLogger(__name__)


class EmbeddingAggregation(nn.Module):
    def __init__(self, aggregation_mode: str = "mean"):
        super().__init__()
        if aggregation_mode not in ['sum', "mean"]:
            raise NotImplementedError(f"mode {aggregation_mode} not implemented!")
        self.aggregation_mode = aggregation_mode

    def forward(self, x: torch.Tensor) -> torch.Tensor | None:
        if self.aggregation_mode == 'sum':
            aggregated = torch.sum(x, dim=1)  # axis
        elif self.aggregation_mode == 'mean':
            aggregated = torch.mean(x, dim=1)  # axis

        return aggregated


class OrchestrationModel(nn.Module):
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


class Orchestrator(ModelInterface):
    embedding_model_name: str = "distiluse-base-multilingual-cased-v2"  # TODO read from metadata

    def __init__(self, model_name: str = "orchestrator_cg_v0.0.1", device: str = "cpu") -> None:
        self.sentence_transformer = SentenceTransformer(self.embedding_model_name)
        self.device = device
        with pkg_resources.resource_stream(__name__, "metadata_cg_v0.0.1.json") as metadata_stream:
            self.metadata: dict[str, Any] = json.load(metadata_stream)

        self.orchestrator: OrchestrationModel = OrchestrationModel(self.metadata["training_setup"])
        self.orchestrator.load_state_dict(self.__load_from_bytes(model_name)["model_state_dict"])

    def __load_from_bytes(self, model_name: str, chunks_num: int = 18) -> dict[str, Any]:
        base_ckpt_path = Path(__file__).parent.joinpath(model_name)

        orchestrator_ckpt_chunk = b''
        for i in range(chunks_num):
            with open(f"{base_ckpt_path.as_posix()}/{model_name}_{i + 1}.pt_chunk", "rb") as chunk_file:
                orchestrator_ckpt_chunk = orchestrator_ckpt_chunk + chunk_file.read()
        buffer = io.BytesIO(orchestrator_ckpt_chunk)
        orchestrator_ckpt = torch.load(buffer)
        return orchestrator_ckpt

    def generate_configs_feature_set(self, intents_dict: dict[str, Any]) -> tuple[torch.Tensor, torch.Tensor]:
        # TODO: add default mode
        relevant_configs: list[int] = []
        non_relevant_configs: list[int] = []

        # relevant configs - some of those which satisfy all the intents
        # non_relevant configs - those which do not satisfy all the intents

        relevant_configs_full, non_relevant_configs_full, minumal_value_config = find_matching_configs(intents_dict,
                                                                                                       configuration2id=self.metadata["configuration2id"])
        if len(relevant_configs_full) == 0:
            relevant_configs = [0]
        else:
            relevant_configs = list(set(random.choices(relevant_configs_full, k=4)))  # TODO fix random selection with historical information loading
        if len(non_relevant_configs_full) == 0:
            non_relevant_configs = [0]
        else:
            non_relevant_configs = list(set(random.choices(non_relevant_configs_full, k=3)))   # TODO fix random selection with historical information loading

        relevant_configs = torch.tensor(relevant_configs, device=self.device, dtype=torch.float32).unsqueeze(0)
        non_relevant_configs = torch.tensor(non_relevant_configs, device=self.device, dtype=torch.float32).unsqueeze(0)

        return relevant_configs, non_relevant_configs

    def predict(self, data: ModelPredictRequest, architecture: str = "arm64") -> ModelPredictResponse:

        logger.info("pod embedding generation")
        pod_embedding = compute_embedding_for_sentence(str(data.pod_request), self.sentence_transformer)
        intents_dict: dict[str, str] = {str(intent.name): intent.value for intent in data.intents}

        relevant_configs, non_relevant_configs = self.generate_configs_feature_set(intents_dict)
        relevant_configs = relevant_configs.type(torch.int32)
        non_relevant_configs = non_relevant_configs.type(torch.int32)

        # model input feature vector
        logger.info("model input feature vector")
        model_input = [pod_embedding, relevant_configs, non_relevant_configs]

        self.orchestrator.eval()
        logits = self.orchestrator.forward(model_input)
        predicted_configuration_id = logits.detach().numpy().argmax()

        predicted_config = self.metadata["id2configuration"][str(predicted_configuration_id)]
        if predicted_config == "none":
            predicted_config_dict: dict[str, str] = {}
            for item in data.intents:
                predicted_config_dict[str(item.name)] = "-1"
        else:
            predicted_config_dict = ast.literal_eval(predicted_config)

        return ModelPredictResponse(
            data.id,
            resource_profile=Resource(
                id=data.id, region=predicted_config_dict['fluidos-intent-location'], cpu=f"{predicted_config_dict['cpu']}",  # TODO: needs fixing, this is using fluidos-intent as per the manifest, not as represented in the request
                memory=f"{predicted_config_dict['memory']}", architecture=architecture)  # TODO: fix requred, here we impose the architecture to be arm64, is it correct? arch is optional in Resource.
        )
