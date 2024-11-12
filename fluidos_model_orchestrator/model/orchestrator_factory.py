from pathlib import Path

from fluidos_model_orchestrator.common import OrchestratorInterface
from fluidos_model_orchestrator.model.candidate_generation.model import Orchestrator as CGOrchestrator
from fluidos_model_orchestrator.model.utils import MODEL_TYPES


class OrchestratorFactory:

    @staticmethod
    def create_orchestrator(
        model_type: str
    ) -> OrchestratorInterface:
        print("Creating orchestrator")
        if model_type == MODEL_TYPES.CG:
            return CGOrchestrator(model_name="fluidos/candidate-generation-v2", device="cpu", feedback_db_path=Path("tests/model/feedback/feedback.csv"))
        elif model_type == MODEL_TYPES.CG_LEGACY:
            return CGOrchestrator(model_name="fluidos/candidate-generation", device="cpu", feedback_db_path=Path("tests/model/feedback/feedback_legacy.csv"))
        else:
            raise ValueError(f"Can't find what model type {model_type} is referring to")
