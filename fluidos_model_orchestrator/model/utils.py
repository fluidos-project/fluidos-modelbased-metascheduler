from enum import Enum

from ..common import KnownIntent


class DATA_DEPENDENCY(Enum):
    DEPENDENCY_INPUTS = "inputs"
    DEPENDENCY_TARGET = "target"


class FLUIDOS_COL_NAMES:
    POD_FILE_NAME = "pod_filename"
    TEMPLATE_RESOURCE_ID = "template_resource_id"
    POD_CPU = "pod_cpu"
    TEMPLATE_RESOURCE_CPU = "template_resource_cpu"
    POD_MEMORY = "pod_memory"
    TEMPLATE_RESOURCE_MEMORY = "template_resource_memory"
    POD_MANIFEST = "pod_manifest"
    TEMPLATE_RESOURCE_THROUGHPUT = "template_resource_throughput"
    OUTPUT = "output_speed"
    TEMPLATE_RESOURCE_LOCATION = "template_resource_location"
    POD_LOCATION = "pod_location"
    POD_THROUGHPUT = "pod_throughput"
    TEMPLATE_RESOURCE_CANDIDATE_ID = "template_resource_config_id"
    ACCEPTABLE_CANDIDATES = 'acceptable_configs'
    NON_ACCEPTABLE_CANDIDATES = "non_acceptable_configs"
    TARGET_BASIC_RESOURCE_AVAIL_AUGMENTATION_COL = "basic_resource_avail"
    TARGET_MOST_OPTIMAL_TEMPLATE_ID = "best_candidate"
    MSPL_INTENT = "mspl_intent"


class FLUIDOS_INPUT_OUTPUT_NAME:
    POD_NAME = "pod"
    TEMPLATE_RESOURCE_NAME = "template_resource"


KNOWN_INTENT_TO_POD_INTENT: dict[str, str] = {
    KnownIntent.cpu.name: FLUIDOS_COL_NAMES.POD_CPU,
    KnownIntent.memory.name: FLUIDOS_COL_NAMES.POD_MEMORY,
    KnownIntent.location.name: FLUIDOS_COL_NAMES.POD_LOCATION,
    KnownIntent.throughput.name: FLUIDOS_COL_NAMES.POD_THROUGHPUT,
    KnownIntent.mspl.name: FLUIDOS_COL_NAMES.MSPL_INTENT,
}


D_TYPE: dict[str, dict[str, type]] = {
    FLUIDOS_COL_NAMES.POD_FILE_NAME: {"type": str},
    FLUIDOS_COL_NAMES.TEMPLATE_RESOURCE_ID: {
        "type": str,
    },
    FLUIDOS_COL_NAMES.POD_MANIFEST: {"type": str},
    FLUIDOS_COL_NAMES.TEMPLATE_RESOURCE_CPU: {"type": int},
    FLUIDOS_COL_NAMES.POD_CPU: {"type": int},
    FLUIDOS_COL_NAMES.POD_MEMORY: {"type": int},
    FLUIDOS_COL_NAMES.TEMPLATE_RESOURCE_MEMORY: {"type": int},
    FLUIDOS_COL_NAMES.TEMPLATE_RESOURCE_THROUGHPUT: {"type": float},
    FLUIDOS_COL_NAMES.POD_THROUGHPUT: {"type": float},
    FLUIDOS_COL_NAMES.OUTPUT: {"type": int},
    FLUIDOS_COL_NAMES.TEMPLATE_RESOURCE_LOCATION: {"type": str},
    FLUIDOS_COL_NAMES.POD_LOCATION: {"type": str},
    FLUIDOS_COL_NAMES.TARGET_BASIC_RESOURCE_AVAIL_AUGMENTATION_COL: {"type": float},
    FLUIDOS_COL_NAMES.MSPL_INTENT: {"type": str},
}

D_UNITS = {
    FLUIDOS_COL_NAMES.POD_CPU: ["m"],
    FLUIDOS_COL_NAMES.POD_MEMORY: ["Mi"],
    FLUIDOS_COL_NAMES.POD_THROUGHPUT: ["Ks"],
    FLUIDOS_COL_NAMES.POD_LOCATION: [""],
    FLUIDOS_COL_NAMES.TARGET_BASIC_RESOURCE_AVAIL_AUGMENTATION_COL: ["%"],

    FLUIDOS_COL_NAMES.TEMPLATE_RESOURCE_CPU: ["m"],
    FLUIDOS_COL_NAMES.TEMPLATE_RESOURCE_MEMORY: ["Mi"],
    FLUIDOS_COL_NAMES.TEMPLATE_RESOURCE_THROUGHPUT: ["Ks"],
    FLUIDOS_COL_NAMES.TEMPLATE_RESOURCE_LOCATION: [""],

    FLUIDOS_COL_NAMES.TEMPLATE_RESOURCE_ID: [""],
    
    FLUIDOS_COL_NAMES.MSPL_INTENT: [""],

}


class MODEL_TYPES:
    CG = "model_cg"
    CG_LEGACY = "model_cg_legacy"
    TEMPLATE_MODEL = "model_template"
