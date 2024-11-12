from fluidos_model_orchestrator.common import KnownIntent


def test_intent_values():
    expected_intents = {
        "fluidos-intent-architecture",
        "fluidos-intent-battery",
        "fluidos-intent-compliance",
        "fluidos-intent-cpu",
        "fluidos-intent-max-delay",
        "fluidos-intent-carbon-aware",
        "fluidos-intent-energy",
        "fluidos-intent-gpu",
        "fluidos-intent-latency",
        "fluidos-intent-location",
        "fluidos-intent-memory",
        "fluidos-intent-resource",
        "fluidos-intent-service",
        "fluidos-intent-throughput",
        "fluidos-intent-bandwidth-against",
        "fluidos-intent-tee-available",
        "fluidos-intent-mspl",
    }

    for intent in KnownIntent:
        assert intent.to_intent_key() in expected_intents


def test_iternal_or_external():
    external_intents = {
        "fluidos-intent-service",
    }

    internal_intents = {
        "fluidos-intent-architecture",
        "fluidos-intent-battery",
        "fluidos-intent-compliance",
        "fluidos-intent-cpu",
        "fluidos-intent-max-delay",
        "fluidos-intent-carbon-aware",
        "fluidos-intent-energy",
        "fluidos-intent-gpu",
        "fluidos-intent-latency",
        "fluidos-intent-location",
        "fluidos-intent-memory",
        "fluidos-intent-resource",
        "fluidos-intent-throughput",
        "fluidos-intent-bandwidth-against",
        "fluidos-intent-tee-available",
        "fluidos-intent-mspl",
    }

    for intent in KnownIntent:
        key = intent.to_intent_key()
        assert (key in internal_intents and key not in external_intents) or (key in external_intents and key not in internal_intents), key
        assert (intent.is_external_requirement() and key in external_intents) or (key in internal_intents), key


def test_intent_validated():
    valid_intents = [
        intent.to_intent_key() for intent in KnownIntent
    ]

    for valid in valid_intents:
        assert KnownIntent.is_supported(valid)

    invalid_intents = [
        "asdfasdf-dfaklsjfa",
        "my-funny-intent",
        "fluidos-intent-not-yet-defined"
    ]

    for invalid in invalid_intents:
        assert not KnownIntent.is_supported(invalid)
