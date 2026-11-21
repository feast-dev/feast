from typing import Dict, Literal

from pydantic import StrictStr

from feast.infra.compute_engines.operator.config import OperatorComputeEngineConfig


class FlinkApplicationComputeEngineConfig(OperatorComputeEngineConfig):
    """Batch Compute Engine config for FlinkDeployment CRs via the Apache Flink
    Kubernetes Operator.

    Shared operator fields (image, namespace, env, timeouts, volumes, ...) are
    inherited from :class:`OperatorComputeEngineConfig`; the fields below are
    Flink-specific.
    """

    type: Literal["flink_application"] = "flink_application"

    flink_version: StrictStr = "v2_2"
    """ FlinkDeployment flinkVersion enum (e.g. v1_20, v2_0, v2_2). Must match
    the Flink version in ``image`` and ``python_jar_uri``. feast[flink] pins
    apache-flink>=2.2.1,<3, so the default targets Flink 2.2. """

    python_jar_uri: StrictStr = "local:///opt/flink/opt/flink-python-2.2.1.jar"
    """ jarURI for Flink's PythonDriver; version must match ``flink_version``. """

    python_executable: StrictStr = "/usr/bin/python3"
    """ Python interpreter used by the Flink client (``-pyclientexec``). """

    main_application_file: StrictStr = "/opt/feast/main.py"
    """ In-image path of the PyFlink entrypoint run by the PythonDriver. """

    parallelism: int = 1
    """ Default Flink job parallelism. """

    task_slots: int = 1
    """ taskmanager.numberOfTaskSlots. """

    jobmanager_cpu: float = 1.0
    jobmanager_memory: StrictStr = "2048m"
    taskmanager_cpu: float = 1.0
    taskmanager_memory: StrictStr = "2048m"

    upgrade_mode: StrictStr = "stateless"
    """ FlinkDeployment job upgradeMode (stateless for run-to-completion). """

    flink_configuration: Dict[str, str] = {}
    """ Extra entries merged into the FlinkDeployment ``flinkConfiguration``. """
