import json
import re

from alluxio.worker_ring import WorkerEntity


def test_worker_entity_from_info_dynamic():
    # Define a mapping of field names to their specific values
    field_values = {
        "version": 1,
        "identifier": "cb157baaafe04b988af01a4645d38456",
        "Host": "192.168.4.36",
        "ContainerHost": "container_host_value",
        "RpcPort": 432423,
        "DataPort": 54237,
        "SecureRpcPort": 23514,
        "NettyDataPort": 45837,
        "WebPort": 65473,
        "DomainSocketPath": "domain_socket_path_value",
        "HttpServerPort": 39282,
    }

    # Dynamically construct worker_info_dict using field_values
    worker_info_dict = {
        "Identity": {
            k: v
            for k, v in field_values.items()
            if k in ["version", "identifier"]
        },
        "WorkerNetAddress": {
            k: v
            for k, v in field_values.items()
            if k not in ["version", "identifier"]
        },
    }
    worker_info_bytes = json.dumps(worker_info_dict).encode("utf-8")

    # Convert worker_info_bytes and instantiate WorkerEntity
    worker_entity = WorkerEntity.from_worker_info(worker_info_bytes)

    # Validate WorkerIdentity fields
    assert worker_entity.worker_identity.version == field_values["version"]
    assert worker_entity.worker_identity.identifier == bytes.fromhex(
        field_values["identifier"]
    )
    # Dynamically validate WorkerNetAddress fields using field_values
    for field_name, expected_value in field_values.items():
        if field_name in [
            "version",
            "identifier",
        ]:  # Skip identity-specific fields
            continue
        # Convert CamelCase field_name to snake_case to match WorkerNetAddress attribute names
        snake_case_field_name = camel_to_snake(field_name)
        actual_value = getattr(
            worker_entity.worker_net_address, snake_case_field_name
        )
        assert (
            actual_value == expected_value
        ), f"Field '{snake_case_field_name}' expected '{expected_value}', got '{actual_value}'"


def camel_to_snake(name):
    """
    Convert a CamelCase name into snake_case.
    """
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()
