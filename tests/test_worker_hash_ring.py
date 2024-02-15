import json

from alluxio.worker_ring import ConsistentHashProvider
from alluxio.worker_ring import WorkerIdentity
from alluxio.worker_ring import WorkerNetAddress


def test_hash_ring():
    hash_provider = ConsistentHashProvider(
        hash_node_per_worker=5,
        etcd_refresh_workers_interval=100000000,
    )
    worker_list_path = "tests/hash_res/workerList.json"
    with open(worker_list_path, "r") as file:
        workers_data = json.load(file)

    worker_info_map = {}
    for worker_data in workers_data:
        worker_identity = WorkerIdentity(
            version=int(worker_data["version"]),
            identifier=bytes.fromhex(worker_data["identifier"]),
        )
        # Assuming you want to use a default WorkerNetAddress for each WorkerIdentity
        default_worker_net_address = WorkerNetAddress()  # Using default values
        worker_info_map[worker_identity] = default_worker_net_address

    hash_provider._update_hash_ring(worker_info_map)
    current_ring = hash_provider.hash_ring

    hash_ring_path = "tests/hash_res/activeNodesMap.json"
    with open(hash_ring_path, "r") as file:
        hash_ring_data = json.load(file)

    not_found_count = 0
    mismatch_count = 0
    for hash_key, worker_identity in hash_ring_data.items():
        key = int(hash_key)
        if key in current_ring:
            # Fetch the WorkerIdentity object from current_ring
            current_worker_identity = current_ring[key]

            # Check if the version and identifier match
            if current_worker_identity.version == worker_identity[
                "version"
            ] and current_worker_identity.identifier == bytes.fromhex(
                worker_identity["identifier"]
            ):
                continue
            else:
                mismatch_count += 1
        else:
            not_found_count += 1

    assert (
        not_found_count == 0
    ), "Some hash keys were not found in the current ring"
    assert mismatch_count == 0, "Some hash keys had mismatched WorkerIdentity"

    file_workers_path = "tests/hash_res/fileUrlWorkers.json"
    with open(file_workers_path, "r") as file:
        file_workers_data = json.load(file)

    for ufs_url, workers in file_workers_data.items():
        current_worker_identities = (
            hash_provider._get_multiple_worker_identities(ufs_url, 5)
        )
        original_set = {
            (worker["version"], bytes.fromhex(worker["identifier"]))
            for worker in workers
        }
        current_set = {
            (worker.version, worker.identifier)
            for worker in current_worker_identities
        }
        assert original_set == current_set
