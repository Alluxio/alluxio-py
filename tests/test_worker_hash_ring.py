import json

from alluxio.worker_ring import ConsistentHashProvider
from alluxio.worker_ring import WorkerIdentity
from alluxio.worker_ring import WorkerNetAddress


def main():
    hash_provider = ConsistentHashProvider(
        etcd_hosts="localhost",
        etcd_port=2379,
        hash_node_per_worker=5,
        etcd_refresh_workers_interval=100000000,
    )
    worker_list_path = "/Users/alluxio/downloads/testData/workerList.json"
    with open(worker_list_path, "r") as file:
        workers_data = json.load(file)

    worker_info_map = {}
    for worker_data in workers_data:
        worker_identity = WorkerIdentity(
            version=worker_data["version"],
            identifier=worker_data["identifier"],
        )
        # Assuming you want to use a default WorkerNetAddress for each WorkerIdentity
        default_worker_net_address = WorkerNetAddress()  # Using default values
        worker_info_map[worker_identity] = default_worker_net_address

    hash_provider.update_hash_ring(worker_info_map)
    current_ring = hash_provider.get_hash_ring()
    for hash_key, worker_identity in current_ring.items():
        print(f"Hash Key: {hash_key}, Worker: {worker_identity}")

    hash_ring_path = "/Users/alluxio/downloads/testData/activeNodesMap.json"
    with open(hash_ring_path, "r") as file:
        hash_ring_data = json.load(file)

    not_found_count = 0
    mismatch_count = 0
    for hash_key, worker_identity in hash_ring_data.items():
        if hash_key in current_ring:
            # Fetch the WorkerIdentity object from current_ring
            current_worker_identity = current_ring[hash_key]

            # Check if the version and identifier match
            if (
                current_worker_identity.version == worker_identity["version"]
                and current_worker_identity.identifier
                == worker_identity["identifier"]
            ):
                continue
            else:
                mismatch_count += 1
        else:
            not_found_count += 1

    print(
        f"Total not found: {not_found_count}, Total mismatches: {mismatch_count}"
    )


if __name__ == "__main__":
    main()
