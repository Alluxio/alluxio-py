import json
import logging
import math
from typing import List
from typing import Set

import etcd3
import mmh3
from sortedcontainers import SortedDict


class WorkerNetAddress:
    DEFAULT_HOST = "localhost"
    DEFAULT_CONTAINER_HOST = ""
    DEFAULT_RPC_PORT = 29999
    DEFAULT_DATA_PORT = 29997
    DEFAULT_SECURE_RPC_PORT = 0
    DEFAULT_NETTY_DATA_PORT = 29997
    DEFAULT_WEB_PORT = 30000
    DEFAULT_DOMAIN_SOCKET_PATH = ""

    def __init__(
        self,
        host=DEFAULT_HOST,
        container_host=DEFAULT_CONTAINER_HOST,
        rpc_port=DEFAULT_RPC_PORT,
        data_port=DEFAULT_DATA_PORT,
        secure_rpc_port=DEFAULT_SECURE_RPC_PORT,
        netty_data_port=DEFAULT_NETTY_DATA_PORT,
        web_port=DEFAULT_WEB_PORT,
        domain_socket_path=DEFAULT_DOMAIN_SOCKET_PATH,
    ):
        self.host = host
        self.container_host = container_host
        self.rpc_port = rpc_port
        self.data_port = data_port
        self.secure_rpc_port = secure_rpc_port
        self.netty_data_port = netty_data_port
        self.web_port = web_port
        self.domain_socket_path = domain_socket_path

    @staticmethod
    def from_worker_info(worker_info):
        try:
            worker_info_string = worker_info.decode("utf-8")
            worker_info_json = json.loads(worker_info_string)
            worker_net_address = worker_info_json.get("WorkerNetAddress", {})

            return WorkerNetAddress(
                host=worker_net_address.get(
                    "Host", WorkerNetAddress.DEFAULT_HOST
                ),
                container_host=worker_net_address.get(
                    "ContainerHost", WorkerNetAddress.DEFAULT_CONTAINER_HOST
                ),
                rpc_port=worker_net_address.get(
                    "RpcPort", WorkerNetAddress.DEFAULT_RPC_PORT
                ),
                data_port=worker_net_address.get(
                    "DataPort", WorkerNetAddress.DEFAULT_DATA_PORT
                ),
                secure_rpc_port=worker_net_address.get(
                    "SecureRpcPort", WorkerNetAddress.DEFAULT_SECURE_RPC_PORT
                ),
                netty_data_port=worker_net_address.get(
                    "NettyDataPort", WorkerNetAddress.DEFAULT_NETTY_DATA_PORT
                ),
                web_port=worker_net_address.get(
                    "WebPort", WorkerNetAddress.DEFAULT_WEB_PORT
                ),
                domain_socket_path=worker_net_address.get(
                    "DomainSocketPath",
                    WorkerNetAddress.DEFAULT_DOMAIN_SOCKET_PATH,
                ),
            )
        except json.JSONDecodeError as e:
            raise ValueError(
                f"Provided worker_info is not a valid JSON string {e}"
            ) from e
        except AttributeError as e:
            raise AttributeError(
                f"Provided worker_info must be a bytes-like object {e}"
            ) from e
        except Exception as e:
            raise Exception(
                f"Failed to process given worker_info {worker_info} {e}"
            ) from e

    @staticmethod
    def from_worker_hosts(worker_hosts):
        worker_addresses = set()
        for host in worker_hosts.split(","):
            worker_address = WorkerNetAddress(host=host)
            worker_addresses.add(worker_address)
        return worker_addresses

    def dump_main_info(self):
        return (
            "WorkerNetAddress{{host={}, containerHost={}, rpcPort={}, dataPort={}, webPort={}, domainSocketPath={}}}"
        ).format(
            self.host,
            self.container_host,
            self.rpc_port,
            self.data_port,
            self.web_port,
            self.domain_socket_path,
        )


class EtcdClient:
    PREFIX = "/DHT/DefaultAlluxioCluster/AUTHORIZED/"
    ALLUXIO_ETCD_USERNAME = "alluxio.etcd.username"
    ALLUXIO_ETCD_PASSWORD = "alluxio.etcd.password"

    def __init__(self, host="localhost", port=2379, options=None):
        self.host = host
        self.port = port

        # Parse options
        self.etcd_username = None
        self.etcd_password = None
        if options:
            if self.ALLUXIO_ETCD_USERNAME in options:
                self.etcd_username = options[self.ALLUXIO_ETCD_USERNAME]
            if self.ALLUXIO_ETCD_PASSWORD in options:
                self.etcd_password = options[self.ALLUXIO_ETCD_PASSWORD]
        if (self.etcd_username is None) != (self.etcd_password is None):
            raise ValueError(
                "Both ETCD username and password must be set or both should be unset."
            )

    def get_worker_addresses(self):
        """
        Retrieve worker addresses from etcd using the specified prefix.

        Returns:
            list: A list of WorkerNetAddress objects.
        """
        # Note that EtcdClient should not be passed through python multiprocessing
        etcd = self._get_etcd_client()
        worker_addresses = set()
        try:
            worker_addresses = {
                WorkerNetAddress.from_worker_info(worker_info)
                for worker_info, _ in etcd.get_prefix(self.PREFIX)
            }
        except Exception as e:
            raise Exception(
                f"Failed to achieve worker info list from ETCD server {self.host}:{self.port} {e}"
            ) from e

        if not worker_addresses:
            # TODO(lu) deal with the alluxio cluster initalizing issue
            raise Exception(
                "Alluxio cluster may still be initializing. No worker registered"
            )
        return worker_addresses

    def _get_etcd_client(self):
        if self.etcd_username:
            return etcd3.client(
                host=self.host,
                port=self.port,
                user=self.etcd_username,
                password=self.etcd_password,
            )
        return etcd3.client(host=self.host, port=self.port)


class ConsistentHashProvider:
    def __init__(
        self,
        etcd_hosts=None,
        worker_hosts=None,
        logger=None,
        num_virtual_nodes=2000,
        max_attempts=100,
        refresh_timeout_seconds=120,
    ):
        self._etcd_hosts = etcd_hosts
        self._worker_addresses = (
            WorkerNetAddress.from_worker_hosts(worker_hosts)
            if worker_hosts
            else None
        )
        self._num_virtual_nodes = num_virtual_nodes
        self._max_attempts = max_attempts
        self._logger = logger or logging.getLogger("ConsistentHashProvider")

        self._lock = threading.Lock()
        self._is_ring_initialized = False
        self._update_hash_ring_if_needed(self._worker_addresses)
        self._start_background_update(refresh_timeout_seconds)

    def get_multiple_workers(
        self, key: str, count: int
    ) -> List[WorkerNetAddress]:
        """
        Retrieve a specified number of worker addresses based on a given key.

        Args:
            key (str): The unique path identifier, e.g., full UFS path.
            count (int): The number of worker addresses to retrieve.

        Returns:
            List[WorkerNetAddress]: A list containing the desired number of WorkerNetAddress objects.
        """
        with self._lock:
            if count >= len(self._worker_addresses):
                return self._worker_addresses
            workers: Set[WorkerNetAddress] = set()
            attempts = 0
            while len(workers) < count and attempts < self._max_attempts:
                attempts += 1
                workers.add(self._get_ceiling_value(self._hash(key, attempts)))
            return list(workers)

    def _start_background_update(self, interval):
        def update_loop():
            time.sleep(interval)
            try:
                self._update_hash_ring_if_needed()
            except Exception as e:
                self._logger.error(f"Error updating hash ring: {e}")

        self._background_thread = threading.Thread(target=update_loop)
        self._background_thread.daemon = True
        self._background_thread.start()

    def _update_hash_ring_if_needed(self):
        if self._etcd_hosts is None:
            if self._is_ring_initialized == False:
                self._update_hash_ring(self._worker_addresses)
            return

        worker_addresses = set()
        etcd_hosts_list = self._etcd_hosts.split(",")
        random.shuffle(etcd_hosts_list)
        for host in etcd_hosts_list:
            try:
                current_addresses = EtcdClient(
                    host=host, options=options
                ).get_worker_addresses()
                worker_addresses.update(current_addresses)
                break
            except Exception as e:
                continue
        if not worker_addresses:
            if self._is_ring_initialized:
                self.logger.info(
                    f"Failed to achieve worker info list from ETCD servers:{etcd_hosts}"
                )
                return
            else:
                raise Exception(
                    f"Failed to achieve worker info list from ETCD servers:{etcd_hosts}"
                )

        if worker_addresses != self._worker_addresses:
            self._update_hash_ring(worker_addresses)

    def _update_hash_ring_internal(
        self, worker_addresses: Set[WorkerNetAddress]
    ):
        with self._lock:
            if worker_addresses == self._worker_addresses:
                return
            hash_ring = SortedDict()
            weight = math.ceil(self._num_virtual_nodes / len(worker_addresses))
            for worker_address in worker_addresses:
                worker_string = worker_address.dump_main_info()
                for i in range(weight):
                    hash_key = self._hash(worker_string, i)
                    hash_ring[hash_key] = worker_address
            self._hash_ring = hash_ring
            self._worker_addresses = worker_addresses
            self._is_ring_initialized = True

    def _get_ceiling_value(self, hash_key: int):
        key_index = self._hash_ring.bisect_right(hash_key)
        if key_index < len(self._hash_ring):
            ceiling_key = self._hash_ring.keys()[key_index]
            ceiling_value = self._hash_ring[ceiling_key]
            return ceiling_value
        else:
            return self._hash_ring.peekitem(0)[1]

    def _hash(self, key: str, index: int) -> int:
        return mmh3.hash(f"{key}{index}".encode("utf-8"))
