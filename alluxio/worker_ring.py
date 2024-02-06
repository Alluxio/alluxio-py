import json
import logging
import math
import random
import threading
import time
from dataclasses import dataclass
from typing import List
from typing import Set

import etcd3
import mmh3
from sortedcontainers import SortedDict

DEFAULT_HOST = "localhost"
DEFAULT_CONTAINER_HOST = ""
DEFAULT_RPC_PORT = 29999
DEFAULT_DATA_PORT = 29997
DEFAULT_SECURE_RPC_PORT = 0
DEFAULT_NETTY_DATA_PORT = 29997
DEFAULT_WEB_PORT = 30000
DEFAULT_DOMAIN_SOCKET_PATH = ""
DEFAULT_HTTP_SERVER_PORT = 28080


@dataclass(frozen=True)
class WorkerNetAddress:
    host: str = DEFAULT_HOST
    container_host: str = DEFAULT_CONTAINER_HOST
    rpc_port: int = DEFAULT_RPC_PORT
    data_port: int = DEFAULT_DATA_PORT
    secure_rpc_port: int = DEFAULT_SECURE_RPC_PORT
    netty_data_port: int = DEFAULT_NETTY_DATA_PORT
    web_port: int = DEFAULT_WEB_PORT
    domain_socket_path: str = DEFAULT_DOMAIN_SOCKET_PATH
    http_server_port: int = DEFAULT_HTTP_SERVER_PORT


@dataclass(frozen=True)
class WorkerIdentity:
    version: int
    identifier: str


@dataclass(frozen=True)
class WorkerEntity:
    worker_identity: WorkerIdentity
    worker_net_address: WorkerNetAddress

    @staticmethod
    def from_worker_info(worker_info):
        try:
            worker_info_string = worker_info.decode("utf-8")
            worker_info_json = json.loads(worker_info_string)
            identity_info = worker_info_json.get("Identity", {})
            worker_identity = WorkerIdentity(
                version=identity_info.get("version"),
                identifier=identity_info.get("identifier"),
            )

            worker_net_address_info = worker_info_json.get(
                "WorkerNetAddress", {}
            )
            worker_net_address = WorkerNetAddress(
                host=worker_net_address_info.get("Host", DEFAULT_HOST),
                container_host=worker_net_address_info.get(
                    "ContainerHost", DEFAULT_CONTAINER_HOST
                ),
                rpc_port=worker_net_address_info.get(
                    "RpcPort", DEFAULT_RPC_PORT
                ),
                data_port=worker_net_address_info.get(
                    "DataPort", DEFAULT_DATA_PORT
                ),
                secure_rpc_port=worker_net_address_info.get(
                    "SecureRpcPort", DEFAULT_SECURE_RPC_PORT
                ),
                netty_data_port=worker_net_address_info.get(
                    "NettyDataPort", DEFAULT_NETTY_DATA_PORT
                ),
                web_port=worker_net_address_info.get(
                    "WebPort", DEFAULT_WEB_PORT
                ),
                domain_socket_path=worker_net_address_info.get(
                    "DomainSocketPath",
                    DEFAULT_DOMAIN_SOCKET_PATH,
                ),
                http_server_port=worker_net_address_info.get(
                    "HttpServerPort",
                    DEFAULT_HTTP_SERVER_PORT,
                ),
            )
            return WorkerEntity(worker_identity, worker_net_address)
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


class EtcdClient:
    PREFIX = "/ServiceDiscovery/DefaultAlluxioCluster/"
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

    def get_worker_entities(self) -> Set[WorkerEntity]:
        """
        Retrieve worker entities from etcd using the specified prefix.

        Returns:
            set: A set of WorkerEntity objects.
        """
        # Note that EtcdClient should not be passed through python multiprocessing
        etcd = self._get_etcd_client()
        worker_entities: Set[WorkerEntity] = set()
        try:
            worker_entities = {
                WorkerEntity.from_worker_info(worker_info)
                for worker_info, _ in etcd.get_prefix(self.PREFIX)
            }
        except Exception as e:
            raise Exception(
                f"Failed to achieve worker info list from ETCD server {self.host}:{self.port} {e}"
            ) from e

        if not worker_entities:
            # TODO(lu) deal with the alluxio cluster initalizing issue
            raise Exception(
                "Alluxio cluster may still be initializing. No worker registered"
            )
        return worker_entities

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
        etcd_port=2379,
        options=None,
        logger=None,
        hash_node_per_worker=5,
        max_attempts=100,
        etcd_refresh_workers_interval=None,
    ):
        self._etcd_hosts = etcd_hosts
        self._etcd_port = etcd_port
        self._options = options
        self._logger = logger or logging.getLogger("ConsistentHashProvider")
        self._hash_node_per_worker = hash_node_per_worker
        self._max_attempts = max_attempts
        self._lock = threading.Lock()
        self._is_ring_initialized = False
        self._worker_info_map = {}
        self._etcd_refresh_workers_interval = etcd_refresh_workers_interval
        if self._etcd_hosts:
            self._fetch_workers_and_update_ring()
            if self._etcd_refresh_workers_interval > 0:
                self._shutdown_background_update_ring_event = threading.Event()
                self._background_thread = None
                self._start_background_update_ring(
                    self._etcd_refresh_workers_interval
                )

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
                return list(self._worker_addresses)
            workers: Set[WorkerNetAddress] = set()
            attempts = 0
            while len(workers) < count and attempts < self._max_attempts:
                attempts += 1
                workers.add(self._get_ceiling_value(self._hash(key, attempts)))
            return list(workers)

    def _start_background_update_ring(self, interval):
        def update_loop():
            while not self._shutdown_background_update_ring_event.is_set():
                try:
                    self._fetch_workers_and_update_ring()
                except Exception as e:
                    self._logger.error(f"Error updating worker hash ring: {e}")
                time.sleep(interval)

        self._background_thread = threading.Thread(target=update_loop)
        self._background_thread.daemon = True
        self._background_thread.start()

    def shutdown_background_update_ring(self):
        if self._etcd_hosts and self._etcd_refresh_workers_interval > 0:
            self._shutdown_background_update_ring_event.set()
            if self._background_thread:
                self._background_thread.join()

    def __del__(self):
        self.shutdown_background_update_ring()

    def _fetch_workers_and_update_ring(self):
        etcd_hosts_list = self._etcd_hosts.split(",")
        random.shuffle(etcd_hosts_list)
        worker_entities: Set[WorkerEntity] = set()
        for host in etcd_hosts_list:
            try:
                worker_entities = EtcdClient(
                    host=host, port=self._etcd_port, options=self._options
                ).get_worker_entities()
                break
            except Exception as e:
                continue
        if not worker_entities:
            if self._is_ring_initialized:
                self._logger.info(
                    f"Failed to achieve worker info list from ETCD servers:{self._etcd_hosts}"
                )
                return
            else:
                raise Exception(
                    f"Failed to achieve worker info list from ETCD servers:{self._etcd_hosts}"
                )

        worker_info_map = {}
        detect_diff_in_worker_info = False
        for worker_entity in worker_entities:
            worker_info_map[
                worker_entity.worker_identity
            ] = worker_entity.worker_net_address
            if worker_entity.worker_identity not in self._worker_info_map:
                detect_diff_in_worker_info = True
            elif (
                self._worker_info_map[worker_entity.worker_identity]
                != worker_entity.worker_net_address
            ):
                detect_diff_in_worker_info = True

        if len(worker_info_map) != len(self._worker_info_map):
            detect_diff_in_worker_info = True

        if detect_diff_in_worker_info:
            self._update_hash_ring(worker_info_map)

    def _update_hash_ring(
        self, worker_info_map: dict[WorkerIdentity, WorkerNetAddress]
    ):
        with self._lock:
            hash_ring = SortedDict()
            for worker_identity in worker_info_map.keys():
                for i in range(self._hash_node_per_worker):
                    hash_key = self._hash(worker_identity, i)
                    hash_ring[hash_key] = worker_identity
            self._hash_ring = hash_ring
            self._worker_info_map = worker_info_map
            self._is_ring_initialized = True

    def _get_ceiling_value(self, hash_key: int):
        key_index = self._hash_ring.bisect_right(hash_key)
        if key_index < len(self._hash_ring):
            ceiling_key = self._hash_ring.keys()[key_index]
            ceiling_value = self._hash_ring[ceiling_key]
            return ceiling_value
        else:
            return self._hash_ring.peekitem(0)[1]

    def _hash(self, worker_identity: WorkerIdentity, index: int) -> int:
        return mmh3.hash(
            f"{worker_identity.identifier}{worker_identity.version}{index}".encode(
                "utf-8"
            )
        )
