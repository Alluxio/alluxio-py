# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.
import json
import logging
import random
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Dict
from typing import List
from typing import Set

import etcd3
import mmh3
from sortedcontainers import SortedDict

from alluxio.config import AlluxioClientConfig
from alluxio.const import ALLUXIO_CLUSTER_NAME_DEFAULT_VALUE
from alluxio.const import ALLUXIO_WORKER_HTTP_SERVER_PORT_DEFAULT_VALUE
from alluxio.const import ETCD_PREFIX_FORMAT
from alluxio.exception import WorkInfoParseException, NoWorkerEntitiesException
from alluxio.logger import set_log_level

DEFAULT_HOST = "localhost"
DEFAULT_CONTAINER_HOST = ""
DEFAULT_RPC_PORT = 29999
DEFAULT_DATA_PORT = 29997
DEFAULT_SECURE_RPC_PORT = 0
DEFAULT_NETTY_DATA_PORT = 29997
DEFAULT_WEB_PORT = 30000
DEFAULT_DOMAIN_SOCKET_PATH = ""
DEFAULT_WORKER_IDENTIFIER_VERSION = 1

logger = logging.getLogger(__name__)


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
    http_server_port: int = ALLUXIO_WORKER_HTTP_SERVER_PORT_DEFAULT_VALUE


@dataclass(frozen=True)
class WorkerIdentity:
    version: int
    identifier: bytes


class NULL_NAMESPACE:
    bytes = b""


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
                version=int(identity_info.get("version")),
                identifier=bytes.fromhex(identity_info.get("identifier")),
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
                    ALLUXIO_WORKER_HTTP_SERVER_PORT_DEFAULT_VALUE,
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
            raise WorkInfoParseException(
                f"Failed to process given worker_info {worker_info} {e}"
            ) from e

    @staticmethod
    def from_host_and_port(worker_host, worker_http_port):
        worker_uuid = uuid.uuid3(NULL_NAMESPACE, worker_host)
        uuid_bytes = worker_uuid.bytes

        worker_identity = WorkerIdentity(
            DEFAULT_WORKER_IDENTIFIER_VERSION, uuid_bytes
        )
        worker_net_address = WorkerNetAddress(
            host=worker_host,
            container_host=DEFAULT_CONTAINER_HOST,
            rpc_port=DEFAULT_RPC_PORT,
            data_port=DEFAULT_DATA_PORT,
            secure_rpc_port=DEFAULT_SECURE_RPC_PORT,
            netty_data_port=DEFAULT_NETTY_DATA_PORT,
            web_port=DEFAULT_WEB_PORT,
            domain_socket_path=DEFAULT_DOMAIN_SOCKET_PATH,
            http_server_port=worker_http_port,
        )
        return WorkerEntity(worker_identity, worker_net_address)


class EtcdClient:
    def __init__(self, config: AlluxioClientConfig, host: str, port: int):
        self._host = host
        self._port = port
        self._etcd_username = None
        self._etcd_password = None
        self._prefix = ETCD_PREFIX_FORMAT.format(
            cluster_name=ALLUXIO_CLUSTER_NAME_DEFAULT_VALUE
        )

        if self._host is None:
            raise ValueError(
                "ETCD host is None. ETCD host must be provided to use ETCD."
            )

        if self._port is None:
            raise ValueError(
                "ETCD port is None. ETCD port must be provided to use ETCD."
            )

        self._etcd_username = config.ectd_config.etcd_username
        self._etcd_password = config.ectd_config.etcd_password

        if (self._etcd_username is None) != (self._etcd_password is None):
            raise ValueError(
                "Both ETCD username and password must be set or both should be unset."
            )

        self._prefix = ETCD_PREFIX_FORMAT.format(
            cluster_name=config.cluster_name
        )

    def get_worker_entities(self) -> Set[WorkerEntity]:
        """
        Retrieve worker entities from etcd using the specified prefix.

        Returns:
            set: A set of WorkerEntity objects.
        """
        # Note that EtcdClient should not be passed through python multiprocessing
        etcd = self._get_etcd_client()
        worker_entities = {
            WorkerEntity.from_worker_info(worker_info)
            for worker_info, _ in etcd.get_prefix(self._prefix)
        }

        if not worker_entities:
            raise NoWorkerEntitiesException(
                "Alluxio cluster may still be initializing. No worker registered"
            )
        return worker_entities

    def _get_etcd_client(self):
        if self._etcd_username:
            return etcd3.client(
                host=self._host,
                port=self._port,
                user=self._etcd_username,
                password=self._etcd_password,
            )
        return etcd3.client(host=self._host, port=self._port)


class ConsistentHashProvider:
    def __init__(
            self,
            config: AlluxioClientConfig,
            max_attempts=100,
            test_options=None,
    ):
        self._config = config
        self._max_attempts = max_attempts
        self._lock = threading.Lock()
        self._is_ring_initialized = False
        self._worker_info_map = {}

        if self._config.worker_address_config is not None:
            self._update_hash_ring(
                _generate_worker_info_map(
                    self._config.worker_address_config.worker_hosts, self._config.worker_address_config.worker_http_port
                )
            )
        if self._config.ectd_config is not None:
            self._fetch_workers_and_update_ring()
            if self._config.ectd_config.etcd_refresh_workers_interval > 0:
                self._shutdown_background_update_ring_event = threading.Event()
                self._background_thread = None
                self._start_background_update_ring(
                    self._config.ectd_config.etcd_refresh_workers_interval
                )
        test_options = test_options or {}
        set_log_level(logger, test_options)

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
            worker_identities = self._get_multiple_worker_identities(
                key, count
            )
            worker_addresses = []
            for worker_identity in worker_identities:
                worker_address = self._worker_info_map.get(worker_identity)
                if worker_address:
                    worker_addresses.append(worker_address)
            return worker_addresses

    def _get_multiple_worker_identities(
            self, key: str, count: int
    ) -> List[WorkerIdentity]:
        """
        This method needs external lock to ensure safety
        """
        count = (
            len(self._worker_info_map)
            if count >= len(self._worker_info_map)
            else count
        )
        workers = []
        attempts = 0
        while len(workers) < count and attempts < self._max_attempts:
            attempts += 1
            worker = self._get_ceiling_value(_hash(key, attempts))
            if worker not in workers:
                workers.append(worker)

        return workers

    def _start_background_update_ring(self, interval):
        def update_loop():
            while not self._shutdown_background_update_ring_event.is_set():
                try:
                    self._fetch_workers_and_update_ring()
                except Exception as e:
                    logger.error(f"Error updating worker hash ring: {e}")
                time.sleep(interval)

        self._background_thread = threading.Thread(target=update_loop)
        self._background_thread.daemon = True
        self._background_thread.start()

    def shutdown_background_update_ring(self):
        if (
                self._config.ectd_config.etcd_hosts
                and self._config.ectd_config.etcd_refresh_workers_interval > 0
        ):
            self._shutdown_background_update_ring_event.set()
            if self._background_thread:
                self._background_thread.join()

    def __del__(self):
        self.shutdown_background_update_ring()

    def _fetch_workers_and_update_ring(self):
        etcd_hosts_list = self._config.ectd_config.etcd_hosts.split(",")
        random.shuffle(etcd_hosts_list)
        worker_entities: Set[WorkerEntity] = set()
        for host in etcd_hosts_list:
            try:
                worker_entities = EtcdClient(
                    host=host,
                    port=self._config.ectd_config.etcd_port,
                    config=self._config,
                ).get_worker_entities()

                break
            except Exception as e:
                logger.error(e)
                continue
        if not worker_entities:
            if self._is_ring_initialized:
                logger.info(
                    f"Failed to achieve worker info list from ETCD servers:{self._config.ectd_config.etcd_hosts}"
                )
                return
            else:
                raise WorkInfoParseException(
                    f"Failed to achieve worker info list from ETCD servers:{self._config.ectd_config.etcd_hosts}"
                )

        worker_info_map = {}
        diff_in_worker_info_detected = False
        for worker_entity in worker_entities:
            worker_info_map[
                worker_entity.worker_identity
            ] = worker_entity.worker_net_address
            if worker_entity.worker_identity not in self._worker_info_map:
                diff_in_worker_info_detected = True
            elif (
                    self._worker_info_map[worker_entity.worker_identity]
                    != worker_entity.worker_net_address
            ):
                diff_in_worker_info_detected = True

        if len(worker_info_map) != len(self._worker_info_map):
            diff_in_worker_info_detected = True

        if diff_in_worker_info_detected:
            self._update_hash_ring(worker_info_map)

    def _update_hash_ring(
            self, worker_info_map: Dict[WorkerIdentity, WorkerNetAddress]
    ):
        with self._lock:
            hash_ring = SortedDict()
            for worker_identity in worker_info_map.keys():
                for i in range(self._config.hash_node_per_worker):
                    hash_key = _hash_worker_identity(worker_identity, i)
                    hash_ring[hash_key] = worker_identity
            self.hash_ring = hash_ring
            self._worker_info_map = worker_info_map
            self._is_ring_initialized = True

    def _get_ceiling_value(self, hash_key: int):
        key_index = self.hash_ring.bisect_right(hash_key)
        if key_index < len(self.hash_ring):
            ceiling_key = self.hash_ring.keys()[key_index]
            ceiling_value = self.hash_ring[ceiling_key]
            return ceiling_value
        else:
            return self.hash_ring.peekitem(0)[1]


def _hash(key: str, index: int) -> int:
    hasher = mmh3.mmh3_32()
    hasher.update(key.encode("utf-8"))
    hasher.update(index.to_bytes(4, "little"))
    return hasher.sintdigest()


def _hash_worker_identity(
        worker: WorkerIdentity, node_index: int
) -> int:
    # Hash the combined bytes
    hasher = mmh3.mmh3_32()
    hasher.update(worker.identifier)
    hasher.update(worker.version.to_bytes(4, "little"))
    hasher.update(node_index.to_bytes(4, "little"))
    return hasher.sintdigest()


def _generate_worker_info_map(worker_hosts, worker_http_port):
    worker_info_map = {}
    host_list = [host.strip() for host in worker_hosts.split(",")]
    for worker_host in host_list:
        worker_entity = WorkerEntity.from_host_and_port(
            worker_host, worker_http_port
        )
        worker_info_map[
            worker_entity.worker_identity
        ] = worker_entity.worker_net_address
    return worker_info_map
