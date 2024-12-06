# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.

from dataclasses import dataclass
from typing import Optional

import humanfriendly

from .const import ALLUXIO_CLUSTER_NAME_DEFAULT_VALUE
from .const import ALLUXIO_HASH_NODE_PER_WORKER_DEFAULT_VALUE
from .const import ALLUXIO_PAGE_SIZE_DEFAULT_VALUE
from .const import ALLUXIO_WORKER_HTTP_SERVER_PORT_DEFAULT_VALUE


@dataclass
class ETCDConfig:
    etcd_hosts: str
    etcd_port: int = 2379

    etcd_username: Optional[str] = None
    etcd_password: Optional[str] = None
    etcd_refresh_workers_interval = 120


@dataclass
class WorkerAddressConfig:
    worker_hosts: str
    worker_http_port = ALLUXIO_WORKER_HTTP_SERVER_PORT_DEFAULT_VALUE


class AlluxioClientConfig:

    def __init__(
            self,
            worker_address_config: WorkerAddressConfig = None,
            etcd_config: ETCDConfig = None,
            hash_node_per_worker=ALLUXIO_HASH_NODE_PER_WORKER_DEFAULT_VALUE,
            page_size=ALLUXIO_PAGE_SIZE_DEFAULT_VALUE,
            cluster_name=ALLUXIO_CLUSTER_NAME_DEFAULT_VALUE,
            concurrency=64
    ):

        assert (
                worker_address_config or etcd_config
        ), "Must supply either 'worker_address_config' or 'etcd_config'"

        assert not (
                worker_address_config and etcd_config
        ), "Supply either 'worker_address_config' or 'etcd_config', not both"

        if etcd_config:
            assert isinstance(etcd_config.etcd_port, int) and (
                    1 <= etcd_config.etcd_port <= 65535
            ), "'etcd_port' should be an integer in the range 1-65535"
            assert isinstance(
                etcd_config.etcd_refresh_workers_interval, int
            ), "'etcd_refresh_workers_interval' should be an integer"
            assert (etcd_config.etcd_username is None) == (
                    etcd_config.etcd_password is None
            ), "Both ETCD username and password must be set or both should be unset."

        if worker_address_config:
            assert isinstance(worker_address_config.worker_http_port, int) and (
                    1 <= worker_address_config.worker_http_port <= 65535
            ), "'worker_http_port' should be an integer in the range 1-65535"

        assert (
                isinstance(concurrency, int) and concurrency > 0
        ), "'concurrency' should be a positive integer"

        assert (
                isinstance(hash_node_per_worker, int) and hash_node_per_worker > 0
        ), "'hash_node_per_worker' should be a positive integer"

        self.ectd_config = etcd_config
        self.worker_address_config = worker_address_config
        self.hash_node_per_worker = hash_node_per_worker
        self.page_size = humanfriendly.parse_size(page_size, binary=True)
        self.cluster_name = cluster_name
        self.concurrency = concurrency
