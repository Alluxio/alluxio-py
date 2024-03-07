from typing import Optional

import humanfriendly

from alluxio.annotations import PublicAPI
from alluxio.const import ALLUXIO_CLUSTER_NAME_DEFAULT_VALUE
from alluxio.const import ALLUXIO_HASH_NODE_PER_WORKER_DEFAULT_VALUE
from alluxio.const import ALLUXIO_PAGE_SIZE_DEFAULT_VALUE
from alluxio.const import ALLUXIO_WORKER_HTTP_SERVER_PORT_DEFAULT_VALUE


@PublicAPI(stability="beta")
class AlluxioClientConfig:
    """
    Class responsible for creating the configuration for Alluxio Client.
    """

    def __init__(
        self,
        etcd_hosts: Optional[str] = None,
        worker_hosts: Optional[str] = None,
        etcd_port=2379,
        worker_http_port=ALLUXIO_WORKER_HTTP_SERVER_PORT_DEFAULT_VALUE,
        etcd_refresh_workers_interval=120,
        page_size=ALLUXIO_PAGE_SIZE_DEFAULT_VALUE,
        hash_node_per_worker=ALLUXIO_HASH_NODE_PER_WORKER_DEFAULT_VALUE,
        cluster_name=ALLUXIO_CLUSTER_NAME_DEFAULT_VALUE,
        etcd_username: Optional[str] = None,
        etcd_password: Optional[str] = None,
        concurrency=64,
    ):
        """
        Initializes Alluxio client configuration.

        Args:
            etcd_hosts (Optional[str], optional): The hostnames of ETCD to get worker addresses from
                in 'host1,host2,host3' format. Either etcd_hosts or worker_hosts should be provided, not both.
            worker_hosts (Optional[str], optional): The worker hostnames in 'host1,host2,host3' format.
                Either etcd_hosts or worker_hosts should be provided, not both.
            concurrency (int, optional): The maximum number of concurrent operations for HTTP requests, default to 64.
            etcd_port (int, optional): The port of each etcd server.
            worker_http_port (int, optional): The port of the HTTP server on each Alluxio worker node.
            etcd_refresh_workers_interval (int, optional): The interval to refresh worker list from ETCD membership service periodically.
                All negative values mean the service is disabled.
        """
        if not (etcd_hosts or worker_hosts):
            raise ValueError(
                "Must supply either 'etcd_hosts' or 'worker_hosts'"
            )
        if etcd_hosts and worker_hosts:
            raise ValueError(
                "Supply either 'etcd_hosts' or 'worker_hosts', not both"
            )
        if not isinstance(etcd_port, int) or not (1 <= etcd_port <= 65535):
            raise ValueError(
                "'etcd_port' should be an integer in the range 1-65535"
            )
        if not isinstance(worker_http_port, int) or not (
            1 <= worker_http_port <= 65535
        ):
            raise ValueError(
                "'worker_http_port' should be an integer in the range 1-65535"
            )
        if not isinstance(concurrency, int) or concurrency <= 0:
            raise ValueError("'concurrency' should be a positive integer")
        if not isinstance(etcd_refresh_workers_interval, int):
            raise ValueError(
                "'etcd_refresh_workers_interval' should be an integer"
            )
        self.etcd_hosts = etcd_hosts
        self.worker_hosts = worker_hosts
        self.etcd_port = etcd_port
        self.worker_http_port = worker_http_port
        self.etcd_refresh_workers_interval = etcd_refresh_workers_interval
        if (
            not isinstance(hash_node_per_worker, int)
            or hash_node_per_worker <= 0
        ):
            raise ValueError(
                "'hash_node_per_worker' should be a positive integer"
            )

        self.hash_node_per_worker = hash_node_per_worker
        self.page_size = humanfriendly.parse_size(page_size, binary=True)
        self.cluster_name = cluster_name

        if (etcd_username is None) != (etcd_password is None):
            raise ValueError(
                "Both ETCD username and password must be set or both should be unset."
            )
        self.etcd_username = etcd_username
        self.etcd_password = etcd_password
        self.concurrency = concurrency
