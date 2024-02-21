ALLUXIO_CLUSTER_NAME_KEY = "alluxio.cluster.name"
ALLUXIO_CLUSTER_NAME_DEFAULT_VALUE = "DefaultAlluxioCluster"
ALLUXIO_ETCD_USERNAME_KEY = "alluxio.etcd.username"
ALLUXIO_ETCD_PASSWORD_KEY = "alluxio.etcd.password"
ALLUXIO_PAGE_SIZE_KEY = "alluxio.worker.page.store.page.size"
ALLUXIO_PAGE_SIZE_DEFAULT_VALUE = "1MB"
ALLUXIO_HASH_NODE_PER_WORKER_KEY = (
    "alluxio.user.consistent.hash.virtual.node.count.per.worker"
)
ALLUXIO_WORKER_HTTP_SERVER_PORT_KEY = "alluxio.worker.http.server.port"
ALLUXIO_WORKER_HTTP_SERVER_PORT_DEFAULT_VALUE = 28080
ALLUXIO_HASH_NODE_PER_WORKER_DEFAULT_VALUE = 5
ALLUXIO_SUCCESS_IDENTIFIER = "success"
LIST_URL_FORMAT = "http://{worker_host}:{http_port}/v1/files"
FULL_PAGE_URL_FORMAT = (
    "http://{worker_host}:{http_port}/v1/file/{path_id}/page/{page_index}"
)
PAGE_URL_FORMAT = "http://{worker_host}:{http_port}/v1/file/{path_id}/page/{page_index}?offset={page_offset}&length={page_length}"
WRITE_PAGE_URL_FORMAT = (
    "http://{worker_host}:{http_port}/v1/file/{path_id}/page/{page_index}"
)
GET_FILE_STATUS_URL_FORMAT = "http://{worker_host}:{http_port}/v1/info"
LOAD_URL_FORMAT = "http://{worker_host}:{http_port}/v1/load"
# TODO (chunxu): Remove the concrete types of LOAD formats. Keep them for asyncio.
LOAD_SUBMIT_URL_FORMAT = (
    "http://{worker_host}:{http_port}/v1/load?path={path}&opType=submit"
)
LOAD_PROGRESS_URL_FORMAT = (
    "http://{worker_host}:{http_port}/v1/load?path={path}&opType=progress"
)
LOAD_STOP_URL_FORMAT = (
    "http://{worker_host}:{http_port}/v1/load?path={path}&opType=stop"
)
ETCD_PREFIX_FORMAT = "/ServiceDiscovery/{cluster_name}/"
