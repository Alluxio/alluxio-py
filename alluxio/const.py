# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.

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
ALLUXIO_COMMON_EXTENSION_ENABLE = "alluxio.common.extension.enable"
ALLUXIO_COMMON_ONDEMANDPOOL_DISABLE = "alluxio.common.ondemandpool.disable"
LIST_URL_FORMAT = "http://{worker_host}:{http_port}/v1/files"
FULL_PAGE_URL_FORMAT = (
    "http://{worker_host}:{http_port}/v1/file/{path_id}/page/{page_index}"
)
FULL_RANGE_URL_FORMAT = "http://{worker_host}:{http_port}/v1/range/{path_id}?ufsFullPath={file_path}&offset={offset}&length={length}"
FULL_CHUNK_URL_FORMAT = "http://{worker_host}:{http_port}/v1/chunk/{path_id}?ufsFullPath={file_path}&chunkSize={chunk_size}"
PAGE_URL_FORMAT = "http://{worker_host}:{http_port}/v1/file/{path_id}/page/{page_index}?offset={page_offset}&length={page_length}"
WRITE_PAGE_URL_FORMAT = (
    "http://{worker_host}:{http_port}/v1/file/{path_id}/page/{page_index}"
)
WRITE_CHUNK_URL_FORMAT = "http://{worker_host}:{http_port}/v1/chunk/{path_id}?ufsFullPath={file_path}&chunkSize={chunk_size}"
MKDIR_URL_FORMAT = "http://{worker_host}:{http_port}/v1/mkdir/{path_id}?ufsFullPath={file_path}"
TOUCH_URL_FORMAT = "http://{worker_host}:{http_port}/v1/touch/{path_id}?ufsFullPath={file_path}"
MV_URL_FORMAT = "http://{worker_host}:{http_port}/v1/mv/{path_id}?srcPath={srcPath}&dstPath={dstPath}"
RM_URL_FORMAT = (
    "http://{worker_host}:{http_port}/v1/rm/{path_id}?ufsFullPath={file_path}"
)
CP_URL_FORMAT = "http://{worker_host}:{http_port}/v1/copy/{path_id}?srcPath={srcPath}&dstPath={dstPath}"
TAIL_URL_FORMAT = "http://{worker_host}:{http_port}/v1/tail/{path_id}?ufsFullPath={file_path}"
HEAD_URL_FORMAT = "http://{worker_host}:{http_port}/v1/head/{path_id}?ufsFullPath={file_path}"
PAGE_PATH_URL_FORMAT = "/v1/file/{path_id}/page/{page_index}"
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
EXCEPTION_CONTENT = (
    "Worker's address: {worker_host}:{http_port}, Error: {error}"
)
