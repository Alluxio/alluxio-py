import logging
import os
import shlex
import subprocess
import time
from urllib.parse import urlparse

import pytest
import requests

from alluxio.alluxio_file_system import AlluxioFileSystem

LOGGER = logging.getLogger("alluxio_test")
TEST_ROOT = os.getenv("TEST_ROOT", "file:///opt/alluxio/ufs/")
# This is the path to the file you want to access
TEST_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets")
LOCAL_FILE_PATH = os.path.join(TEST_DIR, "test.csv")
ALLUXIO_FILE_PATH = "file://{}".format("/opt/alluxio/ufs/test.csv")


def stop_docker(container):
    cmd = shlex.split('docker ps -a -q --filter "name=%s"' % container)
    cid = subprocess.check_output(cmd).strip().decode()
    if cid:
        LOGGER.debug("Stopping existing container %s" % cid)
        subprocess.call(["docker", "rm", "-f", "-v", cid])


@pytest.fixture(scope="module")
def docker_alluxio():
    if "ALLUXIO_URL" in os.environ:
        # assume we already have a server already set up
        yield os.getenv("ALLUXIO_URL")
        return
    master_container = "alluxio-master"
    worker_container = "alluxio-worker"
    network_cmd = "docker network create alluxio_network"

    run_cmd_master = (
        "docker run --platform linux/amd64 -d --rm --net=alluxio_network -p 19999:19999 -p 19998:19998 "
        f"--name=alluxio-master -v {TEST_DIR}:/opt/alluxio/ufs "
        '-e ALLUXIO_JAVA_OPTS=" -Dalluxio.master.hostname=alluxio-master '
        "-Dalluxio.security.authentication.type=NOSASL "
        "-Dalluxio.security.authorization.permission.enabled=false "
        "-Dalluxio.security.authorization.plugins.enabled=false "
        "-Dalluxio.master.journal.type=NOOP "
        "-Dalluxio.master.scheduler.initial.wait.time=1s "
        "-Dalluxio.dora.client.ufs.root=file:/// "
        '-Dalluxio.underfs.xattr.change.enabled=false " alluxio/alluxio:308-SNAPSHOT master'
    )
    run_cmd_worker = (
        "docker run --platform linux/amd64 -d --rm --net=alluxio_network -p 28080:28080 -p 29999:29999 -p 29997:29997 "
        f"--name=alluxio-worker --shm-size=1G -v {TEST_DIR}:/opt/alluxio/ufs "
        '-e ALLUXIO_JAVA_OPTS=" -Dalluxio.master.hostname=alluxio-master '
        "-Dalluxio.security.authentication.type=NOSASL "
        "-Dalluxio.security.authorization.permission.enabled=false "
        "-Dalluxio.security.authorization.plugins.enabled=false "
        "-Dalluxio.dora.client.ufs.root=file:/// "
        '-Dalluxio.underfs.xattr.change.enabled=false " alluxio/alluxio:308-SNAPSHOT worker'
    )

    stop_docker(worker_container)
    stop_docker(master_container)
    subprocess.run(
        shlex.split(network_cmd)
    )  # could return error code if network already exists
    subprocess.check_output(shlex.split(run_cmd_master))
    subprocess.check_output(shlex.split(run_cmd_worker))
    url = "http://127.0.0.1:28080"
    timeout = 10
    while True:
        try:
            LOGGER.debug("trying to connected to alluxio")
            r = requests.get(url + "/v1/files?path=/")
            LOGGER.debug("successfullly connected to alluxio")
            if r.ok:
                yield url
                break
        except Exception as e:  # noqa: E722
            timeout -= 1
            if timeout < 0:
                raise SystemError from e
            time.sleep(10)
    stop_docker(worker_container)
    stop_docker(master_container)


@pytest.fixture
def fs(docker_alluxio):

    LOGGER.debug(f"get AlluxioFileSystem connect to {docker_alluxio}")
    parsed_url = urlparse(docker_alluxio)
    host = parsed_url.hostname
    port = parsed_url.port
    fs = AlluxioFileSystem(worker_hosts=host, worker_http_port=port)
    yield fs
