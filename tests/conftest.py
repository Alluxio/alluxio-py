import os
import shlex
import subprocess
import time
from urllib.parse import urlparse

import pytest
import requests

from alluxio.alluxio_file_system import AlluxioFileSystem

TEST_ROOT = os.getenv("TEST_ROOT", "file://tmp/alluxio_test_root")


def stop_docker(container):
    cmd = shlex.split('docker ps -a -q --filter "name=%s"' % container)
    cid = subprocess.check_output(cmd).strip().decode()
    if cid:
        print("Stopping existing container %s" % cid)
        subprocess.call(["docker", "rm", "-f", "-v", cid])


@pytest.fixture(scope="module")
def docker_alluxio():
    if "ALLUXIO_URL" in os.environ:
        # assume we already have a server already set up
        yield os.getenv("ALLUXIO_URL")
        return
    container = "alluxio-worker"
    network_cmd = "docker network create alluxio_network"
    run_cmd = (
        "docker run --platform linux/amd64 -d --rm --net=alluxio_network -p 28080:28080 "
        "--name=alluxio-worker --shm-size=1G -v /tmp/alluxio_ufs:/opt/alluxio/ufs "
        '-e ALLUXIO_JAVA_OPTS=" -Dalluxio.worker.membership.manager.type=STATIC '
        f'-Dalluxio.dora.client.ufs.root={TEST_ROOT} " alluxio/alluxio:308-SNAPSHOT worker'
    )
    stop_docker(container)
    subprocess.run(
        shlex.split(network_cmd)
    )  # could return error code if network already exists
    subprocess.check_output(shlex.split(run_cmd))
    url = "http://127.0.0.1:28080"
    timeout = 10
    while True:
        try:
            print("trying to connected to alluxio")
            r = requests.get(url + "/v1/files?path=/")
            print("successfullly connected to alluxio")
            if r.ok:
                yield url
                break
        except Exception as e:  # noqa: E722
            timeout -= 1
            if timeout < 0:
                raise SystemError from e
            time.sleep(10)
    stop_docker(container)


@pytest.fixture
def fs(docker_alluxio):
    print(f"get AlluxioFileSystem connect to {docker_alluxio}")
    parsed_url = urlparse(docker_alluxio)
    host = parsed_url.hostname
    port = parsed_url.port
    fs = AlluxioFileSystem(worker_hosts=host, http_port=port)
    yield fs
