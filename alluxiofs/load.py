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
import time
from enum import Enum
from typing import Dict

from requests import Session

from alluxiofs.const import LOAD_URL_FORMAT, ALLUXIO_SUCCESS_IDENTIFIER
from alluxiofs.exception import LoadException

logger = logging.getLogger(__name__)


class OpType(Enum):
    SUBMIT = "submit"
    PROGRESS = "progress"
    STOP = "stop"


class LoadState(Enum):
    RUNNING = "RUNNING"
    VERIFYING = "VERIFYING"
    STOPPED = "STOPPED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


def load_file(session: Session, worker_host: str, worker_http_port: int, path: str, timeout: int,
              verbose: bool) -> bool:
    try:
        params = {
            "path": path,
            "opType": OpType.SUBMIT.value,
            "verbose": json.dumps(verbose),
        }
        response = session.get(
            LOAD_URL_FORMAT.format(
                worker_host=worker_host,
                http_port=worker_http_port,
            ),
            params=params,
        )
        response.raise_for_status()
        content = json.loads(response.content.decode("utf-8"))
        if not content[ALLUXIO_SUCCESS_IDENTIFIER]:
            return False

        params = {
            "path": path,
            "opType": OpType.PROGRESS.value,
            "verbose": json.dumps(verbose),
        }
        load_progress_url = LOAD_URL_FORMAT.format(
            worker_host=worker_host,
            http_port=worker_http_port,
        )
        stop_time = 0
        if timeout is not None:
            stop_time = time.time() + timeout
        while True:
            job_state, content = load_progress_internal(
                session, load_progress_url, params
            )
            if job_state == LoadState.SUCCEEDED:
                return True
            if job_state == LoadState.FAILED:
                logger.error(
                    f"Failed to load path {path} with return message {content}"
                )
                return False
            if job_state == LoadState.STOPPED:
                logger.warning(
                    f"Failed to load path {path} with return message {content}, load stopped"
                )
                return False
            if timeout is None or stop_time - time.time() >= 10:
                time.sleep(10)
            else:
                logger.debug(f"Failed to load path {path} within timeout")
                return False

    except Exception as e:
        logger.debug(
            f"Error when loading file {path} from {worker_host} with timeout {timeout}: error {e}"
        )
        return False


def load_progress_internal(
        session: Session, load_url: str, params: Dict
) -> (LoadState, str):
    try:
        response = session.get(load_url, params=params)
        response.raise_for_status()
        content = json.loads(response.content.decode("utf-8"))
        if "jobState" not in content:
            raise KeyError(
                "The field 'jobState' is missing from the load progress response content"
            )
        state = content["jobState"]
        if "FAILED" in state:
            return LoadState.FAILED, content
        return LoadState(state), content
    except Exception as e:
        raise LoadException(
            f"Error when getting load job progress for {load_url}: error {e}"
        ) from e
