# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.


class AlluxioException(Exception):
    def __init__(self, message):
        self.message = message

    def message(self):
        return self.message


class LoadException(AlluxioException):
    def __init__(self, message):
        super().__init__(message)


class GetFileStatusException(AlluxioException):
    def __init__(self, message):
        super().__init__(message)


class PageReadException(AlluxioException):
    def __init__(self, message):
        super().__init__(message)


class PageWriteException(AlluxioException):
    def __init__(self, message):
        super().__init__(message)


class WorkInfoParseException(AlluxioException):
    def __init__(self, message):
        super().__init__(message)


class NoWorkerEntitiesException(AlluxioException):
    def __init__(self, message):
        super().__init__(message)


class EtcdClientInitializationException(AlluxioException):
    def __init__(self, message):
        super().__init__(message)


class FileOperationException(AlluxioException):
    def __init__(self, message):
        super().__init__(message)
