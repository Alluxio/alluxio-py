# -*- coding: utf-8 -*-
"""Alluxio exceptions.

All exceptions thrown by :class:`alluxio.Client`, :class:`alluxio.Reader`, and
:class:`alluxio.Writer` are one of the following exceptions:

- :class:`AlluxioError`
- subclasses of :class:`AlluxioError`
- :class:`HTTPError`
- Python built-in exceptions

Exceptions raised by the requests library are wrapped by :class:`HTTPError`.
"""


class Status(object):  # pylint: disable=too-few-public-methods
    """A class representing RPC status codes.

    The definitions are from
    https://github.com/Alluxio/alluxio/blob/master/core/common/src/main/java/alluxio/exception/status/Status.java.
    """

    # OK is returned on success.
    OK = 'OK'

    # Canceled indicates the operation was cancelled (typically by the caller).
    CANCELED = 'CANCELED'

    # Unknown error.  An example of where this error may be returned is
    # if a Status value received from another address space belongs to
    # an error-space that is not known in this address space.  Also
    # errors raised by APIs that do not return enough error information
    # may be converted to this error.
    UNKNOWN = 'UNKNOWN'

    # InvalidArgument indicates client specified an invalid argument.
    # Note that this differs from FailedPrecondition. It indicates arguments
    # that are problematic regardless of the state of the system
    # (e.g., a malformed file name).
    INVALID_ARGUMENT = 'INVALID_ARGUMENT'

    # DeadlineExceeded means operation expired before completion.
    # For operations that change the state of the system, this error may be
    # returned even if the operation has completed successfully. For
    # example, a successful response from a server could have been delayed
    # long enough for the deadline to expire.
    DEADLINE_EXCEEDED = 'DEADLINE_EXCEEDED'

    # NotFound means some requested entity (e.g., file or directory) was
    # not found.
    NOT_FOUND = 'NOT_FOUND'

    # AlreadyExists means an attempt to create an entity failed because one
    # already exists.
    ALREADY_EXISTS = 'ALREADY_EXISTS'

    # PermissionDenied indicates the caller does not have permission to
    # execute the specified operation. It must not be used for rejections
    # caused by exhausting some resource (use ResourceExhausted instead for those errors).
    # It must not be used if the caller cannot be identified
    # (use Unauthenticated instead for those errors).
    PERMISSION_DENIED = 'PERMISSION_DENIED'

    # Unauthenticated indicates the request does not have valid
    # authentication credentials for the operation.
    UNAUTHENTICATED = 'UNAUTHENTICATED'

    # ResourceExhausted indicates some resource has been exhausted, perhaps
    # a per-user quota, or perhaps the entire file system is out of space.
    RESOURCE_EXHAUSTED = 'RESOURCE_EXHAUSTED'

    # FailedPrecondition indicates operation was rejected because the
    # system is not in a state required for the operation's execution.
    # For example, directory to be deleted may be non-empty, an rmdir
    # operation is applied to a non-directory, etc.
    #
    # A litmus test that may help a service implementor in deciding
    # between FailedPrecondition, Aborted, and Unavailable:
    #  (a) Use Unavailable if the client can retry the failed call.
    #  (b) Use Aborted if the client should retry at a higher-level
    #      (e.g., restarting a read-modify-write sequence).
    #  (c) Use FailedPrecondition if the client should not retry until
    #      the system state has been explicitly fixed.  E.g., if an "rmdir"
    #      fails because the directory is non-empty, FailedPrecondition
    #      should be returned since the client should not retry unless
    #      they have first fixed up the directory by deleting files from it.
    #  (d) Use FailedPrecondition if the client performs conditional
    #      REST Get/Update/Delete on a resource and the resource on the
    #      server does not match the condition. E.g., conflicting
    #      read-modify-write on the same resource.
    FAILED_PRECONDITION = 'FAILED_PRECONDITION'

    # Aborted indicates the operation was aborted, typically due to a
    # concurrency issue like sequencer check failures, transaction aborts,
    # etc.
    #
    # See litmus test above for deciding between FailedPrecondition,
    # Aborted, and Unavailable.
    ABORTED = 'ABORTED'

    # OutOfRange means operation was attempted past the valid range.
    # E.g., seeking or reading past end of file.
    #
    # Unlike InvalidArgument, this error indicates a problem that may
    # be fixed if the system state changes. For example, a 32-bit file
    # system will generate InvalidArgument if asked to read at an
    # offset that is not in the range [0,2^32-1], but it will generate
    # OutOfRange if asked to read from an offset past the current
    # file size.
    #
    # There is a fair bit of overlap between FailedPrecondition and
    # OutOfRange.  We recommend using OutOfRange (the more specific error)
    # when it applies so that callers who are iterating through
    # a space can easily look for an OutOfRange error to detect when
    # they are done.
    OUT_OF_RANGE = 'OUT_OF_RANGE'

    # Unimplemented indicates operation is not implemented or not
    # supported/enabled in this service.
    UNIMPLEMENTED = 'UNIMPLEMENTED'

    # Internal errors.  Means some invariants expected by underlying
    # system has been broken.  If you see one of these errors,
    # something is very broken.
    INTERNAL = 'INTERNAL'

    # Unavailable indicates the service is currently unavailable.
    # This is a most likely a transient condition and may be corrected
    # by retrying with a backoff.
    #
    # See litmus test above for deciding between FailedPrecondition,
    # Aborted, and Unavailable.
    UNAVAILABLE = 'UNAVAILABLE'

    # DataLoss indicates unrecoverable data loss or corruption.
    DATA_LOSS = 'DATA_LOSS'


class AlluxioError(Exception):
    """Base class for all Alluxio exceptions.

    Args:
        status (str): The status defined in :class:`Status`.
        message (str): The error message.
    """

    def __init__(self, status, message):
        super(AlluxioError, self).__init__(message)
        self.status = status
        self.message = message

    def __str__(self):
        return 'Alluxio exception: status = "{}", message = "{}"'.format(self.status, self.message)


class AbortedError(AlluxioError):
    """Exception indicating that the operation was aborted, typically due to a concurrency issue
    like sequencer check failures, transaction aborts, etc.

    See litmus test in :class:`FailedPreconditionException` for deciding between
    :class:`FailedPreconditionException`, :class:`AbortedException`,
    and :class:`UnavailableException`.

    Args:
        message (str): The error message.
    """

    def __init__(self, message):
        super(AbortedError, self).__init__(Status.ABORTED, message)


class AlreadyExistsError(AlluxioError):
    """Exception indicating that an attempt to create an entity failed because one already exists.

    Args:
        message (str): The error message.
    """

    def __init__(self, message):
        super(AlreadyExistsError, self).__init__(Status.ALREADY_EXISTS, message)


class CanceledError(AlluxioError):
    """Exception indicating that an operation was cancelled (typically by the caller).

    Args:
        message (str): The error message.
    """

    def __init__(self, message):
        super(CanceledError, self).__init__(Status.CANCELED, message)


class DataLossError(AlluxioError):
    """Exception indicating unrecoverable data loss or corruption.

    Args:
        message (str): The error message.
    """

    def __init__(self, message):
        super(DataLossError, self).__init__(Status.DATA_LOSS, message)


class DeadlineExceededError(AlluxioError):
    """Exception indicating that an operation expired before completion. For operations that change
    the state of the system, this exception may be thrown even if the operation has completed
    successfully. For example, a successful response from a server could have been delayed long
    enough for the deadline to expire.

    Args:
        message (str): The error message.
    """

    def __init__(self, message):
        super(DeadlineExceededError, self).__init__(Status.DEADLINE_EXCEEDED, message)


class FailedPreconditionError(AlluxioError):
    """Exception indicating that operation was rejected because the system is not in a state
    required for the operation's execution. For example, directory to be deleted may be non-empty,
    an rmdir operation is applied to a non-directory, etc.

    A litmus test that may help a service implementor in deciding between
    :class:`FailedPreconditionException`, :class:`AbortedException`, and
    :class:`UnavailableException`:

        (a) Use UnavailableException if the client can retry the failed call.
        (b) Use AbortedException if the client should retry at a higher-level (e.g., restarting a
            read-modify-write sequence).
        (c) Use FailedPreconditionException if the client should not retry until the system state
            has been explicitly fixed. E.g., if an "rmdir" fails because the directory is non-empty,
            FailedPreconditionException should be thrown since the client should not retry unless
            they have first fixed up the directory by deleting files from it.
        (d) Use FailedPreconditionException if the client performs conditional REST
            Get/Update/Delete on a resource and the resource on the server does not match the
            condition. E.g. conflicting read-modify-write on the same resource.

    Args:
        message (str): The error message.
    """

    def __init__(self, message):
        super(FailedPreconditionError, self).__init__(Status.FAILED_PRECONDITION, message)


class InternalError(AlluxioError):
    """Exception representing an internal error. This means some invariant expected by the
    underlying system has been broken. If you see one of these errors, something is very broken.

    Args:
        message (str): The error message.
    """

    def __init__(self, message):
        super(InternalError, self).__init__(Status.INTERNAL, message)


class InvalidArgumentError(AlluxioError):
    """Exception indicating that a client specified an invalid argument. Note that this differs from
    :class:`FailedPreconditionException`. It indicates arguments that are problematic regardless of
    the state of the system (e.g., a malformed file name).

    Args:
        message (str): The error message.
    """

    def __init__(self, message):
        super(InvalidArgumentError, self).__init__(Status.INVALID_ARGUMENT, message)


class NotFoundError(AlluxioError):
    """Exception indicating that some requested entity (e.g., file or directory) was not found.

    Args:
        message (str): The error message.
    """

    def __init__(self, message):
        super(NotFoundError, self).__init__(Status.NOT_FOUND, message)


class OutOfRangeError(AlluxioError):
    """Exception indicating that and operation was attempted past the valid range. E.g., seeking or
    reading past end of file.

    Unlike :class:`InvalidArgumentException`, this error indicates a problem that may be fixed if
    the system state changes. For example, a 32-bit file system will generate
    :class:`InvalidArgumentException` if asked to read at an offset that is not in the range
    [0,2^32-1], but it will generate :class:`OutOfRangeException` if asked to read from an offset
    past the current file size.

    There is a fair bit of overlap between :class:`FailedPreconditionException` and
    :class:`OutOfRangeException`.
    We recommend using :class:`OutOfRangeException` (the more specific error) when it applies so
    that callers who are iterating through a space can easily look for an
    :class:`OutOfRangeException` to detect when they are done.

    Args:
        message (str): The error message.
    """

    def __init__(self, message):
        super(OutOfRangeError, self).__init__(Status.OUT_OF_RANGE, message)


class PermissionDeniedError(AlluxioError):
    """Exception indicating that the caller does not have permission to execute the specified operation.
    It must not be used for rejections caused by exhausting some resource (use
    :class:`ResourceExhaustedException` instead for those exceptions).
    It must not be used if the caller cannot be identified
    (use :class:`UnauthenticatedException` instead for those exceptions).

    Args:
        message (str): The error message.
    """

    def __init__(self, message):
        super(PermissionDeniedError, self).__init__(Status.PERMISSION_DENIED, message)


class ResourceExhaustedError(AlluxioError):
    """Exception indicating that some resource has been exhausted, perhaps a per-user quota, or
    perhaps the entire file system is out of space.

    Args:
        message (str): The error message.
    """

    def __init__(self, message):
        super(ResourceExhaustedError, self).__init__(Status.RESOURCE_EXHAUSTED, message)


class UnauthenticatedError(AlluxioError):
    """Exception indicating that the request does not have valid authentication credentials for the
    operation.

    Args:
        message (str): The error message.
    """

    def __init__(self, message):
        super(UnauthenticatedError, self).__init__(Status.UNAUTHENTICATED, message)


class UnavailableError(AlluxioError):
    """Exception indicating that the service is currently unavailable.

    This is a most likely a transient condition and may be corrected by retrying with a backoff.

    See litmus test in :class:`FailedPreconditionException` for deciding between
    :class:`FailedPreconditionException`, :class:`AbortedException`, and
    :class:`UnavailableException`.

    Args:
        message (str): The error message.
    """

    def __init__(self, message):
        super(UnavailableError, self).__init__(Status.UNAVAILABLE, message)


class UnimplementedError(AlluxioError):
    """Exception indicating that an operation is not implemented, or not supported, or not enabled.

    Args:
        message (str): The error message.
    """

    def __init__(self, message):
        super(UnimplementedError, self).__init__(Status.UNIMPLEMENTED, message)


class UnknownError(AlluxioError):
    """Exception representing an unknown error. An example of where this exception may be thrown is
    if a Status value received from another address space belongs to an error-space that is not
    known in this address space. Also errors raised by APIs that do not return enough error
    information may be converted to this error.

    Args:
        message (str): The error message.
    """

    def __init__(self, message):
        super(UnknownError, self).__init__(Status.UNKNOWN, message)


_STATUS_TO_ERROR = {
    Status.CANCELED: CanceledError,
    Status.UNKNOWN: UnknownError,
    Status.INVALID_ARGUMENT: InvalidArgumentError,
    Status.DEADLINE_EXCEEDED: DeadlineExceededError,
    Status.NOT_FOUND: NotFoundError,
    Status.ALREADY_EXISTS: AlreadyExistsError,
    Status.PERMISSION_DENIED: PermissionDeniedError,
    Status.UNAUTHENTICATED: UnauthenticatedError,
    Status.RESOURCE_EXHAUSTED: ResourceExhaustedError,
    Status.FAILED_PRECONDITION: FailedPreconditionError,
    Status.ABORTED: AbortedError,
    Status.OUT_OF_RANGE: OutOfRangeError,
    Status.UNIMPLEMENTED: UnimplementedError,
    Status.INTERNAL: InternalError,
    Status.UNAVAILABLE: UnavailableError,
    Status.DATA_LOSS: DataLossError
}


def new_alluxio_exception(status, message):
    """Creates the appropriate exception for status.

    If status is not defined in :class:`Status`, then creates a general :class:`AlluxioError`.

    Args:
        status (str): The status defined in :class:`Status`.
        message (str): The error message.
    """

    exception_class = _STATUS_TO_ERROR.get(status)
    if exception_class is None:
        return AlluxioError(status, message)
    return exception_class(message)


class HTTPError(Exception):
    """Any error raised by the underlying HTTP client library will be wrapped by this error."""
