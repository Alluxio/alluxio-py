Alluxio Python API v\ |version|
===============================


Client Interface
----------------

.. automodule:: alluxio.client

.. autoclass:: alluxio.Client
   :members:
.. autoclass:: alluxio.client.Reader
   :members:
.. autoclass:: alluxio.client.Writer
   :members:

Client Options
--------------

.. automodule:: alluxio.option

.. autoclass:: alluxio.option.CreateDirectory
.. autoclass:: alluxio.option.CreateFile
.. autoclass:: alluxio.option.Delete
.. autoclass:: alluxio.option.Exists
.. autoclass:: alluxio.option.Free
.. autoclass:: alluxio.option.GetStatus
.. autoclass:: alluxio.option.ListStatus
.. autoclass:: alluxio.option.Mount
.. autoclass:: alluxio.option.Unmount
.. autoclass:: alluxio.option.OpenFile
.. autoclass:: alluxio.option.Rename
.. autoclass:: alluxio.option.SetAttribute

Client Wire Objects
-------------------

.. automodule:: alluxio.wire

.. autoclass:: alluxio.wire.Bits
.. autodata:: alluxio.wire.BITS_NONE
.. autodata:: alluxio.wire.BITS_EXECUTE
.. autodata:: alluxio.wire.BITS_WRITE
.. autodata:: alluxio.wire.BITS_WRITE_EXECUTE
.. autodata:: alluxio.wire.BITS_READ
.. autodata:: alluxio.wire.BITS_READ_EXECUTE
.. autodata:: alluxio.wire.BITS_READ_WRITE
.. autodata:: alluxio.wire.BITS_ALL
.. autoclass:: alluxio.wire.BlockInfo
.. autoclass:: alluxio.wire.BlockLocation
.. autoclass:: alluxio.wire.FileBlockInfo
.. autoclass:: alluxio.wire.FileInfo
.. autoclass:: alluxio.wire.LoadMetadataType
.. autodata:: alluxio.wire.LOAD_METADATA_TYPE_NEVER
.. autodata:: alluxio.wire.LOAD_METADATA_TYPE_ONCE
.. autodata:: alluxio.wire.LOAD_METADATA_TYPE_ALWAYS
.. autoclass:: alluxio.wire.Mode
.. autoclass:: alluxio.wire.ReadType
.. autodata:: alluxio.wire.READ_TYPE_NO_CACHE
.. autodata:: alluxio.wire.READ_TYPE_CACHE
.. autodata:: alluxio.wire.READ_TYPE_CACHE_PROMOTE
.. autoclass:: alluxio.wire.TTLAction
.. autodata:: alluxio.wire.TTL_ACTION_DELETE
.. autodata:: alluxio.wire.TTL_ACTION_FREE
.. autoclass:: alluxio.wire.WorkerNetAddress
.. autoclass:: alluxio.wire.WriteType
.. autodata:: alluxio.wire.WRITE_TYPE_MUST_CACHE
.. autodata:: alluxio.wire.WRITE_TYPE_CACHE_THROUGH
.. autodata:: alluxio.wire.WRITE_TYPE_THROUGH
.. autodata:: alluxio.wire.WRITE_TYPE_ASYNC_THROUGH

Client Exceptions
-----------------

.. automodule:: alluxio.exceptions

.. autoclass:: alluxio.exceptions.Status
.. autoexception:: alluxio.exceptions.AlluxioError
.. autoexception:: alluxio.exceptions.AbortedError
.. autoexception:: alluxio.exceptions.AlreadyExistsError
.. autoexception:: alluxio.exceptions.CanceledError
.. autoexception:: alluxio.exceptions.DataLossError
.. autoexception:: alluxio.exceptions.DeadlineExceededError
.. autoexception:: alluxio.exceptions.FailedPreconditionError
.. autoexception:: alluxio.exceptions.InternalError
.. autoexception:: alluxio.exceptions.InvalidArgumentError
.. autoexception:: alluxio.exceptions.NotFoundError
.. autoexception:: alluxio.exceptions.OutOfRangeError
.. autoexception:: alluxio.exceptions.PermissionDeniedError
.. autoexception:: alluxio.exceptions.ResourceExhaustedError
.. autoexception:: alluxio.exceptions.UnauthenticatedError
.. autoexception:: alluxio.exceptions.UnavailableError
.. autoexception:: alluxio.exceptions.UnimplementedError
.. autoexception:: alluxio.exceptions.UnknownError
.. autofunction:: alluxio.exceptions.new_alluxio_exception

.. autoexception:: alluxio.exceptions.HTTPError
