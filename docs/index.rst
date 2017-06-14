Alluxio Python API v\ |version|
===============================


Client Interface
----------------

.. autoclass:: alluxio.Client
   :members:
.. autoclass:: alluxio.client.Reader
   :members:
.. autoclass:: alluxio.client.Writer
   :members:

Client Options
--------------

.. autoclass:: alluxio.option.CreateDirectory
   :members:
.. autoclass:: alluxio.option.CreateFile
   :members:
.. autoclass:: alluxio.option.Delete
   :members:
.. autoclass:: alluxio.option.Exists
   :members:
.. autoclass:: alluxio.option.Free
   :members:
.. autoclass:: alluxio.option.GetStatus
   :members:
.. autoclass:: alluxio.option.ListStatus
   :members:
.. autoclass:: alluxio.option.Mount
   :members:
.. autoclass:: alluxio.option.OpenFile
   :members:
.. autoclass:: alluxio.option.Rename
   :members:
.. autoclass:: alluxio.option.SetAttribute
   :members:
.. autoclass:: alluxio.option.Unmount
   :members:

Client Wire Objects
----------------------------------------------------

Format of objects sent from the REST API server on wire.

.. autoclass:: alluxio.wire.Bits
   :members:

.. autodata:: alluxio.wire.BITS_NONE
.. autodata:: alluxio.wire.BITS_EXECUTE
.. autodata:: alluxio.wire.BITS_WRITE
.. autodata:: alluxio.wire.BITS_WRITE_EXECUTE
.. autodata:: alluxio.wire.BITS_READ
.. autodata:: alluxio.wire.BITS_READ_EXECUTE
.. autodata:: alluxio.wire.BITS_READ_WRITE
.. autodata:: alluxio.wire.BITS_ALL

.. autoclass:: alluxio.wire.BlockInfo
   :members:
