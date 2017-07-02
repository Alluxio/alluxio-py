# -*- coding: utf-8 -*-
"""Alluxio client, reader, and writer.

This module contains the Alluxio Client. The Reader and Writer are returned
by certain I/O methods of the Client, they are not intended to be created by
the API user.

The Client is based on `Alluxio proxy server's REST API`_, all HTTP requests
are handled by the `requests`_ library.

.. _Alluxio proxy server's REST API:
    http://www.alluxio.org/restdoc/master/proxy/index.html
.. _requests:
    http://requests.readthedocs.io/en/master/api/
"""

from contextlib import contextmanager

import requests

from . import option
from . import wire


_API_PREFIX = "/api/v1"
_PATHS_PREFIX = "paths"
_STREAMS_PREFIX = "streams"


def _paths_url_path(path, action):
    return '%s/%s/%s/%s' % (_API_PREFIX, _PATHS_PREFIX, path, action)


def _streams_url_path(file_id, action):
    return '%s/%s/%d/%s' % (_API_PREFIX, _STREAMS_PREFIX, file_id, action)


def _check_response(r):
    """Check the response of the REST API request.

    Args:
        r (:class:`requests.Response`): The response of the REST API request.

    Raises:
        requests.HTTPError: If the response status is not 200.
    """

    if r.status_code != requests.codes.ok:
        err_msg = 'Response status: %s (%d):\nResponse body:\n%s' % \
            (r.reason, r.status_code, r.content)
        raise requests.HTTPError(err_msg, response=r)


class Client(object):
    """Alluxio client.

    Args:
        host (str): Alluxio proxy server's hostname.
        port (int): Alluxio proxy server's web port.
        timeout (int, optional): Seconds to wait for the REST server to respond
            before giving up. Defaults to 1800.
    """

    def __init__(self, host, port, timeout=1800):
        self.session = requests.Session()
        self.host = host
        self.port = port
        self.timeout = timeout

    def _url(self, url_path):
        """Create the REST API URL.

        Args:
            url_path (str): The URL path.

        Returns:
            The REST API URL.
        """

        return 'http://%s:%s%s' % (self.host, self.port, url_path)

    def _paths_url(self, path, action):
        """Create the URL for REST APIs under the 'paths' prefix.

        Args:
            path (str): The Alluxio filesystem path where the action should be taken on.
            action (str): The action to be taken on the path.

        Returns:
            The REST API URL.
        """

        return self._url(_paths_url_path(path, action))

    def _streams_url(self, file_id, action):
        """Create the URL for REST APIs under the 'streams' prefix.

        Args:
            fild_id (int): The file ID where the action should be taken on.
            action (str): The action to be taken on the file ID.

        Returns:
            The REST API URL.
        """

        return self._url(_streams_url_path(file_id, action))

    def _post(self, url, opt=None, params=None):
        """Send a POST request to the REST API server.

        Args:
            url (str): The REST API URL.
            opt (optional): Option to be marshaled into json and sent as the
                body of the POST request. It should be one of the options in
                alluxio.options which can be marshaled into json through opt.json().
            params (dict, optional): The parameters to be encoded to the query
                parameters in the REST API URL.

        Returns:
            requests.Response: The response of the REST API request.

        Raises:
            requests.HTTPError: If the request fails or the response status is not 200.
        """

        if opt is not None:
            r = self.session.post(url, params=params, json=opt.json(), timeout=self.timeout)
        else:
            r = self.session.post(url, params=params, timeout=self.timeout)
        _check_response(r)
        return r

    def __repr__(self):
        return 'alluxio.Client(host=%s, port=%d, timeout=%d)' % (self.host, self.port, self.timeout)

    def create_directory(self, path, **kwargs):
        """Create a directory in Alluxio.

        By default, the create directory operation enforces that the parent of
        the given path must exist and the path itself does not already exist.
        The directory will be created with access mode bits 'drwxr-xr-x'.
        The created directory will only exist in Alluxio and not in any of its
        under storages. You can change the behavior by setting optional
        parameters in kwargs.

        Args:
            path (str): The path of the directory to be created.
            **kwargs: The same optional parameters for creating :class:`alluxio.option.CreateDirectory`.

        Raises:
            requests.HTTPError: If the request fails or the response status is not 200.

        Examples:
            Create a directory recursively:

            >>> create_directory('/parent/child/', recursive=True)

            Create a directory recursively and persist to under storage:

            >>> create_directory('/parent/child/', recursive=True, write_type=wire.WRITE_TYPE_CACHE_THROUGH)
        """

        url = self._paths_url(path, 'create-directory')
        self._post(url, option.CreateDirectory(**kwargs))

    def delete(self, path, **kwargs):
        """Delete a directory or file in Alluxio.

        By default, if path is a directory which contains files or directories,
        this method will fail. You can change the behavior by setting optional
        parameters in kwargs.

        Args:
            path (str): The path of the directory or file to be deleted.
            **kwargs: The same optional parameters for creating :class:`alluxio.option.Delete`.

        Raises:
            requests.HTTPError: If the request fails or the response status is not 200.
        """

        url = self._paths_url(path, 'delete')
        self._post(url, option.Delete(**kwargs))

    def exists(self, path, **kwargs):
        """Check whether a path exists in Alluxio.

        If a path exists in Alluxio's under storage, but not in Alluxio, this
        will return False.

        Args:
            path (str): The Alluxio path.
            **kwargs: The same optional parameters for creating :class:`alluxio.option.Exists`.

        Returns:
            bool: True if the path exists, False otherwise.

        Raises:
            requests.HTTPError: If the request fails or the response status is not 200.
        """

        url = self._paths_url(path, 'exists')
        return self._post(url, option.Exists(**kwargs)).json()

    def free(self, path, **kwargs):
        """Free a file or directory from Alluxio.

        By default, if the given path is a directory, its files and contained
        directories won't be freed recursively. You can change the behavior by
        setting optional parameters in kwargs.

        Args:
            path (str): The Alluxio path.
            **kwargs: The same optional parameters for creating :class:`alluxio.option.Free`.

        Raises:
            requests.HTTPError: If the request fails or the response status is not 200.
        """

        url = self._paths_url(path, 'free')
        self._post(url, option.Free(**kwargs))

    def get_status(self, path, **kwargs):
        """Get the status of a file or directory at the given path.

        Args:
            path (str): The Alluxio path.
            **kwargs: The same optional parameters for creating :class:`alluxio.option.GetStatus`.

        Returns:
            alluxio.wire.FileInfo: The information of the file or directory.

        Raises:
            requests.HTTPError: If the request fails or the response status is not 200.
        """

        url = self._paths_url(path, 'get-status')
        info = self._post(url, option.GetStatus(**kwargs)).json()
        return wire.FileInfo.from_json(info)

    def list_status(self, path, **kwargs):
        """List the status of a file or directory at the given path.

        The default pattern for loading metadata from under storage when
        listing status under a directory can be changed by setting optional
        parameters in kwargs.

        Args:
            path (str): The Alluxio path, which should be a directory.
            **kwargs: The same optional parameters for creating :class:`alluxio.option.ListStatus`.

        Returns:
            List of :class:`alluxio.wire.FileInfo`: List of information of
            files and direcotries under path.

        Raises:
            requests.HTTPError: If the request fails or the response status is not 200.
        """

        url = self._paths_url(path, 'list-status')
        result = self._post(url, option.ListStatus(**kwargs)).json()
        file_infos = [wire.FileInfo.from_json(info) for info in result]
        file_infos.sort()
        return file_infos

    def ls(self, path, **kwargs):
        """List the names of the files and directories under path.

        To get more information of the files and directories under path, call
        :meth:`.list_status`.

        Args:
            path (str): The Alluxio path, which should be a directory.
            **kwargs: The same optional parameters for creating :class:`alluxio.option.ListStatus`.

        Returns:
            List of str: A list of names of the files and directories under path.

        Raises:
            requests.HTTPError: If the request fails or the response status is not 200.
        """

        return [status.name for status in self.list_status(path, **kwargs)]

    def mount(self, path, src, **kwargs):
        """Mount an under storage specified by src to path in Alluxio.

        Additional information or configuration, such as AWS credentials for
        mounting a S3 bucket or mounting the under storage in read only mode,
        can be provided by setting optional parameters in kwargs.

        Args:
            path (str): The Alluxio path to be mounted to.
            src (str): The under storage endpoint to mount.
            **kwargs: The same optional parameters for creating :class:`alluxio.option.Mount`.

        Raises:
            requests.HTTPError: If the request fails or the response status is not 200.
        """

        url = self._paths_url(path, 'mount')
        self._post(url, option.Mount(**kwargs), {'src': src})

    def unmount(self, path, **kwargs):
        """Unmount an under storage that is mounted at path.

        Args:
            path (str): The Alluxio mount point.
            **kwargs: The same optional parameters for creating :class:`alluxio.option.Unmount`.

        Raises:
            requests.HTTPError: If the request fails or the response status is not 200.
        """

        url = self._paths_url(path, 'unmount')
        self._post(url, option.Unmount(**kwargs))

    def rename(self, path, dst, **kwargs):
        """Rename path to dst in Alluxio.

        Args:
            path (str): The Alluxio path to be renamed.
            dst (str): The Alluxio path to be renamed to.
            **kwargs: The same optional parameters for creating :class:`alluxio.option.Rename`.

        Raises:
            requests.HTTPError: If the request fails or the response status is not 200.
        """

        url = self._paths_url(path, 'rename')
        self._post(url, option.Rename(**kwargs), {'dst': dst})

    def set_attribute(self, path, **kwargs):
        """Set attributes of a path in Alluxio.

        Args:
            path (str): The Alluxio path.
            **kwargs: The same optional parameters for creating :class:`alluxio.option.SetAttribute`.

        Raises:
            requests.HTTPError: If the request fails or the response status is not 200.
        """

        url = self._paths_url(path, 'set-attribute')
        self._post(url, option.SetAttribute(**kwargs))

    def open_file(self, path, **kwargs):
        """Open a file in Alluxio for reading.

        The file must be closed by calling :meth:`alluxio.Client.close`.

        The preferred way to read a file is to use :meth:`.open`.

        Args:
            path (str): The Alluxio path.
            **kwargs: The same optional parameters for creating :class:`alluxio.option.OpenFile`.

        Returns:
            int: The file ID, which can be passed to :meth:`alluxio.Client.read` and :meth:`alluxio.Client.close`.

        Raises:
            requests.HTTPError: If the request fails or the response status is not 200.

        Examples:
            Open a file, read its contents, and close it:

            >>> file_id = open_file('/file')
            >>> reader = read(file_id)
            >>> reader.read()
            >>> reader.close()
            >>> close(file_id)
        """

        url = self._paths_url(path, 'open-file')
        return self._post(url, option.OpenFile(**kwargs)).json()

    def create_file(self, path, **kwargs):
        """Create a file in Alluxio.

        The file must not already exist and must be closed by calling
        :meth:`alluxio.Client.close`.

        A preferred way to write to a file is to use :meth:`.open`,
        see its documentation for details.

        Args:
            path (str): The Alluxio path.
            **kwargs: The same optional parameters for creating :class:`alluxio.option.CreateFile`.

        Returns:
            int: The file ID, which can be passed to :meth:`alluxio.Client.write` and :meth:`alluxio.Client.close`.

        Raises:
            requests.HTTPError: If the request fails or the response status is not 200.

        Examples:
            Create a file and write a string to it both in Alluxio and the under storage,
            finally close it:

            >>> file_id = create_file('/file', write_type=wire.WRITE_TYPE_CACHE_THROUGH)
            >>> writer = write(file_id)
            >>> writer.write('data')
            >>> writer.close()
            >>> close(file_id)
        """

        url = self._paths_url(path, 'create-file')
        return self._post(url, option.CreateFile(**kwargs)).json()

    def close(self, file_id):
        """Close a file.

        When calling :meth:`.open` using a with statement, this method is
        automatically invoked when exiting the with block.

        Args:
            file_id (int): The file ID returned by :meth:`.open_file` or :meth:`.create_file`.

        Raises:
            requests.HTTPError: If the request fails or the response status is not 200.
        """

        url = self._streams_url(file_id, 'close')
        self._post(url)

    def read(self, file_id):
        """Creates a :class:`Reader` for reading a file.

        Args:
            file_id (int): The file ID returned by :meth:`.open_file`.

        Returns:
            Reader: The reader for reading the file as a stream.

        Raises:
            requests.HTTPError: If the request fails or the response status is not 200.
        """

        url = self._streams_url(file_id, 'read')
        return Reader(self.session, url)

    def write(self, file_id):
        """Creates a :class:`Writer` for writing a file.

        Args:
            file_id (int): The file ID returned by :meth:`.create_file`.

        Returns:
            Reader: The reader for reading the file as a stream.

        Raises:
            requests.HTTPError: If the request fails or the response status is not 200.
        """

        url = self._streams_url(file_id, 'write')
        return Writer(self.session, url)

    @contextmanager
    def open(self, path, mode, **kwargs):
        """Open a file for reading or writing.

        It should be called using a with statement so that the reader or writer
        will be automatically closed.

        Args:
            path (str): The Alluxio file to be read from or written to.
            mode (str): Either 'r' for reading or 'w' for writing.
            **kwargs: For reading, it is the same optional parameters for
                creating :class:`alluxio.option.OpenFile`, for writing, it is
                the same optional parameters for creating
                :class:`alluxio.option.CreateFile`.

        Raises:
            ValueError: If mode is neither 'w' nor 'r'.
            requests.HTTPError: If the request fails or the response status is not 200.

        Examples:
            Write a string to a file in Alluxio:

            >>> with open('/file', 'w') as f:
            >>>     f.write('data')

            Copy a file in local filesystem to a file in Alluxio and also persist
            it into Alluxio's under storage, note that the second :func"`open`
            is python's built-in function:

            >>> with alluxio_client.open('/alluxio-file', 'w', write_type=wire.WRITE_TYPE_CACHE_THROUGH) as alluxio_file:
            >>>     with open('/local-file', 'rb') as local_file:
            >>>         alluxio_file.write(local_file)

            Read the first 10 bytes of a file from Alluxio:

            >>> with open('/file', 'r') as f:
            >>>     print f.read(10)
        """

        if mode == 'r':
            file_id = self.open_file(path, **kwargs)
            try:
                reader = self.read(file_id)
                yield reader
            finally:
                reader and reader.close()
                self.close(file_id)
        elif mode == 'w':
            file_id = self.create_file(path, **kwargs)
            try:
                writer = self.write(file_id)
                yield writer
            finally:
                writer and writer.close()
                self.close(file_id)
        else:
            raise ValueError("mode can only be 'w' or 'r'")


class Reader(object):
    """ Alluxio file reader.

    The file is read as a stream; you cannot seek to a previously read section.
    :meth:`alluxio.Reader.close` must be called after the reading
    is done.

    This class is used by :meth:`.Client.open`, it is not intended to be created
    by users directly.

    Args:
        session (:class:`requests.Session`) The requests session.
        url (str): The Alluxio REST URL for reading a file.
    """

    def __init__(self, session, url):
        self.session = session
        self.url = url
        self.r = None

    def read(self, n=None, chunk_size=None):
        """Read the file stream.

        Args:
            n (int, optional): The bytes to read from the stream, if n is None,
                it means read the whole data stream chunk by chunk, the chunk
                size is set in chunk_size. Defaults to None.
            chunk_size (int, optional): Size of the chunk to be read into memory.
                A value of None means reading data as it arrives in whatever
                size the chunks are received. Defaults to None.

        Returns:
            The data in bytes, if all data has been read, returns an empty string.

        Raises:
            requests.HTTPError: If the request fails or the response status is not 200.
        """

        if self.r is None:
            self.r = self.session.post(self.url, stream=True)
            _check_response(self.r)
        if n is None:
            return ''.join([chunk for chunk in self.r.iter_content(chunk_size=chunk_size)])
        return self.r.raw.read(n)

    def close(self):
        """Close the reader.

        If the request fails, this is a no-op. Otherwise, the connection is
        released back into the pool. Once this method has been called,
        :meth:`.read` should not be called again.
        """

        self.r and self.r.close()


class Writer(object):
    """ Alluxio file writer.

    A string or a file-like object can be written as a stream to an Alluxio file.
    :meth:`alluxio.Writer.close` must be called after the writing is done.

    This class is used by :meth:`.Client.open`, it is not intended to be created
    by users directly.

    Args:
        session (:class:`requests.Session`) The requests session.
        url (str): The Alluxio REST URL for writing a file.
    """

    def __init__(self, session, url):
        self.session = session
        self.url = url
        self.r = None

    def write(self, data):
        """Write data as a stream to the file.

        The consequent calls to write will append data to the file.

        Args:
            data: data is either a string or a file-like object in python.

        Returns:
            The number of bytes that have been written.

        Raises:
            requests.HTTPError: If the request fails or the response status is not 200.
        """

        self.r = self.session.post(self.url, data=data, stream=True)
        _check_response(self.r)
        bytes_written = self.r.json()
        return bytes_written

    def close(self):
        """Close the writer.

        If the request fails, this is a nop, otherwise, release the connection
        back to the pool. Once this method has been called, the :meth:`.write`
        should not be called again.
        """

        self.r and self.r.close()
