# -*- coding: utf-8 -*-

from contextlib import contextmanager

import requests

from . import option
from . import wire


class Client(object):
    def __init__(self, host, port, timeout=1800):
        self.host = host
        self.port = port
        self.timeout = timeout

    def url(self, endpoint):
        return 'http://%s:%s/api/v1/%s' % (self.host, self.port, endpoint)

    def paths_url(self, path, action):
        return self.url('paths/%s/%s' % (path, action))
    
    def streams_url(self, file_id, action):
        return self.url('streams/%s/%s' % (file_id, action))

    def _check_response(self, r):
        if r.status_code != requests.codes.ok:
            err_msg = 'Response status: %s (%d):\nResponse body:\n%s' % \
                (r.reason, r.status_code, r.content)
            raise requests.HTTPError(err_msg, response=r)
        return

    def post(self, url, opt=None, params=None) :
        if opt is not None:
            r = requests.post(url, params=params, json=opt.json())
        else:
            r = requests.post(url, params=params)
        self._check_response(r)
        return r

    def create_directory(self, path, opt=None):
        url = self.paths_url(path, 'create-directory')
        self.post(url, opt)
        
    def delete(self, path, opt=None):
        url = self.paths_url(path, 'delete')
        self.post(url, opt)

    def exists(self, path, opt=None):
        url = self.paths_url(path, 'exists')
        return self.post(url, opt).json()

    def free(self, path, opt=None):
        url = self.paths_url(path, 'free')
        self.post(url, opt)

    def get_status(self, path, opt=None):
        url = self.paths_url(path, 'get-status')
        info = self.post(url, opt).json()
        return wire.FileInfo.from_json(info)

    def list_status(self, path, opt=None):
        url = self.paths_url(path, 'list-status')
        result = self.post(url, opt).json()
        file_infos = [wire.FileInfo.from_json(info) for info in result]
        file_infos.sort()
        return file_infos

    def mount(self, path, src, opt=None):
        url = self.paths_url(path, 'mount')
        self.post(url, opt, {'src': src})

    def unmount(self, path, opt=None):
        url = self.paths_url(path, 'unmount')

    def rename(self, path, dst, opt=None):
        url = self.paths_url(path, 'rename')
        self.post(url, opt, {'dst': dst})

    def set_attribute(self, path, opt=None):
        url = self.paths_url(path, 'set-attribute')
        self.post(url, opt)

    def open_file(self, path, opt=None):
        url = self.paths_url(path, 'open-file')
        file_id = self.post(url, opt).json()
        return file_id

    def create_file(self, path, opt=None):
        url = self.paths_url(path, 'create-file')
        file_id = self.post(url, opt).json()
        return file_id

    def close(self, file_id):
        url = self.streams_url(file_id, 'close')
        self.post(url)

    def read(self, file_id):
        url = self.streams_url(file_id, 'read')
        return Reader(url)

    def write(self, file_id):
        url = self.streams_url(file_id, 'write')
        return Writer(url)

    @contextmanager
    def open(self, path, mode, opt=None):
        if mode == 'r':
            file_id = self.open_file(path, opt)
            try:
                reader = self.read(file_id)
                yield reader
            finally:
                reader and reader.close()
                self.close(file_id)
        elif mode == 'w':
            file_id = self.create_file(path, opt)
            try:
                writer = self.write(file_id)
                yield writer
            finally:
                writer and writer.close()
                self.close(file_id)
        else:
            raise ValueError("mode can only be 'w' or 'r'")


class Reader(object):
    def __init__(self, url):
        self.r = requests.post(url, stream=True)

    def read(self, n=None):
        if n is None:
            return ''.join([chunk for chunk in self.r.iter_content(chunk_size=128)])
        return self.r.raw.read(n)

    def close(self):
        self.r and self.r.close()


class Writer(object):
    def __init__(self, url):
        self.url = url
        self.r = None

    def write(self, data):
        self.r = requests.post(self.url, data=data, stream=True)
        bytes_written = self.r.json()
        return bytes_written

    def close(self):
        self.r and self.r.close()
