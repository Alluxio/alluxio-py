try:
    from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
except ImportError:
    from http.server import HTTPServer, BaseHTTPRequestHandler
import socket
from threading import Thread
import json
import random
import urllib

import alluxio

from random_option import *
from random_wire import *
from util import random_str, random_int


def get_free_port():
    s = socket.socket(socket.AF_INET, type=socket.SOCK_STREAM)
    s.bind(('localhost', 0))
    address, port = s.getsockname()
    s.close()
    return port


def setup_client(handler):
    host = 'localhost'
    port = get_free_port()
    print port
    server = HTTPServer((host, port), handler)
    server_thread = Thread(target=server.serve_forever)
    server_thread.setDaemon(True)
    server_thread.start()
    client = alluxio.Client(host, port, timeout=60)
    return client, lambda: server.shutdown


def paths_handler(path, action, params=None, input=None, output=None):
    class _(BaseHTTPRequestHandler):
        def do_POST(self):
            # Assert that URL path is expected.
            expected_path = alluxio.client._paths_url_path(path, action)
            if params is not None:
                expected_path += '?'
                for i, (k, v) in enumerate(params.items()):
                    if i != 0:
                        expected_path += '&'
                    expected_path += '{}={}'.format(k,
                                                    urllib.quote(v, safe=''))
            assert self.path == expected_path

            if input is not None:
                # Assert that request body is expected.
                content_len = int(self.headers.getheader('content-length', 0))
                body = self.rfile.read(content_len)
                assert json.loads(body) == input.json()

            # Respond.
            self.send_response(200)
            if output is not None:
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(output))

    return _


def test_create_directory():
    path = '/foo'
    option = random_create_directory()
    client, cleanup = setup_client(paths_handler(
        path, 'create-directory', input=option))
    client.create_directory(path, **option.__dict__)
    cleanup()


def test_create_file():
    path = '/foo'
    option = random_create_file()
    expected_file_id = 1
    client, cleanup = setup_client(paths_handler(
        path, 'create-file', input=option, output=expected_file_id))
    file_id = client.create_file(path, **option.__dict__)
    cleanup()
    assert file_id == expected_file_id


def test_delete():
    path = '/foo'
    option = random_delete()
    client, cleanup = setup_client(paths_handler(path, 'delete', input=option))
    client.delete(path, **option.__dict__)
    cleanup()


def test_exists():
    path = '/foo'
    expected_output = True
    client, cleanup = setup_client(paths_handler(
        path, 'exists', output=expected_output))
    output = client.exists(path)
    cleanup()
    assert output == expected_output


def test_free():
    path = '/foo'
    option = random_free()
    client, cleanup = setup_client(paths_handler(path, 'free', input=option))
    client.free(path, **option.__dict__)
    cleanup()


def test_get_status():
    path = '/foo'
    expected_output = random_file_info()
    client, cleanup = setup_client(paths_handler(
        path, 'get-status', output=expected_output.json()))
    output = client.get_status(path)
    cleanup()
    assert output == expected_output


def test_list_status():
    path = '/foo'
    option = random_list_status()
    expected_file_infos = []
    for _ in range(random.randint(1, 10)):
        expected_file_infos.append(random_file_info())
    expected_output = [info.json() for info in expected_file_infos]
    expected_names = [info.name for info in expected_file_infos]
    client, cleanup = setup_client(paths_handler(
        path, 'list-status', input=option, output=expected_output))
    infos = client.list_status(path, **option.__dict__)
    names = client.ls(path, **option.__dict__)
    cleanup()
    expected_file_infos.sort()
    assert infos == expected_file_infos
    expected_names.sort()
    assert names == expected_names


def test_mount():
    path = '/foo'
    src = random_str()
    option = random_mount()
    client, cleanup = setup_client(paths_handler(
        path, 'mount', params={'src': src}, input=option))
    client.mount(path, src, **option.__dict__)
    cleanup()


def test_open_file():
    path = '/foo'
    expected_file_id = random_int()
    option = random_open_file()
    client, cleanup = setup_client(paths_handler(
        path, 'open-file', input=option, output=expected_file_id))
    file_id = client.open_file(path, **option.__dict__)
    cleanup()
    assert file_id == expected_file_id


def test_rename():
    src = '/foo'
    dst = '/bar'
    client, cleanup = setup_client(
        paths_handler(src, 'rename', params={'dst': dst}))
    client.rename(src, dst)
    cleanup()


def test_set_attribute():
    option = random_set_attribute()
    path = '/foo'
    client, cleanup = setup_client(
        paths_handler(path, 'set-attribute', input=option))
    client.set_attribute(path, **option.__dict__)
    cleanup()


def test_unmount():
    path = '/foo'
    client, cleanup = setup_client(paths_handler(path, 'unmount'))
    client.unmount(path)
    cleanup()
