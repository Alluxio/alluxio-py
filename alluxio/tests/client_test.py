try:
    from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
except ImportError:
    from http.server import HTTPServer, BaseHTTPRequestHandler
import socket
from threading import Thread
import json
import random
import urllib
try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

import alluxio

from random_option import *
from random_wire import *
from util import random_str, random_int


def get_http_header(request, key):
    try:
        return request.headers.getheader(key, 0)
    except AttributeError:
        return request.headers.get(key, 0)


def get_free_port():
    s = socket.socket(socket.AF_INET, type=socket.SOCK_STREAM)
    s.bind(('localhost', 0))
    address, port = s.getsockname()
    s.close()
    return port


def setup_client(handler):
    host = 'localhost'
    port = get_free_port()
    print(port)
    server = HTTPServer((host, port), handler)
    server_thread = Thread(target=server.serve_forever)
    server_thread.setDaemon(True)
    server_thread.start()
    client = alluxio.Client(host, port, timeout=60)
    return client, lambda: server.shutdown


def handle_paths_request(request, path, action, params=None, input=None, output=None):
    # Assert that URL path is expected.
    expected_path = alluxio.client._paths_url_path(path, action)
    if params is not None:
        expected_path += '?'
        for i, (k, v) in enumerate(params.items()):
            if i != 0:
                expected_path += '&'
            try:
                quoted_v = urllib.quote(v, safe='')
            except AttributeError:
                quoted_v = urllib.parse.quote(v, safe='')
            expected_path += '{}={}'.format(k, quoted_v)
    assert request.path == expected_path

    if input is not None:
        # Assert that request body is expected.
        content_len = int(get_http_header(request, 'content-length'))
        body = request.rfile.read(content_len)
        assert json.loads(body) == input.json()

    # Respond.
    request.send_response(200)
    if output is not None:
        request.send_header('Content-Type', 'application/json')
        request.end_headers()
        request.wfile.write(json.dumps(output).encode())
    else:
        request.end_headers()


def paths_handler(path, action, params=None, input=None, output=None):
    class _(BaseHTTPRequestHandler):
        def do_POST(self):
            handle_paths_request(self, path, action,
                                 params=params, input=input, output=output)

    return _


def test_create_directory():
    path = '/foo'
    option = random_create_directory()
    client, cleanup = setup_client(paths_handler(
        path, 'create-directory', input=option))
    client.create_directory(path, option)
    cleanup()


def test_create_file():
    path = '/foo'
    option = random_create_file()
    expected_file_id = 1
    client, cleanup = setup_client(paths_handler(
        path, 'create-file', input=option, output=expected_file_id))
    file_id = client.create_file(path, option)
    cleanup()
    assert file_id == expected_file_id


def test_delete():
    path = '/foo'
    option = random_delete()
    client, cleanup = setup_client(paths_handler(path, 'delete', input=option))
    client.delete(path, option)
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
    client.free(path, option)
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
    infos = client.list_status(path, option)
    names = client.ls(path, option)
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
    client.mount(path, src, option)
    cleanup()


def test_open_file():
    path = '/foo'
    expected_file_id = random_int()
    option = random_open_file()
    client, cleanup = setup_client(paths_handler(
        path, 'open-file', input=option, output=expected_file_id))
    file_id = client.open_file(path, option)
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
    client.set_attribute(path, option)
    cleanup()


def test_unmount():
    path = '/foo'
    client, cleanup = setup_client(paths_handler(path, 'unmount'))
    client.unmount(path)
    cleanup()


def handle_streams_request(request, file_id, action, input=None, output=None):
    # Assert that URL path is expected.
    expected_path = alluxio.client._streams_url_path(file_id, action)
    assert request.path == expected_path

    content_len = 0
    if input is not None:
        # Assert that request body is expected.
        content_len = int(get_http_header(request, 'content-length'))
        body = request.rfile.read(content_len).decode()
        assert body == input

    # Respond.
    request.send_response(200)
    if output is not None:
        request.send_header('Content-Type', 'application/octet-stream')
        request.end_headers()
        request.wfile.write(output.encode())
    else:
        request.send_header('Content-Type', 'application/json')
        request.end_headers()
        request.wfile.write(json.dumps(content_len).encode())


def streams_handler(file_id, action, input=None, output=None):
    class _(BaseHTTPRequestHandler):
        def do_POST(self):
            handle_streams_request(self, file_id, action,
                                   input=input, output=output)

    return _


def test_close():
    file_id = random_int()
    client, cleanup = setup_client(streams_handler(file_id, 'close'))
    client.close(file_id)
    cleanup()


def test_read():
    file_id = random_int()
    message = random_str()
    client, cleanup = setup_client(
        streams_handler(file_id, 'read', output=message))
    reader = client.read(file_id)
    got = reader.read()
    reader.close()
    assert got.decode() == message


def test_write():
    file_id = random_int()
    message = random_str()
    client, cleanup = setup_client(
        streams_handler(file_id, 'write', input=message))
    writer = client.write(file_id)
    length = writer.write(message)
    writer.close()
    assert length == len(message)


def combined_handler(path, path_action, file_id, stream_action, path_input=None, path_output=None, stream_input=None, stream_output=None):
    class _(BaseHTTPRequestHandler):
        def do_POST(self):
            request_path = urlparse(self.path).path
            paths_path = alluxio.client._paths_url_path(path, path_action)
            streams_path = alluxio.client._streams_url_path(
                file_id, stream_action)
            close_path = alluxio.client._streams_url_path(file_id, 'close')
            if request_path == paths_path:
                handle_paths_request(
                    self, path, path_action, input=path_input, output=path_output)
            elif request_path == streams_path:
                handle_streams_request(
                    self, file_id, stream_action, input=stream_input, output=stream_output)
            elif request_path == close_path:
                self.send_response(200)
                self.end_headers()

    return _


def test_open_read():
    path = '/foo'
    file_id = random_int()
    message = random_str()
    handler = combined_handler(
        path, 'open-file', file_id, 'read', path_output=file_id, stream_output=message)
    client, cleanup = setup_client(handler)
    got = None
    with client.open(path, 'r') as f:
        got = f.read()
    cleanup()
    assert got == message.encode()


def test_open_write():
    path = '/foo'
    file_id = random_int()
    message = random_str()
    handler = combined_handler(path, 'create-file', file_id, 'write',
                               path_output=file_id, stream_input=message)
    client, cleanup = setup_client(handler)
    written_len = None
    with client.open(path, 'w') as f:
        written_len = f.write(message)
    cleanup()
    assert written_len == len(message)
