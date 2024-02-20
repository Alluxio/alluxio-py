from collections import defaultdict

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from alluxio.alluxio_file_system import AlluxioAsyncFileSystem

pytestmark = pytest.mark.asyncio


@pytest.fixture
def server(event_loop):
    async def get_file_handler(request: web.Request) -> web.Response:
        alluxio: dict = request.app["alluxio"]
        bytes = alluxio[request.match_info["path_id"]][
            request.match_info["page_index"]
        ]

        offset = int(request.query.get("offset", 0))
        length = int(request.query.get("length", 0))
        return web.Response(
            status=200,
            body=bytes[offset : offset + length],
        )

    async def put_file_handler(request: web.Request) -> web.Response:
        data = await request.read()
        alluxio: dict = request.app["alluxio"]
        alluxio[request.match_info["path_id"]] = {
            request.match_info["page_index"]: data
        }
        return web.json_response(
            {
                "path_id": request.match_info["path_id"],
            }
        )

    async def startup(app: web.Application):
        app["alluxio"] = defaultdict(dict)

    app = web.Application()
    app.on_startup.append(startup)
    app.router.add_get(
        "/v1/file/{path_id}/page/{page_index}", get_file_handler
    )
    app.router.add_post(
        "/v1/file/{path_id}/page/{page_index}", put_file_handler
    )
    server = TestServer(app)
    event_loop.run_until_complete(server.start_server())
    return server


@pytest.mark.asyncio
async def test_read_page(server):
    fs = AlluxioAsyncFileSystem(
        worker_hosts=server.host, http_port=server.port
    )
    assert await fs.write_page("s3://a/a.txt", 0, b"test")
    data = await fs.read_range("s3://a/a.txt", 1, 2)
    assert data == b"es"


@pytest.mark.asyncio
async def test_put_page(server):
    fs = AlluxioAsyncFileSystem(
        worker_hosts=server.host, http_port=server.port
    )
    assert await fs.write_page("s3://a/a.txt", 1, b"test")
