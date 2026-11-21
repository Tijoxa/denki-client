import json
import os
from io import BytesIO
from zipfile import ZipFile

import httpx2
import pytest

from denki_client import EntsoeClient
from denki_client.exceptions import ParseError

_REST_KEY = "rest-api-test-key"
_FILE_LIBRARY_TOKEN = "file-library-test-token"
_FILENAME = "2025_01_ActivatedBalancingEnergy_17.1.E_r2.csv"
_FILE_ID = "c707990b-174b-4a40-8943-f07a505baccf"


async def test_list_and_download_activated_balancing_energy_archive():
    extract = b"DateTime\tActivatedBalancingEnergy\n2025-01-01 00:00\t42.5\n"
    archive = BytesIO()
    with ZipFile(archive, "w") as zipped:
        zipped.writestr(_FILENAME, extract)

    def handler(request: httpx2.Request) -> httpx2.Response:
        assert request.method == "POST"
        assert request.url.host == "fms.tp.entsoe.eu"
        assert request.headers["Authorization"] == f"Bearer {_FILE_LIBRARY_TOKEN}"
        assert "securityToken" not in request.url.params
        payload = json.loads(request.content)
        if request.url.path == "/listFolder":
            assert payload == {
                "path": "/TP_Legacy_Publications/ActivatedBalancingEnergy_17.1.E_r2/",
                "sorterList": [{"key": "periodCovered.from", "ascending": True}],
                "pageInfo": {"pageIndex": 0, "pageSize": 5000},
            }
            return httpx2.Response(200, json={"contentItemList": [{"name": _FILENAME, "fileId": _FILE_ID}]})

        assert request.url.path == "/downloadFileContent"
        assert payload == {
            "topLevelFolder": "TP_Legacy_Publications",
            "fileIdList": [_FILE_ID],
            "downloadAsZip": True,
        }
        return httpx2.Response(200, content=archive.getvalue())

    client = EntsoeClient(
        _REST_KEY,
        transport=httpx2.MockTransport(handler),
        auth=("rest-user", _REST_KEY),
        params={"securityToken": _REST_KEY},
    )
    async with client.session:
        files = await client.list_activated_balancing_energy_archives(file_library_token=_FILE_LIBRARY_TOKEN)
        assert files == {_FILENAME: _FILE_ID}
        body = await client.download_activated_balancing_energy_archive(
            files[_FILENAME], file_library_token=_FILE_LIBRARY_TOKEN
        )

    with ZipFile(BytesIO(body)) as downloaded:
        assert downloaded.read(_FILENAME) == extract


async def test_file_library_http_error_omits_the_bearer_token():
    client = EntsoeClient(
        _REST_KEY,
        transport=httpx2.MockTransport(lambda _: httpx2.Response(403, text=_FILE_LIBRARY_TOKEN)),
    )
    async with client.session:
        with pytest.raises(httpx2.HTTPStatusError) as error:
            await client.list_activated_balancing_energy_archives(file_library_token=_FILE_LIBRARY_TOKEN)

    assert isinstance(error.value, httpx2.HTTPStatusError)
    assert error.value.response.status_code == 403
    assert "Authorization" not in error.value.request.headers
    assert _FILE_LIBRARY_TOKEN not in error.value.response.text


async def test_archive_download_rejects_a_success_response_without_a_zip():
    client = EntsoeClient(_REST_KEY, transport=httpx2.MockTransport(lambda _: httpx2.Response(200, json={})))
    async with client.session:
        with pytest.raises(ParseError, match="Invalid ZIP"):
            await client.download_activated_balancing_energy_archive(_FILE_ID, file_library_token=_FILE_LIBRARY_TOKEN)


@pytest.mark.live
async def test_activated_balancing_energy_archive_live():
    token = os.environ.get("ENTSOE_FILE_LIBRARY_TOKEN", "")
    if not token.strip():
        pytest.skip("ENTSOE_FILE_LIBRARY_TOKEN is required for the File Library test")

    client = EntsoeClient("", timeout=60)
    async with client.session:
        files = await client.list_activated_balancing_energy_archives(file_library_token=token)
        assert files
        body = await client.download_activated_balancing_energy_archive(
            next(iter(files.values())), file_library_token=token
        )

    with ZipFile(BytesIO(body)) as archive:
        assert any(not member.is_dir() and member.file_size > 0 for member in archive.infolist())
