"""Access to the historical A83 extracts in the ENTSO-E File Library."""

from io import BytesIO
from zipfile import is_zipfile

import httpx2

from denki_client.exceptions import ParseError

from ._transport import _Transport

_FILE_LIBRARY_URL = "https://fms.tp.entsoe.eu"
_LEGACY_FOLDER = "TP_Legacy_Publications"
_A83_FOLDER = f"/{_LEGACY_FOLDER}/ActivatedBalancingEnergy_17.1.E_r2/"
_PAGE_SIZE = 5000


class FileLibraryQueries(_Transport):
    async def list_activated_balancing_energy_archives(self, *, file_library_token: str) -> dict[str, str]:
        """List historical A83 extracts as a mapping from filename to File Library file ID."""
        files: dict[str, str] = {}
        page_index = 0
        while True:
            response = await self._file_library_request(
                "listFolder",
                file_library_token,
                {
                    "path": _A83_FOLDER,
                    "sorterList": [{"key": "periodCovered.from", "ascending": True}],
                    "pageInfo": {"pageIndex": page_index, "pageSize": _PAGE_SIZE},
                },
            )
            try:
                items = response.json()["contentItemList"]
                if not isinstance(items, list):
                    raise TypeError
                for item in items:
                    name, file_id = item["name"], item["fileId"]
                    if not isinstance(name, str) or not isinstance(file_id, str):
                        raise TypeError
                    files[name] = file_id
            except (ValueError, KeyError, TypeError):
                raise ParseError("Invalid ENTSO-E File Library folder listing") from None
            if len(items) < _PAGE_SIZE:
                return files
            page_index += 1

    async def download_activated_balancing_energy_archive(self, file_id: str, *, file_library_token: str) -> bytes:
        """Download the ZIP for an A83 file ID returned by the archive listing."""
        response = await self._file_library_request(
            "downloadFileContent",
            file_library_token,
            {"topLevelFolder": _LEGACY_FOLDER, "fileIdList": [file_id], "downloadAsZip": True},
        )
        if not is_zipfile(BytesIO(response.content)):
            raise ParseError("Invalid ZIP from the ENTSO-E File Library")
        return response.content

    async def _file_library_request(self, endpoint: str, token: str, payload: dict) -> httpx2.Response:
        if not token.strip():
            raise ValueError("A File Library bearer token is required")
        # Build the request without the shared client's REST headers and query parameters.
        request = httpx2.Request(
            "POST", f"{_FILE_LIBRARY_URL}/{endpoint}", headers={"Authorization": f"Bearer {token}"}, json=payload
        )
        try:
            response = await self._send(request, auth=None, follow_redirects=False)
        except httpx2.RequestError as exc:
            message = str(exc).replace(token, "<redacted>")
            raise type(exc)(self._redact(message), request=self._safe_request(request)) from None
        if not response.is_success:
            raise self._http_error(response)
        return response
