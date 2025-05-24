import httpx
import pytest

from denki_client.exceptions import (
    InvalidBusinessParameterError,
    InvalidPSRTypeError,
    NoMatchingDataError,
    PaginationError,
    raise_response_error,
)


@pytest.fixture(scope="session")
def mock_request():
    """Fixture to create a mock request object for responses."""
    return httpx.Request("GET", "https://example.com")


def test_successful_response(mock_request):
    """Test that a successful response is returned unchanged."""
    response = httpx.Response(
        200, content=b"<text>Valid data</text>", headers={"content-type": "application/xml"}, request=mock_request
    )
    result = raise_response_error(response)
    assert result == response


def test_no_matching_data_error_from_status(mock_request):
    """Test that NoMatchingDataError is raised when the error is in the status."""
    response = httpx.Response(400, content=b"<text>No matching data found</text>", request=mock_request)
    with pytest.raises(NoMatchingDataError):
        raise_response_error(response)


def test_no_matching_data_error_from_content(mock_request):
    """Test that NoMatchingDataError is raised when the error is in the content with status 200."""
    response = httpx.Response(
        200,
        content=b"<text>No matching data found</text>",
        headers={"content-type": "application/xml"},
        request=mock_request,
    )
    with pytest.raises(NoMatchingDataError):
        raise_response_error(response)


def test_invalid_business_parameter_error(mock_request):
    """Test that InvalidBusinessParameterError is raised for the corresponding error."""
    response = httpx.Response(
        400, content=b"<text>check you request against dependency tables</text>", request=mock_request
    )
    with pytest.raises(InvalidBusinessParameterError):
        raise_response_error(response)


def test_invalid_psr_type_error(mock_request):
    """Test that InvalidPSRTypeError is raised for the corresponding error."""
    response = httpx.Response(400, content=b"<text>is not valid for this area</text>", request=mock_request)
    with pytest.raises(InvalidPSRTypeError):
        raise_response_error(response)


def test_pagination_error_first_case(mock_request):
    """Test that PaginationError is raised for the first case with correct message."""
    content = b"<text>amount of requested data exceeds allowed limit of 500 items. Requested 600</text>"
    response = httpx.Response(400, content=content, request=mock_request)
    with pytest.raises(PaginationError) as exc_info:
        raise_response_error(response)
        assert "500" in str(exc_info.value)
        assert "600" in str(exc_info.value)


def test_pagination_error_second_case(mock_request):
    """Test that PaginationError is raised for the second case with correct message."""
    content = b"<text>requested data to be gathered via the offset parameter exceeds the allowed limit (500 items). Requested 600</text>"
    response = httpx.Response(400, content=content, request=mock_request)
    with pytest.raises(PaginationError) as exc_info:
        raise_response_error(response)
        assert "500" in str(exc_info.value)
        assert "600" in str(exc_info.value)


def test_other_http_error(mock_request):
    """Test that other HTTP errors are propagated correctly."""
    response = httpx.Response(500, content=b"<text>Internal server error</text>", request=mock_request)
    with pytest.raises(httpx.HTTPError):
        raise_response_error(response)


def test_non_xml_content_with_200_status(mock_request):
    """Test that non-XML content with 200 status is returned unchanged."""
    response = httpx.Response(
        200, content=b"Some non-XML data", headers={"content-type": "text/plain"}, request=mock_request
    )
    result = raise_response_error(response)
    assert result == response


def test_zip_content_with_200_status(mock_request):
    """Test that zip content with 200 status is returned unchanged."""
    response = httpx.Response(
        200, content=b"ZIP data", headers={"content-type": "application/zip"}, request=mock_request
    )
    result = raise_response_error(response)
    assert result == response


def test_empty_response(mock_request):
    """Test that an empty response raises HTTPError."""
    response = httpx.Response(400, content=b"", request=mock_request)
    with pytest.raises(httpx.HTTPError):
        raise_response_error(response)


def test_response_without_text_tag(mock_request):
    """Test that a response without text tag raises HTTPError."""
    response = httpx.Response(400, content=b"<html><body>Error</body></html>", request=mock_request)
    with pytest.raises(httpx.HTTPError):
        raise_response_error(response)
