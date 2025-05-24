import re

import httpx


class PaginationError(Exception):
    pass


class NoMatchingDataError(Exception):
    pass


class InvalidPSRTypeError(Exception):
    pass


class InvalidBusinessParameterError(Exception):
    pass


class InvalidParameterError(Exception):
    pass


class TzNaiveError(Exception):
    pass


class ParseError(Exception):
    pass


def raise_response_error(response: httpx.Response):
    """Raises correct error from Entsoe server response.

    :param httpx.Response response:
    :raises NoMatchingDataError:
    :raises InvalidBusinessParameterError:
    :raises InvalidPSRTypeError:
    :raises PaginationError:
    :return httpx.Response:
    """
    try:
        response.raise_for_status()
    except httpx.HTTPError as e:
        error_text = ""
        text_match = re.search(r"<text>(.*?)</text>", response.text, re.DOTALL)
        if text_match:
            error_text = text_match.group(1).strip()

        if error_text:
            if "No matching data found" in error_text:
                raise NoMatchingDataError
            elif "check you request against dependency tables" in error_text:
                raise InvalidBusinessParameterError
            elif "is not valid for this area" in error_text:
                raise InvalidPSRTypeError
            elif "amount of requested data exceeds allowed limit" in error_text:
                numbers = [int(s) for s in error_text.split() if s.isdigit()]
                if len(numbers) >= 2:
                    allowed, requested = numbers[-2], numbers[-1]
                else:
                    allowed, requested = "unknown", "unknown"
                raise PaginationError(
                    f"The API is limited to {allowed} elements per request. This query requested {requested} documents."
                )
            elif "requested data to be gathered via the offset parameter exceeds the allowed limit" in error_text:
                numbers = [int(s) for s in error_text.split() if s.isdigit()]
                if len(numbers) >= 1:
                    allowed = numbers[0]
                    requested_match = re.search(r"Requested[^\d]*(\d+)", error_text)
                    requested = requested_match.group(1) if requested_match else "unknown"
                else:
                    allowed, requested = "unknown", "unknown"
                raise PaginationError(
                    f"The API is limited to {allowed} elements per request. This query requested {requested} documents."
                )
        raise e
    # ENTSO-e has changed their server to also respond with 200 if there is no data but all parameters are valid
    # this means we need to check the contents for this error even when status code 200 is returned
    # to prevent parsing the full response do a text matching instead of full parsing
    # also only do this when response type content is text and not for example a zip file.
    if response.headers.get("content-type", "") == "application/xml":
        if "No matching data found" in response.text:
            raise NoMatchingDataError
    return response
