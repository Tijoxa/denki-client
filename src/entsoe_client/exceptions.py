import httpx
from bs4 import BeautifulSoup


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
        soup = BeautifulSoup(response.text, "html.parser")
        text = soup.find_all("text")
        if len(text) > 0:
            error_text: str = soup.find("text").text
            if "No matching data found" in error_text:
                raise NoMatchingDataError
            elif "check you request against dependency tables" in error_text:
                raise InvalidBusinessParameterError
            elif "is not valid for this area" in error_text:
                raise InvalidPSRTypeError
            elif "amount of requested data exceeds allowed limit" in error_text:
                requested = error_text.split(" ")[-2]
                allowed = error_text.split(" ")[-5]
                raise PaginationError(
                    f"The API is limited to {allowed} elements per "
                    f"request. This query requested for {requested} "
                    f"documents and cannot be fulfilled as is."
                )
            elif "requested data to be gathered via the offset parameter exceeds the allowed limit" in error_text:
                requested = error_text.split(" ")[-9]
                allowed = error_text.split(" ")[-30][:-2]
                raise PaginationError(
                    f"The API is limited to {allowed} elements per "
                    f"request. This query requested for {requested} "
                    f"documents and cannot be fulfilled as is."
                )
        raise e
    else:
        """ENTSO-E has changed their server to also respond with 200 if there is no data but all parameters are valid
        this means we need to check the contents for this error even when status code 200 is returned
        to prevent parsing the full response do a text matching instead of full parsing
        also only do this when response type content is text and not for example a zip file.
        """
        if response.headers.get("content-type", "") == "application/xml":
            if "No matching data found" in response.text:
                raise NoMatchingDataError
        return response
