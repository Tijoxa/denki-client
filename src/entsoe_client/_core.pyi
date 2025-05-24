def hello_from_bin() -> str:
    """Returns a str from Rust backend."""

def parse_timeseries_generic(
    xml_text: str, labels: list[str], metadata: list[str], period_name: str
) -> dict[str, list[str]]:
    """Parses XML-formatted string to a Python dictionnary.

    :param str xml_text: XML-formatted string
    :param list[str] labels: list of XML tags to retreive inside a `period_name` tag
    :param list[str] metadata: list of XML tags to retreive between a `TimeSeries` and a `period_name` tag
    :param str period_name: usually 'period'
    :return dict[str, list[str]]:
    """
