from datetime import datetime
from typing import Any

def parse_timeseries_generic(
    xml_text: str | bytes, labels: list[str], metadata: list[str], period_name: str
) -> dict[str, list[str | int | float | datetime | None]]:
    """Project the requested point measurements and series metadata into aligned columns."""

def parse_xml(xml_bytes: bytes) -> dict[str, Any]:
    """Parse XML bytes into a nested tree preserving names, namespaces, attributes, text, and order."""
