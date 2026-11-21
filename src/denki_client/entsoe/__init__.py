"""Asynchronous ENTSO-E client returning Polars DataFrames."""

from ._bids import BidQueries
from ._combinations import CombinationQueries
from ._outages import OutageQueries
from ._reference import ReferenceQueries
from ._reserves import ReserveQueries


class EntsoeClient(CombinationQueries, BidQueries, ReserveQueries, OutageQueries, ReferenceQueries):
    """Query the ENTSO-E Transparency Platform using an API token."""


__all__ = ["EntsoeClient"]
