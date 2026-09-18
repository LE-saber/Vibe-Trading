"""Contract for the initial liveness and version responses."""

from typing import Literal, TypedDict


class ServiceStatus(TypedDict):
    service: Literal["competition-core"]
    status: Literal["ok"]
    version: Literal["0.1.0"]
