from __future__ import annotations

from typing import Any

import requests


class PublicApiClient:
    def __init__(self, service_key: str, service_url: str, timeout: int = 30) -> None:
        self.service_key = service_key
        self.service_url = service_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()

    def fetch(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.service_url}{endpoint}"

        request_params = {
            "serviceKey": self.service_key,
            "type": "json",
            **{k: v for k, v in params.items() if v not in ("", None)},
        }

        response = self.session.get(url, params=request_params, timeout=self.timeout)
        response.raise_for_status()
        return response.json()