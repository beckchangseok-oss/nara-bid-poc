from __future__ import annotations

from typing import Any

import requests


class BidApiClient:
    def __init__(self, service_key: str, service_url: str, endpoint: str, timeout: int = 30) -> None:
        self.service_key = service_key
        self.service_url = service_url.rstrip("/")
        self.endpoint = endpoint
        self.timeout = timeout
        self.session = requests.Session()

    @property
    def request_url(self) -> str:
        if self.endpoint.startswith("http://") or self.endpoint.startswith("https://"):
            return self.endpoint
        return f"{self.service_url}{self.endpoint}"

    def fetch_bid_list(
        self,
        inqry_bgn_dt: str,
        inqry_end_dt: str,
        page_no: int = 1,
        num_of_rows: int = 50,
        inqry_div: str = "1",
        type_: str = "json",
        extra_params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "serviceKey": self.service_key,
            "pageNo": page_no,
            "numOfRows": num_of_rows,
            "inqryDiv": inqry_div,
            "inqryBgnDt": inqry_bgn_dt,
            "inqryEndDt": inqry_end_dt,
            "type": type_,
        }

        if extra_params:
            params.update(extra_params)

        response = self.session.get(self.request_url, params=params, timeout=self.timeout)
        response.raise_for_status()
        return response.json()