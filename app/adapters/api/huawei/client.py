
# app/adapters/api/huawei/client.py
import json
import logging
from datetime import datetime, timedelta, timezone

import requests

from exceptions import HuaweiRateLimitException

logger = logging.getLogger(__name__)


class HuaweiClient:
    def __init__(self, *, username: str, password: str, base_url: str):
        self.username = username
        self.password = password
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.logged_in = False
        self.token_expiration: datetime | None = None

    def _login(self):
        url = f"{self.base_url}/login"
        payload = {"userName": self.username, "systemCode": self.password}

        response = self.session.post(
            url, data=json.dumps(payload), headers={"Content-Type": "application/json"}, timeout=10
        )

        if not response.ok:
            raise Exception(f"Huawei login failed: {response.status_code}")

        result = response.json()
        if not result.get("success"):
            raise Exception(f"Huawei login error: {result}")

        xsrf = self.session.cookies.get("XSRF-TOKEN")
        if not xsrf:
            raise Exception("Huawei login failed: missing XSRF token")

        self.session.headers.update({"XSRF-TOKEN": xsrf, "Content-Type": "application/json"})
        self.token_expiration = datetime.now(timezone.utc) + timedelta(minutes=25)
        self.logged_in = True

    def _ensure_login(self):
        if not self.logged_in or (
            self.token_expiration and datetime.now(timezone.utc) >= self.token_expiration
        ):
            self._login()

    def _post(self, endpoint: str, payload: dict | None = None) -> dict:
        self._ensure_login()
        url = f"{self.base_url}/{endpoint}"

        response = self.session.post(url, data=json.dumps(payload or {}), timeout=10)

        if response.status_code == 401:
            self._login()
            response = self.session.post(url, data=json.dumps(payload or {}), timeout=10)

        if not response.ok:
            raise Exception(f"Huawei API error {response.status_code}")

        result = response.json()

        if not result.get("success"):
            if result.get("failCode") == 407:
                raise HuaweiRateLimitException()
            raise Exception(f"Huawei API failure: {result}")

        self.token_expiration = datetime.now(timezone.utc) + timedelta(minutes=25)
        return result

    def get_devices_for_station(self, station_code: str) -> list[dict]:
        return self._post("getDevList", {"stationCodes": station_code}).get("data", [])

    def get_production(self, device_id: str) -> dict:
        return self._post(
            "getDevRealKpi", {"devTypeId": "1", "devIds": device_id}
        ).get("data", [])
