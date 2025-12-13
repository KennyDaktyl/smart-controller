# app/adapters/api/huawei/adapter.py
from datetime import datetime, timezone

from app.adapters.base import ProviderAdapter
from app.adapters.api.huawei.cache import get_huawei_client
from app.domain.provider_state import ProviderState


class HuaweiPVAdapter(ProviderAdapter):
    def fetch_state(self) -> ProviderState:
        client = get_huawei_client(self.provider.id, self.provider.config)

        station_code = self.provider.config["station_code"]
        devices = client.get_devices_for_station(station_code)

        if not devices:
            raise Exception("No devices found for Huawei station")

        inverter = devices[0]
        data = client.get_production(inverter["devId"])

        active_power_w = data[0].get("activePower", 0)

        return ProviderState(
            value=active_power_w / 1000,
            unit="kW",
            measured_at=datetime.now(timezone.utc),
            raw=data,
        )
