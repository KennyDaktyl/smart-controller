# app/adapters/registry.py

from smart_common.enums.provider import ProviderKind, ProviderType

from app.adapters.api.huawei.adapter import HuaweiPVAdapter
from app.adapters.base import ProviderAdapter


def get_adapter(provider) -> ProviderAdapter:
    if provider.provider_type == ProviderType.API:
        if provider.kind == ProviderKind.PV_INVERTER:
            if provider.vendor.lower() == "huawei":
                return HuaweiPVAdapter(provider)

    raise ValueError(
        f"No adapter registered for "
        f"type={provider.provider_type}, "
        f"kind={provider.kind}, "
        f"vendor={provider.vendor}"
    )
