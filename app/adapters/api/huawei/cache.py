# app/adapters/api/huawei/cache.py
from app.adapters.api.huawei.client import HuaweiClient

_client_cache: dict[int, HuaweiClient] = {}


def get_huawei_client(provider_id: int, config: dict) -> HuaweiClient:
    if provider_id in _client_cache:
        return _client_cache[provider_id]

    client = HuaweiClient(
        username=config["username"],
        password=config["password"],
        base_url=config["base_url"],
    )
    _client_cache[provider_id] = client
    return client


def clear_huawei_client(provider_id: int):
    _client_cache.pop(provider_id, None)
