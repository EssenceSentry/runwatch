"""Notification configuration compatibility and destination topology helpers."""

from __future__ import annotations

from copy import deepcopy
from typing import cast
from urllib.parse import quote, urlsplit, urlunsplit

from .models import NotificationSettings


def notification_destinations(
    settings: NotificationSettings,
) -> list[tuple[str, str]]:
    """Return unique delivery destinations in stable configuration order."""

    destinations = [("webhook", url) for url in settings.webhook_urls]
    if settings.ntfy_base_url and settings.ntfy_topic:
        destinations.append(
            (
                "ntfy",
                f"{settings.ntfy_base_url.rstrip('/')}/"
                f"{quote(settings.ntfy_topic, safe='-_')}",
            )
        )
    return list(dict.fromkeys(destinations))


def notification_topology(
    settings: NotificationSettings,
) -> tuple[tuple[str, int], ...]:
    """Describe destination kinds without retaining credential-bearing values."""

    counts: dict[str, int] = {}
    for kind, _destination in notification_destinations(settings):
        counts[kind] = counts.get(kind, 0) + 1
    return tuple(sorted(counts.items()))


def compatible_notification_settings(value: object) -> NotificationSettings:
    """Validate persisted settings after narrowly upgrading old policy defaults."""

    if not isinstance(value, dict):
        return NotificationSettings.model_validate(value)
    mapping = deepcopy(cast(dict[object, object], value))
    _upgrade_legacy_notification_mapping(mapping)
    return NotificationSettings.model_validate(mapping)


def compatible_runwatch_config(value: object) -> object:
    """Copy a persisted config and upgrade pre-policy notification settings.

    User-authored configuration continues to use ``RunwatchConfig`` directly. This
    compatibility path is reserved for already-persisted manifests and metadata that
    predate the explicit HTTP opt-in and one-minute periodic lower bound.
    """

    if not isinstance(value, dict):
        return value
    mapping = deepcopy(cast(dict[object, object], value))
    notifications = mapping.get("notifications")
    if isinstance(notifications, dict):
        _upgrade_legacy_notification_mapping(cast(dict[object, object], notifications))
    return mapping


def _upgrade_legacy_notification_mapping(mapping: dict[object, object]) -> None:
    webhook_urls = mapping.get("webhook_urls")
    if isinstance(webhook_urls, list):
        mapping["webhook_urls"] = [
            _without_fragment(value) for value in cast(list[object], webhook_urls)
        ]
    if "ntfy_base_url" in mapping:
        mapping["ntfy_base_url"] = _without_fragment(mapping.get("ntfy_base_url"))
    normalized_webhooks = mapping.get("webhook_urls")
    urls = (
        list(cast(list[object], normalized_webhooks))
        if isinstance(normalized_webhooks, list)
        else []
    )
    urls.append(mapping.get("ntfy_base_url"))
    if "allow_insecure_http" not in mapping and any(
        _is_plain_http_url(value) for value in urls
    ):
        mapping["allow_insecure_http"] = True
    periodic = mapping.get("periodic_seconds")
    if (
        isinstance(periodic, (int, float))
        and not isinstance(periodic, bool)
        and 0 < periodic < 60
    ):
        mapping["periodic_seconds"] = 60


def _is_plain_http_url(value: object) -> bool:
    return isinstance(value, str) and urlsplit(value).scheme.lower() == "http"


def _without_fragment(value: object) -> object:
    if not isinstance(value, str):
        return value
    parts = urlsplit(value)
    if not parts.fragment:
        return value
    return urlunsplit((parts.scheme, parts.netloc, parts.path, parts.query, ""))
