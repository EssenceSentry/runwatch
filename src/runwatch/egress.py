from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import cast
from urllib.parse import urlsplit, urlunsplit

REDACTED = "[REDACTED]"

_URL_PATTERN = re.compile(r"https?://[^\s<>'\"\])}]+", re.IGNORECASE)
_BEARER_PATTERN = re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]+")
_ASSIGNMENT_PATTERN = re.compile(
    r"(?i)\b(token|api[_-]?key|secret|password|passwd|signature|credential)"
    r"(\s*[:=]\s*)([^\s,;&]+)"
)


def bounded_text(value: object, *, max_chars: int) -> str:
    """Return a deterministic, prefix-preserving bounded string."""

    text = "" if value is None else str(value)
    if len(text) <= max_chars:
        return text
    if max_chars <= 1:
        return text[:max_chars]
    return text[: max_chars - 1] + "…"


@dataclass(frozen=True)
class SecretRedactor:
    """Best-effort redaction for values crossing a presentation boundary."""

    secrets: tuple[str, ...] = ()

    @classmethod
    def from_values(cls, values: Iterable[object]) -> SecretRedactor:
        secrets: set[str] = set()
        for value in values:
            secrets.update(_secret_tokens(value))
        return cls(tuple(sorted(secrets, key=len, reverse=True)))

    def text(self, value: object, *, max_chars: int) -> str:
        text = "" if value is None else str(value)
        text = _URL_PATTERN.sub(self._sanitize_url_match, text)
        text = _BEARER_PATTERN.sub(f"Bearer {REDACTED}", text)
        text = _ASSIGNMENT_PATTERN.sub(
            lambda match: f"{match.group(1)}{match.group(2)}{REDACTED}", text
        )
        for secret in self.secrets:
            if len(secret) >= 4:
                text = text.replace(secret, REDACTED)
        return bounded_text(text, max_chars=max_chars)

    def json(
        self,
        value: object,
        *,
        max_depth: int = 4,
        max_items: int = 32,
        max_string_chars: int = 1_024,
    ) -> object:
        return self._json(
            value,
            depth=0,
            max_depth=max_depth,
            max_items=max_items,
            max_string_chars=max_string_chars,
        )

    def _json(
        self,
        value: object,
        *,
        depth: int,
        max_depth: int,
        max_items: int,
        max_string_chars: int,
    ) -> object:
        if value is None or isinstance(value, (bool, int, float)):
            return value
        if isinstance(value, str):
            return self.text(value, max_chars=max_string_chars)
        if depth >= max_depth:
            return "[TRUNCATED]"
        if isinstance(value, list):
            items = cast(list[object], value)[:max_items]
            return [
                self._json(
                    item,
                    depth=depth + 1,
                    max_depth=max_depth,
                    max_items=max_items,
                    max_string_chars=max_string_chars,
                )
                for item in items
            ]
        if isinstance(value, dict):
            mapping = cast(dict[object, object], value)
            result: dict[str, object] = {}
            for key, item in list(mapping.items())[:max_items]:
                if not isinstance(key, str):
                    continue
                result[self.text(key, max_chars=128)] = self._json(
                    item,
                    depth=depth + 1,
                    max_depth=max_depth,
                    max_items=max_items,
                    max_string_chars=max_string_chars,
                )
            return result
        return self.text(type(value).__name__, max_chars=max_string_chars)

    @staticmethod
    def _sanitize_url_match(match: re.Match[str]) -> str:
        raw = match.group(0)
        try:
            parts = urlsplit(raw)
            hostname = parts.hostname or ""
            if ":" in hostname and not hostname.startswith("["):
                hostname = f"[{hostname}]"
            authority = hostname
            if parts.port is not None:
                authority += f":{parts.port}"
            return urlunsplit((parts.scheme, authority, parts.path, "", ""))
        except ValueError:
            return REDACTED


def _secret_tokens(value: object) -> set[str]:
    if value is None:
        return set()
    text = str(value).strip()
    if not text:
        return set()
    secrets = {text}
    parts = urlsplit(text)
    if parts.scheme.lower() not in {"http", "https"} or not parts.hostname:
        return secrets
    secrets.update(item for item in (parts.username, parts.password) if item)
    secrets.update(
        item
        for _key, separator, item in (
            piece.partition("=") for piece in parts.query.split("&")
        )
        if separator and item
    )
    secrets.update(segment for segment in parts.path.split("/") if len(segment) >= 8)
    return secrets
