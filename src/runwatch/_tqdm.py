from __future__ import annotations

from textwrap import dedent

from .schema_versions import RESOURCE_EVENT_SCHEMA_VERSION

_TQDM_BOOTSTRAP = dedent("""
    import math as _runwatch_math
    import time as _runwatch_time
    import uuid as _runwatch_uuid

    try:
        from IPython.display import display as _runwatch_display
        from IPython.display import update_display as _runwatch_update_display
        from tqdm.std import tqdm as _runwatch_standard_tqdm
    except Exception:
        pass
    else:
        _runwatch_tqdm_classes = [_runwatch_standard_tqdm]
        try:
            from tqdm.notebook import tqdm as _runwatch_notebook_tqdm
        except Exception:
            pass
        else:
            if _runwatch_notebook_tqdm is not _runwatch_standard_tqdm:
                _runwatch_tqdm_classes.append(_runwatch_notebook_tqdm)

        def _runwatch_number(value):
            try:
                converted = float(value)
            except (TypeError, ValueError, OverflowError):
                return None
            return converted if _runwatch_math.isfinite(converted) else None

        def _runwatch_tqdm_payload(bar, *, closed):
            values = bar.format_dict
            completed = _runwatch_number(values.get("n", getattr(bar, "n", None)))
            if completed is None:
                return None
            completed = max(0.0, completed)
            total = _runwatch_number(
                values.get("total", getattr(bar, "total", None))
            )
            if total is not None and (total <= 0 or completed > total):
                total = None

            description = values.get("prefix", getattr(bar, "desc", None))
            message = str(description).strip().rstrip(":") if description else None
            postfix = values.get("postfix")
            if postfix:
                postfix_text = str(postfix).strip()
                message = (
                    f"{message} · {postfix_text}" if message else postfix_text
                )

            progress_id = getattr(bar, "_runwatch_progress_id", None)
            if progress_id is None:
                progress_id = _runwatch_uuid.uuid4().hex
                setattr(bar, "_runwatch_progress_id", progress_id)
            position = abs(int(getattr(bar, "pos", 0) or 0))
            metrics = {
                "source": "tqdm",
                "progress_id": progress_id,
                "position": position,
                "closed": bool(closed),
            }
            rate = _runwatch_number(values.get("rate"))
            if rate is not None and rate >= 0:
                metrics["rate"] = rate
            elapsed = _runwatch_number(values.get("elapsed"))
            if elapsed is not None and elapsed >= 0:
                metrics["elapsed_seconds"] = elapsed

            unit = values.get("unit", getattr(bar, "unit", None))
            return {
                "schema_version": __RUNWATCH_RESOURCE_EVENT_SCHEMA_VERSION__,
                "event_id": str(_runwatch_uuid.uuid4()),
                "event": "progress",
                "completed": completed,
                "total": total,
                "unit": str(unit) if unit else None,
                "message": message,
                "metrics": metrics,
            }

        def _runwatch_emit_tqdm(bar, args, kwargs):
            close_signal = bool(kwargs.get("close"))
            if len(args) >= 3:
                close_signal = close_signal or bool(args[2])
            bar_style = kwargs.get("bar_style")
            if len(args) >= 4 and bar_style is None:
                bar_style = args[3]
            closed = bool(
                close_signal
                or bar_style in {"success", "danger"}
                or getattr(bar, "disable", False)
            )
            payload = _runwatch_tqdm_payload(bar, closed=closed)
            if payload is None:
                return

            now = _runwatch_time.monotonic()
            last_time = getattr(bar, "_runwatch_last_emit_time", None)
            total = payload["total"]
            finished = total is not None and payload["completed"] >= total
            force = last_time is None or closed or finished
            if (
                not force
                and now - last_time < __RUNWATCH_TQDM_MIN_INTERVAL_SECONDS__
            ):
                return
            signature = (
                payload["completed"],
                payload["total"],
                payload["message"],
                closed,
            )
            if signature == getattr(bar, "_runwatch_last_emit_signature", None):
                return

            display_id = getattr(bar, "_runwatch_display_id", None)
            if display_id is None:
                display_id = f"runwatch-tqdm-{payload['metrics']['progress_id']}"
                setattr(bar, "_runwatch_display_id", display_id)
            bundle = {"application/vnd.runwatch.event+json": payload}
            if getattr(bar, "_runwatch_has_display", False):
                _runwatch_update_display(
                    bundle,
                    raw=True,
                    display_id=display_id,
                )
            else:
                _runwatch_display(bundle, raw=True, display_id=display_id)
                setattr(bar, "_runwatch_has_display", True)
            setattr(bar, "_runwatch_last_emit_time", now)
            setattr(bar, "_runwatch_last_emit_signature", signature)

        def _runwatch_wrap_tqdm_display(tqdm_class):
            if tqdm_class.__dict__.get("_runwatch_display_patched", False):
                return
            original_display = tqdm_class.display

            def wrapped_display(self, *args, **kwargs):
                result = original_display(self, *args, **kwargs)
                try:
                    _runwatch_emit_tqdm(self, args, kwargs)
                except Exception:
                    pass
                return result

            wrapped_display.__name__ = getattr(
                original_display,
                "__name__",
                "display",
            )
            wrapped_display.__doc__ = getattr(original_display, "__doc__", None)
            tqdm_class.display = wrapped_display
            tqdm_class._runwatch_display_patched = True

        try:
            for _runwatch_tqdm_class in _runwatch_tqdm_classes:
                _runwatch_wrap_tqdm_display(_runwatch_tqdm_class)
        except Exception:
            pass
    """)


def tqdm_bootstrap_code(min_interval_seconds: float) -> str:
    """Return isolated Python kernel code that installs tqdm progress capture."""

    payload = _TQDM_BOOTSTRAP.replace(
        "__RUNWATCH_TQDM_MIN_INTERVAL_SECONDS__",
        repr(float(min_interval_seconds)),
    )
    payload = payload.replace(
        "__RUNWATCH_RESOURCE_EVENT_SCHEMA_VERSION__",
        repr(RESOURCE_EVENT_SCHEMA_VERSION),
    )
    return f"exec({payload!r}, {{}})"
