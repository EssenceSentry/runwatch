from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from os import PathLike
from typing import Any, Protocol, TextIO

from nbformat import v4 as v4
from nbformat.notebooknode import NotebookNode as NotebookNode
from nbformat.notebooknode import from_dict as from_dict

current_nbformat: int
current_nbformat_minor: int
NO_CONVERT: object
__version__: str

class ValidationError(Exception): ...
class NBFormatError(ValueError): ...

class _ReadableText(Protocol):
    def read(self) -> str: ...

class _WritableText(Protocol):
    def write(self, s: str, /) -> Any: ...

Pathish = str | PathLike[str]
ReadSource = Pathish | TextIO | _ReadableText
WriteTarget = Pathish | TextIO | _WritableText

def reads(
    s: str,
    as_version: int | object,
    capture_validation_error: MutableMapping[str, Any] | None = None,
    **kwargs: Any,
) -> NotebookNode: ...
def read(
    fp: ReadSource,
    as_version: int | object,
    capture_validation_error: MutableMapping[str, Any] | None = None,
    **kwargs: Any,
) -> NotebookNode: ...
def writes(
    nb: NotebookNode | Mapping[str, Any],
    version: int | object = NO_CONVERT,
    capture_validation_error: MutableMapping[str, Any] | None = None,
    **kwargs: Any,
) -> str: ...
def write(
    nb: NotebookNode | Mapping[str, Any],
    fp: WriteTarget,
    version: int | object = NO_CONVERT,
    capture_validation_error: MutableMapping[str, Any] | None = None,
    **kwargs: Any,
) -> None: ...
def validate(
    nbdict: NotebookNode | Mapping[str, Any],
    ref: str | None = None,
    version: int | None = None,
    version_minor: int | None = None,
    relax_add_props: bool = False,
    nbjson: Any | None = None,
    repair_duplicate_cell_ids: bool = True,
    strip_invalid_metadata: bool = False,
) -> None: ...
def convert(nb: NotebookNode | Mapping[str, Any], to_version: int) -> NotebookNode: ...
