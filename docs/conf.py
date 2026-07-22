from __future__ import annotations

import sys
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover - documentation CI uses Python 3.12
    import tomli as tomllib

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

project = "Runwatch"
author = "Agustín Sellanes"
copyright = "2026, Agustín Sellanes"
with (ROOT / "pyproject.toml").open("rb") as pyproject_file:
    release = str(tomllib.load(pyproject_file)["project"]["version"])

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx_copybutton",
]

source_suffix = {".md": "markdown", ".rst": "restructuredtext"}
root_doc = "index"
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]
myst_enable_extensions = ["attrs_block", "attrs_inline", "colon_fence", "deflist"]
myst_heading_anchors = 3

nitpicky = True
keep_warnings = True
suppress_warnings = [
    "ref.class",
    "ref.data",
    "ref.exc",
    "ref.func",
    "ref.meth",
    "ref.mod",
    "ref.obj",
]

autodoc_default_options = {"members": True, "show-inheritance": True}
autodoc_preserve_defaults = True
autodoc_typehints = "none"
autoclass_content = "both"
autosummary_generate = True
napoleon_google_docstring = False
napoleon_numpy_docstring = True

html_theme = "pydata_sphinx_theme"
html_title = "Runwatch"
html_theme_options = {
    "github_url": "https://github.com/EssenceSentry/runwatch",
    "navbar_center": [],
    "show_toc_level": 2,
    "navigation_depth": 3,
}

copybutton_prompt_text = r">>> |\.\.\. |\$ "
copybutton_prompt_is_regexp = True
