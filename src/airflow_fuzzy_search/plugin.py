"""Airflow plugin that replaces the UI's Cmd/Ctrl+K search with a fuzzy search over DAGs and tasks.

The plugin has two parts:

* a tiny FastAPI app that serves one static JavaScript file and nothing else, and
* a React app mounted in the UI's base layout, which is that JavaScript file.

The script runs inside the logged-in Airflow page and reads DAGs and tasks from Airflow's own
public REST API with the user's session, so it sees exactly what the user may see. The plugin
adds no endpoint that returns data, so there is nothing of its own to authenticate.
"""

from __future__ import annotations

from importlib.metadata import version
from pathlib import Path

from airflow.configuration import conf
from airflow.plugins_manager import AirflowPlugin
from fastapi import FastAPI
from fastapi.responses import FileResponse

URL_PREFIX = "/fuzzy-search"
SCRIPT = Path(__file__).parent / "static" / "search.js"
VERSION = version("airflow-fuzzy-search")
# Searching docs costs the browser one more API request per DAG when it builds its index. On large
# instances, switch it off: [fuzzy_search] include_docs = False (AIRFLOW__FUZZY_SEARCH__INCLUDE_DOCS).
INCLUDE_DOCS = conf.getboolean("fuzzy_search", "include_docs", fallback=True)

app = FastAPI(title="Fuzzy search assets", docs_url=None, redoc_url=None, openapi_url=None)


@app.get("/search.js", include_in_schema=False)
def search_script() -> FileResponse:
    # Static code only, no data: served without a login, like Airflow's own UI bundle.
    # no-cache makes the browser revalidate (ETag), so an upgrade is picked up on the next page load.
    return FileResponse(SCRIPT, media_type="text/javascript", headers={"Cache-Control": "no-cache"})


class FuzzySearchPlugin(AirflowPlugin):
    name = "fuzzy_search"
    fastapi_apps = [{"app": app, "url_prefix": URL_PREFIX, "name": "Fuzzy search assets"}]
    react_apps = [
        {
            "name": "Fuzzy search",
            # Relative to the UI's <base href>, so Airflow behind a sub-path works too.
            # The script reads its settings from these query parameters (import.meta.url).
            "bundle_url": f"{URL_PREFIX.lstrip('/')}/search.js?v={VERSION}&docs={int(INCLUDE_DOCS)}",
            "destination": "base",
            "url_route": "fuzzy-search",
        }
    ]
