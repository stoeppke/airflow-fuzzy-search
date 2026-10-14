# Contributing

Thanks for wanting to help. This project was written by an AI coding agent and nobody reviews it
full-time, so answers may take a while. Contributions written with AI tools are welcome as long as
the pull request says so.

## How the code is laid out

There are two files that matter. `src/airflow_fuzzy_search/plugin.py` registers the plugin with
Airflow: it serves the script and tells the UI to mount it on every page, passing the one setting
along in the script's address. `src/airflow_fuzzy_search/static/search.js` is the whole overlay,
plain JavaScript without a build step or dependencies: loading DAGs and tasks from the REST API,
the matching, and the overlay itself. The matching functions are exported so the tests can use
them without a browser.

## Trying a change

The folder `dev` holds a Compose file for a throwaway Airflow with the plugin installed from your
checkout and four example DAGs with documentation. Start it with
`docker compose -f dev/compose.yml up -d` and open http://localhost:28080; everybody who reaches it
is an admin without logging in, which is why it only listens on your own machine. Set
`LOAD_EXAMPLES=True` for Airflow's own eighty-odd example DAGs on top, to see the search over a
longer list. A change to the script shows after reloading the page; a change to `plugin.py` needs a
restart of the container.

To put your checkout into an Airflow Compose stack you already have, `dev/override-for-existing-stack.yml`
adds the plugin to its API server without changing the stack's own files; the comment at its top
shows how.

## Checks

The continuous integration runs the same checks you can run yourself: the matcher tests with
`node --test tests/`, Ruff over the Python code, a package build, and a start of the throwaway
Airflow to see that the plugin loads. A change to the matching should come with a test.
