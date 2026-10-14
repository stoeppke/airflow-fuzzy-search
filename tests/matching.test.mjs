// Matcher tests. Run with: node --test tests/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

const source = readFileSync(new URL("../src/airflow_fuzzy_search/static/search.js", import.meta.url), "utf8");
const { search, prepareItem, snippet } = await import(`data:text/javascript,${encodeURIComponent(source)}`);

const dag = (dagId, tags = [], display = dagId) =>
  prepareItem({
    kind: "dag",
    key: `dag:${dagId}`,
    dagId,
    label: display,
    secondary: display === dagId ? "" : dagId,
    tags,
    paused: false,
    href: "",
  });
const task = (dagId, taskId, operator = "PythonOperator") =>
  prepareItem({
    kind: "task",
    key: `task:${dagId}:${taskId}`,
    dagId,
    dagLabel: dagId,
    label: taskId,
    secondary: operator,
    href: "",
  });
const documented = (item, doc) => prepareItem({ ...item, doc });

const items = [
  dag("weather_ingest", ["ingest"]),
  dag("sales_report_daily", ["reporting"]),
  dag("customer_churn_model"),
  dag("warehouse_maintenance"),
  task("weather_ingest", "fetch_observations"),
  task("weather_ingest", "load_raw"),
  task("customer_churn_model", "train_model"),
  task("warehouse_maintenance", "compact_tables"),
];
const keys = (query) => search(items, query).map((r) => r.item.key);

test("separators and spaces are interchangeable", () => {
  assert.equal(keys("weather ingest")[0], "dag:weather_ingest");
  assert.equal(keys("sales-report")[0], "dag:sales_report_daily");
});

test("abbreviations match word starts", () => {
  assert.equal(keys("wi")[0], "dag:weather_ingest");
  assert.equal(keys("ccm")[0], "dag:customer_churn_model");
});

test("a DAG name alone does not list its tasks", () => {
  assert.deepEqual(keys("weather ingest"), ["dag:weather_ingest"]);
});

test("DAG and task words combine", () => {
  assert.equal(keys("weather fetch")[0], "task:weather_ingest:fetch_observations");
  assert.equal(keys("churn train")[0], "task:customer_churn_model:train_model");
});

test("tags are searched", () => {
  assert.ok(keys("reporting").includes("dag:sales_report_daily"));
});

test("every word must match", () => {
  assert.deepEqual(keys("weather zzz"), []);
});

test("highlights point at the matched characters", () => {
  const [first] = search(items, "wthr ing");
  assert.equal(first.item.key, "dag:weather_ingest");
  const marked = [...first.highlights.label].sort((a, b) => a - b).map((i) => first.item.label[i]);
  assert.equal(marked.join(""), "wthring");
});

test("punctuation is ignored", () => {
  assert.equal(keys("?weather, ingest!")[0], "dag:weather_ingest");
});

test("the current DAG's tasks come first among equals", () => {
  const twins = [task("a_dag", "run_1"), task("b_dag", "run_1")];
  assert.equal(search(twins, "run 1")[0].item.dagId, "a_dag");
  assert.equal(search(twins, "run 1", "b_dag")[0].item.dagId, "b_dag");
});

test("docs match only after every name match, in a section of their own", () => {
  const withDocs = [
    ...items,
    documented(dag("billing_export"), "Exports the month's **invoices** to the accounting system."),
    documented(task("weather_ingest", "validate_schema"), "Rejects observations without a station id."),
  ];
  const results = search(withDocs, "invoices");
  assert.deepEqual(
    results.map((r) => [r.item.key, r.section ?? "name"]),
    [["dag:billing_export", "doc"]],
  );
  const sections = search(withDocs, "observations").map((r) => r.section ?? "name");
  assert.ok(sections.includes("doc"));
  assert.ok(sections.indexOf("doc") > sections.lastIndexOf("name"));
});

test("docs need every word at a word start, not scattered letters", () => {
  const item = documented(dag("x"), "Compacts the warehouse tables and expires old snapshots.");
  assert.equal(search([item], "warehouse snapshots").length, 1);
  assert.equal(search([item], "ware snap").length, 1);
  assert.equal(search([item], "arehouse").length, 0);
  assert.equal(search([item], "wsn").length, 0);
});

test("an item found by name is not repeated among doc hits", () => {
  const item = documented(dag("weather_ingest"), "Pulls weather observations every hour.");
  assert.equal(search([item], "weather").length, 1);
  assert.equal(search([item], "weather")[0].section, undefined);
});

test("snippets collapse whitespace, drop markdown and keep the marks on the hit", () => {
  const doc = "# Title\n\nSome **bold**   text about invoices here.";
  const item = documented(dag("x"), doc);
  const [hit] = search([item], "invoices");
  const { text, marks } = snippet(doc, hit.doc.positions, hit.doc.first);
  assert.equal(text, "Title Some bold text about invoices here.");
  assert.equal([...marks].map((i) => text[i]).join(""), "invoices");
});

test("doc hits stay out when the user switched docs off", () => {
  const item = documented(dag("x"), "Compacts the warehouse tables.");
  assert.equal(search([item], "warehouse", null, true).length, 1);
  assert.equal(search([item], "warehouse", null, false).length, 0);
});
