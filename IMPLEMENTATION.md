# Implementation notes & design decisions

Deep-dive rationale for `sec_13f_pipeline` — why things are built the way they are, and the real
bugs found (and fixed) by actually running this against live SEC data at full scale. See
[README.md](README.md) for the architecture overview, table schemas, and how to run it.

## Why Glue is mocked with moto instead of LocalStack

LocalStack's **Community** edition (free) doesn't implement the Glue API at all — every Glue
call returns `API for service 'glue' not yet implemented or pro feature`; Glue emulation is a
LocalStack Pro feature. moto mocks Glue for free, but at the Python-process level: `build_silver_table`
wraps its Glue calls in `with mock_aws():`, which patches `boto3`/`botocore` only for the duration
of that block, inside that one task's process.

**Consequence: there is no way to verify the Glue tables after the fact.** Each Airflow task runs
in its own process, so a table "registered" inside one task's `mock_aws()` block is gone the
moment that block exits — a different task (or a manual `boto3` check afterward) starts with a
blank, unrelated mock catalog. The DAG still makes the real `create_database` / `create_table`
API calls with a correctly Iceberg-shaped `TableInput` (`Parameters.table_type=ICEBERG`,
`Parameters.metadata_location` pointing at the table's actual current metadata.json in S3), so it
demonstrates the registration logic correctly; it just can't be inspected downstream (including
from the `aws glue` CLI examples in README.md's "Running it" section). This is a known, accepted
limitation — not a bug to fix — until this points at a real AWS Glue Catalog or a LocalStack Pro
license.

One important side effect: `mock_aws()` patches **all** boto3 calls process-wide while active, not
just Glue. `build_silver_table` writes to S3 (LocalStack) *before* entering the `mock_aws()`
block and only creates the Glue client *inside* it — reversing that order would cause moto to
intercept the S3 calls too, and nothing would actually reach LocalStack.

## Why Spark runs local[*], not a standalone cluster

Silver merging used to be pure pandas; it's now PySpark. A previous iteration of this project ran
Spark as a real standalone cluster (`spark-master` + `spark-worker` containers, jobs submitted from
Airflow via `spark-submit`) for the same kind of workload, and that setup broke in several
non-obvious, version-sensitive ways: the pyspark client has to match the cluster's Spark version
*exactly* (Java serialization isn't patch-compatible — a minor mismatch fails every task with
`InvalidClassException`), and the driver's Python minor version has to match the executors' (a
mismatch fails every task with `PYTHON_VERSION_MISMATCH`). Both are easy to get wrong and painful
to debug.

`build_silver_table` sidesteps that whole class of bug by running Spark in `local[*]` mode inside
the same task process: driver and executors are the same JVM, so there's no second Spark
installation or Python interpreter to keep in sync. The tradeoff is that this doesn't reflect a
real multi-node cluster — that's a reasonable simplification for a local dev pipeline, but worth
knowing if this ever needs to point at an actual EMR/Glue Spark cluster.

Extraction (pulling a file out of a zip) still stays in Python rather than moving to Spark: Spark
has no zip codec, so `spark.read.csv()` can't read a zip member directly — each bronze zip gets
downloaded and unzipped one period at a time into a local staging directory, and only the
merge-across-periods and the Iceberg write are Spark's job. One consequence worth knowing: because
each of the seven `build_silver_*` tasks builds its own `local[*]` session, and Airflow's
`LocalExecutor` can run several tasks concurrently, multiple JVMs can end up competing for the
same CPU cores at once — harmless for correctness, just not tuned for throughput.

## Why Spark writes Iceberg tables directly to S3, and via JdbcCatalog specifically

Earlier (pre-Iceberg) versions of `build_silver_table` wrote plain Parquet to a local temp
directory and let `boto3` upload the finished files to LocalStack, the same pattern bronze uses.
That doesn't work for Iceberg: an Iceberg table's metadata (manifest lists, manifest files,
`metadata.json`) embeds the *absolute* location of every data file. Write locally and the metadata
would point at local paths that don't exist once anything tries to read the table from S3.
Relocating an already-written Iceberg table means rewriting all of that embedded metadata, which
is a different, harder problem than just moving bytes — so Spark writes straight to LocalStack S3
here via Iceberg's own `S3FileIO` (AWS SDK v2; independent of Hadoop's `s3a`/`hadoop-aws`).

Getting a working *catalog* for that write took two attempts:

1. **Iceberg's GlueCatalog** was never tried at all — it would make Glue API calls from inside the
   JVM via Java's AWS SDK, completely invisible to moto, which only patches `boto3`/`botocore` in
   this Python process. Using it would silently break the whole "Glue mocked via moto" approach.
2. **Iceberg's path-based `hadoop` catalog type** was tried next, and failed:
   `org.apache.hadoop.fs.UnsupportedFileSystemException: No FileSystem for scheme "s3"`. Some of
   `HadoopCatalog`'s own catalog-level bookkeeping (namespace/table existence checks) goes through
   Hadoop's classic `FileSystem` API, not Iceberg's `S3FileIO` — and that needs `hadoop-aws` on the
   classpath even though the actual data/metadata file I/O doesn't.
3. **Iceberg's `JdbcCatalog`** is what's actually used: it stores namespace/table pointers as rows
   in a JDBC database — reusing the same Postgres already running for Airflow's own metadata DB
   (catalog bookkeeping only, not pipeline data) — and never touches Hadoop's `FileSystem` API at
   all. Glue registration afterward reads the table's current `metadata_location` straight out of
   JdbcCatalog's own `iceberg_tables` row via `psycopg2`, rather than guessing a filename
   convention (which would also be catalog-implementation-specific — `HadoopCatalog` uses a
   `version-hint.text` marker file; `JdbcCatalog` just has a column for it).

This needs one more jar than the plain-Parquet version did: the PostgreSQL JDBC driver
(`org.postgresql:postgresql`), alongside `iceberg-spark-runtime` and `iceberg-aws-bundle`.

## Sizing Spark's heap for Iceberg's writer

`local[*]` runs the "executor" inside the driver JVM, so `spark.driver.memory` is the only heap
size that matters here, and Spark's 1g default isn't enough once real data volume is involved.
Iceberg's Spark writer buffers multiple open per-partition Parquet writers at once (its
`FanoutWriter`, used for a table's first, partition-defining write) rather than assuming the
incoming rows already arrive grouped by partition — running the full 54-period INFOTABLE history
(the same dataset that used to OOM-kill the pandas version, ~124M rows once merged) through this
at the 1g default fails with `OutOfMemoryError: Java heap space`; `spark.driver.memory=4g` handles
it comfortably (confirmed: peak container memory stayed well within the ~7.7 GB available, and all
54 partitions were written and independently verified readable afterward).

## Why the bronze data-quality stage validates against an explicit schema, not silver's inferred one

Silver's own schema is *inferred* fresh from the data on every run (see "Schema inference..." in
Implementation notes below) — there's nothing fixed to validate incoming rows against if the thing
you'd validate against is itself derived from those same rows. `check_bronze_quality` instead
validates against `SILVER_SCHEMAS`, a hand-maintained `StructType` per table that's a snapshot of
what silver's inference has actually, verifiably produced against the real full-history data (the
same types documented in README.md's "Silver table schemas"). This makes the data-quality check a
real contract: if SEC's data ever legitimately drifts wider than this (e.g. a share count finally
exceeds `bigint`), that surfaces as quarantined rows demanding a deliberate update to
`SILVER_SCHEMAS`, rather than silver's own inference just silently widening the column and nobody
noticing the drift happened at all.

Validation itself uses `try_cast(col AS <type>)` (via `F.expr`, since PySpark's Python API doesn't
expose `try_cast` directly on this Spark version) rather than a UDF or Python-side row loop —
`try_cast` is a Catalyst expression, so it runs at native Spark speed even across the full
124M-row INFOTABLE history. It returns NULL on a value that doesn't fit the target type instead of
raising, which is exactly the "did this parse or not" signal needed per column; a row is only
flagged if some column has an actual (non-blank) value that fails to cast — a genuinely missing
value is not a data-quality failure, silver already treats it as a legitimate null.

This stage re-extracts from bronze independently rather than consuming or gating silver's input:
`build_silver_table` keeps working exactly as it did before this stage existed, unaffected by
whatever this one finds. The tradeoff is duplicated work — bronze zips get downloaded and unzipped
twice per DAG run, once for each stage — in exchange for the two stages being fully decoupled and
independently testable; wiring this stage as a true gate (silver reading only the rows this stage
already validated) would remove that duplication but couple silver's correctness to this stage's
implementation. `check_bronze_quality_<table> >> build_silver_<table>` in the DAG still enforces
that the check *runs* before silver, satisfying "between bronze and silver," even though the two
don't share data.

Quarantine tables live in their own Iceberg catalog (`bronze_dq`, not `silver`), namespace
(`sec13f_dq`), S3 prefix (`bronze/dq_failed`, deliberately under `bronze/` — this is bronze-layer
output, not a modeled silver table), and Glue database (`sec13f_dq_failed`) — the same JdbcCatalog
mechanics as silver (same Postgres, same reasons), just pointed at different coordinates so
quarantined data can never show up mixed in with the real tables, and a database listing alone
(`sec13f_staging` vs. `sec13f_dq_failed`) tells you which is which.

One more memory data point: `check_bronze_quality_infotable`'s full 54-period run peaked around
~5.1 GB container memory, noticeably higher than `build_silver_infotable`'s ~2.2 GB for the same
124M rows. The extra cost is `.persist()` on the tagged (all-string-plus-`dq_failed_columns`)
DataFrame, used so the row count and the quarantine write don't each redo the `try_cast` work from
scratch — a deliberate trade of memory for avoiding a second full pass over 124M rows. Still
comfortably inside the ~7.7 GB available alongside `spark.driver.memory=4g`, but a real, measured
difference worth knowing if this ever needs to run somewhere tighter on memory.

## Analysis: the first real check_bronze_quality failure, and why it gets retries now

The above memory numbers aren't theoretical — they're why `check_bronze_quality_coverpage` (and,
in the same run, `check_bronze_quality_infotable`) actually failed once in a real Airflow-triggered
run, not just a hand-run test. What happened, reconstructed from Postgres' `task_instance` table
and the attempt-1 task logs on disk:

- Six `check_bronze_quality_*` tasks (`signature`, `othermanager`, `othermanager2`, `submission`,
  `summarypage`, `coverpage`) all started within the same 0.2-second window
  (`17:31:09.76`–`17:31:09.97`), because they all become runnable the instant `download_zip_to_bronze`
  finishes and Airflow's `LocalExecutor` had room to run them all at once. Each one builds its own
  independent `local[*]` Spark session configured for `spark.driver.memory=4g`.
- `check_bronze_quality_coverpage`'s attempt 1 failed at `17:33:27`, mid-task, inside
  `tagged.count()` — not a data or logic error, but `py4j.protocol.Py4JError`, followed by every
  subsequent Python→JVM call (`.unpersist()`, `spark.stop()`) failing the same way with
  `ConnectionRefusedError: [Errno 111] Connection refused`. That's Py4J's socket to its own Spark
  JVM being refused — the JVM process itself was gone. `check_bronze_quality_infotable`'s attempt 1,
  in the same run, failed with the identical signature at `17:33:51`, right at the tail of the same
  crowded window.
- The container itself was not killed (`docker inspect .State.OOMKilled` — `false`), which points at
  the Linux kernel's OOM killer selecting an individual JVM process as its victim from *inside* the
  container's cgroup once combined memory demand from that many concurrent 4g-capable JVMs exceeded
  what the container had available (~7.7 GB) — not the container's own memory limit being hit as a
  whole.
- Both tasks succeeded cleanly on their very next attempt (`try_number=2`), once several sibling
  tasks had already finished and freed their memory. Nothing about the data or the code changed
  between attempt 1 and attempt 2 — only how much else was running at the same time.

That "worked the very next time, once contention eased, no code or data changed" pattern is exactly
what Airflow's `retries` mechanism is for, which is why `check_bronze_quality` now sets `retries=3`
with exponential backoff starting at 2 minutes (capped at 10): backoff specifically, rather than a
fixed delay, because the fix this failure needs is *time for sibling tasks to finish*, not anything
this task itself could do differently on a faster retry. `build_silver_table` isn't touched here —
it's exposed to the same theoretical risk (it also builds its own `local[*]` session per task,
which is exactly what "Why Spark runs local[*]..." above already flagged as a throughput
consideration) but hasn't actually failed this way, and the ask driving this change was specifically
the quality checks.

## Gold: how "largest holders of a security for a quarter" was worked out

Most of this design lives in `build_gold_holder_positions`'s own docstring in
`dags/sec_13f_pipeline.py`, which is long and specific on purpose — it's the direct answer to "what
transformations would be necessary," worked out and *verified against real data* before any code
was written (see the conversation history: checked whether `periodofreport` actually diverges from
`filing_period` within a batch — it does, one batch spanned back to `2010-12-31` — checked whether a
single filing repeats a CUSIP across multiple `infotable` rows — it does, one filing had 138 —
checked the `isamendment`/`amendmenttype`/`submissiontype` relationship and found a real
original+RESTATEMENT pair to validate the dedup rule against, rather than guessing at the schema's
semantics). This section covers what's specific to *how* it's built, not *what* it computes.

**One Spark session, two Iceberg catalogs.** Every other Iceberg-writing task in this DAG reads and
writes through a single catalog (`silver` for `build_silver_table`, `bronze_dq` for
`check_bronze_quality`). Gold is the first task that needs two at once — read from `silver`
(`infotable`, `coverpage`, `submission`), write to `gold` (`holder_positions`) — which just means
configuring both `spark.sql.catalog.silver.*` and `spark.sql.catalog.gold.*` on the same
`SparkSession.builder`, each pointed at its own `warehouse` root but sharing the same underlying
JdbcCatalog Postgres connection. No new mechanism, just two of the existing one in the same
session.

**Why `RANK()` and not `ROW_NUMBER()`.** Ties in `total_value` (two holders with the exact same
summed position) should show the same rank rather than an arbitrary tiebreak ordering — this is a
leaderboard, not a sequence.

**Performance and memory, measured, not assumed.** `build_gold_holder_positions` reads the full
124M-row `infotable` (the same one that drives `build_silver_infotable`'s and
`check_bronze_quality_infotable`'s memory sizing above) and joins/aggregates it, then writes
79,657,796 gold rows. Full 54-period run: ~10 minutes wall clock, peaked around 5.2 GB container
memory — between `build_silver_infotable`'s ~2.2 GB and `check_bronze_quality_infotable`'s ~5.1 GB,
consistent with doing genuinely more work (a three-way join plus a grouped aggregation plus a
window function) than either of those single-table passes. `spark.driver.memory=4g` was set from
the start here rather than discovered by an OOM, since the previous two tasks already established
that Spark's 1g default isn't viable against this dataset at all.

**A known, accepted gap: shared investment discretion.** `infotable.othermanager` can reference
`othermanager2.sequencenumber` when a position's investment discretion is shared across multiple
managers (`investmentdiscretion` = `DFND` or `OTR` rather than `SOLE`). `holder_positions`
attributes the full position to the filer who reported it and doesn't attempt to split or
cross-reference shared positions — a real simplification, not an oversight, and one worth revisiting
if this table's consumers ever need to answer "how much of this security does this specific manager
*directly* control" as opposed to "how much did this filing report."

## Why Trino connects with `iceberg.catalog.type=jdbc`, not a Hive metastore or Glue

Trino's Iceberg connector supports several ways to discover tables — a Hive metastore, AWS Glue,
Nessie, a REST catalog, or a JDBC-backed catalog. None of those needed standing up: every Iceberg
table this pipeline writes (`silver`, `bronze_dq`, `gold`) already lives in a JdbcCatalog backed by
the same Postgres used for Airflow's own metadata DB (see "Why Spark writes Iceberg tables directly
to S3..." above). Pointing Trino's Iceberg connector at that same Postgres with
`iceberg.catalog.type=jdbc` means Trino reads the exact same catalog state Spark wrote — no second
metastore to keep in sync, no export/import step. Each of the three Trino catalog files
(`trino/catalog/{gold,silver,bronze_dq}.properties`) sets `iceberg.jdbc-catalog.catalog-name` to
the matching Spark catalog alias (`gold`, `silver`, `bronze_dq`) and
`iceberg.jdbc-catalog.default-warehouse-dir` to the matching `warehouse` — get either wrong and
Trino would connect to Postgres successfully but see zero tables, since JdbcCatalog scopes
everything by `catalog_name`.

S3 access uses Trino's native S3 filesystem (`fs.native-s3.enabled=true` plus `s3.*` properties)
pointed at the same LocalStack endpoint everything else in this stack uses, with the same static
`test`/`test` credentials.

Two real, verified-by-running-it findings, not assumptions:

- **`node.environment` (in `trino/node.properties`) can't contain a hyphen.** The first config
  (`sec13f-dev`) failed Trino's own startup validation immediately: `Invalid configuration property
  node.environment: should match [a-z0-9][_a-z0-9]*`. Fixed to `sec13f_dev`. A small thing, but it's
  the kind of error that's only obvious once you actually start the container and read what it says
  — guessing at the fix instead would've been a waste of a cycle.
- **The property names guessed from Trino's docs on the first attempt were exactly right** —
  `iceberg.jdbc-catalog.driver-class`, `-connection-url`, `-connection-user`, `-connection-password`,
  `-catalog-name`, `-default-warehouse-dir` — confirmed by Trino's own startup log echoing back every
  catalog property it parsed, one line per property, with the actual value taking effect (not just
  "no error"). Given how often this project's other integrations needed a second attempt once
  actually tested (LocalStack's Glue gap, `bitnami/spark`'s Docker Hub move, Iceberg's `hadoop`
  catalog type failing against S3), that was worth confirming directly rather than assuming a clean
  startup log meant the config was being read at all.

End-to-end verification, against the real running stack, not a synthetic check: `SELECT count(*)
FROM gold.sec13f_gold.holder_positions` via the Trino CLI returned exactly `79657796` — the same
number Spark reported when it wrote the table. The largest-holders-of-Apple-in-Q1-2019 query from
the gold table's own docs, run through Trino instead of Spark, returned the identical ranked list
(UBS Asset Management Americas Inc first, `$3,394,155,006`, down to Bartlett & Co. Wealth Management
at rank 10) — proof Trino is reading the actual Iceberg table Spark wrote, not some other data.

## Analysis: download_zip_to_bronze failing under its own concurrency

`download_zip_to_bronze` is a dynamically-mapped task — one instance per zip `discover_zip_files`
finds, currently ~54. All 54 become eligible to run the moment `discover_zip_files` finishes, and
nothing was capping how many of them Airflow's `LocalExecutor` would start at once. A real run hit
this directly: `task_instance` shows the executor ran as many as 13 concurrently, and 9 of them
failed with `requests.exceptions.ChunkedEncodingError: Connection broken: IncompleteRead` — one
stopped at 18MB of an expected 23MB, mid-download, not a timeout or a 4xx/5xx response. That
signature means the TCP connection itself was cut, not that SEC.gov ever finished responding —
consistent with either SEC.gov's own per-IP throttling or this container's shared network path
being unable to sustain that many simultaneous multi-second transfers, and there's no way to tell
which from the client side; the fix is the same either way (send fewer requests at once), so it
wasn't necessary to determine which.

Fixed with `max_active_tis_per_dagrun=4` on `download_zip_to_bronze` — deliberately
`max_active_tis_per_dagrun` (caps concurrency of this task's mapped instances *within one DAG run*)
rather than `max_active_tis_per_dag` (a cluster-wide cap across all runs): this DAG's mapped fan-out
is entirely within a single run, so the run-scoped limit is the one that actually targets the
problem, and it doesn't accidentally throttle two genuinely-different runs against each other if
one is ever manually triggered while another is still active. 4 is deliberately conservative —
comfortably under the 13 that was observed failing — and not derived from any documented SEC
concurrency limit, since none was found; it's a starting point to tune from, not a researched
optimum.

Verified by fixing forward on the real broken run rather than a synthetic reproduction: cleared
exactly the 9 failed `download_zip_to_bronze` instances plus their 15 `upstream_failed` dependents
(scoped precisely to that one `run_id` via `DAG.clear(start_date=execution_date,
end_date=execution_date, only_failed=True)` — `only_failed` covers both `failed` and
`upstream_failed`, and using the exact execution_date timestamp rather than the CLI's day-only
`--start-date`/`--end-date` avoided sweeping in the many other DAG runs already triggered earlier
the same day). All 9 succeeded on retry, and — confirmed from `task_instance.start_date`, not
assumed — in a visible batch-of-4 pattern: four started within the same second, and each
subsequent download only started once an earlier one freed a slot, rather than all nine racing to
start at once like before.

## The gold-api/gold-ui UI

Three choices worth recording, none of them forced by the tooling:

**A FastAPI layer between the browser and Trino, rather than the browser querying Trino
directly.** Trino's HTTP protocol is stateful (a query returns a `nextUri` to poll) and its client
libraries assume a server-side caller — there's no supported browser-JS client, and even if there
were, shipping Trino's connection details (and an open, unauthenticated query endpoint) straight
to the browser is a different trust boundary than a same-origin JSON API with four fixed,
parameter-validated routes. `gold-api` is that boundary: it's the only new thing that talks to
Trino, and it exposes exactly the four queries the UI needs, nothing ad hoc.

**Every value that reaches SQL is a bound parameter, not a string-interpolated one** —
`cursor.execute(sql, params)` with `?` placeholders. Confirmed directly against the installed
client (`trino==0.328.0`, `trino.dbapi.paramstyle == "qmark"`) by running a real parameterized
query against the live `gold` catalog, rather than assumed: it initially failed with
`Cannot apply operator: date = varchar(10)` when a `periodofreport` filter was bound as a plain
ISO string, and succeeded once bound as an actual `datetime.date` — worth remembering if a future
route filters on another date/timestamp column. `cik` and `cusip` are still regex-validated
(`CIK_RE`/`CUSIP_RE` in `ui/api/main.py`) before use, but only for a clean 400 instead of a Trino
type error, not as the injection defense — the bound parameter is what actually prevents that.
The one hand-escaped value is the LIKE search term (`_like_term`), and that escaping is for a
different reason: LIKE's own `%`/`_` wildcards inside a *bound* value still mean "match anything"
to LIKE, so a literal search for `50%` needed its `%` escaped to search for that literal text
rather than becoming a wildcard — bound parameters stop SQL injection, not LIKE-wildcard
injection, and the two needed separate fixes.

**`gold-ui`'s container runs `vite preview`, not a build copied into nginx.** This is a
single-page, low-traffic local dev tool — `vite preview` already serves the production `dist/`
build correctly over plain HTTP, and adding an nginx stage would just be another image and another
config file to keep in sync for no behavioral difference at this scale. Revisit if the UI ever
needs to run somewhere that isn't a docker-compose dev stack.

One non-choice worth being explicit about: `VITE_API_BASE_URL` (`ui/web/.env`) is baked in at
*build* time as `http://localhost:8000`, not read at container runtime — Vite only inlines
`import.meta.env.VITE_*` values during `vite build`, and more fundamentally, the code reading that
value runs in the user's browser, outside the compose network entirely, so it was never going to
be able to resolve `gold-api` (the compose service name) even if the value were runtime-configurable.
The host-published port is the only address that's ever reachable from there.

## Implementation notes

- **SEC User-Agent policy**: SEC.gov requires a descriptive `User-Agent` header with contact info
  on automated requests, or it will throttle/block the client. The DAG sends
  `CapitalTG-SEC13F-Pipeline/1.0 (ahuynh@capitaltg.com)` — update the contact if you fork this.
- **SEC filename formats vary**: through 2024 quarterly files were named `<YYYY>q<N>_form13f.zip`;
  since the mid-2024 rolling-window change they're `<DDmonYYYY>-<DDmonYYYY>_form13f.zip`.
  `discover_zip_files` uses the zip's filename stem itself as the partition key so it doesn't need
  to know which scheme a given file uses.
- **Bronze is untouched bytes**: `download_zip_to_bronze` uploads the exact response body from
  SEC, under the original filename, with no unzip/parse step — that's the "as-is" bronze
  guarantee. All parsing happens in the silver task.
- **Schema inference, and why a few columns are forced back to string**: silver used to read every
  column as `StringType` (no `inferSchema`) specifically to dodge type mismatches across periods.
  It now infers real types (`int`, `bigint`, etc. — see README.md's "Silver table schemas") by
  reading *all* staged periods for a table in one combined `spark.read.csv([...])` call rather than
  inferring per period and unioning the results: `unionByName(allowMissingColumns=True)` only
  reconciles *missing* columns, not two DataFrames disagreeing on a shared column's *type* (e.g.
  one period inferring `value` as `int`, another as `bigint`) — one inference pass across the whole
  table's history avoids that class of failure entirely, and confirmed real, at full 54-period
  scale: `sshprnamt` and the `voting_auth_*` columns only widen to `bigint` once enough history is
  present that some value exceeds the 32-bit range.
  Full-column inference also turned out to matter for a different reason:
  `filingmanager_zipcode` correctly inferred as `string` instead of `int` specifically because
  Spark scans the *entire* column (not a sample) and found at least one hyphenated value
  (`100-210`) somewhere across the full history — a sample-based inference could easily have missed
  that and produced a column that silently breaks the first time a real ZIP+4/foreign postal code
  shows up.
  A handful of columns are force-kept as `string` regardless of what inference would otherwise
  pick (`PROTECTED_STRING_COLUMNS` in the DAG): `cik` and `crdnumber` are confirmed zero-padded in
  real filings (e.g. CIK `0000823621`, CRD `000307644`) — numeric inference silently strips those
  leading zeros, and unlike `filingmanager_zipcode`'s hyphens, nothing in a *purely-numeric*
  zero-padded value would ever naturally stop Spark from inferring it as an integer. This was
  caught the hard way: an early version of this fix only protected `cik`, verified against a
  period where `crdnumber` happened to be entirely blank, and concluded (wrongly) that it didn't
  need protecting — running a period where `crdnumber` was actually populated showed the same
  leading-zero corruption. `accession_number`, `secfilenumber`, and `form13ffilenumber` are
  protected too, even though their hyphens already keep them out of numeric inference in every
  period checked so far — trusting "looks safe in the periods I checked" is exactly what went
  wrong with `crdnumber`.
- **Missing files across periods are skipped, not errors**: e.g. `SUMMARYPAGE.tsv` doesn't exist
  in the earliest zips. `build_silver_table` skips a bronze zip that doesn't contain the file it's
  looking for, logging a warning so a genuinely missing file is at least visible in the task log.
- **Zip members are matched by basename, not full path**: most SEC zips store their flat files at
  the zip root (`INFOTABLE.tsv`), but at least one real period (`01jun2025-31aug2025`) nests
  everything under a subdirectory instead (`01JUN2025-31AUG2025_form13f/INFOTABLE.tsv`) — found by
  actually running this against the live SEC archive, not assumed. An exact full-path match treated
  that as "file missing" and silently dropped that entire period from every one of the seven
  canonical tables, not just INFOTABLE. Matching on `os.path.basename(member)` instead handles both
  layouts.
- **Memory safety is Spark's job now, not a hand-rolled loop**: an earlier pandas version
  downloaded every bronze zip, decompressed its target file, and held every period's DataFrame in
  a list before one big `pd.concat` + single Parquet write. Against the real SEC history (54
  periods back to 2013, ~3 GB of zips) that OOM-killed `build_silver_infotable` — `INFOTABLE.tsv`
  is the actual per-position holdings table and dwarfs the other six files (a single early,
  small-filer quarter already had an 8.5 MB `INFOTABLE.tsv` against 30 KB for `COVERPAGE.tsv`);
  held as pandas `object`-dtype strings for all 54 periods simultaneously, peak memory reached
  into the tens of GB and the kernel `SIGKILL`'d the process. Airflow can't distinguish a killed
  process from a hung one — it just saw the task's heartbeat go stale and reported it as a zombie.
  The Spark version reads each staged period as its own DataFrame, `unionByName`s them (lazy — no
  data actually moves until the write action), and writes the result — Spark's own out-of-core
  execution engine keeps this memory-bounded rather than a manual "flush after each period" loop.
  (The Iceberg write itself needs its own heap-size tuning on top of this — see "Sizing Spark's
  heap for Iceberg's writer" above.) `unionByName(allowMissingColumns=True)` also handles schema
  drift between periods (e.g. a column SEC added partway through) directly, instead of relying on
  separate per-period files never being read together.
- **Silver tables are genuinely partitioned Iceberg tables, not one big file**: each
  `build_silver_*` task creates `silver.sec13f.<table>` (Iceberg's own naming inside its catalog)
  `partitionedBy("filing_period")`, landing at
  `s3://sec-13f-lake/silver/form13f/sec13f/<table>/{data,metadata}/...` — Parquet data files under
  `data/filing_period=<value>/`, and Avro/JSON Iceberg metadata (manifests, manifest lists,
  `metadata.json`) under `metadata/`. Verified against the full 124M-row, 54-partition INFOTABLE
  history: re-reading the table back with a fresh Spark session confirms Iceberg's own
  `.partitions` metadata table sums to exactly the same row count as `SELECT count(*)`.
- **Extraction is shared, not duplicated, between the two bronze-facing stages**: both
  `check_bronze_quality` and `build_silver_table` pull the zip-download/basename-match/stream-to-
  disk logic from the same `_stage_periods` helper, so there's exactly one implementation of "how
  to get a canonical file's rows out of a bronze zip" to keep correct — the nested-subdirectory
  fix and the memory-safe one-zip-at-a-time streaming both apply to both stages automatically,
  rather than needing to be ported between two copies of the same loop.
- **The quarantine mechanism was verified with a real failure, not just a passing run**: a
  synthetic bronze zip (`COVERPAGE.tsv` with one row whose `amendmentno` is the literal text
  `NOT_A_NUMBER`) was uploaded and run through `check_bronze_quality_coverpage` for real. Result:
  1 of 2 rows quarantined, the failing row present in `bronze_dq.sec13f_dq.coverpage` with its
  original, unmodified `amendmentno` value intact and `dq_failed_columns = "amendmentno"`; the
  clean row not present. Every other verification in this project up to this point had only ever
  exercised the "data is clean" path (real SEC data essentially always validates, since SEC
  enforces its own format at filing time) — this is the one test that actually proves the failure
  path works, not just that it doesn't get in the way when nothing fails.
