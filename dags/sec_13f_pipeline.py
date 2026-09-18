"""Medallion-style ingestion of the SEC's Form 13F structured data sets.

Bronze : scrape the SEC index page, download each quarterly/period zip, and
         persist it to S3 exactly as downloaded (no parsing).
Bronze data-quality gate: an additional bronze-layer stage (output lives
         under bronze/, not silver/) that runs after the raw zip download and
         before silver. Re-extracts each canonical file the same way silver
         does and validates every row against SILVER_SCHEMAS — the explicit,
         hand-maintained type contract silver's own schema inference is
         checked against — by attempting to cast each non-string column's
         raw text to its declared type. Rows with at least one value that
         fails to cast are quarantined, untouched and all-string, into a
         mirrored Iceberg table (same columns as the corresponding silver
         table, but every column string-typed, plus `dq_failed_columns`
         naming what tripped it up) for review and remediation. This is a
         visibility/remediation queue, not a hard gate — silver still
         independently re-extracts and re-derives its own types from bronze
         regardless of what this stage finds.
Silver : for each canonical flat-file name (INFOTABLE.tsv, COVERPAGE.tsv, ...),
         pull that file out of every bronze zip *one period at a time* into a
         local staging directory (plain Python — Spark has no zip codec),
         then hand all staged periods to a local PySpark session in one
         schema-inferring read (stronger, per-column types instead of
         treating every field as a string) and write the result as an
         Apache Iceberg table (Parquet data files + Avro/JSON Iceberg
         metadata) directly to S3, and register it as a Glue staging table
         pointing at that metadata.
Gold   : build_gold_holder_positions builds one curated fact table,
         holder_positions, answering "who are the largest holders of a given
         security for a specified quarter?" — joins silver's infotable,
         coverpage, and submission (not bronze), resolves the true reporting
         quarter, deduplicates amendments, and aggregates per (holder,
         quarter, security) before ranking by position size. See that task's
         docstring and IMPLEMENTATION.md for the full analysis.

S3 calls go to LocalStack (AWS_ENDPOINT_URL_S3). Glue calls are mocked with
moto's mock_aws() instead, since LocalStack's Community edition doesn't
implement Glue at all. moto patches boto3/botocore for the lifetime of the
`with mock_aws():` block only, and each Airflow task runs in its own process,
so a Glue table "registered" in one task's mock session is gone by the time
any other task (or a manual verification step) could look for it. That's a
known, accepted limitation for now, not a bug to fix.

Spark runs in local[*] mode inside the same task process rather than against
a standalone cluster — driver and executors are the same JVM, so there's no
cross-process Spark/Python version to keep in sync (a real, previously-hit
failure mode: see git history).

Silver tables use Iceberg's JdbcCatalog (backed by the same Postgres already
running for Airflow's metadata DB — catalog bookkeeping only, not pipeline
data), not Iceberg's own GlueCatalog. That's deliberate, not a missing
feature: Iceberg's GlueCatalog would make Glue API calls from inside the
JVM, via Java's AWS SDK — completely invisible to moto, which only patches
boto3/botocore in this Python process. Iceberg's path-based "hadoop" catalog
type was tried first instead of JdbcCatalog and rejected: some of its
catalog-level bookkeeping goes through Hadoop's classic FileSystem API
(`UnsupportedFileSystemException: No FileSystem for scheme "s3"` without
hadoop-aws on the classpath) even though data/metadata file I/O otherwise
goes through Iceberg's own S3FileIO (AWS SDK v2, independent of Hadoop's
s3a/hadoop-aws) — JdbcCatalog never touches Hadoop's FileSystem API at all.
Either way, this Python process still does the actual Glue registration
afterward via boto3 + mock_aws(), exactly like the Parquet version did, just
describing the table as Iceberg (Parameters.table_type=ICEBERG +
Parameters.metadata_location, read straight out of JdbcCatalog's own
Postgres row) instead of a plain Hive/Parquet external table.

Unlike plain Parquet, Iceberg's metadata (manifest lists, manifest files,
metadata.json) embeds the *absolute* location of every data file. That's why
Spark writes straight to LocalStack S3 here instead of the "write to local
disk, then boto3-upload" pattern used for bronze and the earlier Parquet
silver tables — relocating an Iceberg table's files after the fact would
require rewriting all of that embedded metadata, which is a different
problem than just moving bytes.
"""
from __future__ import annotations

import logging
import os
import re
import shutil
import tempfile
import zipfile
from datetime import datetime, timedelta
from functools import reduce

import boto3
import psycopg2
import requests
from bs4 import BeautifulSoup
from moto import mock_aws
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from airflow.decorators import dag, task

SEC_13F_INDEX_URL = "https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets"
RAW_BUCKET = "sec-13f-lake"
BRONZE_PREFIX = "bronze/form13f"
SILVER_PREFIX = "silver/form13f"
GLUE_DATABASE = "sec13f_staging"
# Namespace inside Spark's path-based Iceberg catalog (see build_silver_table)
# — unrelated to GLUE_DATABASE, which is what the table is registered under
# in Glue itself.
ICEBERG_NAMESPACE = "sec13f"
# Data-quality quarantine tables (check_bronze_quality) live under bronze/,
# not silver/ — they're a bronze-layer concern (raw data that failed a type
# check), not a modeled silver output — in their own Iceberg catalog
# namespace and Glue database so they never show up mixed in with the real
# tables.
BRONZE_DQ_PREFIX = "bronze/dq_failed"
DQ_NAMESPACE = "sec13f_dq"
GLUE_DQ_DATABASE = "sec13f_dq_failed"
DQ_FAILED_COLUMNS_FIELD = "dq_failed_columns"
# Gold: business-ready aggregates built from silver, not from bronze — own
# Iceberg catalog namespace/Glue database, same reasoning as bronze_dq.
GOLD_PREFIX = "gold/form13f"
GOLD_NAMESPACE = "sec13f_gold"
GLUE_GOLD_DATABASE = "sec13f_gold"
# Iceberg's JdbcCatalog stores table/namespace pointers as rows in this same
# Postgres instance (already running for Airflow's own metadata DB) — this is
# catalog bookkeeping only, not pipeline data, so reusing it is fine.
ICEBERG_CATALOG_PG_HOST = "postgres"
ICEBERG_CATALOG_PG_DB = "airflow"
ICEBERG_CATALOG_JDBC_URI = f"jdbc:postgresql://{ICEBERG_CATALOG_PG_HOST}:5432/{ICEBERG_CATALOG_PG_DB}"
ICEBERG_CATALOG_JDBC_USER = "airflow"
ICEBERG_CATALOG_JDBC_PASSWORD = "airflow"
CANONICAL_FILES = [
    "SUBMISSION.tsv",
    "COVERPAGE.tsv",
    "OTHERMANAGER.tsv",
    "OTHERMANAGER2.tsv",
    "SIGNATURE.tsv",
    "SUMMARYPAGE.tsv",
    "INFOTABLE.tsv",
]
# SEC requires a descriptive User-Agent with contact info on automated requests.
USER_AGENT = "CapitalTG-SEC13F-Pipeline/1.0 (ahuynh@capitaltg.com)"
# Identifier columns forced to stay string regardless of what schema
# inference would otherwise pick, because they're not measures — inferring
# them numeric would be actively wrong, not just less useful. CIK and
# CRDNUMBER are confirmed zero-padded in real filings (e.g. CIK
# "0000823621", CRDNUMBER "000307644"); numeric inference silently strips
# those leading zeros, corrupting the canonical form. ACCESSION_NUMBER,
# SECFILENUMBER, and FORM13FFILENUMBER are protected defensively even though
# their hyphens (e.g. "0001487438-13-000014", "801-131761") already keep
# them out of numeric inference in every period checked so far — an
# assumption from limited sampling is exactly what let CRDNUMBER's zero
# padding get silently stripped in the first place.
PROTECTED_STRING_COLUMNS = {
    "accession_number",
    "cik",
    "crdnumber",
    "secfilenumber",
    "form13ffilenumber",
}

# The explicit type contract check_bronze_quality validates bronze data
# against. Silver's own schema is *inferred*, not read from here — this is a
# separate, hand-maintained snapshot of what that inference has actually
# produced against the real, full-history data (verified and documented in
# README.md's "Silver table schemas"), used as a stable target for data
# quality checks. If SEC's data legitimately drifts wider than this (e.g. a
# share count finally exceeds bigint), that shows up as quarantined rows
# here rather than silently reinterpreting itself the way pure inference
# would — this registry needs a deliberate update in that case, not silence.
SILVER_SCHEMAS: dict[str, StructType] = {
    "submission": StructType(
        [
            StructField("accession_number", StringType(), True),
            StructField("filing_date", StringType(), True),
            StructField("submissiontype", StringType(), True),
            StructField("cik", StringType(), True),
            StructField("periodofreport", StringType(), True),
        ]
    ),
    "coverpage": StructType(
        [
            StructField("accession_number", StringType(), True),
            StructField("reportcalendarorquarter", StringType(), True),
            StructField("isamendment", StringType(), True),
            StructField("amendmentno", IntegerType(), True),
            StructField("amendmenttype", StringType(), True),
            StructField("confdeniedexpired", StringType(), True),
            StructField("datedeniedexpired", StringType(), True),
            StructField("datereported", StringType(), True),
            StructField("reasonfornonconfidentiality", StringType(), True),
            StructField("filingmanager_name", StringType(), True),
            StructField("filingmanager_street1", StringType(), True),
            StructField("filingmanager_street2", StringType(), True),
            StructField("filingmanager_city", StringType(), True),
            StructField("filingmanager_stateorcountry", StringType(), True),
            StructField("filingmanager_zipcode", StringType(), True),
            StructField("reporttype", StringType(), True),
            StructField("form13ffilenumber", StringType(), True),
            StructField("crdnumber", StringType(), True),
            StructField("secfilenumber", StringType(), True),
            StructField("provideinfoforinstruction5", StringType(), True),
            StructField("additionalinformation", StringType(), True),
        ]
    ),
    "othermanager": StructType(
        [
            StructField("accession_number", StringType(), True),
            StructField("othermanager_sk", IntegerType(), True),
            StructField("cik", StringType(), True),
            StructField("form13ffilenumber", StringType(), True),
            StructField("crdnumber", StringType(), True),
            StructField("secfilenumber", StringType(), True),
            StructField("name", StringType(), True),
        ]
    ),
    "othermanager2": StructType(
        [
            StructField("accession_number", StringType(), True),
            StructField("sequencenumber", IntegerType(), True),
            StructField("cik", StringType(), True),
            StructField("form13ffilenumber", StringType(), True),
            StructField("crdnumber", StringType(), True),
            StructField("secfilenumber", StringType(), True),
            StructField("name", StringType(), True),
        ]
    ),
    "signature": StructType(
        [
            StructField("accession_number", StringType(), True),
            StructField("name", StringType(), True),
            StructField("title", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("signature", StringType(), True),
            StructField("city", StringType(), True),
            StructField("stateorcountry", StringType(), True),
            StructField("signaturedate", StringType(), True),
        ]
    ),
    "summarypage": StructType(
        [
            StructField("accession_number", StringType(), True),
            StructField("otherincludedmanagerscount", IntegerType(), True),
            StructField("tableentrytotal", IntegerType(), True),
            StructField("tablevaluetotal", LongType(), True),
            StructField("isconfidentialomitted", StringType(), True),
        ]
    ),
    "infotable": StructType(
        [
            StructField("accession_number", StringType(), True),
            StructField("infotable_sk", IntegerType(), True),
            StructField("nameofissuer", StringType(), True),
            StructField("titleofclass", StringType(), True),
            StructField("cusip", StringType(), True),
            StructField("figi", StringType(), True),
            StructField("value", LongType(), True),
            StructField("sshprnamt", LongType(), True),
            StructField("sshprnamttype", StringType(), True),
            StructField("putcall", StringType(), True),
            StructField("investmentdiscretion", StringType(), True),
            StructField("othermanager", StringType(), True),
            StructField("voting_auth_sole", LongType(), True),
            StructField("voting_auth_shared", LongType(), True),
            StructField("voting_auth_none", LongType(), True),
        ]
    ),
}

logger = logging.getLogger(__name__)


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL_S3"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
    )


def _stage_periods(s3, bronze_keys: list[str], filename: str, stage_dir: str) -> list[str]:
    """Extract `filename` out of every bronze zip, one period at a time, into
    stage_dir/filing_period=<period>/data.tsv. Shared by check_bronze_quality
    and build_silver_table so both stages see exactly the same staged data.

    One zip is downloaded, opened, and discarded before the next starts —
    never held as one big in-memory blob — because INFOTABLE alone is tens
    of millions of rows across 13+ years; see IMPLEMENTATION.md for the OOM
    this avoids. A period is skipped, with a warning, if its zip doesn't
    contain `filename` at all (e.g. SUMMARYPAGE.tsv doesn't exist in the
    earliest zips) — not an error, since that's expected schema evolution.
    """
    periods_staged = []
    for key in bronze_keys:
        period = key.split("/")[-2]
        fd, local_zip_path = tempfile.mkstemp(suffix=".zip")
        os.close(fd)
        try:
            s3.download_file(RAW_BUCKET, key, local_zip_path)
            with zipfile.ZipFile(local_zip_path) as archive:
                # Match on basename, not the full path: most SEC zips store
                # files flat at the zip root, but at least one observed
                # period (01jun2025-31aug2025) nests everything under a
                # subdirectory instead.
                member = next(
                    (m for m in archive.namelist() if os.path.basename(m).lower() == filename.lower()),
                    None,
                )
                if member is None:
                    logger.warning("%s has no %s — skipping this period", key, filename)
                    continue
                period_dir = os.path.join(stage_dir, f"filing_period={period}")
                os.makedirs(period_dir, exist_ok=True)
                with archive.open(member) as src, open(os.path.join(period_dir, "data.tsv"), "wb") as dst:
                    shutil.copyfileobj(src, dst)
        finally:
            os.remove(local_zip_path)
        periods_staged.append(period)
    return periods_staged


def _tag_schema_failures(df, target_schema: StructType):
    """Tag every row of an all-string DataFrame with which of its columns
    (if any) don't conform to target_schema's declared type, by attempting
    `try_cast(col AS <type>)` — which yields NULL on a bad value instead of
    raising — for every non-string column. A column is only flagged if it
    has an actual (non-blank) value that fails to cast; a genuinely missing
    value is not a data-quality failure. String-typed target columns are
    never flagged — any text is a valid string.

    Adds one column, `dq_failed_columns` (array<string>), rather than
    mutating anything — the row's original values are untouched either way,
    which is what lets a failed row be quarantined completely unmodified.
    """
    failure_markers = [
        F.when(
            F.col(f"`{field.name}`").isNotNull()
            & (F.trim(F.col(f"`{field.name}`")) != "")
            & F.expr(f"try_cast(`{field.name}` as {field.dataType.simpleString()})").isNull(),
            F.lit(field.name),
        )
        for field in target_schema.fields
        if not isinstance(field.dataType, StringType) and field.name in df.columns
    ]
    if not failure_markers:
        return df.withColumn(DQ_FAILED_COLUMNS_FIELD, F.expr("cast(array() as array<string>)"))
    return df.withColumn(DQ_FAILED_COLUMNS_FIELD, F.filter(F.array(*failure_markers), lambda x: x.isNotNull()))


def _glue_column_type(spark_type) -> str:
    """Map an inferred Spark/Iceberg column type to the Hive/Glue type string
    Glue's Columns list expects."""
    if isinstance(spark_type, StringType):
        return "string"
    if isinstance(spark_type, BooleanType):
        return "boolean"
    if isinstance(spark_type, (ByteType, ShortType)):
        return "smallint"
    if isinstance(spark_type, IntegerType):
        return "int"
    if isinstance(spark_type, LongType):
        return "bigint"
    if isinstance(spark_type, FloatType):
        return "float"
    if isinstance(spark_type, DoubleType):
        return "double"
    if isinstance(spark_type, DecimalType):
        return f"decimal({spark_type.precision},{spark_type.scale})"
    if isinstance(spark_type, DateType):
        return "date"
    if isinstance(spark_type, TimestampType):
        return "timestamp"
    return "string"


def _glue_iceberg_table_input(
    table_name: str, columns: list[tuple[str, str]], s3_location: str, metadata_location: str
) -> dict:
    """Glue's actual representation of a native Iceberg table: the schema/
    partition source of truth is the Iceberg metadata.json referenced by
    Parameters.metadata_location, not StorageDescriptor — Columns is included
    for basic introspection compatibility, same as real AWS Glue does.
    """
    return {
        "Name": table_name,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "table_type": "ICEBERG",
            "metadata_location": metadata_location,
        },
        "StorageDescriptor": {
            "Columns": [{"Name": name, "Type": glue_type} for name, glue_type in columns],
            "Location": s3_location,
        },
    }


@dag(
    dag_id="sec_13f_pipeline",
    schedule="@quarterly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sec", "13f", "bronze", "data-quality", "silver", "gold", "glue"],
)
def sec_13f_pipeline():
    @task
    def discover_zip_files() -> list[dict]:
        response = requests.get(SEC_13F_INDEX_URL, headers={"User-Agent": USER_AGENT}, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        discovered = []
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if not href.lower().endswith(".zip"):
                continue
            url = href if href.startswith("http") else f"https://www.sec.gov{href}"
            basename = href.rsplit("/", 1)[-1]
            # Filenames were "<YYYY>q<N>_form13f.zip" through 2024; since the
            # mid-2024 rolling-window change they're
            # "<DDmonYYYY>-<DDmonYYYY>_form13f.zip". Use the filename stem
            # itself as the partition key so both schemes get a unique,
            # collision-free period label.
            match = re.match(r"(.+?)_form13f\.zip$", basename, re.IGNORECASE)
            period = match.group(1).lower() if match else basename.lower()
            discovered.append({"url": url, "period": period, "basename": basename})
        return discovered

    @task(max_active_tis_per_dagrun=4)
    def download_zip_to_bronze(file_info: dict) -> str:
        """Bronze layer: persist the SEC zip exactly as downloaded, no parsing.

        `max_active_tis_per_dagrun=4` caps how many of this task's ~54
        mapped instances (one per discovered zip) Airflow will run at once
        within a single DAG run — found necessary by actually running this:
        with all 54 eligible simultaneously, Airflow's LocalExecutor ran as
        many as 13 concurrently, and several failed with
        `requests.exceptions.ChunkedEncodingError: Connection broken:
        IncompleteRead` mid-download (one stopped at 18MB of an expected
        23MB) — that many large, multi-second downloads competing for the
        same network path (SEC.gov's own per-IP throttling, this container's
        shared bandwidth, or both) was enough to break connections outright,
        not just slow them down. 4 is a deliberately conservative number
        picked to sit well under the 13 that was observed failing, not a
        value derived from a documented SEC limit — see IMPLEMENTATION.md.
        """
        response = requests.get(file_info["url"], headers={"User-Agent": USER_AGENT}, timeout=180)
        response.raise_for_status()

        key = f"{BRONZE_PREFIX}/{file_info['period']}/{file_info['basename']}"
        _s3_client().put_object(Bucket=RAW_BUCKET, Key=key, Body=response.content)
        return key

    bronze_keys = download_zip_to_bronze.expand(file_info=discover_zip_files())

    @task(
        # check_bronze_quality tasks for every canonical file all become
        # runnable the moment their bronze download finishes, so several of
        # their local[*] Spark sessions (each asking for up to
        # spark.driver.memory=4g) regularly start within the same
        # fraction of a second and run concurrently. A real run hit this
        # directly: 6 check_bronze_quality_* tasks started within 0.2s of
        # each other, and 2 of them (coverpage, infotable) failed mid-task
        # with `ConnectionRefusedError: [Errno 111]` from Py4J — the JVM
        # backing their Spark session had died (most likely the Linux
        # kernel's OOM killer picking a victim process under the combined
        # memory pressure of that many concurrent JVMs; the container
        # itself wasn't killed, `docker inspect .State.OOMKilled` was
        # false, consistent with a per-process kill inside the cgroup, not
        # the whole container). Both failures cleared on their very next
        # attempt, once sibling tasks had finished and freed memory — the
        # signature of a transient, connectivity-shaped failure, which is
        # exactly what retries are for; see IMPLEMENTATION.md for the full
        # analysis. Backoff (not a fixed delay) matters here specifically
        # because the contention comes from *sibling tasks*, not this task
        # itself — the fix is time for them to finish, not for this task to
        # do anything differently, so each retry should wait longer than
        # the last rather than immediately re-competing for the same memory.
        retries=3,
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=10),
    )
    def check_bronze_quality(filename: str, bronze_keys: list[str]) -> dict:
        """Additional bronze-layer stage, between bronze and silver: validate
        every bronze row for `filename` against SILVER_SCHEMAS and quarantine
        the ones that fail into a mirrored, all-string Iceberg table for
        review and remediation. See the module docstring and
        IMPLEMENTATION.md for the full rationale.

        Retries 3 times with exponential backoff (starting at 2 minutes) on
        any failure — motivated specifically by an observed transient
        connectivity failure between this task's Python driver and its own
        local Spark JVM under concurrent-task memory pressure; see the
        `@task(...)` arguments above and IMPLEMENTATION.md for the full
        analysis. This does *not* discriminate by exception type — a
        genuine bug in this task would also be retried 3 times before
        ultimately failing, costing time but not correctness.

        Re-extracts `filename` from bronze exactly like build_silver_table
        does (shared via _stage_periods), but reads every period with no
        schema at all — Spark's CSV reader defaults to all-string when
        `inferSchema` isn't set — and unions them by name. Unlike silver,
        there's no type-inference-mismatch risk here to design around: every
        column is uniformly a string on every side of the union, so
        `unionByName(allowMissingColumns=True)` only ever has column
        *presence* differences to reconcile, never a type clash.

        Each row is tagged with which columns (if any) fail a `try_cast` to
        SILVER_SCHEMAS' declared type (_tag_schema_failures); only the
        failing rows — still in their original, unmodified string form — are
        written out, to `bronze_dq.<DQ_NAMESPACE>.<table>` (S3 prefix
        BRONZE_DQ_PREFIX, Glue database GLUE_DQ_DATABASE). Passing rows are
        counted for the task log but not persisted anywhere: this stage is a
        quality report and remediation queue, not a data store, and silver
        doesn't read from it — silver independently re-extracts and
        re-derives its own types from bronze regardless of what this stage
        finds.
        """
        s3 = _s3_client()
        table_name = filename.removesuffix(".tsv").lower()
        target_schema = SILVER_SCHEMAS[table_name]
        table_key_prefix = f"{BRONZE_DQ_PREFIX}/{DQ_NAMESPACE}/{table_name}"

        stage_dir = tempfile.mkdtemp(prefix=f"dq_stage_{table_name}_")
        try:
            periods_staged = _stage_periods(s3, bronze_keys, filename, stage_dir)
            if not periods_staged:
                raise ValueError(f"No bronze zip contained {filename}")

            s3_endpoint = os.environ["AWS_ENDPOINT_URL_S3"]
            region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
            warehouse = f"s3://{RAW_BUCKET}/{BRONZE_DQ_PREFIX}"

            spark = (
                SparkSession.builder.appName(f"sec-13f-dq-{table_name}")
                .master("local[*]")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.memory", "4g")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.bronze_dq", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.bronze_dq.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
                .config("spark.sql.catalog.bronze_dq.uri", ICEBERG_CATALOG_JDBC_URI)
                .config("spark.sql.catalog.bronze_dq.jdbc.user", ICEBERG_CATALOG_JDBC_USER)
                .config("spark.sql.catalog.bronze_dq.jdbc.password", ICEBERG_CATALOG_JDBC_PASSWORD)
                .config("spark.sql.catalog.bronze_dq.warehouse", warehouse)
                .config("spark.sql.catalog.bronze_dq.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .config("spark.sql.catalog.bronze_dq.s3.endpoint", s3_endpoint)
                .config("spark.sql.catalog.bronze_dq.s3.path-style-access", "true")
                .config("spark.sql.catalog.bronze_dq.client.region", region)
                .getOrCreate()
            )
            try:
                period_frames = []
                for period in periods_staged:
                    period_path = os.path.join(stage_dir, f"filing_period={period}", "data.tsv")
                    frame = spark.read.option("header", True).option("sep", "\t").csv(period_path)
                    frame = frame.toDF(*[c.lower() for c in frame.columns])
                    frame = frame.withColumn("filing_period", F.lit(period))
                    period_frames.append(frame)
                raw_df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), period_frames)

                tagged = _tag_schema_failures(raw_df, target_schema).persist()
                try:
                    total_rows = tagged.count()
                    invalid_df = tagged.filter(F.size(DQ_FAILED_COLUMNS_FIELD) > 0).withColumn(
                        DQ_FAILED_COLUMNS_FIELD, F.concat_ws(",", F.col(DQ_FAILED_COLUMNS_FIELD))
                    )
                    invalid_rows = invalid_df.count()

                    all_columns = [(field.name, "string") for field in target_schema.fields] + [
                        ("filing_period", "string"),
                        (DQ_FAILED_COLUMNS_FIELD, "string"),
                    ]

                    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS bronze_dq.{DQ_NAMESPACE}")
                    (
                        invalid_df.writeTo(f"bronze_dq.{DQ_NAMESPACE}.{table_name}")
                        .using("iceberg")
                        .partitionedBy("filing_period")
                        .createOrReplace()
                    )
                finally:
                    tagged.unpersist()
            finally:
                spark.stop()
        finally:
            shutil.rmtree(stage_dir, ignore_errors=True)

        pg_conn = psycopg2.connect(
            host=ICEBERG_CATALOG_PG_HOST,
            dbname=ICEBERG_CATALOG_PG_DB,
            user=ICEBERG_CATALOG_JDBC_USER,
            password=ICEBERG_CATALOG_JDBC_PASSWORD,
        )
        try:
            with pg_conn.cursor() as cur:
                cur.execute(
                    "SELECT metadata_location FROM iceberg_tables "
                    "WHERE catalog_name = %s AND table_namespace = %s AND table_name = %s",
                    ("bronze_dq", DQ_NAMESPACE, table_name),
                )
                metadata_location = cur.fetchone()[0]
        finally:
            pg_conn.close()
        s3_location = f"s3://{RAW_BUCKET}/{table_key_prefix}/"

        with mock_aws():
            glue = boto3.client("glue", region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
            glue.create_database(DatabaseInput={"Name": GLUE_DQ_DATABASE})
            glue.create_table(
                DatabaseName=GLUE_DQ_DATABASE,
                TableInput=_glue_iceberg_table_input(table_name, all_columns, s3_location, metadata_location),
            )
            logger.info(
                "Data quality check for %s: %d/%d rows failed schema validation, quarantined at %s",
                table_name,
                invalid_rows,
                total_rows,
                metadata_location,
            )

        return {"table": table_name, "total_rows": total_rows, "invalid_rows": invalid_rows}

    @task
    def build_silver_table(filename: str, bronze_keys: list[str]) -> str:
        """Silver layer: pull `filename` out of every bronze zip, merge across
        periods with Spark, and write the result as an Apache Iceberg table
        directly in S3, then register it as a Glue staging table.

        Three phases:

        1. Extraction (plain Python): one bronze zip at a time is downloaded
           to a temp file, the target member is located by basename (some
           SEC zips nest files under a subdirectory instead of storing them
           flat — an exact full-path match silently drops those periods) and
           streamed straight to a local staging file, never held as one big
           in-memory blob. This has to be Python — Spark has no zip codec.
        2. Merge + write (Spark, local[*], Iceberg): every staged period is
           read in *one* combined `spark.read.csv([...periods])` call with
           `inferSchema=True`, so Spark infers a single, consistent type per
           column across the whole table's history instead of inferring per
           period and risking a type clash when combining them (e.g. one
           period's `value` column parsing as integer, another's as double —
           `unionByName` reconciles missing *columns*, not mismatched
           *types* for a column present on both sides). `filing_period` is
           recovered from each row's source path afterward. The result is
           written via `writeTo(...).using("iceberg")` into a JdbcCatalog
           rooted at this table's S3 prefix. Unlike the plain-Parquet
           version, Spark writes straight to S3 here — an Iceberg table's
           metadata embeds the absolute location of every data file, so it
           can't be written locally and relocated after.
        3. Glue registration (plain Python + moto): JdbcCatalog never talks
           to Glue itself (that's the point — it lets moto keep mocking Glue
           at the boto3 level instead of needing to intercept a JVM-side
           Glue client it can't see), so this process reads the table's
           current metadata.json location straight out of JdbcCatalog's own
           Postgres row and registers it in Glue itself, with Iceberg-shaped
           `TableInput` reflecting the inferred column types.
        """
        s3 = _s3_client()
        table_name = filename.removesuffix(".tsv").lower()
        table_key_prefix = f"{SILVER_PREFIX}/{ICEBERG_NAMESPACE}/{table_name}"

        stage_dir = tempfile.mkdtemp(prefix=f"silver_stage_{table_name}_")
        try:
            periods_staged = _stage_periods(s3, bronze_keys, filename, stage_dir)
            if not periods_staged:
                raise ValueError(f"No bronze zip contained {filename}")

            s3_endpoint = os.environ["AWS_ENDPOINT_URL_S3"]
            region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
            warehouse = f"s3://{RAW_BUCKET}/{SILVER_PREFIX}"

            spark = (
                SparkSession.builder.appName(f"sec-13f-silver-{table_name}")
                .master("local[*]")
                .config("spark.ui.enabled", "false")
                # local[*] runs the "executor" inside the driver JVM, so this
                # is the only heap size that matters. Spark's 1g default is
                # too small once Iceberg's writer buffers multiple open
                # per-partition Parquet writers at once (its FanoutWriter,
                # used for this table's first, partition-defining write) —
                # the full 54-period INFOTABLE history OOMs
                # (`OutOfMemoryError: Java heap space`) at the 1g default.
                .config("spark.driver.memory", "4g")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.silver.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
                .config("spark.sql.catalog.silver.uri", ICEBERG_CATALOG_JDBC_URI)
                .config("spark.sql.catalog.silver.jdbc.user", ICEBERG_CATALOG_JDBC_USER)
                .config("spark.sql.catalog.silver.jdbc.password", ICEBERG_CATALOG_JDBC_PASSWORD)
                .config("spark.sql.catalog.silver.warehouse", warehouse)
                .config("spark.sql.catalog.silver.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .config("spark.sql.catalog.silver.s3.endpoint", s3_endpoint)
                .config("spark.sql.catalog.silver.s3.path-style-access", "true")
                .config("spark.sql.catalog.silver.client.region", region)
                .getOrCreate()
            )
            try:
                period_paths = [
                    os.path.join(stage_dir, f"filing_period={period}", "data.tsv") for period in periods_staged
                ]

                # Infer a schema across *all* staged periods in one pass, not
                # per period unioned afterward, so there's exactly one
                # inferred type per column for the whole table's history.
                inferred_schema = (
                    spark.read.option("header", True)
                    .option("sep", "\t")
                    .option("inferSchema", True)
                    .csv(period_paths)
                    .schema
                )
                # Force PROTECTED_STRING_COLUMNS back to string — numeric
                # inference on an identifier column can silently corrupt it
                # (lost leading zeros), and by the time a DataFrame exists
                # those digits are already gone. Patch the schema *before*
                # the real read rather than casting back to string after.
                read_schema = StructType(
                    [
                        StructField(field.name, StringType(), field.nullable)
                        if field.name.lower() in PROTECTED_STRING_COLUMNS
                        else field
                        for field in inferred_schema.fields
                    ]
                )

                raw = spark.read.option("header", True).option("sep", "\t").schema(read_schema).csv(period_paths)
                raw = raw.toDF(*[c.lower() for c in raw.columns])
                merged = raw.withColumn(
                    "filing_period",
                    F.regexp_extract(F.input_file_name(), r"filing_period=([^/]+)/data\.tsv$", 1),
                )
                all_columns = [(field.name, _glue_column_type(field.dataType)) for field in merged.schema.fields]

                spark.sql(f"CREATE NAMESPACE IF NOT EXISTS silver.{ICEBERG_NAMESPACE}")
                (
                    merged.writeTo(f"silver.{ICEBERG_NAMESPACE}.{table_name}")
                    .using("iceberg")
                    .partitionedBy("filing_period")
                    .createOrReplace()
                )
            finally:
                spark.stop()
        finally:
            shutil.rmtree(stage_dir, ignore_errors=True)

        # JdbcCatalog tracks each table's current metadata.json location as a
        # plain column in its own iceberg_tables row — read it straight back
        # rather than guessing a filename convention.
        pg_conn = psycopg2.connect(
            host=ICEBERG_CATALOG_PG_HOST,
            dbname=ICEBERG_CATALOG_PG_DB,
            user=ICEBERG_CATALOG_JDBC_USER,
            password=ICEBERG_CATALOG_JDBC_PASSWORD,
        )
        try:
            with pg_conn.cursor() as cur:
                cur.execute(
                    "SELECT metadata_location FROM iceberg_tables "
                    "WHERE catalog_name = %s AND table_namespace = %s AND table_name = %s",
                    ("silver", ICEBERG_NAMESPACE, table_name),
                )
                metadata_location = cur.fetchone()[0]
        finally:
            pg_conn.close()
        s3_location = f"s3://{RAW_BUCKET}/{table_key_prefix}/"

        # moto's mock_aws() patches boto3/botocore process-wide, so the Glue
        # client must be created *inside* this block and the S3 calls above
        # must happen *outside* it — otherwise moto would intercept them too
        # and nothing would actually reach LocalStack.
        with mock_aws():
            glue = boto3.client("glue", region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
            glue.create_database(DatabaseInput={"Name": GLUE_DATABASE})
            glue.create_table(
                DatabaseName=GLUE_DATABASE,
                TableInput=_glue_iceberg_table_input(table_name, all_columns, s3_location, metadata_location),
            )
            registered = [t["Name"] for t in glue.get_tables(DatabaseName=GLUE_DATABASE)["TableList"]]
            logger.info(
                "Registered in this task's mocked Glue session (not persisted): %s (%d period partitions, %s)",
                registered,
                len(periods_staged),
                metadata_location,
            )

        return table_name

    @task
    def build_gold_holder_positions() -> dict:
        """Gold layer: "who are the largest holders of a given security for a
        specified quarter?" — a curated fact table built from silver's
        infotable + coverpage + submission (not from bronze), one row per
        (cik, periodofreport, cusip), ranked by position size.

        Three real correctness problems have to be solved to answer that
        query correctly, each confirmed against real data rather than
        assumed (see IMPLEMENTATION.md for the full analysis):

        1. "Specified quarter" means `submission.periodofreport` (the
           quarter-end date holdings are *as of*), not `filing_period` (when
           the pipeline happened to ingest the zip) — a single filing_period
           batch was found to contain periodofreport values spanning back
           several years, from late/amended filings. periodofreport is
           SEC's `DD-MON-YYYY` text; parsed here via
           `to_date(..., 'dd-MMM-yyyy')` into a real date so it can be
           filtered and partitioned correctly instead of compared as a
           string.
        2. Amendments have to be resolved to one effective set of holdings
           per (cik, periodofreport), or a holder's position either gets
           double-counted or silently dropped:
           - RESTATEMENT amendments are a full, self-contained replacement —
             if one exists, only the *latest* RESTATEMENT is used, ignoring
             the original filing and any other amendments for that quarter.
           - NEW HOLDINGS amendments are additive, not a replacement — with
             no restatement present, the original filing and every NEW
             HOLDINGS amendment are all included and summed together.
           (A real CIK/quarter pair with exactly the original+RESTATEMENT
           pattern was found and used to validate this.)
        3. A single filing routinely reports the same CUSIP across multiple
           `infotable` rows (split by voting authority, put/call, or lots) —
           one real filing had 138 separate rows for the same CUSIP. These
           are summed per (cik, periodofreport, cusip) before ranking, or
           "largest holder" rankings fragment across duplicate rows.

        `total_value` (thousands of USD, SEC's own convention) is the
        ranking basis rather than share count, since `sshprnamt` mixes SH
        (shares) and PRN (bond principal amount) — not a comparable unit
        across security types; `total_shares` is still carried, summed only
        over SH-type rows, for reference.

        Does not attempt to split positions reported under shared
        investment discretion across `othermanager2`, and the ~0.0007% of
        cover pages with `isamendment='Y'` but a null `amendmenttype` (a
        real but tiny data anomaly — 3 rows out of ~410k) fall into the
        additive branch and could in principle double-count; see
        IMPLEMENTATION.md.
        """
        s3_endpoint = os.environ["AWS_ENDPOINT_URL_S3"]
        region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
        silver_warehouse = f"s3://{RAW_BUCKET}/{SILVER_PREFIX}"
        gold_warehouse = f"s3://{RAW_BUCKET}/{GOLD_PREFIX}"

        spark = (
            SparkSession.builder.appName("sec-13f-gold-holder-positions")
            .master("local[*]")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.memory", "4g")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.silver.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
            .config("spark.sql.catalog.silver.uri", ICEBERG_CATALOG_JDBC_URI)
            .config("spark.sql.catalog.silver.jdbc.user", ICEBERG_CATALOG_JDBC_USER)
            .config("spark.sql.catalog.silver.jdbc.password", ICEBERG_CATALOG_JDBC_PASSWORD)
            .config("spark.sql.catalog.silver.warehouse", silver_warehouse)
            .config("spark.sql.catalog.silver.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.catalog.silver.s3.endpoint", s3_endpoint)
            .config("spark.sql.catalog.silver.s3.path-style-access", "true")
            .config("spark.sql.catalog.silver.client.region", region)
            .config("spark.sql.catalog.gold", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.gold.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
            .config("spark.sql.catalog.gold.uri", ICEBERG_CATALOG_JDBC_URI)
            .config("spark.sql.catalog.gold.jdbc.user", ICEBERG_CATALOG_JDBC_USER)
            .config("spark.sql.catalog.gold.jdbc.password", ICEBERG_CATALOG_JDBC_PASSWORD)
            .config("spark.sql.catalog.gold.warehouse", gold_warehouse)
            .config("spark.sql.catalog.gold.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.catalog.gold.s3.endpoint", s3_endpoint)
            .config("spark.sql.catalog.gold.s3.path-style-access", "true")
            .config("spark.sql.catalog.gold.client.region", region)
            .getOrCreate()
        )
        try:
            submission = spark.table(f"silver.{ICEBERG_NAMESPACE}.submission")
            coverpage = spark.table(f"silver.{ICEBERG_NAMESPACE}.coverpage")
            infotable = spark.table(f"silver.{ICEBERG_NAMESPACE}.infotable")

            filings = (
                submission.select("accession_number", "cik", "periodofreport", "filing_date")
                .join(
                    coverpage.select("accession_number", "isamendment", "amendmenttype", "filingmanager_name"),
                    "accession_number",
                )
                .withColumn("periodofreport", F.to_date("periodofreport", "dd-MMM-yyyy"))
                .withColumn("filing_date", F.to_date("filing_date", "dd-MMM-yyyy"))
            )

            cik_period = Window.partitionBy("cik", "periodofreport")
            filings = filings.withColumn(
                "has_restatement",
                F.max(F.when(F.col("amendmenttype") == "RESTATEMENT", 1).otherwise(0)).over(cik_period),
            )

            latest_first = Window.partitionBy("cik", "periodofreport").orderBy(F.col("filing_date").desc())
            restated = (
                filings.filter((F.col("has_restatement") == 1) & (F.col("amendmenttype") == "RESTATEMENT"))
                .withColumn("rn", F.row_number().over(latest_first))
                .filter("rn = 1")
            )
            additive = filings.filter(
                (F.col("has_restatement") == 0)
                & (F.col("amendmenttype").isNull() | (F.col("amendmenttype") == "NEW HOLDINGS"))
            )
            effective_filings = restated.unionByName(additive, allowMissingColumns=True).select(
                "accession_number", "cik", "periodofreport", "filingmanager_name"
            )

            positions = (
                infotable.select(
                    "accession_number", "cusip", "nameofissuer", "titleofclass", "value", "sshprnamt", "sshprnamttype"
                )
                .join(effective_filings, "accession_number")
                .groupBy("cik", "periodofreport", "cusip")
                .agg(
                    F.sum("value").alias("total_value"),
                    F.sum(F.when(F.col("sshprnamttype") == "SH", F.col("sshprnamt")).otherwise(0)).alias(
                        "total_shares"
                    ),
                    F.first("nameofissuer", ignorenulls=True).alias("nameofissuer"),
                    F.first("titleofclass", ignorenulls=True).alias("titleofclass"),
                    F.first("filingmanager_name", ignorenulls=True).alias("filingmanager_name"),
                    F.count(F.lit(1)).alias("lot_count"),
                    F.concat_ws(",", F.sort_array(F.collect_set("accession_number"))).alias(
                        "source_accession_numbers"
                    ),
                )
            )

            ranked = positions.withColumn(
                "rank",
                F.rank().over(Window.partitionBy("cusip", "periodofreport").orderBy(F.col("total_value").desc())),
            )

            table_columns = [(field.name, _glue_column_type(field.dataType)) for field in ranked.schema.fields]

            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS gold.{GOLD_NAMESPACE}")
            (
                ranked.writeTo(f"gold.{GOLD_NAMESPACE}.holder_positions")
                .using("iceberg")
                .partitionedBy("periodofreport")
                .createOrReplace()
            )
        finally:
            spark.stop()

        pg_conn = psycopg2.connect(
            host=ICEBERG_CATALOG_PG_HOST,
            dbname=ICEBERG_CATALOG_PG_DB,
            user=ICEBERG_CATALOG_JDBC_USER,
            password=ICEBERG_CATALOG_JDBC_PASSWORD,
        )
        try:
            with pg_conn.cursor() as cur:
                cur.execute(
                    "SELECT metadata_location FROM iceberg_tables "
                    "WHERE catalog_name = %s AND table_namespace = %s AND table_name = %s",
                    ("gold", GOLD_NAMESPACE, "holder_positions"),
                )
                metadata_location = cur.fetchone()[0]
        finally:
            pg_conn.close()
        s3_location = f"s3://{RAW_BUCKET}/{GOLD_PREFIX}/{GOLD_NAMESPACE}/holder_positions/"

        with mock_aws():
            glue = boto3.client("glue", region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
            glue.create_database(DatabaseInput={"Name": GLUE_GOLD_DATABASE})
            glue.create_table(
                DatabaseName=GLUE_GOLD_DATABASE,
                TableInput=_glue_iceberg_table_input(
                    "holder_positions", table_columns, s3_location, metadata_location
                ),
            )
            logger.info("Gold table holder_positions registered (not persisted): %s", metadata_location)

        return {"table": "holder_positions", "metadata_location": metadata_location}

    silver_tasks = {}
    for filename in CANONICAL_FILES:
        table_name = filename.removesuffix(".tsv").lower()
        dq_task = check_bronze_quality.override(task_id=f"check_bronze_quality_{table_name}")(
            filename=filename, bronze_keys=bronze_keys
        )
        silver_task = build_silver_table.override(task_id=f"build_silver_{table_name}")(
            filename=filename, bronze_keys=bronze_keys
        )
        dq_task >> silver_task
        silver_tasks[table_name] = silver_task

    gold_task = build_gold_holder_positions()
    [silver_tasks["infotable"], silver_tasks["coverpage"], silver_tasks["submission"]] >> gold_task


sec_13f_pipeline()
