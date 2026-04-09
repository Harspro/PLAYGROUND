# High-Level Design: Regression Testing Framework for Terraform-Based Schema Changes

**Document Version:** 2.0  
**Project:** Terminus Data Platform  
**Date:** April 2026  
**Status:** Draft for Stakeholder Review

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Problem Statement](#2-problem-statement)
3. [Solution Overview](#3-solution-overview)
4. [Architecture](#4-architecture)
5. [Technology Stack](#5-technology-stack)
6. [Component Specifications](#6-component-specifications)
7. [Security & Access Control](#7-security--access-control)
8. [Implementation Plan](#8-implementation-plan)
9. [Cost Estimation](#9-cost-estimation)
10. [Success Criteria](#10-success-criteria)
11. [Risks & Mitigations](#11-risks--mitigations)
12. [Appendix](#12-appendix)

---

## 1. Executive Summary

| Field | Details |
|-------|---------|
| **Project Name** | Terminus Data Platform - Regression Testing Framework |
| **Objective** | Automated detection and validation of schema changes in BigQuery tables managed via Terraform |
| **Scope** | GCP-based data platform with BigQuery, GCS, and Terraform-managed infrastructure |
| **Approach** | Baseline comparison model integrated directly into CI/CD |
| **Target Environment** | UET (User Environment Testing) — dedicated regression environment |
| **Estimated Duration** | 4 weeks (2 phases) |

A lightweight, automated regression testing framework that detects schema changes introduced by Terraform and validates BigQuery table integrity before changes reach production.

**Design Principles:**
- **Fully GCP-Native** — All data, triggers, and execution within GCP
- **Minimal Infrastructure** — Single Cloud Run Job handles all operations
- **Self-Contained** — Input data generated and managed within GCP (no external CI dependency)
- **Cost Optimized** — Pay only when tests run (~$20-60/month)

---

## 2. Problem Statement

The Terminus data platform operates at scale with hundreds of BigQuery tables managed via Terraform. Key challenges:

- **No automated regression detection** when Terraform changes are applied
- **Schema changes can break downstream consumers** — failures discovered only after deployment
- **No visibility** into what changed in schema before vs. after a deployment cycle
- **Manual regression testing** is slow and does not scale

### Current Pain Points

| Pain Point | Impact |
|------------|--------|
| Schema changes via Terraform not tracked | Silent downstream breakages |
| No fixed-input test harness | Cannot verify output consistency |
| No dedicated regression environment | CI/CD cannot simulate real BigQuery dependencies |
| Manual validation effort | Hours of engineer time per release |

---

## 3. Solution Overview

A fully GCP-native regression testing framework that:

- **Captures baseline schema snapshots** from BigQuery `INFORMATION_SCHEMA`
- **Generates test input data** dynamically from production samples or BigQuery scripts
- **Stores all data in GCS/BigQuery** — no external storage dependencies
- **Triggers via Cloud Build or Eventarc** when Terraform state changes
- **Reports results** to BigQuery and Slack

### Core Principle: GCP-Native Data Management

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     INPUT/OUTPUT DATA FLOW                              │
└─────────────────────────────────────────────────────────────────────────┘

  INPUT DATA GENERATION (Automated within GCP)
  ────────────────────────────────────────────────────────────────────────
                                                      
  Option 1: Production Sampling                Option 2: Synthetic Generation
  ┌────────────────────────┐                   ┌────────────────────────┐
  │  Production BigQuery   │                   │  BigQuery Scripted     │
  │  Tables                │                   │  Data Generator        │
  │                        │                   │                        │
  │  SELECT * FROM table   │                   │  INSERT INTO test_data │
  │  WHERE date = '...'    │                   │  SELECT generated_rows │
  │  LIMIT 1000            │                   │  FROM UNNEST(...)      │
  └───────────┬────────────┘                   └───────────┬────────────┘
              │                                            │
              └────────────────────┬───────────────────────┘
                                   ▼
                    ┌──────────────────────────┐
                    │  GCS: Input Data Store   │
                    │  gs://regression-data/   │
                    │    └── inputs/           │
                    │        ├── table_a.json  │
                    │        ├── table_b.json  │
                    │        └── manifest.json │
                    └──────────────────────────┘

  OUTPUT DATA MANAGEMENT
  ────────────────────────────────────────────────────────────────────────

  ┌──────────────────────┐     ┌──────────────────────┐
  │  Cloud Run Job       │────▶│  GCS: Output Store   │
  │  Regression Tester   │     │  gs://regression-data│
  │                      │     │    └── outputs/      │
  │  - Run tests         │     │        └── {run_id}/ │
  │  - Compare results   │     │            ├── schema_diff.json
  │  - Generate reports  │     │            ├── data_validation.json
  └──────────────────────┘     │            └── summary.json
                               └──────────────────────┘
                                          │
                                          ▼
                               ┌──────────────────────┐
                               │  BigQuery Results    │
                               │  (Long-term storage) │
                               │                      │
                               │  regression_dataset. │
                               │    results           │
                               └──────────────────────┘
```

---

## 4. Architecture

### 4.1 High-Level Architecture (Fully GCP-Native)

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        TERMINUS REGRESSION TESTING                      │
│                        GCP-NATIVE ARCHITECTURE                          │
└─────────────────────────────────────────────────────────────────────────┘

  TRIGGERS (Choose one or combine)
  ┌─────────────────────────────────────────────────────────────────────┐
  │                                                                     │
  │  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐        │
  │  │ Cloud Build    │  │ Cloud Scheduler│  │ Eventarc       │        │
  │  │ (on TF apply)  │  │ (daily/weekly) │  │ (GCS trigger)  │        │
  │  └───────┬────────┘  └───────┬────────┘  └───────┬────────┘        │
  │          └───────────────────┼───────────────────┘                 │
  │                              ▼                                     │
  └─────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │                      CLOUD RUN JOB                                  │
  │                   (regression-tester)                               │
  │                                                                     │
  │   ┌─────────────────────────────────────────────────────────────┐   │
  │   │  1. INPUT GENERATION                                        │   │
  │   │     - Generate test data from BQ queries                    │   │
  │   │     - OR load existing samples from GCS                     │   │
  │   │     - Store in gs://regression-data/inputs/                 │   │
  │   └─────────────────────────────────────────────────────────────┘   │
  │                              │                                      │
  │                              ▼                                      │
  │   ┌─────────────────────────────────────────────────────────────┐   │
  │   │  2. BASELINE CAPTURE / COMPARE                              │   │
  │   │     - Query INFORMATION_SCHEMA for current state            │   │
  │   │     - Compare against stored baseline                       │   │
  │   │     - Detect schema changes                                 │   │
  │   └─────────────────────────────────────────────────────────────┘   │
  │                              │                                      │
  │                              ▼                                      │
  │   ┌─────────────────────────────────────────────────────────────┐   │
  │   │  3. DATA VALIDATION                                         │   │
  │   │     - Run validation queries against test tables            │   │
  │   │     - Compare row counts, checksums                         │   │
  │   │     - Generate validation report                            │   │
  │   └─────────────────────────────────────────────────────────────┘   │
  │                              │                                      │
  │                              ▼                                      │
  │   ┌─────────────────────────────────────────────────────────────┐   │
  │   │  4. OUTPUT & REPORTING                                      │   │
  │   │     - Write results to gs://regression-data/outputs/        │   │
  │   │     - Insert summary to BigQuery                            │   │
  │   │     - Send Slack notification (failures only)               │   │
  │   └─────────────────────────────────────────────────────────────┘   │
  └─────────────────────────────────────────────────────────────────────┘
                                 │
           ┌─────────────────────┼─────────────────────┐
           ▼                     ▼                     ▼
  ┌────────────────┐   ┌────────────────┐    ┌────────────────┐
  │  GCS Buckets   │   │   BigQuery     │    │    Slack       │
  │                │   │                │    │                │
  │  - inputs/     │   │  - baselines   │    │  - Alerts      │
  │  - outputs/    │   │  - results     │    │                │
  │  - baselines/  │   │                │    │                │
  └────────────────┘   └────────────────┘    └────────────────┘
```

### 4.2 GCS Data Organization

```
gs://regression-data/
│
├── inputs/                          # Test input data (generated or sampled)
│   ├── manifest.json                # Lists all input datasets
│   ├── customers/
│   │   ├── data.json                # Sample customer records
│   │   └── schema.json              # Expected schema
│   ├── transactions/
│   │   ├── data.json
│   │   └── schema.json
│   └── products/
│       ├── data.json
│       └── schema.json
│
├── baselines/                       # Schema and data baselines
│   ├── current/
│   │   └── baseline.json            # Current active baseline
│   └── history/
│       ├── 2026-04-01T00:00:00Z.json
│       └── 2026-04-08T00:00:00Z.json
│
└── outputs/                         # Test run outputs
    └── {run_id}/                    # UUID for each run
        ├── schema_diff.json         # Schema comparison results
        ├── data_validation.json     # Data validation results
        ├── summary.json             # Overall pass/fail summary
        └── logs.txt                 # Execution logs
```

### 4.3 Trigger Options

| Trigger | Use Case | Configuration |
|---------|----------|---------------|
| **Cloud Build** | Run after `terraform apply` | Add step in cloudbuild.yaml |
| **Cloud Scheduler** | Daily/weekly baseline refresh | Cron: `0 2 * * 0` (Sunday 2 AM) |
| **Eventarc (GCS)** | When Terraform state changes | Monitor `gs://tf-state/` bucket |
| **Manual** | On-demand testing | `gcloud run jobs execute` |

### 4.4 Complete Execution Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     COMPLETE EXECUTION FLOW                             │
└─────────────────────────────────────────────────────────────────────────┘

  PHASE 1: TRIGGER (GCP Native)
  ─────────────────────────────────────────────
  Terraform apply completes
          │
          ▼
  Cloud Build triggers regression job
          │
          └── OR Cloud Scheduler (weekly baseline refresh)
          └── OR Manual: gcloud run jobs execute regression-tester

  PHASE 2: INPUT GENERATION (Inside Cloud Run Job)
  ─────────────────────────────────────────────
  Load input configuration from GCS
          │
          ├── Query BigQuery for sample data
          ├── Generate synthetic test cases (if configured)
          └── Save generated inputs to gs://regression-data/inputs/

  PHASE 3: REGRESSION TESTING
  ─────────────────────────────────────────────
  Load baseline from GCS
          │
          ├── Query current INFORMATION_SCHEMA
          ├── Compare schemas: detect changes
          │         │
          │   [PASS] No breaking changes
          │   [FAIL] Breaking changes → flag for alert
          │
          └── Validate data integrity
                    │
              [PASS] Checksums match
              [FAIL] Data mismatch → flag for alert

  PHASE 4: OUTPUT & REPORTING
  ─────────────────────────────────────────────
  Generate run outputs
          │
          ├── Save detailed JSON to gs://regression-data/outputs/{run_id}/
          ├── Insert summary to BigQuery (regression_dataset.results)
          ├── Send Slack alert (if failures)
          └── Return exit code (0=pass, 1=fail)
```

---

## 5. Technology Stack

| Layer | Technology | Rationale |
|-------|------------|-----------|
| **Compute** | Cloud Run Job | Pay-per-use, up to 1hr execution, no infra |
| **Triggers** | Cloud Build / Cloud Scheduler / Eventarc | GCP-native, no external CI dependency |
| **Storage** | GCS (data + baselines), BigQuery (results) | Native GCP, cost-effective |
| **Language** | Python 3.11+ | google-cloud SDK, robust BigQuery support |
| **Notifications** | Slack Webhook | Simple, no additional setup |
| **IaC** | Terraform | Consistent with existing infrastructure |

**Why This Stack:**
- **Fully GCP-Native** — No GitHub/GitLab dependency for execution
- **Cloud Build integration** — Triggers after `terraform apply`
- **Self-contained data management** — Inputs generated from BigQuery, outputs stored in GCS
- **Minimal maintenance** — Single Cloud Run Job, automated triggers

---

## 6. Component Specifications

### 6.1 Input Data Generator

Generates test input data dynamically within GCP — no external file uploads needed.

**Strategy Options:**

| Strategy | Use Case | Implementation |
|----------|----------|----------------|
| **Production Sampling** | Realistic data | BQ query with LIMIT + WHERE filters |
| **Synthetic Generation** | Controlled edge cases | BQ scripted INSERT with GENERATE_ARRAY |
| **Snapshot Restore** | Point-in-time data | BQ snapshot table or GCS export |

**Input Generator Code:**

```python
class InputDataGenerator:
    """Generate test input data from BigQuery."""
    
    def __init__(self, bq_client, gcs_client, config: dict):
        self.bq = bq_client
        self.gcs = gcs_client
        self.bucket = config["input_bucket"]
        self.config = config
    
    def generate_inputs(self) -> dict:
        """Generate input data for all configured tables."""
        manifest = {"generated_at": datetime.utcnow().isoformat(), "tables": {}}
        
        for table_config in self.config["tables"]:
            table_name = table_config["table"]
            strategy = table_config.get("strategy", "sample")
            
            if strategy == "sample":
                data = self._sample_from_production(table_config)
            elif strategy == "synthetic":
                data = self._generate_synthetic(table_config)
            elif strategy == "snapshot":
                data = self._restore_snapshot(table_config)
            
            # Save to GCS
            gcs_path = f"inputs/{table_name}/data.json"
            self._save_to_gcs(gcs_path, data)
            
            manifest["tables"][table_name] = {
                "path": gcs_path,
                "row_count": len(data),
                "strategy": strategy
            }
        
        # Save manifest
        self._save_to_gcs("inputs/manifest.json", manifest)
        return manifest
    
    def _sample_from_production(self, config: dict) -> list:
        """Sample N rows from production table with optional filters."""
        query = f"""
            SELECT *
            FROM `{config['source_table']}`
            WHERE {config.get('filter', '1=1')}
            ORDER BY {config.get('order_by', 'RAND()')}
            LIMIT {config.get('sample_size', 1000)}
        """
        return [dict(row) for row in self.bq.query(query).result()]
    
    def _generate_synthetic(self, config: dict) -> list:
        """Generate synthetic test data using BigQuery."""
        # Uses BigQuery scripting for deterministic data generation
        query = f"""
            SELECT
                GENERATE_UUID() as id,
                CONCAT('test_', CAST(n AS STRING)) as name,
                RAND() * 1000 as amount,
                DATE_SUB(CURRENT_DATE(), INTERVAL CAST(RAND()*365 AS INT64) DAY) as date
            FROM UNNEST(GENERATE_ARRAY(1, {config.get('row_count', 100)})) as n
        """
        return [dict(row) for row in self.bq.query(query).result()]
```

**Input Configuration (stored in GCS):**

```json
{
  "tables": [
    {
      "table": "customers",
      "source_table": "production.customers",
      "strategy": "sample",
      "sample_size": 500,
      "filter": "created_date >= '2026-01-01'",
      "order_by": "customer_id"
    },
    {
      "table": "transactions",
      "source_table": "production.transactions", 
      "strategy": "sample",
      "sample_size": 2000,
      "filter": "transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)"
    },
    {
      "table": "test_edge_cases",
      "strategy": "synthetic",
      "row_count": 100
    }
  ]
}
```

### 6.2 Output Manager

Manages all test outputs within GCS and BigQuery.

```python
class OutputManager:
    """Manage regression test outputs in GCS and BigQuery."""
    
    def __init__(self, bq_client, gcs_client, config: dict):
        self.bq = bq_client
        self.gcs = gcs_client
        self.bucket = config["output_bucket"]
        self.dataset = config["results_dataset"]
    
    def save_run_results(self, run_id: str, results: dict) -> dict:
        """Save complete run results to GCS and BigQuery."""
        output_path = f"outputs/{run_id}/"
        
        # 1. Save detailed results to GCS (JSON files)
        files_saved = {
            "schema_diff": self._save_to_gcs(
                f"{output_path}schema_diff.json", 
                results.get("schema_diff", {})
            ),
            "data_validation": self._save_to_gcs(
                f"{output_path}data_validation.json",
                results.get("data_validation", {})
            ),
            "summary": self._save_to_gcs(
                f"{output_path}summary.json",
                self._generate_summary(results)
            )
        }
        
        # 2. Insert summary row to BigQuery (for querying/dashboards)
        self._insert_to_bigquery(run_id, results)
        
        # 3. Generate human-readable report
        report_url = self._generate_report_url(run_id)
        
        return {
            "run_id": run_id,
            "gcs_path": f"gs://{self.bucket}/{output_path}",
            "files": files_saved,
            "report_url": report_url,
            "status": results.get("overall_status", "UNKNOWN")
        }
    
    def _insert_to_bigquery(self, run_id: str, results: dict):
        """Insert results summary to BigQuery for long-term storage."""
        rows = []
        
        # Schema changes
        for change in results.get("schema_diff", {}).get("changes", []):
            rows.append({
                "run_id": run_id,
                "run_timestamp": datetime.utcnow().isoformat(),
                "test_type": "schema",
                "table_name": change.get("table"),
                "status": "FAIL" if change.get("breaking") else "PASS",
                "details": json.dumps(change)
            })
        
        # Data validations
        for validation in results.get("data_validation", {}).get("results", []):
            rows.append({
                "run_id": run_id,
                "run_timestamp": datetime.utcnow().isoformat(),
                "test_type": "data",
                "table_name": validation.get("table"),
                "status": validation.get("status"),
                "details": json.dumps(validation)
            })
        
        table_ref = f"{self.dataset}.results"
        self.bq.insert_rows_json(table_ref, rows)
    
    def get_run_outputs(self, run_id: str) -> dict:
        """Retrieve outputs for a specific run from GCS."""
        output_path = f"outputs/{run_id}/"
        return {
            "summary": self._read_from_gcs(f"{output_path}summary.json"),
            "schema_diff": self._read_from_gcs(f"{output_path}schema_diff.json"),
            "data_validation": self._read_from_gcs(f"{output_path}data_validation.json")
        }
    
    def cleanup_old_runs(self, retention_days: int = 30):
        """Delete output files older than retention period."""
        cutoff = datetime.utcnow() - timedelta(days=retention_days)
        # GCS lifecycle policy handles this automatically
        pass
```

### 6.3 Baseline Manager

| Attribute | Details |
|-----------|---------|
| **Purpose** | Capture and version schema snapshots |
| **Storage** | `gs://regression-data/baselines/current/baseline.json` |
| **History** | `gs://regression-data/baselines/history/{timestamp}.json` |
| **Retention** | Last 10 versions (automatic via GCS lifecycle) |

**Baseline Schema (Simplified):**

```json
{
  "version": "2026-04-08T18:00:00Z",
  "environment": "uet",
  "tables": {
    "dataset.table_name": {
      "columns": [
        {"name": "id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "amount", "type": "FLOAT64", "mode": "NULLABLE"}
      ],
      "row_count": 5000,
      "checksum": "abc123"
    }
  },
  "total_tables": 150
}
```

**Capture Logic:**

```python
def capture_baseline(self) -> dict:
    """Capture schema baseline from BigQuery."""
    query = """
        SELECT table_schema, table_name, column_name, 
               data_type, is_nullable
        FROM region-us.INFORMATION_SCHEMA.COLUMNS
        WHERE table_schema NOT IN ('INFORMATION_SCHEMA')
        ORDER BY table_schema, table_name, ordinal_position
    """
    baseline = {"tables": {}, "version": datetime.utcnow().isoformat()}
    
    for row in self.bq_client.query(query).result():
        key = f"{row.table_schema}.{row.table_name}"
        if key not in baseline["tables"]:
            baseline["tables"][key] = {"columns": []}
        baseline["tables"][key]["columns"].append({
            "name": row.column_name,
            "type": row.data_type,
            "mode": "REQUIRED" if row.is_nullable == "NO" else "NULLABLE"
        })
    
    # Save to GCS
    self._save_baseline(baseline)
    return baseline
```

### 6.3 Schema Comparator

Parses `terraform plan -json` to detect schema changes before apply:

```python
def compare_schema(self, plan_file: str) -> dict:
    """Compare terraform plan against baseline."""
    with open(plan_file) as f:
        plan = json.load(f)
    
    baseline = self._load_baseline()
    changes = {"breaking": [], "additive": [], "unchanged": []}
    
    for resource in plan.get("resource_changes", []):
        if resource["type"] != "google_bigquery_table":
            continue
        
        action = resource["change"]["actions"]
        table_name = resource["change"]["after"]["table_id"]
        
        if "delete" in action:
            changes["breaking"].append({
                "table": table_name,
                "change": "TABLE_DELETED"
            })
        elif "create" in action:
            changes["additive"].append({
                "table": table_name,
                "change": "TABLE_ADDED"
            })
        elif "update" in action:
            # Compare column changes
            before = resource["change"]["before"]["schema"]
            after = resource["change"]["after"]["schema"]
            column_changes = self._diff_columns(before, after)
            
            for change in column_changes:
                if change["type"] in ("COLUMN_DELETED", "TYPE_CHANGED"):
                    changes["breaking"].append(change)
                else:
                    changes["additive"].append(change)
    
    return {
        "status": "FAIL" if changes["breaking"] else "PASS",
        "changes": changes
    }
```

**Change Types:**

| Type | Breaking? | Description |
|------|-----------|-------------|
| `TABLE_DELETED` | Yes | Table removed |
| `COLUMN_DELETED` | Yes | Column removed |
| `TYPE_CHANGED` | Yes | Column type modified |
| `MODE_CHANGED` | Maybe | NULLABLE → REQUIRED is breaking |
| `TABLE_ADDED` | No | New table created |
| `COLUMN_ADDED` | No | New column added |

### 6.4 Data Validator

Validates data integrity using efficient BigQuery operations:

```python
def validate_data(self) -> dict:
    """Validate table data integrity against baseline."""
    baseline = self._load_baseline()
    results = []
    
    for table, expected in baseline["tables"].items():
        # Use single query for efficiency
        query = f"""
            SELECT 
                COUNT(*) as row_count,
                FARM_FINGERPRINT(STRING_AGG(TO_JSON_STRING(t), '' ORDER BY 1)) as checksum
            FROM `{table}` t
        """
        actual = list(self.bq_client.query(query).result())[0]
        
        status = "PASS"
        if actual.row_count != expected.get("row_count"):
            status = "FAIL"
        if actual.checksum != expected.get("checksum"):
            status = "FAIL"
        
        results.append({
            "table": table,
            "status": status,
            "expected_rows": expected.get("row_count"),
            "actual_rows": actual.row_count
        })
    
    return {
        "status": "FAIL" if any(r["status"] == "FAIL" for r in results) else "PASS",
        "results": results
    }
```

### 6.5 Results & Notifications

**BigQuery Results Table (Single Table):**

```sql
CREATE TABLE `regression_dataset.results` (
  run_id            STRING NOT NULL,
  run_timestamp     TIMESTAMP NOT NULL,
  pr_number         INT64,
  table_name        STRING NOT NULL,
  test_type         STRING NOT NULL,  -- 'schema' or 'data'
  status            STRING NOT NULL,  -- 'PASS' or 'FAIL'
  details           JSON,
  execution_time_ms INT64
)
PARTITION BY DATE(run_timestamp);
```

**Slack Notification (on failures only):**

```python
def notify_slack(self, results: dict, pr_url: str):
    """Send Slack notification for failures."""
    if results["status"] == "PASS":
        return  # No notification for success
    
    payload = {
        "text": f"⚠️ Regression Test Failed",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*PR:* <{pr_url}|View PR>\n"
                            f"*Breaking Changes:* {len(results['changes']['breaking'])}"
                }
            }
        ]
    }
    requests.post(os.environ["SLACK_WEBHOOK_URL"], json=payload)
```

---

## 7. Security & Access Control

### 7.1 Service Account (Minimal Permissions)

```hcl
resource "google_service_account" "regression_sa" {
  account_id   = "regression-tester"
  display_name = "Regression Tester"
}

# Read-only BigQuery access
resource "google_project_iam_member" "bq_viewer" {
  project = var.project_id
  role    = "roles/bigquery.dataViewer"
  member  = "serviceAccount:${google_service_account.regression_sa.email}"
}

# Write to regression dataset only
resource "google_bigquery_dataset_iam_member" "results_writer" {
  dataset_id = "regression_dataset"
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.regression_sa.email}"
}

# GCS access for baselines
resource "google_storage_bucket_iam_member" "baselines" {
  bucket = "regression-baselines"
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.regression_sa.email}"
}
```

### 7.2 Cloud Build Service Account

```hcl
# Cloud Build needs permission to invoke Cloud Run Jobs
resource "google_project_iam_member" "cloudbuild_run_invoker" {
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${data.google_project.project.number}@cloudbuild.gserviceaccount.com"
}

# Cloud Build needs permission to read Terraform state
resource "google_storage_bucket_iam_member" "cloudbuild_tf_state" {
  bucket = var.terraform_state_bucket
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${data.google_project.project.number}@cloudbuild.gserviceaccount.com"
}
```

### 7.3 Secrets Management

| Secret | Storage | Access |
|--------|---------|--------|
| Slack Webhook URL | Secret Manager | Cloud Run Job only |
| Service Account Key | Not required | Uses attached SA identity |

---

## 8. Implementation Plan

### Phase 1: Core Framework (Week 1–2)

| Task | Owner | Deliverable |
|------|-------|-------------|
| Create GCS bucket (`gs://regression-data/`) | Infra | Bucket with lifecycle policies |
| Create BigQuery results dataset | Infra | `regression_dataset.results` |
| Implement input generator | Backend | `InputDataGenerator` class |
| Implement baseline manager | Backend | `capture_baseline()` working |
| Implement schema comparator | Backend | `compare_schema()` working |
| Set up Cloud Run Job | Infra | Job deployable via Terraform |
| Configure Cloud Build trigger | Infra | Post-terraform-apply trigger |

### Phase 2: Validation & Alerts (Week 3–4)

| Task | Owner | Deliverable |
|------|-------|-------------|
| Implement output manager | Backend | `OutputManager` class |
| Implement data validator | Backend | `validate_data()` working |
| Configure Cloud Scheduler | Infra | Weekly baseline refresh |
| Slack integration | DevOps | Failure alerts working |
| End-to-end testing | QA | Full workflow validated |
| Documentation | All | Runbook + README |

**Total Duration: 4 weeks** (vs. 8 weeks in original design)

---

## 9. Cost Estimation

**Monthly Cost Estimate (Fully GCP-Native):**

| Component | Est. Cost | Notes |
|-----------|-----------|-------|
| Cloud Run Job | $10 – $25 | ~100 invocations/month, 2 vCPU, 2GB RAM |
| GCS Storage | $2 – $8 | Inputs + outputs + baselines (~5 GB) |
| BigQuery | $15 – $40 | Query costs for input generation + results |
| Cloud Scheduler | $1 – $3 | Weekly triggers |
| Cloud Build | $0 – $5 | First 120 min/day free |
| **TOTAL** | **$28 – $81** | Per month |

**Cost Comparison:**

| Architecture | Monthly Cost | Maintenance |
|--------------|--------------|-------------|
| Original (4 Cloud Functions + GitHub) | $185 | High |
| Simplified (GitHub Actions + Cloud Run) | $50 | Medium |
| **GCP-Native (Cloud Build + Cloud Run)** | **$60** | **Low** |

> **Note:** GCP-native is slightly higher than GitHub Actions but eliminates external dependencies and keeps all data within GCP.

---

## 10. Success Criteria

| Metric | Target |
|--------|--------|
| Detection Rate | 100% of schema changes detected before merge |
| False Positives | < 5% |
| Execution Time | < 10 minutes per run |
| Alert Latency | < 2 minutes after run completes |
| Coverage | All Terraform-managed tables |
| Maintenance Effort | < 2 hours/month |

---

## 11. Risks & Mitigations

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| **Baseline drift** | Medium | High | Auto-refresh baseline weekly via Cloud Scheduler |
| **Large table checksums slow** | Medium | Medium | Use row count only for tables > 10M rows |
| **Terraform state not synced** | Low | Medium | Cross-validate with live INFORMATION_SCHEMA |
| **Input data becomes stale** | Medium | Medium | Regenerate inputs weekly; use date-relative filters |

---

## 12. Appendix

### 12.1 Cloud Build Configuration

```yaml
# cloudbuild.yaml - Integrated with Terraform deployment
steps:
  # Step 1: Terraform Init & Plan
  - name: 'hashicorp/terraform:1.7'
    entrypoint: 'sh'
    args:
      - '-c'
      - |
        cd terraform/
        terraform init -backend-config="bucket=${_TF_STATE_BUCKET}"
        terraform plan -json -out=plan.tfplan > /workspace/plan_output.json

  # Step 2: Terraform Apply
  - name: 'hashicorp/terraform:1.7'
    entrypoint: 'sh'
    args:
      - '-c'
      - |
        cd terraform/
        terraform apply -auto-approve plan.tfplan

  # Step 3: Run Regression Tests (after apply)
  - name: 'gcr.io/google.com/cloudsdktool/cloud-sdk'
    entrypoint: 'gcloud'
    args:
      - 'run'
      - 'jobs'
      - 'execute'
      - 'regression-tester'
      - '--region=us-central1'
      - '--wait'
      - '--args=--mode=full'
    waitFor: ['terraform-apply']

substitutions:
  _TF_STATE_BUCKET: 'your-tf-state-bucket'

options:
  logging: CLOUD_LOGGING_ONLY
```

### 12.2 Cloud Scheduler Configuration

```hcl
# Terraform: Weekly baseline refresh
resource "google_cloud_scheduler_job" "baseline_refresh" {
  name        = "regression-baseline-refresh"
  description = "Weekly baseline capture for regression testing"
  schedule    = "0 2 * * 0"  # Sunday 2 AM UTC
  time_zone   = "UTC"

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/regression-tester:run"
    
    body = base64encode(jsonencode({
      overrides = {
        containerOverrides = [{
          args = ["--mode=capture"]
        }]
      }
    }))

    oauth_token {
      service_account_email = google_service_account.regression_sa.email
    }
  }
}

# Eventarc trigger for Terraform state changes (optional)
resource "google_eventarc_trigger" "tf_state_change" {
  name     = "regression-on-tf-state-change"
  location = var.region

  matching_criteria {
    attribute = "type"
    value     = "google.cloud.storage.object.v1.finalized"
  }
  matching_criteria {
    attribute = "bucket"
    value     = var.terraform_state_bucket
  }

  destination {
    cloud_run_service {
      service = google_cloud_run_v2_job.regression_tester.name
      region  = var.region
    }
  }

  service_account = google_service_account.regression_sa.email
}
```

### 12.3 Cloud Run Job Definition (Terraform)

```hcl
resource "google_cloud_run_v2_job" "regression_tester" {
  name     = "regression-tester"
  location = var.region

  template {
    template {
      containers {
        image = "gcr.io/${var.project_id}/regression-tester:latest"
        
        resources {
          limits = {
            cpu    = "2"
            memory = "2Gi"
          }
        }

        env {
          name  = "GCP_PROJECT"
          value = var.project_id
        }
        env {
          name  = "INPUT_BUCKET"
          value = google_storage_bucket.regression_data.name
        }
        env {
          name  = "OUTPUT_BUCKET"
          value = google_storage_bucket.regression_data.name
        }
        env {
          name = "SLACK_WEBHOOK_URL"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.slack_webhook.secret_id
              version = "latest"
            }
          }
        }
      }

      service_account = google_service_account.regression_sa.email
      timeout         = "600s"
    }
  }
}

# GCS bucket for all regression data
resource "google_storage_bucket" "regression_data" {
  name     = "${var.project_id}-regression-data"
  location = var.region

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 90  # Delete outputs older than 90 days
    }
    action {
      type = "Delete"
    }
  }

  versioning {
    enabled = true
  }
}
```

### 12.4 Input Configuration Example

```json
{
  "version": "1.0",
  "description": "Regression test input configuration",
  "tables": [
    {
      "name": "customers",
      "source_table": "production_dataset.customers",
      "strategy": "sample",
      "sample_size": 500,
      "filter": "created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)",
      "order_by": "customer_id"
    },
    {
      "name": "transactions",
      "source_table": "production_dataset.transactions",
      "strategy": "sample",
      "sample_size": 2000,
      "filter": "transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)"
    },
    {
      "name": "edge_cases",
      "strategy": "synthetic",
      "row_count": 100,
      "generator_query": "SELECT GENERATE_UUID() as id, 'test' as name, RAND()*1000 as amount FROM UNNEST(GENERATE_ARRAY(1, 100))"
    }
  ],
  "refresh_schedule": "weekly"
}
```

### 12.5 Glossary

| Term | Definition |
|------|------------|
| **UET** | User Environment Testing — dedicated non-production GCP project |
| **Baseline** | Versioned snapshot of schemas and data checksums |
| **Breaking Change** | Schema change that may break existing consumers |
| **Checksum** | FARM_FINGERPRINT hash of table data for integrity validation |
| **Cloud Build** | GCP CI/CD service that runs build steps in containers |
| **Eventarc** | GCP event routing service that triggers Cloud Run on GCS/Pub/Sub events |

### 12.6 References

- [BigQuery INFORMATION_SCHEMA](https://cloud.google.com/bigquery/docs/information-schema-intro)
- [Terraform Plan JSON Output](https://developer.hashicorp.com/terraform/internals/json-format)
- [Cloud Run Jobs](https://cloud.google.com/run/docs/create-jobs)
- [Cloud Build Triggers](https://cloud.google.com/build/docs/automating-builds/create-manage-triggers)
- [Cloud Scheduler](https://cloud.google.com/scheduler/docs)
- [Eventarc GCS Triggers](https://cloud.google.com/eventarc/docs/creating-triggers)

