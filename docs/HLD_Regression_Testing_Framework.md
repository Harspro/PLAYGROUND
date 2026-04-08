# High-Level Design: Regression Testing Framework for Terraform-Based Schema Changes

**Document Version:** 1.0  
**Project:** Terminus Data Platform  
**Date:** April 2026  
**Status:** Draft for Stakeholder Review

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Problem Statement](#2-problem-statement)
3. [Solution Overview](#3-solution-overview)
4. [Architecture Diagrams](#4-architecture-diagrams)
5. [Technology Stack](#5-technology-stack)
6. [Detailed Component Specifications](#6-detailed-component-specifications)
7. [Deployment Architecture](#7-deployment-architecture)
8. [Security & Access Control](#8-security--access-control)
9. [Implementation Phases](#9-implementation-phases)
10. [Cost Estimation](#10-cost-estimation)
11. [Success Criteria](#11-success-criteria)
12. [Risks & Mitigations](#12-risks--mitigations)
13. [Appendix](#13-appendix)

---

## 1. Executive Summary

| Field | Details |
|-------|---------|
| **Project Name** | Terminus Data Platform - Regression Testing Framework |
| **Objective** | Automated detection and validation of schema changes in BigQuery tables managed via Terraform |
| **Scope** | GCP-based data platform with BigQuery, GCS, and Terraform-managed infrastructure |
| **Approach** | Baseline comparison model with fixed input/output validation |
| **Target Environment** | UET (User Environment Testing) — dedicated regression environment |
| **Estimated Duration** | 8 weeks (4 phases) |

The Terminus Data Platform requires a robust, automated regression testing framework that can detect schema changes introduced by Terraform, validate that existing pipelines produce consistent outputs given fixed inputs, and alert the team before changes reach production.

This document provides the high-level architecture, component design, technology stack, and implementation roadmap for building this framework.

---

## 2. Problem Statement

The Terminus data platform operates at significant scale:

- **1,000+ Airflow DAGs** running across BigQuery, GCS, Dataproc, and Cloud Functions
- **No automated regression detection** when Terraform or shared library changes are applied
- **Manual regression testing** of hundreds of pipelines is slow and does not scale
- **Schema changes can break downstream pipelines silently** — failures are only discovered after deployment
- **No visibility** into what changed in schema or data output before vs. after a deployment cycle

### Current Pain Points

| Pain Point | Impact |
|------------|--------|
| Manual regression across 1000+ DAGs | Days of engineer time per release cycle |
| Schema changes via Terraform not tracked comprehensively | Silent downstream breakages |
| No fixed-input test harness | Cannot deterministically verify output consistency |
| No dedicated regression environment | CI/CD cannot simulate real BigQuery query dependencies |
| Shared library changes ripple across all DAGs | No automated way to identify all impacted pipelines |

---

## 3. Solution Overview

A lightweight, automated regression testing framework that:

- **Captures baseline schema snapshots** before changes are applied
- **Detects schema modifications** via Terraform state or BigQuery metadata comparison
- **Validates fixed input produces fixed output** (deterministic testing)
- **Generates comparison reports** with pass/fail status per DAG and table
- **Alerts the team** on unexpected schema changes or output deviations
- **NO complex orchestration** (Airflow) required — simple, scriptable solution

### Core Principle: Fixed Input → Fixed Output

```
Given:  Fixed mock input data (immutable, versioned)
When:   Pipeline runs in dedicated UET environment
Then:   Output hash / row count / aggregate value must match baseline

If output ≠ baseline → REGRESSION DETECTED → Alert + Block deployment
```

---

## 4. Architecture Diagrams

### 4.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        TERMINUS REGRESSION TESTING                      │
│                          HIGH-LEVEL ARCHITECTURE                        │
└─────────────────────────────────────────────────────────────────────────┘

  ┌──────────────┐     ┌────────────────┐     ┌──────────────────────────┐
  │  Developers  │────▶│ Source Control │────▶│  Dedicated Regression    │
  │              │     │   (GitHub)     │     │  Environment (UET)       │
  │  - Airflow   │     │                │     │                          │
  │  - Spark     │     │  - Airflow DAG │     │  - All changes deployed  │
  │  - Terraform │     │    changes     │     │    to UET first          │
  └──────────────┘     │  - Terraform   │     │  - Mirrors production    │
                       │    changes     │     │    GCP project           │
                       │  - Spark code  │     │  - Fixed mock data       │
                       └────────────────┘     └──────────────┬───────────┘
                                                             │
                                                             ▼
                                              ┌──────────────────────────┐
                                              │  Regression Testing      │
                                              │  Engine                  │
                                              │                          │
                                              │  ┌────────────────────┐  │
                                              │  │ Baseline Manager   │  │
                                              │  └────────────────────┘  │
                                              │  ┌────────────────────┐  │
                                              │  │ Schema Comparator  │  │
                                              │  └────────────────────┘  │
                                              │  ┌────────────────────┐  │
                                              │  │ Data Validator     │  │
                                              │  └────────────────────┘  │
                                              │  ┌────────────────────┐  │
                                              │  │ Reporting Engine   │  │
                                              │  └────────────────────┘  │
                                              └──────────────┬───────────┘
                                                             │
                                                             ▼
                                              ┌──────────────────────────┐
                                              │  Output & Notifications  │
                                              │                          │
                                              │  - BigQuery results table│
                                              │  - Slack alerts          │
                                              │  - Email notifications   │
                                              │  - Looker Studio reports │
                                              └──────────────────────────┘
```

### 4.2 Component Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     REGRESSION TESTING ENGINE                           │
│                      COMPONENT ARCHITECTURE                             │
└─────────────────────────────────────────────────────────────────────────┘

  ┌─────────────────────────────────────────────────────────────────────┐
  │                        BASELINE MANAGER                             │
  │                                                                     │
  │   Input: Terraform state, BQ INFORMATION_SCHEMA, terraform plan    │
  │   Output: baseline_schemas.json → GCS                              │
  │           baseline_data_hashes → BigQuery table                    │
  │   Storage: gs://regression-baselines/{env}/{version}/              │
  └─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │                      SCHEMA COMPARATOR                              │
  │                                                                     │
  │   Methods: Terraform plan parse | State diff | BQ metadata query   │
  │   Detects: TABLE_ADDED | TABLE_DELETED | COLUMN_ADDED              │
  │            COLUMN_DELETED | COLUMN_TYPE_CHANGED | MODE_CHANGED     │
  │   Output:  schema_diff_report.json → GCS                           │
  └─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │                       DATA VALIDATOR                                │
  │                                                                     │
  │   Strategies:  Hash-Based (FARM_FINGERPRINT)                       │
  │                Row Count Comparison                                 │
  │                Aggregate Checksum (SUM, COUNT, MIN, MAX)           │
  │   Input:   Fixed mock data in GCS                                  │
  │   Output:  validation_results → BigQuery table                     │
  └─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │                      REPORTING ENGINE                               │
  │                                                                     │
  │   Outputs: BigQuery regression_results table                       │
  │            Slack webhook notification                               │
  │            Email via SendGrid                                       │
  │            Looker Studio dashboard                                  │
  └─────────────────────────────────────────────────────────────────────┘
```

### 4.3 Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          DATA FLOW DIAGRAM                              │
└─────────────────────────────────────────────────────────────────────────┘

  PHASE 1: BASELINE CAPTURE (Before Deployment)
  ───────────────────────────────────────────────
  BigQuery INFORMATION_SCHEMA ──┐
  Terraform State File          ├──▶ Baseline Manager ──▶ GCS Baseline Store
  terraform plan -json output ──┘          │
                                           └──▶ BigQuery baseline_hashes table

  PHASE 2: DEPLOY CHANGES
  ───────────────────────────────────────────────
  GitHub PR merge ──▶ GitHub Actions ──▶ Deploy to UET (Terraform apply)
                                              │
                                              ├──▶ Airflow DAG updates
                                              ├──▶ Schema changes applied
                                              └──▶ Shared library updates

  PHASE 3: REGRESSION TESTING
  ───────────────────────────────────────────────
  Trigger (manual/scheduled)
          │
          ├──▶ Schema Comparator ──▶ Compare baseline vs current BQ schema
          │         │                      │
          │         │              [PASS] Schema unchanged
          │         │              [FAIL] Schema changed → Alert immediately
          │
          ├──▶ Data Validator ──▶ Run DAGs against fixed mock input
          │         │                      │
          │         │              Compute FARM_FINGERPRINT hash
          │         │              Compare vs baseline hash
          │         │              [PASS] Hashes match
          │         │              [FAIL] Hash mismatch → Log + Alert
          │
          └──▶ Reporting Engine ──▶ Aggregate results ──▶ Notify stakeholders
```

---

## 5. Technology Stack

| Layer | Technology |
|-------|------------|
| Infrastructure as Code | Terraform, GCP Provider, GitHub Actions |
| Compute | Cloud Functions (Gen 2), Cloud Run Jobs |
| Data Storage | BigQuery (Tables & Results), GCS (Baselines & Inputs), Terraform State (GCS) |
| Application Logic | Python 3.10+, Google Cloud SDK |
| Scheduling | Cloud Scheduler, Manual Trigger |
| Notifications | Slack Webhook, Email (SendGrid), Cloud Monitoring |
| Visualization | Looker Studio, Data Studio |

---

## 6. Detailed Component Specifications

### 6.1 Baseline Manager

| Attribute | Details |
|-----------|---------|
| **Purpose** | Capture and version schema/data baselines |
| **Trigger** | Manual or before each deployment cycle |
| **Input Sources** | Terraform state file, BigQuery `INFORMATION_SCHEMA`, `terraform plan -json` output |
| **Output** | `baseline_schemas.json` (GCS), `baseline_data_hashes` table (BigQuery) |
| **Storage Location** | `gs://regression-baselines/{environment}/{version}/` |
| **Retention** | Last 10 versions with timestamp |

**Baseline Schema Structure (JSON):**

```json
{
  "version": "2026-04-08T18:00:00Z",
  "environment": "uet",
  "captured_by": "regression-sa@project.iam.gserviceaccount.com",
  "tables": {
    "project.dataset.table_name": {
      "schema": [
        {
          "name": "customer_id",
          "type": "STRING",
          "mode": "REQUIRED",
          "description": "Unique customer identifier"
        },
        {
          "name": "transaction_amount",
          "type": "FLOAT64",
          "mode": "NULLABLE",
          "description": "Transaction value in USD"
        }
      ],
      "row_count": 5000,
      "last_modified": "2026-04-07T12:00:00Z",
      "data_hash": "FARM_FINGERPRINT_VALUE"
    }
  },
  "terraform_state_hash": "sha256:abc123...",
  "total_tables": 342
}
```

**Baseline Capture Logic:**

```python
def capture_baseline(project_id: str, environment: str) -> dict:
    """Capture schema baseline from BigQuery INFORMATION_SCHEMA."""
    client = bigquery.Client(project=project_id)

    query = """
        SELECT
            table_catalog,
            table_schema,
            table_name,
            column_name,
            ordinal_position,
            data_type,
            is_nullable
        FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
        ORDER BY table_schema, table_name, ordinal_position
    """

    baseline = {}
    results = client.query(query).result()
    for row in results:
        table_key = f"{row.table_schema}.{row.table_name}"
        if table_key not in baseline:
            baseline[table_key] = {"columns": []}
        baseline[table_key]["columns"].append({
            "name": row.column_name,
            "type": row.data_type,
            "nullable": row.is_nullable,
            "position": row.ordinal_position
        })

    return baseline
```

---

### 6.2 Schema Comparator

| Attribute | Details |
|-----------|---------|
| **Purpose** | Detect schema changes between baseline and current state |
| **Detection Methods** | Terraform plan parsing, Terraform state comparison, BigQuery metadata query |

**Change Types Detected:**

| Change Type | Description |
|-------------|-------------|
| `TABLE_ADDED` | A new table was created via Terraform |
| `TABLE_DELETED` | An existing table was removed |
| `COLUMN_ADDED` | A new column was added to a table |
| `COLUMN_DELETED` | A column was removed from a table |
| `COLUMN_TYPE_CHANGED` | A column's data type was modified |
| `COLUMN_MODE_CHANGED` | A column's mode changed (e.g., NULLABLE → REQUIRED) |

**Comparison Logic Flow:**

```
Load baseline_schemas.json from GCS
          │
          ▼
Query current BigQuery INFORMATION_SCHEMA
          │
          ▼
For each table in baseline:
  ├── If table NOT in current state → TABLE_DELETED
  └── For each column in baseline table:
        ├── If column NOT in current → COLUMN_DELETED
        ├── If type changed → COLUMN_TYPE_CHANGED
        └── If mode changed → COLUMN_MODE_CHANGED

For each table in current state:
  └── If table NOT in baseline → TABLE_ADDED
        └── For each column → COLUMN_ADDED (all columns)
          │
          ▼
Generate schema_diff_report.json
  {
    "total_changes": N,
    "breaking_changes": [...],
    "additive_changes": [...],
    "overall_status": "PASS" | "FAIL"
  }
```

---

### 6.3 Data Validator

| Attribute | Details |
|-----------|---------|
| **Purpose** | Verify fixed input produces fixed output |
| **Input Data** | Static mock data stored in GCS (`gs://regression-inputs/{dag_id}/`) |

**Validation Strategies:**

**1. Hash-Based Validation (Preferred)**

```sql
-- Generate whole-table hash using FARM_FINGERPRINT
SELECT
  FARM_FINGERPRINT(
    TO_JSON_STRING(t)
  ) AS table_hash,
  COUNT(*) AS row_count
FROM `project.dataset.table_name` t
ORDER BY 1  -- deterministic ordering required
```

**2. Row Count Validation**

```sql
SELECT COUNT(*) AS current_row_count
FROM `project.dataset.table_name`
-- Compare against baseline: baseline_row_count
```

**3. Aggregate Checksum**

```sql
SELECT
  COUNT(*) AS row_count,
  SUM(numeric_column) AS sum_check,
  MIN(date_column) AS min_date,
  MAX(date_column) AS max_date
FROM `project.dataset.table_name`
```

**Pass/Fail Criteria:**

| Validation Type | PASS Condition | FAIL Condition |
|----------------|----------------|----------------|
| Hash-Based | `current_hash == baseline_hash` | `current_hash != baseline_hash` |
| Row Count | `current_count == baseline_count` | `current_count != baseline_count` |
| Aggregate | All aggregate values match | Any aggregate value differs |

---

### 6.4 Reporting Engine

**BigQuery Results Table Schema (`regression_results`):**

```sql
CREATE TABLE `project.regression_dataset.regression_results` (
  run_id            STRING NOT NULL,
  run_timestamp     TIMESTAMP NOT NULL,
  environment       STRING NOT NULL,
  dag_id            STRING NOT NULL,
  table_name        STRING,
  test_type         STRING NOT NULL,
  status            STRING NOT NULL,
  baseline_value    STRING,
  current_value     STRING,
  change_type       STRING,
  error_message     STRING,
  execution_time_ms INT64
)
PARTITION BY DATE(run_timestamp)
CLUSTER BY environment, status;
```

**Notification Triggers:**

| Condition | Notification Type | Recipients |
|-----------|------------------|------------|
| Any `COLUMN_DELETED` or `TABLE_DELETED` | Immediate Slack + Email | Full team |
| `COLUMN_TYPE_CHANGED` | Slack alert | Data Engineering team |
| `COLUMN_ADDED` | Slack info message | Data Engineering team |
| Data hash mismatch | Slack + Email | DAG owner + Team lead |
| Full regression pass | Summary Slack message | Team channel |
| Regression run error | PagerDuty / Email | On-call engineer |

---

## 7. Deployment Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    GCP PROJECT: terminus-uet                            │
│                      DEPLOYMENT ARCHITECTURE                            │
└─────────────────────────────────────────────────────────────────────────┘

  Cloud Functions (Gen 2)
  ├── baseline-capture-fn       [Triggered: manual / pre-deployment]
  ├── schema-comparator-fn      [Triggered: post-deployment]
  ├── data-validator-fn         [Triggered: Cloud Scheduler / manual]
  └── notification-fn           [Triggered: Pub/Sub from results table]

  Cloud Storage
  ├── gs://regression-baselines/
  │   └── uet/{version}/baseline_schemas.json
  ├── gs://regression-inputs/
  │   └── {dag_id}/mock_input_{version}.csv
  └── gs://regression-reports/
      └── {run_id}/schema_diff_report.json

  BigQuery
  ├── regression_dataset.baseline_data_hashes
  ├── regression_dataset.regression_results
  └── regression_dataset.schema_change_history

  Cloud Scheduler
  ├── weekly-schema-capture     [Every Sunday 02:00 UTC]
  └── manual-regression-trigger [On-demand]

  IAM
  └── regression-sa@terminus-uet.iam.gserviceaccount.com
      ├── roles/bigquery.dataViewer
      ├── roles/bigquery.dataEditor (regression dataset only)
      ├── roles/storage.objectAdmin (regression buckets only)
      └── roles/cloudfunctions.invoker
```

---

## 8. Security & Access Control

### 8.1 Service Account Configuration

```hcl
# Terraform: Regression Testing Service Account
resource "google_service_account" "regression_sa" {
  account_id   = "regression-testing-sa"
  display_name = "Regression Testing Service Account"
  project      = var.project_id
}

resource "google_project_iam_member" "regression_bq_viewer" {
  project = var.project_id
  role    = "roles/bigquery.metadataViewer"
  member  = "serviceAccount:${google_service_account.regression_sa.email}"
}
```

### 8.2 Terraform State Security

- Terraform state stored in GCS with **versioning enabled**
- State bucket has **uniform bucket-level access** (no ACLs)
- State encryption uses **Cloud KMS customer-managed keys**
- State access restricted to CI/CD service account and regression SA

### 8.3 Network Security

- Cloud Functions deployed in **VPC connector** for private BigQuery access
- No public IP on Cloud Functions (ingress: `ALLOW_INTERNAL_ONLY`)
- Baseline GCS buckets are **not publicly accessible**

### 8.4 Secrets Management

- All credentials stored in **Google Secret Manager**
- Slack webhook URL: `projects/{project}/secrets/slack-regression-webhook`
- SendGrid API key: `projects/{project}/secrets/sendgrid-api-key`
- Secrets accessed at runtime via Cloud Functions environment bindings

---

## 9. Implementation Phases

### Phase 1: Foundation (Week 1–2)

| Task | Owner | Status |
|------|-------|--------|
| Set up dedicated GCS buckets for baselines and inputs | Infrastructure | Pending |
| Create regression BigQuery dataset and tables | Infrastructure | Pending |
| Implement `baseline-capture-fn` Cloud Function | Backend | Pending |
| Write mock data generation scripts | Data Engineering | Pending |
| Set up IAM and service accounts | Infrastructure | Pending |

**Deliverable:** Baseline capture running against UET environment.

### Phase 2: Schema Comparison (Week 3–4)

| Task | Owner | Status |
|------|-------|--------|
| Implement `schema-comparator-fn` Cloud Function | Backend | Pending |
| Integrate Terraform plan JSON parsing | Backend | Pending |
| Build schema diff report generation | Backend | Pending |
| Unit tests for schema comparison logic | QA | Pending |

**Deliverable:** Schema change detection working end-to-end for all UET tables.

### Phase 3: Data Validation (Week 5–6)

| Task | Owner | Status |
|------|-------|--------|
| Implement `data-validator-fn` Cloud Function | Backend | Pending |
| Build FARM_FINGERPRINT hash validation | Backend | Pending |
| Implement row count and aggregate checks | Backend | Pending |
| Load fixed mock data for top 50 critical DAGs | Data Engineering | Pending |

**Deliverable:** Data validation running for all critical DAGs with pass/fail reporting.

### Phase 4: Reporting & Alerts (Week 7–8)

| Task | Owner | Status |
|------|-------|--------|
| Implement `notification-fn` Cloud Function | Backend | Pending |
| Set up Slack webhook integration | DevOps | Pending |
| Create Looker Studio regression dashboard | Analytics | Pending |
| Set up Cloud Scheduler for automated runs | Infrastructure | Pending |
| End-to-end testing and documentation | All | Pending |

**Deliverable:** Full regression framework operational with alerting and dashboards.

---

## 10. Cost Estimation

**Monthly Cost Estimate (UET Environment):**

| Component | Est. Cost | Notes |
|-----------|-----------|-------|
| Cloud Functions | $10 – $50 | Based on invocations (~500/month) |
| Cloud Storage | $5 – $20 | Baseline storage (~10 GB) |
| BigQuery | $20 – $100 | Query costs + storage (~500 GB) |
| Cloud Scheduler | $1 – $5 | Minimal cost |
| Cloud Monitoring | $0 – $10 | Basic alerting |
| **TOTAL** | **$36 – $185** | Per month |

> **Note:** Costs are estimates based on expected regression cycle frequency of 2–4 runs per month. Actual costs may vary based on number of DAGs tested and data volume.

---

## 11. Success Criteria

| Metric | Target |
|--------|--------|
| Detection Rate | 100% of schema changes detected before production |
| False Positives | < 5% false positive rate |
| Execution Time | Full regression run < 30 minutes |
| Alert Latency | Notification within 5 minutes of failure |
| Coverage | All critical tables covered in baseline |
| Manual Effort | Reduce manual regression testing by 80% |

---

## 12. Risks & Mitigations

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| **Baseline drift** — baseline becomes stale if not refreshed | Medium | High | Automated baseline refresh every 2 weeks; alert if baseline > 30 days old |
| **Large table hash computation** — FARM_FINGERPRINT on 100M+ row tables may be slow/costly | Medium | Medium | Use row count + aggregate checks for large tables; hash only sampled subset |
| **Terraform state not updated** — state file doesn't reflect actual deployed schema | Low | High | Cross-validate Terraform state with live BigQuery `INFORMATION_SCHEMA` |
| **Mock data not representative** — regression passes but real data breaks | Medium | Medium | Include edge-case records in mock data; document mock data coverage |
| **Alert fatigue** — too many non-actionable notifications | Medium | Low | Tune notification thresholds; group alerts by run ID; use severity tiers |

---

## 13. Appendix

### 13.1 Glossary

| Term | Definition |
|------|------------|
| **UET** | User Environment Testing — dedicated non-production GCP project used for regression runs |
| **DAG** | Directed Acyclic Graph — an Airflow workflow definition |
| **Baseline** | A versioned snapshot of schemas and data hashes captured before a deployment |
| **Regression** | Unexpected change in pipeline output given the same fixed input |
| **FARM_FINGERPRINT** | A BigQuery built-in function that computes a fast, deterministic hash of data |
| **Terraform Plan** | A preview of infrastructure changes that Terraform will apply |
| **INFORMATION_SCHEMA** | BigQuery metadata views containing table and column definitions |
| **Fixed Input** | Immutable mock data used as the input for deterministic regression runs |
| **Schema Hash** | A fingerprint of a table's column structure used to detect schema changes |

### 13.2 References

- [BigQuery INFORMATION_SCHEMA Documentation](https://cloud.google.com/bigquery/docs/information-schema-intro)
- [Terraform GCP Provider — BigQuery Resources](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_table)
- [Cloud Functions Gen 2 Documentation](https://cloud.google.com/functions/docs/concepts/version-comparison)
- [FARM_FINGERPRINT Function Reference](https://cloud.google.com/bigquery/docs/reference/standard-sql/hash_functions#farm_fingerprint)
- [Google Secret Manager](https://cloud.google.com/secret-manager/docs)
