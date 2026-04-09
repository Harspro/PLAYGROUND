# Requirements Document: Regression Testing Framework for Terminus Data Platform

**Document Version:** 1.0  
**Date:** April 2026  
**Status:** Draft for Stakeholder Review  
**Source:** Extracted from stakeholder meeting transcript

---

## Table of Contents

1. [Document Overview](#1-document-overview)
2. [Background](#2-background)
3. [Scope Clarifications](#3-scope-clarifications)
4. [Key Requirements](#4-key-requirements)
5. [Technical Constraints](#5-technical-constraints)
6. [Process Requirements](#6-process-requirements)
7. [Out of Scope](#7-out-of-scope)
8. [Key Decisions from Meeting](#8-key-decisions-from-meeting)
9. [Open Questions / Items to Define](#9-open-questions--items-to-define)
10. [Glossary](#10-glossary)

---

## 1. Document Overview

### 1.1 Document Details

| Field | Details |
|-------|---------|
| **Document Title** | Requirements: Regression Testing Framework for Terminus Data Platform |
| **Version** | 1.0 |
| **Date** | April 2026 |
| **Source** | Stakeholder meeting transcript |

### 1.2 Stakeholders

| Name | Role | Organization |
|------|------|--------------|
| Daniel Zhao | Product / Architecture Lead | PCB |
| Harshit Patel | Contractor — Backend Engineering | PCB-C |
| Madhu Chatterjee | Data Engineering | PCB |
| Izhaan Zubair | Engineering Lead | LCL |
| Chris Chalissery | Contractor — Engineering | LCL-C |

---

## 2. Background

The **Terminus Data Platform** is a GCP-based enterprise data platform with significant operational scale:

- **1,000+ Airflow DAGs** running across BigQuery, GCS, Dataproc, and Cloud Functions
- Each DAG represents a data pipeline that may load files, transform tables, publish Kafka/Pub/Sub messages, or execute complex BigQuery queries
- **Shared libraries** are used across many DAGs — a change to a shared library can impact hundreds of pipelines simultaneously
- **Terraform** is used to manage all infrastructure, including BigQuery table schemas

### Current Challenges

| Challenge | Detail |
|-----------|--------|
| No automated regression testing | When shared libraries or Terraform changes are made, the team must manually test hundreds of DAGs |
| Manual testing does not scale | Manually testing even a fraction of the 1,000+ DAGs per release cycle requires significant engineer time |
| Schema changes are not comprehensively tracked | The existing data management tool only tracks schemas for some tables, not all |
| No fixed-input test harness | Daily data changes make it impossible to deterministically compare pipeline outputs |
| No dedicated regression environment | The CI/CD pipeline environment cannot simulate real BigQuery query dependencies |

---

## 3. Scope Clarifications

> These clarifications were explicitly stated by Daniel Zhao during the meeting.

### 3.1 NOT Data Quality Testing

**This framework is specifically about regression testing — not data quality testing.**

- Data quality is a **separate consideration / initiative**
- While there may be some overlap in tooling or concepts, the **primary focus is regression testing**
- Regression testing asks: *"Did this pipeline's output change given the same input?"*
- Data quality asks: *"Is the data correct, complete, and consistent?"* — this is out of scope here

### 3.2 NOT CI/CD Pipeline Testing

**Regression tests will NOT run as part of the CI/CD pipeline.**

- The CI/CD environment is fundamentally different from the real environment (DEV, UET, production)
- Many DAGs depend on BigQuery query results, real GCS data, and live infrastructure that cannot be simulated in CI/CD
- Even if tests pass in CI/CD, they do not guarantee the DAG will run successfully in the real environment

---

## 4. Key Requirements

### 4.1 Environment Requirements

| Requirement | Detail |
|-------------|--------|
| **REQ-ENV-01** | A dedicated environment (separate from CI/CD) must be used for regression testing |
| **REQ-ENV-02** | The UET (User Environment Testing) environment shall be reserved exclusively for regression testing |
| **REQ-ENV-03** | All changes — Airflow DAG changes, Spark code changes, Terraform infrastructure changes — must be deployed to UET before regression testing runs |
| **REQ-ENV-04** | The UET environment must mirror the production GCP project configuration as closely as possible |

### 4.2 Testing Approach

| Requirement | Detail |
|-------------|--------|
| **REQ-TEST-01** | Testing must follow the **Fixed Input → Fixed Output** principle |
| **REQ-TEST-02** | Input data must be fixed and static (mock data that does not change between runs) |
| **REQ-TEST-03** | Given the same fixed input, the output must always be identical |
| **REQ-TEST-04** | If the output changes between runs given the same input, the regression is considered **FAILED** |
| **REQ-TEST-05** | Actual production data must NOT be used for regression testing (data changes daily, making deterministic comparison impossible) |

### 4.3 Verification Methods

> Keep verification simple. Do NOT over-engineer with field-by-field comparison.

| Requirement | Detail |
|-------------|--------|
| **REQ-VER-01** | Row count comparison must be supported as a verification method |
| **REQ-VER-02** | Aggregated value comparison (e.g., SUM of a numeric column) must be supported |
| **REQ-VER-03** | Hash-based verification of entire files or tables must be supported |
| **REQ-VER-04** | `FARM_FINGERPRINT` shall be used for whole-table hash computation in BigQuery |
| **REQ-VER-05** | Field-by-field (column-by-column) comparison is **NOT required** and should be avoided — it adds complexity without proportional value |

### 4.4 Scenarios to Cover

The framework must cover the following pipeline scenarios:

| Scenario | Verification Approach |
|----------|----------------------|
| **File Loading** | Generic approach — hash the output file or table after load |
| **Table Loading** | Similar to file loading — hash or aggregate checks on the loaded table |
| **Kafka / Pub/Sub Messages** | Verify that published messages match the expected output message set |
| **Schema Changes (Terraform)** | Capture schema JSON before and after; compare for unexpected changes |

### 4.5 Schema Change Detection

| Requirement | Detail |
|-------------|--------|
| **REQ-SCH-01** | Schema changes must be tracked via Terraform |
| **REQ-SCH-02** | The framework must capture ALL table schemas (not just tables tracked by the existing data management tool) |
| **REQ-SCH-03** | Schema must be captured **before** deploying changes (baseline) and **after** (current state) |
| **REQ-SCH-04** | The framework must compare baseline schema vs. current schema and report differences |
| **REQ-SCH-05** | Schema comparison must detect: column additions, column deletions, type changes, and mode changes |

### 4.6 Test Coverage

| Requirement | Detail |
|-------------|--------|
| **REQ-COV-01** | When a shared library changes, **all DAGs impacted by that library must be tested** — not just a sample |
| **REQ-COV-02** | A full regression run must cover all DAGs in the UET environment |
| **REQ-COV-03** | Individual test cases (mock data sets) should be **small and focused** to minimize execution time and BigQuery costs |
| **REQ-COV-04** | A "subset approach" is acceptable — test cases can be a representative subset of production data volume, but coverage must be comprehensive across all DAGs |

---

## 5. Technical Constraints

| Constraint | Detail |
|------------|--------|
| **CON-01** | **Great Expectations** cannot be installed within the Airflow environment — it has known compatibility conflicts with Airflow dependencies |
| **CON-02** | If Great Expectations or similar tools are used, they must run in a separate Docker container or Cloud Function environment |
| **CON-03** | The solution must be **simple and scriptable** — no complex orchestration frameworks |
| **CON-04** | The solution must not require running Airflow as part of the regression test harness itself |
| **CON-05** | BigQuery queries cannot be easily simulated in CI/CD — the real GCP environment is required for meaningful tests |

---

## 6. Process Requirements

| Requirement | Detail |
|-------------|--------|
| **REQ-PROC-01** | There must be a mechanism to **trigger a full regression run on demand** at a specified time (e.g., "run full regression over the next two weeks") |
| **REQ-PROC-02** | The framework must automatically verify pass or fail status for every DAG included in the regression run |
| **REQ-PROC-03** | Results must be reported in a centralized, accessible format (e.g., BigQuery results table, dashboard) |
| **REQ-PROC-04** | Alerts must be sent when regressions are detected |
| **REQ-PROC-05** | The regression cycle should be schedulable (e.g., every two weeks or before each major deployment) |

---

## 7. Out of Scope

The following are **explicitly out of scope** for this initiative:

| Item | Reason |
|------|--------|
| **Data quality validation** | Separate initiative; not the focus of this framework |
| **CI/CD pipeline-based testing** | The CI/CD environment cannot replicate real data dependencies |
| **Field-by-field data comparison** | Too complex; hash-based and aggregate checks are sufficient |
| **Great Expectations integration within Airflow** | Known compatibility conflict; would require separate environment adding complexity |
| **Real-time / production data monitoring** | This is regression testing, not production monitoring |
| **Performance testing** | Out of scope for v1.0 |

---

## 8. Key Decisions from Meeting

| # | Decision | Decision Maker |
|---|----------|---------------|
| **1** | Regression testing will **NOT** run in the CI/CD pipeline | Daniel Zhao |
| **2** | The dedicated **UET environment** will be used for all regression runs | Daniel Zhao |
| **3** | **Mock/fixed data** will be used instead of actual production data | Daniel Zhao, Madhu Chatterjee |
| **4** | **Hash-based verification** is preferred over detailed field-by-field comparison | Daniel Zhao |
| **5** | **All impacted DAGs** must be tested when a shared library changes — not just a sample | Madhu Chatterjee, Chris Chalissery |
| **6** | **Great Expectations** cannot be used within Airflow due to dependency conflicts | Daniel Zhao |
| **7** | Test cases should be **small and focused** (subset of production data volume) | Madhu Chatterjee |
| **8** | The framework must provide **automated pass/fail results** for each DAG | Daniel Zhao |

---

## 9. Open Questions / Items to Define

| # | Question | Priority | Owner |
|---|----------|----------|-------|
| **1** | Which tables should be included in the initial baseline capture? | High | Data Engineering |
| **2** | How will mock data be generated or selected for each DAG? | High | Data Engineering |
| **3** | What is the expected frequency of full regression cycles? | Medium | Daniel Zhao |
| **4** | What is the alert escalation process when a regression is detected? | Medium | Team Lead |
| **5** | Should schema additions (new columns) trigger a WARNING or a FAIL? | Medium | Daniel Zhao |
| **6** | How should Kafka/Pub/Sub regression verification be implemented specifically? | High | Backend Engineering |
| **7** | Which DAGs are "critical" and must always be included in every regression run? | High | Data Engineering |
| **8** | What is the target SLA for completing a full regression run? | Medium | Daniel Zhao |

---

## 10. Glossary

| Term | Definition |
|------|------------|
| **UET** | User Environment Testing — the dedicated non-production GCP environment reserved for regression testing |
| **DAG** | Directed Acyclic Graph — an Airflow workflow that defines a data pipeline |
| **Regression** | An unexpected change in pipeline output given the same fixed input, indicating a code or schema change has broken existing behavior |
| **Fixed Input** | Immutable mock data used as the input for deterministic regression testing |
| **Schema Hash** | A fingerprint (hash) of a table's column structure used to detect schema changes |
| **FARM_FINGERPRINT** | A BigQuery built-in hash function that computes a fast, deterministic fingerprint of data rows or tables |
| **Baseline** | A versioned snapshot of schemas and expected output hashes captured before a deployment |
| **Terraform** | An Infrastructure-as-Code tool used to manage GCP resources including BigQuery table schemas |
| **Shared Library** | A common Python library used across multiple Airflow DAGs; changes to a shared library can impact all DAGs that depend on it |
| **Great Expectations** | An open-source data quality tool that has known compatibility issues when installed in the Airflow environment |
