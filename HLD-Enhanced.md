# High-Level Design: Regression Testing Framework for Terraform-Based Schema Changes

**Document Version:** 3.0 (Enhanced)  
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
7. [Multi-Environment Strategy](#7-multi-environment-strategy)
8. [Cross-Zone Validation](#8-cross-zone-validation)
9. [Security & Access Control](#9-security--access-control)
10. [Data Privacy & PII Handling](#10-data-privacy--pii-handling)
11. [Implementation Plan](#11-implementation-plan)
12. [Cost Estimation](#12-cost-estimation)
13. [Success Criteria & SLAs](#13-success-criteria--slas)
14. [Risks & Mitigations](#14-risks--mitigations)
15. [Rollback Strategy](#15-rollback-strategy)
16. [Appendix](#16-appendix)

---

## 1. Executive Summary

| Field | Details |
|-------|---------|
| **Project Name** | Terminus Data Platform - Regression Testing Framework |
| **Objective** | Automated detection and validation of schema changes in BigQuery tables managed via Terragrunt/Terraform |
| **Scope** | GCP-based data platform with BigQuery, GCS, and Terragrunt-managed infrastructure across Dev/UAT/Prod |
| **Approach** | Baseline comparison model integrated directly into GitLab CI/CD |
| **Target Environments** | Dev, UAT (User Acceptance Testing), Prod |
| **Estimated Duration** | 5 weeks (3 phases) |

A lightweight, automated regression testing framework that:
- Detects schema changes introduced by Terragrunt across **three data zones** (Landing, Processing, Curated)
- Validates BigQuery table integrity across **three environments** (Dev, UAT, Prod)
- Integrates with existing **GitLab CI/CD pipelines** and **HashiCorp Vault**
- Handles **50+ BigQuery datasets** with prioritized testing

**Design Principles:**
- **Fully GCP-Native** — All data, triggers, and execution within GCP
- **GitLab CI Integrated** — Extends existing `.gitlab-ci.yml` pipeline patterns
- **Terragrunt-Aware** — Handles module versions, dependencies, and cross-zone relationships
- **Multi-Environment** — Separate baselines and thresholds per environment
- **PII-Safe** — Production sampling with data masking
- **Cost Optimized** — Pay only when tests run (~$30-80/month per environment)

---

## 2. Problem Statement

The Terminus data platform operates at scale with **150+ BigQuery tables** across **three data zones** managed via Terragrunt. Key challenges:

- **No automated regression detection** when Terragrunt changes are applied
- **Schema changes can break downstream consumers** — failures discovered only after deployment
- **Cross-zone dependencies** — Landing depends on Processing outputs, which depend on Curated
- **No visibility** into what changed in schema before vs. after a deployment cycle
- **Module version drift** — Different environments running different `terraform-gcp-terminus` versions
- **Manual regression testing** is slow and does not scale

### Current Pain Points

| Pain Point | Impact | Current State |
|------------|--------|---------------|
| Schema changes via Terragrunt not tracked | Silent downstream breakages | No detection |
| No fixed-input test harness | Cannot verify output consistency | Manual validation |
| No dedicated regression environment | CI/CD cannot simulate real BigQuery dependencies | Dev used for all testing |
| Cross-zone dependencies not validated | Processing changes break Landing | Manual coordination |
| Module version drift | Prod may have different schema than Dev | No tracking |
| PII in production data | Cannot sample for testing | No safe sampling |
| Manual validation effort | Hours of engineer time per release | ~4 hours/release |

### Current Infrastructure Context

```
pcf-gcp-terminus/
├── live/
│   ├── Dev/                          # Development Environment
│   │   ├── terragrunt.hcl            # Common config (pcb-dev-002--tfstate)
│   │   ├── 01_landing/               # pcb-dev-landing
│   │   ├── 02_processing/            # pcb-dev-processing
│   │   ├── 03_curated/               # pcb-dev-curated
│   │   └── 44_venafi_cert_scheduler/
│   ├── Uat/                          # UAT Environment
│   │   ├── terragrunt.hcl
│   │   ├── 01_landing/
│   │   ├── 02_processing/
│   │   └── 03_curated/
│   └── Prod/                         # Production Environment
│       ├── terragrunt.hcl            # Common config (pcb-prod-001--tfstate)
│       ├── 00_api_enablement/        # API enablement module
│       ├── 01_landing/               # pcb-prod-landing
│       ├── 02_processing/            # pcb-prod-processing
│       ├── 03_curated/               # pcb-prod-curated
│       └── 44_venafi_cert_scheduler/
```

---

## 3. Solution Overview

A fully GCP-native regression testing framework that:

- **Captures baseline schema snapshots** from BigQuery `INFORMATION_SCHEMA` per environment
- **Generates test input data** dynamically with PII masking from production samples
- **Validates cross-zone dependencies** (Landing → Processing → Curated)
- **Tracks module version drift** across environments
- **Stores all data in GCS/BigQuery** — no external storage dependencies
- **Integrates with GitLab CI** as additional pipeline stages
- **Reports results** to BigQuery and environment-specific Slack channels

### Core Principle: Zone-Aware, Environment-Specific Testing

```
┌─────────────────────────────────────────────────────────────────────────┐
│                 ZONE-AWARE DEPENDENCY VALIDATION                        │
└─────────────────────────────────────────────────────────────────────────┘

  ZONE EXECUTION ORDER (Based on Dependencies)
  ────────────────────────────────────────────────────────────────────────
  
  ┌───────────────┐      ┌───────────────┐      ┌───────────────┐
  │ 02_processing │─────▶│ 03_curated    │─────▶│ 01_landing    │
  │               │      │               │      │               │
  │ - Independent │      │ - Depends on  │      │ - Depends on  │
  │ - Run first   │      │   processing  │      │   processing  │
  │               │      │               │      │   and curated │
  └───────────────┘      └───────────────┘      └───────────────┘
         │                      │                      │
         ▼                      ▼                      ▼
  ┌───────────────────────────────────────────────────────────────────────┐
  │                    REGRESSION TEST EXECUTION                          │
  │                                                                       │
  │   1. Validate Processing zone schema                                  │
  │   2. Validate Curated zone schema (uses Processing outputs)           │
  │   3. Validate Landing zone schema (uses Processing + Curated outputs) │
  │   4. Cross-zone dependency validation                                 │
  └───────────────────────────────────────────────────────────────────────┘

  ENVIRONMENT PROMOTION FLOW
  ────────────────────────────────────────────────────────────────────────
  
  ┌─────────┐     ┌─────────┐     ┌─────────┐
  │   Dev   │────▶│   UAT   │────▶│  Prod   │
  │         │     │         │     │         │
  │ v1.54.3 │     │ v1.54.3 │     │ v1.54.3 │
  └─────────┘     └─────────┘     └─────────┘
       │               │               │
       ▼               ▼               ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │         ENVIRONMENT SCHEMA COMPATIBILITY CHECK                       │
  │                                                                      │
  │  Before promoting UAT → Prod:                                        │
  │    - Compare UAT baseline against Prod baseline                      │
  │    - Detect breaking changes that would affect Prod consumers        │
  │    - Validate module version compatibility                           │
  └─────────────────────────────────────────────────────────────────────┘
```

---

## 4. Architecture

### 4.1 High-Level Architecture (GitLab CI + GCP Native)

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        TERMINUS REGRESSION TESTING                      │
│                     GITLAB CI + GCP-NATIVE ARCHITECTURE                 │
└─────────────────────────────────────────────────────────────────────────┘

  GITLAB CI PIPELINE INTEGRATION
  ┌─────────────────────────────────────────────────────────────────────┐
  │                                                                     │
  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │
  │  │ tfplan-*     │  │ tfapply-*    │  │ regression-* │              │
  │  │ (existing)   │──▶│ (existing)   │──▶│ (NEW STAGE)  │              │
  │  └──────────────┘  └──────────────┘  └──────┬───────┘              │
  │                                             │                       │
  │  Stages: init → tfplan-processing → tfapply-processing              │
  │          → tfplan-curated → tfapply-curated                         │
  │          → tfplan-landing → tfapply-landing                         │
  │          → regression-test (NEW)                                    │
  │                                             │                       │
  └─────────────────────────────────────────────┼───────────────────────┘
                                                │
                ┌───────────────────────────────┘
                ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │                      CLOUD RUN JOB                                  │
  │                   (regression-tester-{env})                         │
  │                                                                     │
  │   ┌─────────────────────────────────────────────────────────────┐   │
  │   │  1. INPUT GENERATION (PII-Masked)                           │   │
  │   │     - Sample production data with masking                   │   │
  │   │     - Generate synthetic edge cases                         │   │
  │   │     - Store in gs://pcb-{env}-regression-data/inputs/       │   │
  │   └─────────────────────────────────────────────────────────────┘   │
  │                              │                                      │
  │                              ▼                                      │
  │   ┌─────────────────────────────────────────────────────────────┐   │
  │   │  2. CROSS-ZONE BASELINE CAPTURE / COMPARE                   │   │
  │   │     - Capture Processing → Curated → Landing (in order)     │   │
  │   │     - Compare against environment-specific baseline         │   │
  │   │     - Detect schema changes + dependency impacts            │   │
  │   └─────────────────────────────────────────────────────────────┘   │
  │                              │                                      │
  │                              ▼                                      │
  │   ┌─────────────────────────────────────────────────────────────┐   │
  │   │  3. MODULE VERSION TRACKING                                 │   │
  │   │     - Parse terraform_gcp_terminus_revision                 │   │
  │   │     - Compare versions across environments                  │   │
  │   │     - Alert on version drift                                │   │
  │   └─────────────────────────────────────────────────────────────┘   │
  │                              │                                      │
  │                              ▼                                      │
  │   ┌─────────────────────────────────────────────────────────────┐   │
  │   │  4. DATA VALIDATION (Prioritized Datasets)                  │   │
  │   │     - Critical: domain_payments, domain_fraud, domain_aml   │   │
  │   │     - Standard: All other domain_* datasets                 │   │
  │   │     - Low: cots_* datasets                                  │   │
  │   └─────────────────────────────────────────────────────────────┘   │
  │                              │                                      │
  │                              ▼                                      │
  │   ┌─────────────────────────────────────────────────────────────┐   │
  │   │  5. OUTPUT & REPORTING                                      │   │
  │   │     - Write results to GCS + BigQuery                       │   │
  │   │     - Send to environment-specific Slack channel            │   │
  │   │     - Update Prometheus metrics (existing monitoring)       │   │
  │   └─────────────────────────────────────────────────────────────┘   │
  └─────────────────────────────────────────────────────────────────────┘
                                 │
           ┌─────────────────────┼─────────────────────────────────┐
           ▼                     ▼                                 ▼
  ┌────────────────┐   ┌────────────────┐              ┌────────────────┐
  │  GCS Buckets   │   │   BigQuery     │              │    Slack       │
  │  (per env)     │   │  (per env)     │              │  (per env)     │
  │                │   │                │              │                │
  │  pcb-{env}-    │   │  pcb-{env}-    │              │ #gcp-dev-alerts│
  │  regression-   │   │  processing.   │              │ #gcp-uat-alerts│
  │  data/         │   │  regression_   │              │ #gcp-prod-alert│
  │                │   │  results       │              │                │
  └────────────────┘   └────────────────┘              └────────────────┘
```

### 4.2 Environment-Specific GCS Data Organization

```
gs://pcb-{env}-regression-data/           # Per environment bucket
│
├── inputs/                               # Test input data (PII-masked)
│   ├── manifest.json                     # Lists all input datasets
│   ├── domain_payments/
│   │   ├── data.json                     # Masked payment records
│   │   └── schema.json                   # Expected schema
│   ├── domain_fraud/
│   │   ├── data.json
│   │   └── schema.json
│   └── domain_customer_management/
│       ├── data.json                     # Masked customer data
│       └── schema.json
│
├── baselines/                            # Schema and data baselines
│   ├── current/
│   │   ├── processing_baseline.json      # 02_processing zone baseline
│   │   ├── curated_baseline.json         # 03_curated zone baseline
│   │   ├── landing_baseline.json         # 01_landing zone baseline
│   │   └── module_versions.json          # Tracked module versions
│   └── history/
│       ├── 2026-04-01T00:00:00Z/
│       │   ├── processing_baseline.json
│       │   ├── curated_baseline.json
│       │   └── landing_baseline.json
│       └── 2026-04-08T00:00:00Z/
│
├── outputs/                              # Test run outputs
│   └── {run_id}/
│       ├── schema_diff.json
│       ├── cross_zone_validation.json    # NEW: Zone dependency results
│       ├── module_drift.json             # NEW: Version drift report
│       ├── data_validation.json
│       └── summary.json
│
└── config/                               # Configuration files
    ├── input_config.json                 # Input generation config
    ├── priority_datasets.json            # Dataset priority tiers
    └── pii_masking_rules.json            # PII masking configuration
```

### 4.3 State Bucket Monitoring

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    TERRAFORM STATE BUCKET TRIGGERS                      │
└─────────────────────────────────────────────────────────────────────────┘

  Environment State Buckets (Existing):
  ─────────────────────────────────────────────────────────────────────────
  
  ┌────────────────────────┐     ┌────────────────────────┐
  │  pcb-dev-002--tfstate  │     │  pcb-uat-XXX--tfstate  │
  │  (Dev environment)     │     │  (UAT environment)     │
  └───────────┬────────────┘     └───────────┬────────────┘
              │                              │
              │    ┌────────────────────────┐│
              │    │ pcb-prod-001--tfstate  ││
              │    │ (Prod environment)     ││
              │    └───────────┬────────────┘│
              │                │             │
              └────────────────┼─────────────┘
                               │
                               ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │                        EVENTARC TRIGGERS                            │
  │                                                                     │
  │   On state file update (google.cloud.storage.object.v1.finalized):  │
  │     → Trigger regression-tester-{env} Cloud Run Job                 │
  │     → Pass environment context via --args                           │
  │                                                                     │
  └─────────────────────────────────────────────────────────────────────┘
```

### 4.4 GitLab CI Pipeline Integration

```yaml
# NEW STAGES added to existing .gitlab-ci.yml
stages:
  - init
  - tfplan-processing
  - tfapply-processing
  - tfplan-curated
  - tfapply-curated
  - tfplan-landing
  - tfapply-landing
  - regression-test          # NEW: Regression testing stage
  - regression-baseline      # NEW: Baseline capture stage

# Regression test job (runs after all tf-apply jobs)
regression-test-dev:
  stage: regression-test
  dependencies:
    - tf-apply-dev-processing
    - tf-apply-dev-curated
    - tf-apply-dev-landing
  # ... configuration below in Appendix
```

---

## 5. Technology Stack

| Layer | Technology | Rationale |
|-------|------------|-----------|
| **CI/CD** | GitLab CI | Existing pipeline infrastructure |
| **Compute** | Cloud Run Job | Pay-per-use, up to 1hr execution, no infra |
| **Triggers** | GitLab CI / Cloud Scheduler / Eventarc | Environment-specific triggers |
| **Storage** | GCS (per env), BigQuery (per env) | Native GCP, environment isolation |
| **Secrets** | HashiCorp Vault | Existing secret management |
| **Auth** | Workload Identity Federation | Existing auth pattern |
| **Language** | Python 3.11+ | google-cloud SDK, Vault integration |
| **Notifications** | Slack (env-specific channels) | Existing channel structure |
| **Monitoring** | Prometheus | Existing monitoring-prometheus SA |
| **IaC** | Terragrunt/Terraform | Consistent with existing infrastructure |

**Why This Stack:**
- **GitLab CI native** — Extends existing `.gitlab-ci.yml` patterns
- **Vault integration** — Uses existing `dp-gcp-terminus-terraformer` role
- **WIF authentication** — Matches existing `WI_POOL_PROVIDER` setup
- **Environment-specific** — Separate resources per Dev/UAT/Prod
- **Minimal new infrastructure** — Reuses existing patterns and service accounts

---

## 6. Component Specifications

### 6.1 Input Data Generator (PII-Safe)

Generates test input data with PII masking for safe production sampling.

**Masking Strategies:**

| Data Type | Masking Strategy | Example |
|-----------|------------------|---------|
| Customer ID | Hash with salt | `CUST123` → `a1b2c3d4` |
| Email | Domain preservation | `john@example.com` → `masked_1@example.com` |
| Phone | Last 4 digits only | `416-555-1234` → `XXX-XXX-1234` |
| SIN/SSN | Full mask | `123-456-789` → `XXX-XXX-XXX` |
| Account Number | Partial mask | `1234567890` → `XXXXXX7890` |
| Name | Synthetic replacement | `John Smith` → `Test User 1` |
| Address | City only | `123 Main St, Toronto` → `Toronto, ON` |

**Input Generator Code:**

```python
class InputDataGenerator:
    """Generate PII-masked test input data from BigQuery."""
    
    def __init__(self, bq_client, gcs_client, vault_client, config: dict):
        self.bq = bq_client
        self.gcs = gcs_client
        self.vault = vault_client
        self.bucket = config["input_bucket"]
        self.config = config
        self.masking_rules = self._load_masking_rules()
    
    def _load_masking_rules(self) -> dict:
        """Load PII masking rules from GCS config."""
        return self._read_from_gcs("config/pii_masking_rules.json")
    
    def generate_inputs(self, environment: str) -> dict:
        """Generate masked input data for all configured tables."""
        manifest = {
            "generated_at": datetime.utcnow().isoformat(),
            "environment": environment,
            "tables": {}
        }
        
        for table_config in self.config["tables"]:
            table_name = table_config["table"]
            dataset_id = table_config["dataset"]
            
            if table_config.get("strategy") == "production_sample":
                raw_data = self._sample_from_production(
                    dataset_id, 
                    table_name,
                    table_config
                )
                # Apply PII masking
                masked_data = self._apply_masking(raw_data, table_config)
                data = masked_data
            else:
                data = self._generate_synthetic(table_config)
            
            # Save to GCS
            self._save_to_gcs(f"inputs/{table_name}/data.json", data)
            manifest["tables"][table_name] = {
                "row_count": len(data),
                "strategy": table_config.get("strategy", "synthetic"),
                "masked": table_config.get("strategy") == "production_sample"
            }
        
        self._save_to_gcs("inputs/manifest.json", manifest)
        return manifest
    
    def _apply_masking(self, data: list, config: dict) -> list:
        """Apply PII masking rules to sampled data."""
        masked_data = []
        
        for row in data:
            masked_row = dict(row)
            for column, value in row.items():
                if column in self.masking_rules:
                    rule = self.masking_rules[column]
                    masked_row[column] = self._mask_value(value, rule)
            masked_data.append(masked_row)
        
        return masked_data
    
    def _mask_value(self, value: Any, rule: dict) -> Any:
        """Apply specific masking rule to a value."""
        strategy = rule["strategy"]
        
        if strategy == "hash":
            salt = self.vault.read_secret("regression/masking_salt")
            return hashlib.sha256(f"{salt}{value}".encode()).hexdigest()[:16]
        elif strategy == "partial_mask":
            keep_last = rule.get("keep_last", 4)
            return "X" * (len(str(value)) - keep_last) + str(value)[-keep_last:]
        elif strategy == "synthetic":
            return f"{rule.get('prefix', 'masked')}_{hash(value) % 10000}"
        elif strategy == "full_mask":
            return rule.get("replacement", "XXXXX")
        
        return value
```

**PII Masking Configuration (stored in GCS):**

```json
{
  "version": "1.0",
  "columns": {
    "customer_id": {
      "strategy": "hash",
      "description": "Hash customer IDs for referential integrity"
    },
    "email": {
      "strategy": "synthetic",
      "prefix": "test_user",
      "suffix": "@example.com"
    },
    "phone_number": {
      "strategy": "partial_mask",
      "keep_last": 4
    },
    "sin_number": {
      "strategy": "full_mask",
      "replacement": "XXX-XXX-XXX"
    },
    "account_number": {
      "strategy": "partial_mask",
      "keep_last": 4
    },
    "first_name": {
      "strategy": "synthetic",
      "prefix": "TestFirst"
    },
    "last_name": {
      "strategy": "synthetic",
      "prefix": "TestLast"
    },
    "address": {
      "strategy": "full_mask",
      "replacement": "123 Test Street"
    }
  }
}
```

### 6.2 Dataset Priority Manager

Manages prioritized testing of 50+ BigQuery datasets.

```python
class DatasetPriorityManager:
    """Manage dataset testing priorities."""
    
    # Priority tiers based on business criticality
    PRIORITY_TIERS = {
        "critical": {
            "datasets": [
                "domain_payments",
                "domain_fraud",
                "domain_aml",
                "domain_ledger",
                "domain_account_management",
                "domain_customer_management"
            ],
            "validation_level": "full",
            "alert_on_any_change": True,
            "sla_minutes": 5
        },
        "high": {
            "datasets": [
                "domain_scoring",
                "domain_securitization",
                "domain_treasury",
                "domain_dispute",
                "domain_communication",
                "domain_customer_acquisition",
                "domain_validation_verification"
            ],
            "validation_level": "full",
            "alert_on_any_change": True,
            "sla_minutes": 10
        },
        "standard": {
            "datasets": [
                "domain_loyalty",
                "domain_marketing",
                "domain_retail",
                "domain_technical",
                "domain_audit",
                "domain_customer_service",
                "domain_talent_acquisiton",
                "domain_consent",
                "domain_iam",
                "domain_dns",
                "domain_payments_ops",
                "domain_tax_slips",
                "domain_cdic",
                "domain_card_management",
                "domain_ops_adhoc",
                "domain_approval",
                "domain_fraud_risk_intelligence",
                "domain_customer_contact_centre",
                "domain_cri_compliance",
                "domain_data_logs",
                "domain_poa",
                "domain_movemoney_technical"
            ],
            "validation_level": "schema_only",
            "alert_on_breaking_change": True,
            "sla_minutes": 15
        },
        "low": {
            "datasets": [
                "cots_fraud_ffm",
                "cots_hr",
                "cots_customer_service_blockworx",
                "cots_customer_service_ctt",
                "cots_security_ciam",
                "cots_mft_sterling",
                "cots_aml_sas",
                "cots_alm_tbsm",
                "cots_aml_verafin",
                "cots_cdic",
                "cots_scms_tkm",
                "cots_scms_andis_se",
                "cots_digital_marketing",
                "cots_loyalty_teradata"
            ],
            "validation_level": "existence_only",
            "alert_on_deletion": True,
            "sla_minutes": 30
        }
    }
    
    def __init__(self, config: dict):
        self.config = config
        self.priorities = self.PRIORITY_TIERS
    
    def get_ordered_datasets(self, zone: str) -> list:
        """Get datasets ordered by priority for a specific zone."""
        ordered = []
        for tier in ["critical", "high", "standard", "low"]:
            tier_config = self.priorities[tier]
            for dataset in tier_config["datasets"]:
                ordered.append({
                    "dataset": dataset,
                    "priority": tier,
                    "validation_level": tier_config["validation_level"],
                    "sla_minutes": tier_config["sla_minutes"]
                })
        return ordered
    
    def get_validation_level(self, dataset: str) -> str:
        """Get validation level for a specific dataset."""
        for tier, config in self.priorities.items():
            if dataset in config["datasets"]:
                return config["validation_level"]
        return "existence_only"  # Default for unknown datasets
    
    def should_alert(self, dataset: str, change_type: str) -> bool:
        """Determine if an alert should be sent for this change."""
        for tier, config in self.priorities.items():
            if dataset in config["datasets"]:
                if config.get("alert_on_any_change"):
                    return True
                if config.get("alert_on_breaking_change") and change_type == "breaking":
                    return True
                if config.get("alert_on_deletion") and change_type == "deletion":
                    return True
        return False
```

### 6.3 Cross-Zone Dependency Validator

Validates dependencies between zones based on Terragrunt configuration.

```python
class CrossZoneValidator:
    """Validate cross-zone dependencies in Terragrunt configurations."""
    
    # Zone dependency graph (from terragrunt.hcl analysis)
    ZONE_DEPENDENCIES = {
        "01_landing": {
            "depends_on": ["02_processing", "03_curated"],
            "outputs_used": [
                "dataproc_sa",
                "project_id",
                "domain_audit_sa",
                "data_foundation_composer_sa",
                "data_foundation_composer_web_uri",
                "money_movement_composer_sa",
                "money_movement_composer_web_uri"
            ]
        },
        "02_processing": {
            "depends_on": [],
            "outputs_produced": [
                "dataproc_sa",
                "project_id",
                "domain_audit_sa",
                "data_foundation_composer_sa",
                "data_foundation_composer_web_uri",
                "money_movement_composer_sa",
                "money_movement_composer_web_uri"
            ]
        },
        "03_curated": {
            "depends_on": ["02_processing"],
            "outputs_used": [
                # Curated uses processing outputs
            ]
        }
    }
    
    def __init__(self, bq_client, gcs_client, environment: str):
        self.bq = bq_client
        self.gcs = gcs_client
        self.environment = environment
    
    def validate_dependencies(self, changed_zone: str, changes: dict) -> dict:
        """Validate if changes in one zone break dependent zones."""
        results = {
            "zone": changed_zone,
            "impacts": [],
            "status": "PASS"
        }
        
        # Find zones that depend on the changed zone
        dependent_zones = self._get_dependent_zones(changed_zone)
        
        for dep_zone in dependent_zones:
            dep_config = self.ZONE_DEPENDENCIES[dep_zone]
            outputs_used = dep_config.get("outputs_used", [])
            
            # Check if any changed outputs are used by dependent zone
            for output in outputs_used:
                if self._is_output_affected(output, changes):
                    results["impacts"].append({
                        "dependent_zone": dep_zone,
                        "affected_output": output,
                        "severity": "HIGH",
                        "message": f"Zone {dep_zone} uses output '{output}' which was modified"
                    })
                    results["status"] = "FAIL"
        
        return results
    
    def _get_dependent_zones(self, zone: str) -> list:
        """Get list of zones that depend on the given zone."""
        dependents = []
        for z, config in self.ZONE_DEPENDENCIES.items():
            if zone in config.get("depends_on", []):
                dependents.append(z)
        return dependents
    
    def validate_promotion(self, source_env: str, target_env: str) -> dict:
        """Validate schema compatibility for environment promotion."""
        results = {
            "source": source_env,
            "target": target_env,
            "breaking_changes": [],
            "status": "PASS"
        }
        
        # Load baselines for both environments
        source_baseline = self._load_env_baseline(source_env)
        target_baseline = self._load_env_baseline(target_env)
        
        # Compare each zone
        for zone in ["02_processing", "03_curated", "01_landing"]:
            zone_diff = self._compare_zone_baselines(
                source_baseline.get(zone, {}),
                target_baseline.get(zone, {})
            )
            
            if zone_diff["breaking_changes"]:
                results["breaking_changes"].extend(zone_diff["breaking_changes"])
                results["status"] = "FAIL"
        
        return results
```

### 6.4 Module Version Tracker

Tracks Terraform module versions across environments.

```python
class ModuleVersionTracker:
    """Track terraform-gcp-terminus module versions across environments."""
    
    def __init__(self, gcs_client, environments: list):
        self.gcs = gcs_client
        self.environments = environments  # ["dev", "uat", "prod"]
    
    def capture_versions(self, environment: str) -> dict:
        """Capture current module versions for an environment."""
        versions = {
            "environment": environment,
            "captured_at": datetime.utcnow().isoformat(),
            "modules": {}
        }
        
        # Parse terragrunt.hcl for module versions
        # In actual implementation, this would read from GCS or git
        terragrunt_config = self._read_terragrunt_config(environment)
        
        versions["modules"]["terraform_gcp_terminus"] = {
            "version": terragrunt_config.get("terraform_gcp_terminus_revision"),
            "source": "git::https://gitlab.lblw.ca/.../terraform-gcp-terminus.git"
        }
        
        return versions
    
    def check_drift(self) -> dict:
        """Check for version drift across environments."""
        drift_report = {
            "checked_at": datetime.utcnow().isoformat(),
            "environments": {},
            "drift_detected": False,
            "recommendations": []
        }
        
        versions_by_env = {}
        for env in self.environments:
            versions_by_env[env] = self.capture_versions(env)
            drift_report["environments"][env] = versions_by_env[env]
        
        # Compare versions
        terminus_versions = {
            env: v["modules"]["terraform_gcp_terminus"]["version"]
            for env, v in versions_by_env.items()
        }
        
        unique_versions = set(terminus_versions.values())
        if len(unique_versions) > 1:
            drift_report["drift_detected"] = True
            drift_report["version_summary"] = terminus_versions
            
            # Generate recommendations
            prod_version = terminus_versions.get("prod")
            for env, version in terminus_versions.items():
                if version != prod_version and env != "prod":
                    drift_report["recommendations"].append({
                        "environment": env,
                        "current_version": version,
                        "recommended_version": prod_version,
                        "action": f"Update {env} to match prod version {prod_version}"
                    })
        
        return drift_report
    
    def alert_on_drift(self, drift_report: dict, slack_webhook: str):
        """Send Slack alert if version drift detected."""
        if not drift_report["drift_detected"]:
            return
        
        payload = {
            "text": "⚠️ Module Version Drift Detected",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "*terraform-gcp-terminus Version Drift*"
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Dev:* {drift_report['version_summary'].get('dev', 'N/A')}"},
                        {"type": "mrkdwn", "text": f"*UAT:* {drift_report['version_summary'].get('uat', 'N/A')}"},
                        {"type": "mrkdwn", "text": f"*Prod:* {drift_report['version_summary'].get('prod', 'N/A')}"}
                    ]
                }
            ]
        }
        requests.post(slack_webhook, json=payload)
```

### 6.5 Baseline Manager (Multi-Zone)

```python
class BaselineManager:
    """Manage schema baselines for multiple zones and environments."""
    
    def __init__(self, bq_client, gcs_client, environment: str):
        self.bq = bq_client
        self.gcs = gcs_client
        self.environment = environment
        self.bucket = f"pcb-{environment}-regression-data"
    
    def capture_all_zones(self) -> dict:
        """Capture baselines for all zones in order of dependencies."""
        all_baselines = {
            "environment": self.environment,
            "captured_at": datetime.utcnow().isoformat(),
            "zones": {}
        }
        
        # Capture in dependency order
        zone_projects = {
            "02_processing": f"pcb-{self.environment}-processing",
            "03_curated": f"pcb-{self.environment}-curated",
            "01_landing": f"pcb-{self.environment}-landing"
        }
        
        for zone, project in zone_projects.items():
            all_baselines["zones"][zone] = self._capture_zone_baseline(project)
        
        # Save baselines
        self._save_baselines(all_baselines)
        return all_baselines
    
    def _capture_zone_baseline(self, project_id: str) -> dict:
        """Capture schema baseline for a specific zone/project."""
        query = f"""
            SELECT 
                table_schema,
                table_name,
                column_name,
                data_type,
                is_nullable,
                ordinal_position
            FROM `{project_id}.region-northamerica-northeast1.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_schema NOT IN ('INFORMATION_SCHEMA')
            ORDER BY table_schema, table_name, ordinal_position
        """
        
        baseline = {"tables": {}, "project_id": project_id}
        
        for row in self.bq.query(query).result():
            key = f"{row.table_schema}.{row.table_name}"
            if key not in baseline["tables"]:
                baseline["tables"][key] = {"columns": []}
            
            baseline["tables"][key]["columns"].append({
                "name": row.column_name,
                "type": row.data_type,
                "mode": "REQUIRED" if row.is_nullable == "NO" else "NULLABLE",
                "position": row.ordinal_position
            })
        
        baseline["table_count"] = len(baseline["tables"])
        return baseline
    
    def compare_with_baseline(self, zone: str) -> dict:
        """Compare current schema against stored baseline for a zone."""
        current = self._capture_zone_baseline(
            f"pcb-{self.environment}-{zone.split('_')[1]}"
        )
        stored = self._load_zone_baseline(zone)
        
        changes = {
            "zone": zone,
            "breaking": [],
            "additive": [],
            "unchanged": []
        }
        
        # ... comparison logic (similar to original design)
        
        return changes
```

### 6.6 Environment-Specific Notifier

```python
class EnvironmentNotifier:
    """Send notifications to environment-specific Slack channels."""
    
    # Channel mapping (from existing .gitlab-ci.yml)
    SLACK_CHANNELS = {
        "dev": "#gcp-dev-alerts",
        "uat": "#gcp-uat-alerts",
        "prod": "#gcp-prod-alerts",
        "pipeline": "#gitlab-pipeline-alerts"
    }
    
    def __init__(self, vault_client, environment: str):
        self.vault = vault_client
        self.environment = environment
        self.webhook_url = self._get_webhook_url()
    
    def _get_webhook_url(self) -> str:
        """Get Slack webhook URL from Vault."""
        secret = self.vault.read(
            "pcf-engineering/platforms/pcf-gcp-management/pcf-terminus/pcf-gcp-terminus/SLACK_WEBHOOK_URL"
        )
        return secret["data"]["value"]
    
    def notify_regression_result(self, results: dict, job_url: str):
        """Send regression test results to appropriate channel."""
        channel = self.SLACK_CHANNELS.get(self.environment, "#gcp-dev-alerts")
        
        if results["overall_status"] == "PASS":
            # Only notify on failure or for prod
            if self.environment != "prod":
                return
            color = "good"
            status_emoji = "✅"
        else:
            color = "danger"
            status_emoji = "❌"
        
        # Build failure details
        failure_summary = []
        for zone_result in results.get("zone_results", []):
            if zone_result["status"] == "FAIL":
                failure_summary.append(
                    f"• *{zone_result['zone']}*: {len(zone_result['breaking_changes'])} breaking changes"
                )
        
        payload = {
            "channel": channel,
            "text": f"{status_emoji} Regression Test {results['overall_status']}",
            "attachments": [
                {
                    "color": color,
                    "blocks": [
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": f"*Environment:* {self.environment.upper()}\n" +
                                       f"*Status:* {results['overall_status']}\n" +
                                       f"*Run ID:* {results['run_id']}"
                            }
                        },
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": "*Failures:*\n" + "\n".join(failure_summary) if failure_summary else "No failures"
                            }
                        },
                        {
                            "type": "actions",
                            "elements": [
                                {
                                    "type": "button",
                                    "text": {"type": "plain_text", "text": "View Job"},
                                    "url": job_url
                                }
                            ]
                        }
                    ]
                }
            ]
        }
        
        requests.post(self.webhook_url, json=payload)
    
    def notify_module_drift(self, drift_report: dict):
        """Send module version drift notification."""
        if not drift_report["drift_detected"]:
            return
        
        channel = self.SLACK_CHANNELS["pipeline"]
        
        payload = {
            "channel": channel,
            "text": "⚠️ Module Version Drift",
            "attachments": [
                {
                    "color": "warning",
                    "text": f"terraform-gcp-terminus versions differ across environments:\n"
                           f"• Dev: {drift_report['version_summary'].get('dev', 'N/A')}\n"
                           f"• UAT: {drift_report['version_summary'].get('uat', 'N/A')}\n"
                           f"• Prod: {drift_report['version_summary'].get('prod', 'N/A')}"
                }
            ]
        }
        
        requests.post(self.webhook_url, json=payload)
```

---

## 7. Multi-Environment Strategy

### 7.1 Environment Configuration

| Environment | State Bucket | Projects | Vault Address | Slack Channel |
|-------------|--------------|----------|---------------|---------------|
| Dev | `pcb-dev-002--tfstate` | `pcb-dev-{landing,processing,curated}` | `web-vault.dolphin.azure.nonprod` | `#gcp-dev-alerts` |
| UAT | `pcb-uat-XXX--tfstate` | `pcb-uat-{landing,processing,curated}` | `web-vault.dolphin.azure.nonprod` | `#gcp-uat-alerts` |
| Prod | `pcb-prod-001--tfstate` | `pcb-prod-{landing,processing,curated}` | `web-vault.octopus.azure.prod` | `#gcp-prod-alerts` |

### 7.2 Baseline Isolation

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    ENVIRONMENT BASELINE ISOLATION                       │
└─────────────────────────────────────────────────────────────────────────┘

  Each environment maintains independent baselines:
  
  ┌─────────────────────────────────────────────────────────────────────┐
  │                                                                     │
  │  gs://pcb-dev-regression-data/baselines/                            │
  │    ├── current/                                                     │
  │    │   ├── processing_baseline.json   (pcb-dev-processing)         │
  │    │   ├── curated_baseline.json      (pcb-dev-curated)            │
  │    │   └── landing_baseline.json      (pcb-dev-landing)            │
  │    └── history/...                                                  │
  │                                                                     │
  │  gs://pcb-uat-regression-data/baselines/                            │
  │    ├── current/                                                     │
  │    │   ├── processing_baseline.json   (pcb-uat-processing)         │
  │    │   ├── curated_baseline.json      (pcb-uat-curated)            │
  │    │   └── landing_baseline.json      (pcb-uat-landing)            │
  │    └── history/...                                                  │
  │                                                                     │
  │  gs://pcb-prod-regression-data/baselines/                           │
  │    ├── current/                                                     │
  │    │   ├── processing_baseline.json   (pcb-prod-processing)        │
  │    │   ├── curated_baseline.json      (pcb-prod-curated)           │
  │    │   └── landing_baseline.json      (pcb-prod-landing)           │
  │    └── history/...                                                  │
  │                                                                     │
  └─────────────────────────────────────────────────────────────────────┘
```

### 7.3 Environment Promotion Validation

Before promoting changes from one environment to another:

```python
def validate_environment_promotion(source_env: str, target_env: str) -> dict:
    """
    Validate schema compatibility before environment promotion.
    
    Example: Before deploying UAT changes to Prod
    """
    validator = CrossZoneValidator(bq_client, gcs_client, source_env)
    
    return validator.validate_promotion(source_env, target_env)
```

| Promotion | Validation Level | Blocking Failures |
|-----------|------------------|-------------------|
| Dev → UAT | Schema compatibility check | Breaking changes in critical datasets |
| UAT → Prod | Full regression suite | Any breaking changes |

### 7.4 Environment-Specific Thresholds

| Metric | Dev | UAT | Prod |
|--------|-----|-----|------|
| Max execution time | 15 min | 10 min | 5 min |
| Alert on additive changes | No | No | Yes |
| Alert on any schema change | No | Critical only | All datasets |
| Data validation | Schema only | Schema + row counts | Full validation |
| Rollback on failure | No | Manual | Automatic (planned) |

---

## 8. Cross-Zone Validation

### 8.1 Zone Dependency Graph

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      ZONE DEPENDENCY GRAPH                              │
│                   (Based on terragrunt.hcl analysis)                    │
└─────────────────────────────────────────────────────────────────────────┘

                        ┌───────────────────┐
                        │   02_processing   │
                        │                   │
                        │ • Independent     │
                        │ • No dependencies │
                        │                   │
                        │ Outputs:          │
                        │ - dataproc_sa     │
                        │ - project_id      │
                        │ - domain_audit_sa │
                        │ - composer_sa     │
                        │ - composer_uri    │
                        └─────────┬─────────┘
                                  │
                    ┌─────────────┴─────────────┐
                    │                           │
                    ▼                           ▼
        ┌───────────────────┐       ┌───────────────────┐
        │   03_curated      │       │   01_landing      │
        │                   │       │                   │
        │ Dependencies:     │       │ Dependencies:     │
        │ - processing      │       │ - processing      │
        │                   │       │ - curated         │
        │ Uses:             │       │                   │
        │ - (internal)      │       │ Uses:             │
        │                   │       │ - dataproc_sa     │
        └───────────────────┘       │ - project_id      │
                    │               │ - domain_audit_sa │
                    │               │ - composer_sa     │
                    └───────┬───────┘
                            │
                            ▼
                ┌───────────────────────┐
                │   VALIDATION ORDER    │
                │                       │
                │ 1. processing (first) │
                │ 2. curated            │
                │ 3. landing (last)     │
                └───────────────────────┘
```

### 8.2 Cross-Zone Impact Analysis

When a change is detected in one zone, analyze impact on dependent zones:

```python
# Example: Processing zone change impact
{
    "changed_zone": "02_processing",
    "change_type": "output_modified",
    "affected_output": "dataproc_sa",
    "impacts": [
        {
            "dependent_zone": "01_landing",
            "uses_output": "dataproc_sa",
            "impact_severity": "HIGH",
            "recommendation": "Verify Landing zone still receives correct SA"
        }
    ]
}
```

### 8.3 Cross-Zone Validation Rules

| Rule | Description | Severity |
|------|-------------|----------|
| Output removal | Zone removes an output used by dependents | CRITICAL |
| Output type change | Output type changes (string → list) | HIGH |
| Output value format | Output value format changes | MEDIUM |
| New output | Zone adds new output | INFO |

---

## 9. Security & Access Control

### 9.1 Service Account (Per Environment)

```hcl
# Minimal permissions per environment
resource "google_service_account" "regression_sa" {
  for_each = toset(["dev", "uat", "prod"])
  
  account_id   = "regression-tester-${each.key}"
  display_name = "Regression Tester - ${upper(each.key)}"
  project      = "pcb-${each.key}-processing"
}

# Read-only BigQuery access to all zone projects
resource "google_project_iam_member" "bq_viewer" {
  for_each = {
    for pair in setproduct(["dev", "uat", "prod"], ["landing", "processing", "curated"]) :
    "${pair[0]}-${pair[1]}" => {
      env  = pair[0]
      zone = pair[1]
    }
  }
  
  project = "pcb-${each.value.env}-${each.value.zone}"
  role    = "roles/bigquery.dataViewer"
  member  = "serviceAccount:regression-tester-${each.value.env}@pcb-${each.value.env}-processing.iam.gserviceaccount.com"
}

# Write to regression dataset only
resource "google_bigquery_dataset_iam_member" "results_writer" {
  for_each = toset(["dev", "uat", "prod"])
  
  project    = "pcb-${each.key}-processing"
  dataset_id = "regression_results"
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:regression-tester-${each.key}@pcb-${each.key}-processing.iam.gserviceaccount.com"
}

# GCS access for regression data bucket
resource "google_storage_bucket_iam_member" "regression_data" {
  for_each = toset(["dev", "uat", "prod"])
  
  bucket = "pcb-${each.key}-regression-data"
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:regression-tester-${each.key}@pcb-${each.key}-processing.iam.gserviceaccount.com"
}
```

### 9.2 Workload Identity Federation (Existing Pattern)

```hcl
# Reuse existing WIF configuration
locals {
  wif_pool_provider = "//iam.googleapis.com/projects/${var.gcp_wi_federation_project}/locations/global/workloadIdentityPools/gitlab-oidc-platform/providers/gitlab-oidc-platform-terminus"
}

# Cloud Run Job uses WIF, not service account keys
resource "google_cloud_run_v2_job" "regression_tester" {
  name     = "regression-tester-${var.environment}"
  location = var.region

  template {
    template {
      service_account = google_service_account.regression_sa[var.environment].email
      
      containers {
        image = "${var.jfrog_registry}/regression-tester:${var.version}"
        
        env {
          name  = "ENVIRONMENT"
          value = var.environment
        }
        
        env {
          name  = "VAULT_ADDR"
          value = var.vault_addr  # Existing vault address per environment
        }
      }
    }
  }
}
```

### 9.3 Vault Integration (Existing Pattern)

```hcl
# Reuse existing Vault configuration from terragrunt.hcl
provider "vault" {
  address         = var.vault_addr
  skip_tls_verify = true
  
  auth_login_jwt {
    role = "dp-gcp-terminus-terraformer"  # Existing role
  }
}

# Secrets accessed by regression tester
# Path: pcf-engineering/platforms/pcf-gcp-management/pcf-terminus/pcf-gcp-terminus/
data "vault_generic_secret" "slack_webhook" {
  path = "pcf-engineering/platforms/pcf-gcp-management/pcf-terminus/pcf-gcp-terminus/SLACK_WEBHOOK_URL"
}

data "vault_generic_secret" "masking_salt" {
  path = "pcf-engineering/platforms/pcf-gcp-management/pcf-terminus/pcf-gcp-terminus/MASKING_SALT"
}
```

### 9.4 GitLab CI Authentication

```yaml
# Extend existing auth pattern from .gitlab-ci.yml
.regression-auth:
  id_tokens:
    TERRAFORM_VAULT_AUTH_JWT:
      aud: "${VAULT_SERVER_URL}"
  extends:
    - .vault-auth
    - .google-oidc:auth
  variables:
    WI_POOL_PROVIDER: "//iam.googleapis.com/projects/$GCP_WI_FEDERATION_PROJECT/locations/global/workloadIdentityPools/gitlab-oidc-platform/providers/gitlab-oidc-platform-terminus"
    SERVICE_ACCOUNT: "regression-tester-${ENV_NAME}@pcb-${ENV_NAME}-processing.iam.gserviceaccount.com"
```

---

## 10. Data Privacy & PII Handling

### 10.1 PII Classification

| Category | Examples | Handling |
|----------|----------|----------|
| **Highly Sensitive** | SIN, Account Numbers, Full CC | Full mask, never sample |
| **Sensitive** | Name, Address, Phone, Email | Hash or synthetic replacement |
| **Quasi-Identifiers** | DOB, Postal Code | Generalize (year only, first 3 chars) |
| **Non-Sensitive** | Transaction types, Product codes | Sample directly |

### 10.2 Masking Implementation

```python
class PIIMaskingEngine:
    """
    PII masking engine for production data sampling.
    
    Ensures no real PII leaves production when creating test data.
    """
    
    HIGHLY_SENSITIVE_COLUMNS = [
        "sin_number", "ssn", "account_number", "card_number",
        "cvv", "pin", "password", "secret"
    ]
    
    SENSITIVE_COLUMNS = [
        "first_name", "last_name", "email", "phone", "address",
        "customer_name", "beneficiary_name"
    ]
    
    def __init__(self, vault_client):
        self.vault = vault_client
        self.salt = self._get_salt()
    
    def _get_salt(self) -> str:
        """Get masking salt from Vault (rotated monthly)."""
        secret = self.vault.read(
            "pcf-engineering/platforms/pcf-gcp-management/pcf-terminus/pcf-gcp-terminus/MASKING_SALT"
        )
        return secret["data"]["value"]
    
    def mask_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply PII masking to entire dataframe."""
        masked_df = df.copy()
        
        for column in df.columns:
            column_lower = column.lower()
            
            if any(hs in column_lower for hs in self.HIGHLY_SENSITIVE_COLUMNS):
                # Full mask - replace with placeholder
                masked_df[column] = "REDACTED"
            
            elif any(s in column_lower for s in self.SENSITIVE_COLUMNS):
                # Hash with salt for referential integrity
                masked_df[column] = df[column].apply(
                    lambda x: self._hash_value(x) if pd.notna(x) else None
                )
            
            elif "date" in column_lower and "birth" in column_lower:
                # Generalize DOB to year only
                masked_df[column] = pd.to_datetime(df[column]).dt.year
            
            elif "postal" in column_lower or "zip" in column_lower:
                # Keep first 3 characters only
                masked_df[column] = df[column].str[:3] + "XXX"
        
        return masked_df
    
    def _hash_value(self, value: Any) -> str:
        """Hash a value with salt for consistent masking."""
        if value is None:
            return None
        return hashlib.sha256(f"{self.salt}{value}".encode()).hexdigest()[:16]
```

### 10.3 Data Sampling Query (BigQuery)

```sql
-- Production sampling query with built-in masking
-- Used by InputDataGenerator for "production_sample" strategy

SELECT
  -- Highly sensitive: full redaction
  'REDACTED' AS account_number,
  'REDACTED' AS sin_number,
  
  -- Sensitive: hash with salt
  TO_HEX(SHA256(CONCAT(@masking_salt, CAST(customer_id AS STRING)))) AS customer_id_masked,
  TO_HEX(SHA256(CONCAT(@masking_salt, email))) AS email_masked,
  CONCAT('XXX-XXX-', RIGHT(phone_number, 4)) AS phone_masked,
  
  -- Quasi-identifiers: generalize
  EXTRACT(YEAR FROM date_of_birth) AS birth_year,
  LEFT(postal_code, 3) AS postal_prefix,
  
  -- Non-sensitive: keep as-is
  transaction_type,
  transaction_amount,
  transaction_date,
  product_code,
  status
  
FROM `pcb-{env}-{zone}.{dataset}.{table}`
WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
ORDER BY FARM_FINGERPRINT(CAST(customer_id AS STRING))  -- Deterministic sampling
LIMIT 1000
```

---

## 11. Implementation Plan

### Phase 1: Core Framework (Week 1–2)

| Task | Owner | Deliverable | Priority |
|------|-------|-------------|----------|
| Create GCS buckets per environment | Infra | `pcb-{dev,uat,prod}-regression-data` | P0 |
| Create BigQuery results datasets | Infra | `regression_results` in each processing project | P0 |
| Create service accounts with WIF | Infra | `regression-tester-{env}` SAs | P0 |
| Implement PII masking engine | Backend | `PIIMaskingEngine` class | P0 |
| Implement input generator | Backend | `InputDataGenerator` with masking | P0 |
| Implement baseline manager (multi-zone) | Backend | `BaselineManager` class | P0 |
| Set up Cloud Run Jobs per environment | Infra | Jobs deployable via Terraform | P0 |

### Phase 2: Cross-Zone & Priority (Week 3–4)

| Task | Owner | Deliverable | Priority |
|------|-------|-------------|----------|
| Implement cross-zone validator | Backend | `CrossZoneValidator` class | P0 |
| Implement dataset priority manager | Backend | `DatasetPriorityManager` class | P1 |
| Implement module version tracker | Backend | `ModuleVersionTracker` class | P1 |
| Add GitLab CI regression stages | DevOps | Updated `.gitlab-ci.yml` | P0 |
| Configure Eventarc triggers | Infra | State bucket monitoring | P1 |
| Implement environment notifier | Backend | `EnvironmentNotifier` class | P0 |

### Phase 3: Integration & Rollout (Week 5)

| Task | Owner | Deliverable | Priority |
|------|-------|-------------|----------|
| Configure Cloud Scheduler | Infra | Weekly baseline refresh per env | P1 |
| Integrate with existing Prometheus | DevOps | Regression metrics exposed | P2 |
| End-to-end testing (Dev) | QA | Full workflow validated | P0 |
| Documentation | All | Runbook + README | P0 |
| Rollout to UAT | DevOps | UAT pipeline enabled | P0 |
| Rollout to Prod | DevOps | Prod pipeline enabled | P0 |

**Total Duration: 5 weeks**

---

## 12. Cost Estimation

### Monthly Cost Per Environment

| Component | Dev | UAT | Prod | Notes |
|-----------|-----|-----|------|-------|
| Cloud Run Job | $15 | $15 | $20 | More runs in Prod |
| GCS Storage | $3 | $3 | $5 | ~10 GB per env |
| BigQuery Queries | $20 | $20 | $30 | INFORMATION_SCHEMA queries |
| BigQuery Storage | $5 | $5 | $5 | Results tables |
| Cloud Scheduler | $1 | $1 | $1 | Weekly triggers |
| Eventarc | $1 | $1 | $1 | State bucket triggers |
| **Subtotal** | **$45** | **$45** | **$62** | Per environment |

**Total Monthly Cost: ~$152** (all environments)

### Cost Comparison

| Architecture | Monthly Cost | Maintenance |
|--------------|--------------|-------------|
| Original HLD (single env, Cloud Build) | $60 | Medium |
| **Enhanced (3 envs, GitLab CI, full features)** | **$152** | **Low** |
| Alternative (external testing tool) | $500+ | High |

---

## 13. Success Criteria & SLAs

### 13.1 Success Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Schema change detection rate | 100% | Breaking changes detected before apply |
| False positive rate | < 5% | Non-breaking changes flagged as breaking |
| Execution time (critical datasets) | < 5 min | Time to validate critical tier |
| Execution time (full suite) | < 15 min | Time to validate all datasets |
| Alert latency | < 2 min | Time from failure to Slack |
| Coverage | 100% | All Terragrunt-managed tables |
| Environment coverage | 3/3 | Dev, UAT, Prod all monitored |
| Maintenance effort | < 4 hrs/month | Engineering time |

### 13.2 Service Level Objectives (SLOs)

| SLO | Target | Measurement Period |
|-----|--------|-------------------|
| Regression test availability | 99.5% | Monthly |
| Critical dataset validation SLA | < 5 min in 99% of runs | Monthly |
| Alert delivery | < 2 min in 99% of failures | Monthly |
| Baseline freshness | < 7 days old | Continuous |

### 13.3 Alerting Thresholds

| Condition | Dev Alert | UAT Alert | Prod Alert |
|-----------|-----------|-----------|------------|
| Breaking change (critical dataset) | Slack | Slack | Slack + PagerDuty |
| Breaking change (standard dataset) | No | Slack | Slack |
| Additive change | No | No | Slack |
| Test execution failure | Slack | Slack | Slack + PagerDuty |
| Module version drift | Slack (pipeline) | Slack (pipeline) | Slack (pipeline) |
| SLA breach | No | Slack | Slack + PagerDuty |

---

## 14. Risks & Mitigations

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| **Baseline drift** | Medium | High | Weekly auto-refresh via Cloud Scheduler |
| **Large table checksums slow** | Medium | Medium | Row count only for tables > 10M rows; use TABLESAMPLE |
| **Cross-zone dependency miss** | Medium | High | Parse terragrunt.hcl dependencies automatically |
| **PII leak in test data** | Low | Critical | Multiple masking layers; no direct prod access |
| **Module version incompatibility** | Medium | Medium | Track versions; alert on drift |
| **GitLab CI timeout** | Low | Medium | Parallelize zone validation; use Cloud Run for heavy lifting |
| **Input data becomes stale** | Medium | Medium | Regenerate inputs weekly; use date-relative filters |
| **False positives in schema comparison** | Medium | Medium | Whitelist known acceptable changes |
| **Vault unavailability** | Low | High | Cache secrets; fallback to Secret Manager |

---

## 15. Rollback Strategy

### 15.1 Automated Rollback (Future Enhancement)

```
┌─────────────────────────────────────────────────────────────────────────┐
│                       ROLLBACK DECISION FLOW                            │
└─────────────────────────────────────────────────────────────────────────┘

  Regression test completes
          │
          ▼
  ┌───────────────────┐
  │ Evaluate results  │
  └─────────┬─────────┘
            │
      ┌─────┴─────┐
      │           │
      ▼           ▼
  [PASS]      [FAIL]
      │           │
      ▼           ▼
  Continue   ┌───────────────────┐
             │ Check failure     │
             │ severity          │
             └─────────┬─────────┘
                       │
         ┌─────────────┼─────────────┐
         │             │             │
         ▼             ▼             ▼
     [CRITICAL]    [HIGH]       [MEDIUM/LOW]
         │             │             │
         ▼             ▼             ▼
   Auto-rollback   Manual       Log only
   + PagerDuty     approval     + Slack
                   required
```

### 15.2 Manual Rollback Procedure

```bash
# 1. Identify the previous successful state
PREVIOUS_STATE_VERSION=$(gsutil ls -l gs://pcb-${ENV}-001--tfstate/${ZONE}/ | sort -k2 | tail -2 | head -1 | awk '{print $3}')

# 2. Restore previous state (requires approval)
gsutil cp ${PREVIOUS_STATE_VERSION} gs://pcb-${ENV}-001--tfstate/${ZONE}/default.tfstate

# 3. Re-run terraform apply with previous state
cd live/${ENV}/${ZONE}
terragrunt apply -auto-approve

# 4. Re-run regression tests to verify
gcloud run jobs execute regression-tester-${ENV} --args="--zone=${ZONE}"
```

### 15.3 Rollback SLAs

| Environment | Rollback Decision Time | Rollback Execution Time |
|-------------|------------------------|-------------------------|
| Dev | N/A (informational only) | N/A |
| UAT | 30 minutes (manual) | 15 minutes |
| Prod | 5 minutes (auto for critical) | 10 minutes |

---

## 16. Appendix

### 16.1 GitLab CI Configuration (Enhanced)

```yaml
# .gitlab-ci.yml additions for regression testing

stages:
  - init
  - tfplan-processing
  - tfapply-processing
  - tfplan-curated
  - tfapply-curated
  - tfplan-landing
  - tfapply-landing
  - regression-test           # NEW STAGE
  - regression-baseline       # NEW STAGE

# Base regression job template
.regression-base:
  image: ${JFROG_BETA}/docker/build/google/regression-tester:v1.0.0
  extends:
    - .vault-auth
    - .google-oidc:auth
  variables:
    SERVICE_ACCOUNT: "regression-tester-${ENV_NAME}@pcb-${ENV_NAME}-processing.iam.gserviceaccount.com"

# Dev regression test
regression-test-dev:
  extends:
    - .regression-base
  stage: regression-test
  variables:
    ENV_NAME: "dev"
    ENV_DIR: "live/Dev"
    PROJECT_ID: "pcb-dev-processing"
    VAULT_SERVER_URL: $VAULT_AZURE_BETA
  script:
    - |
      set +e
      
      # Authenticate to GCP via WIF
      gcloud auth login --cred-file=$GOOGLE_APPLICATION_CREDENTIALS
      
      # Run regression tester
      python /app/regression_tester.py \
        --environment=${ENV_NAME} \
        --mode=full \
        --zones=processing,curated,landing \
        --priority-tier=all
      
      export REGRESSION_EXIT_CODE=$?
      set -e
      
      if [ "$REGRESSION_EXIT_CODE" -ne 0 ]; then
        echo "Regression tests failed"
        curl -X POST --data-urlencode "payload={
          \"channel\": \"$DEV_SLACK_CHANNEL\",
          \"text\": \"Regression Test Failed\",
          \"attachments\": [
            {
              \"text\": \"Schema regression detected in Dev environment. Job URL: ${CI_JOB_URL}\",
              \"color\": \"danger\"
            }
          ]
        }" "$SLACK_WEBHOOK_URL"
        exit 1
      fi
  dependencies:
    - tf-apply-dev-processing
    - tf-apply-dev-curated
    - tf-apply-dev-landing
  only:
    - main
  allow_failure: false

# UAT regression test
regression-test-uat:
  extends:
    - .regression-base
  stage: regression-test
  variables:
    ENV_NAME: "uat"
    ENV_DIR: "live/Uat"
    PROJECT_ID: "pcb-uat-processing"
    VAULT_SERVER_URL: $VAULT_AZURE_BETA
  script:
    - |
      # Similar script as dev with UAT-specific settings
      python /app/regression_tester.py \
        --environment=${ENV_NAME} \
        --mode=full \
        --zones=processing,curated,landing \
        --priority-tier=all \
        --strict-mode  # More strict for UAT
  dependencies:
    - tf-apply-uat-processing
    - tf-apply-uat-curated
    - tf-apply-uat-landing
  only:
    - main
  when: manual

# Prod regression test (strictest)
regression-test-prod:
  extends:
    - .regression-base
  stage: regression-test
  variables:
    ENV_NAME: "prod"
    ENV_DIR: "live/Prod"
    PROJECT_ID: "pcb-prod-processing"
    VAULT_SERVER_URL: $VAULT_AZURE_PROD
  script:
    - |
      python /app/regression_tester.py \
        --environment=${ENV_NAME} \
        --mode=full \
        --zones=processing,curated,landing \
        --priority-tier=all \
        --strict-mode \
        --fail-on-any-change  # Most strict for Prod
  dependencies:
    - tf-apply-prod-processing
    - tf-apply-prod-curated
    - tf-apply-prod-landing
  only:
    - main
  when: manual

# Weekly baseline refresh job (scheduled)
baseline-refresh:
  extends:
    - .regression-base
  stage: regression-baseline
  variables:
    ENV_NAME: "${CI_ENVIRONMENT_NAME}"
  script:
    - |
      python /app/regression_tester.py \
        --environment=${ENV_NAME} \
        --mode=capture-baseline \
        --zones=processing,curated,landing
  rules:
    - if: $CI_PIPELINE_SOURCE == "schedule"
```

### 16.2 Cloud Run Job Definition (Terraform)

```hcl
# Per-environment Cloud Run Job

variable "environments" {
  default = ["dev", "uat", "prod"]
}

variable "vault_addresses" {
  default = {
    dev  = "https://web-vault.dolphin.azure.nonprod.pcfcloud.io"
    uat  = "https://web-vault.dolphin.azure.nonprod.pcfcloud.io"
    prod = "https://web-vault.octopus.azure.prod.pcfcloud.io"
  }
}

resource "google_cloud_run_v2_job" "regression_tester" {
  for_each = toset(var.environments)
  
  name     = "regression-tester-${each.key}"
  location = var.region
  project  = "pcb-${each.key}-processing"

  template {
    parallelism = 1
    task_count  = 1
    
    template {
      service_account = "regression-tester-${each.key}@pcb-${each.key}-processing.iam.gserviceaccount.com"
      timeout         = "900s"  # 15 minutes max
      max_retries     = 1
      
      containers {
        image = "${var.jfrog_registry}/regression-tester:${var.regression_tester_version}"
        
        resources {
          limits = {
            cpu    = "2"
            memory = "4Gi"
          }
        }
        
        env {
          name  = "ENVIRONMENT"
          value = each.key
        }
        
        env {
          name  = "VAULT_ADDR"
          value = var.vault_addresses[each.key]
        }
        
        env {
          name  = "GCS_BUCKET"
          value = "pcb-${each.key}-regression-data"
        }
        
        env {
          name  = "BQ_RESULTS_DATASET"
          value = "pcb-${each.key}-processing.regression_results"
        }
        
        env {
          name = "SLACK_WEBHOOK_URL"
          value_source {
            secret_key_ref {
              secret  = "slack-webhook"
              version = "latest"
            }
          }
        }
      }
    }
  }
}

# GCS buckets for regression data (per environment)
resource "google_storage_bucket" "regression_data" {
  for_each = toset(var.environments)
  
  name     = "pcb-${each.key}-regression-data"
  location = var.region
  project  = "pcb-${each.key}-processing"

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 90  # Delete outputs older than 90 days
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      num_newer_versions = 10  # Keep last 10 baseline versions
    }
    action {
      type = "Delete"
    }
  }

  versioning {
    enabled = true
  }
}

# BigQuery dataset for results (per environment)
resource "google_bigquery_dataset" "regression_results" {
  for_each = toset(var.environments)
  
  dataset_id = "regression_results"
  project    = "pcb-${each.key}-processing"
  location   = var.gcp_multiregion

  default_table_expiration_ms = 7776000000  # 90 days

  labels = {
    environment = each.key
    purpose     = "regression-testing"
  }
}

# Results table
resource "google_bigquery_table" "results" {
  for_each = toset(var.environments)
  
  dataset_id = google_bigquery_dataset.regression_results[each.key].dataset_id
  table_id   = "results"
  project    = "pcb-${each.key}-processing"

  time_partitioning {
    type  = "DAY"
    field = "run_timestamp"
  }

  schema = jsonencode([
    {
      name = "run_id"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "run_timestamp"
      type = "TIMESTAMP"
      mode = "REQUIRED"
    },
    {
      name = "environment"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "zone"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "dataset_name"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "table_name"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "test_type"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "status"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "change_type"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "details"
      type = "JSON"
      mode = "NULLABLE"
    },
    {
      name = "execution_time_ms"
      type = "INT64"
      mode = "NULLABLE"
    },
    {
      name = "pipeline_url"
      type = "STRING"
      mode = "NULLABLE"
    }
  ])
}

# Cloud Scheduler for weekly baseline refresh
resource "google_cloud_scheduler_job" "baseline_refresh" {
  for_each = toset(var.environments)
  
  name        = "regression-baseline-refresh-${each.key}"
  description = "Weekly baseline capture for ${each.key} environment"
  schedule    = each.key == "prod" ? "0 3 * * 0" : "0 2 * * 0"  # Prod runs later
  time_zone   = "America/Toronto"
  project     = "pcb-${each.key}-processing"
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/pcb-${each.key}-processing/jobs/regression-tester-${each.key}:run"
    
    body = base64encode(jsonencode({
      overrides = {
        containerOverrides = [{
          args = ["--mode=capture-baseline", "--zones=processing,curated,landing"]
        }]
      }
    }))

    oauth_token {
      service_account_email = "regression-tester-${each.key}@pcb-${each.key}-processing.iam.gserviceaccount.com"
    }
  }
}

# Eventarc triggers for state bucket changes
resource "google_eventarc_trigger" "tf_state_change" {
  for_each = {
    dev  = "pcb-dev-002--tfstate"
    prod = "pcb-prod-001--tfstate"
    # Add UAT when bucket name is known
  }
  
  name     = "regression-on-tf-state-${each.key}"
  location = var.region
  project  = "pcb-${each.key}-processing"

  matching_criteria {
    attribute = "type"
    value     = "google.cloud.storage.object.v1.finalized"
  }
  matching_criteria {
    attribute = "bucket"
    value     = each.value
  }

  destination {
    cloud_run_service {
      service = google_cloud_run_v2_job.regression_tester[each.key].name
      region  = var.region
    }
  }

  service_account = "regression-tester-${each.key}@pcb-${each.key}-processing.iam.gserviceaccount.com"
}
```

### 16.3 Priority Dataset Configuration

```json
{
  "version": "1.0",
  "description": "Dataset priority configuration for regression testing",
  "priority_tiers": {
    "critical": {
      "description": "Business-critical datasets requiring full validation",
      "sla_minutes": 5,
      "validation_level": "full",
      "alert_policy": "any_change",
      "datasets": [
        "domain_payments",
        "domain_fraud", 
        "domain_aml",
        "domain_ledger",
        "domain_account_management",
        "domain_customer_management"
      ]
    },
    "high": {
      "description": "Important datasets requiring schema validation",
      "sla_minutes": 10,
      "validation_level": "full",
      "alert_policy": "breaking_only",
      "datasets": [
        "domain_scoring",
        "domain_securitization",
        "domain_treasury",
        "domain_dispute",
        "domain_communication",
        "domain_customer_acquisition",
        "domain_validation_verification"
      ]
    },
    "standard": {
      "description": "Standard datasets with schema-only validation",
      "sla_minutes": 15,
      "validation_level": "schema_only",
      "alert_policy": "breaking_only",
      "datasets": [
        "domain_loyalty",
        "domain_marketing",
        "domain_retail",
        "domain_technical",
        "domain_audit",
        "domain_customer_service",
        "domain_talent_acquisiton",
        "domain_consent",
        "domain_iam",
        "domain_dns",
        "domain_payments_ops",
        "domain_tax_slips",
        "domain_cdic",
        "domain_card_management",
        "domain_ops_adhoc",
        "domain_approval",
        "domain_fraud_risk_intelligence",
        "domain_customer_contact_centre",
        "domain_cri_compliance",
        "domain_data_logs",
        "domain_poa",
        "domain_movemoney_technical"
      ]
    },
    "low": {
      "description": "COTS datasets with existence-only validation",
      "sla_minutes": 30,
      "validation_level": "existence_only",
      "alert_policy": "deletion_only",
      "datasets": [
        "cots_fraud_ffm",
        "cots_hr",
        "cots_customer_service_blockworx",
        "cots_customer_service_ctt",
        "cots_security_ciam",
        "cots_mft_sterling",
        "cots_aml_sas",
        "cots_alm_tbsm",
        "cots_aml_verafin",
        "cots_cdic",
        "cots_scms_tkm",
        "cots_scms_andis_se",
        "cots_digital_marketing",
        "cots_loyalty_teradata"
      ]
    }
  }
}
```

### 16.4 Glossary

| Term | Definition |
|------|------------|
| **Zone** | Data lake tier: Landing, Processing, or Curated |
| **Environment** | Deployment stage: Dev, UAT, or Prod |
| **Baseline** | Versioned snapshot of schemas and metadata |
| **Breaking Change** | Schema change that may break existing consumers |
| **Cross-Zone Dependency** | When one zone uses outputs from another |
| **Module Version Drift** | Different terraform-gcp-terminus versions across environments |
| **PII Masking** | Replacing sensitive data with safe alternatives |
| **WIF** | Workload Identity Federation - keyless authentication |
| **Terragrunt** | Terraform wrapper for DRY configurations |

### 16.5 References

- [BigQuery INFORMATION_SCHEMA](https://cloud.google.com/bigquery/docs/information-schema-intro)
- [Terragrunt Documentation](https://terragrunt.gruntwork.io/docs/)
- [Cloud Run Jobs](https://cloud.google.com/run/docs/create-jobs)
- [GitLab CI/CD](https://docs.gitlab.com/ee/ci/)
- [Workload Identity Federation](https://cloud.google.com/iam/docs/workload-identity-federation)
- [HashiCorp Vault GCP Auth](https://developer.hashicorp.com/vault/docs/auth/gcp)
- [Eventarc GCS Triggers](https://cloud.google.com/eventarc/docs/creating-triggers)
