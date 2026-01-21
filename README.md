# FINAL DEMO DOCUMENTATION  
## End-to-End Data Engineering Platform with Master Data Management (MDM)

**Demo Day:** Day 20 (Final Consolidated Demo)  
**Demo Duration:** 20 Minutes  
**Project Type:** End-to-End Data Engineering + MDM + Governance
**Author:** <Rihana Shaik>  
**Cloud Platform:** AWS  

---

## 1. Project Overview

This project implements a **production-grade data engineering pipeline** with an integrated **Master Data Management (MDM)** layer and **data governance controls**.

The system ingests batch data, processes it through a governed data lake architecture, manages historical master data using **Slowly Changing Dimensions (SCD Type 2)**, and serves analytics through **Amazon Redshift** and **Amazon Athena**.

This demo represents a **consolidated implementation** of all tasks performed across Weeks 1–4 and is presented as a **fully working system** on Day 20.

---

## 2. Data Lake Zonal Architecture

### 2.1 Raw Zone
- Stores source data in original format
- Immutable storage
- No transformations

**Governance Purpose:**  
Auditability, replay capability, and compliance.

---

### 2.2 Processed Zone
- Data cleansing and validation
- Type casting and null handling
- Business-rule enforcement

**Governance Purpose:**  
Separates data quality enforcement from analytics logic.

---

### 2.3 Golden Zone (MDM Input Layer)
- Standardized and conformed master datasets
- Prepared for MDM processing

**Governance Purpose:**  
Single source of standardized master data.

---

### 2.4 Master Data Zone (SCD Type 2 – Delta Lake)

Managed dimensions:
- `dim_zones`
- `dim_vendors`

Stored using **Delta Lake** with:
- ACID transactions
- Versioning
- MERGE support
- Time-travel capability

**Why Delta Lake:**  
Required to safely implement SCD Type 2 logic and historical tracking.

---

### 2.5 Curated Zone
- Analytics-ready fact datasets
- Fully conformed with master dimensions
- Stored as Parquet

---

### 2.6 Redshift Export Zone
- Parquet-only format
- No Delta logs
- Used exclusively for Redshift COPY operations

**Justification:**  
Amazon Redshift does not natively support Delta Lake.

---

## 3. Master Data Management (MDM) Documentation

### 3.1 Master Data Entities

| Entity | Business Key |
|---|---|
| Zone | LocationID |
| Vendor | VendorID |

These entities are treated as **master data** because:
- They are shared across multiple facts
- They change slowly over time
- Historical correctness is critical

---

### 3.2 Deduplication & Matching Strategy

- Records are matched using business keys
- Hash-based change detection (`hash_diff`)
- Duplicate records are resolved before versioning

---

### 3.3 SCD Type 2 Implementation

Each dimension maintains:
- Surrogate key
- Business key
- `version_number`
- `start_date`
- `end_date`
- `is_current`

**Change Handling Logic:**
1. Detect attribute change using hash comparison
2. Expire existing record (`is_current = false`)
3. Insert new version with updated attributes

**Why SCD Type 2:**  
Ensures full historical traceability of master data changes.

---

## 4. Fact Table Design & Justification

### Fact Table: `fact_trips`

**Grain:**  
One row per completed taxi trip.

**Measures:**
- Trip distance
- Fare amount
- Total amount
- Trip duration
- Speed

**Foreign Keys:**
- Zone surrogate key
- Vendor surrogate key

---

### Data Quality Flags
- `zone_orphan_flag`
- `vendor_orphan_flag`

**Justification:**  
Flags are preferred over record rejection to:
- Avoid silent data loss
- Preserve analytical completeness
- Enable downstream data quality reporting

---

## 5. Data Governance Framework

### 5.1 Data Quality Checks
- Null checks
- Referential integrity checks
- Completeness thresholds
- Row count validation

Failures are captured and surfaced without corrupting downstream data.

---

### 5.2 Audit Logging

Each Glue job writes audit metadata including:
- Run timestamp
- Input and output paths
- Record counts
- Data quality metrics
- Job status

Stored as **JSON files in Amazon S3**.

**Governance Value:**  
Provides traceability, observability, and compliance readiness.

---

## 6. Orchestration with AWS Step Functions

Workflow stages:
1. Raw → Processed
2. Processed → Golden
3. Golden → SCD2 Dimensions
4. Fact generation
5. Redshift export

**Why Step Functions:**
- Visual orchestration
- Retry and failure handling
- Clear operational visibility

---

## 7. Analytics & Serving Layer

### 7.1 Amazon Athena
- Used for ad-hoc queries
- Validation of curated datasets
- Quick exploration without data movement

---

### 7.2 Amazon Redshift Serverless

Schema: `nyc_analytics`

Tables:
- `zone_dim`
- `vendor_dim`
- `fact_trips`

**Loading Strategy:**
- Parquet COPY from S3
- Controlled staging-to-final loads
- Data quality validation post-load

---

## 8. Monitoring & Observability

- Glue job logs via CloudWatch
- Step Function execution history
- Audit logs stored in S3

**Outcome:**  
Improved operational reliability and faster issue detection.

---

## 9. CI/CD & Version Control

- Source code managed in GitHub
- Infrastructure defined using AWS CDK
- Glue scripts, SQL, and configs versioned
- Repeatable deployments ensured

---

## 10. Key Design Decisions & Justifications

| Decision | Justification |
|---|---|
| Zonal data lake | Governance and separation of concerns |
| Delta Lake for dimensions | Required for SCD Type 2 |
| Parquet export for Redshift | Redshift compatibility |
| Data quality flags | Prevent silent data loss |
| Audit logging | Governance and traceability |

---

## 11. Final Summary

This project demonstrates:
- End-to-end data engineering
- Master Data Management
- Strong data governance practices
- Analytics-ready warehouse design
- Production-style orchestration and monitoring

---
