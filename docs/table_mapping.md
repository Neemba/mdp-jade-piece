# Informix to Snowflake Table Mapping - Company Module

## Overview
This document describes the mapping between Informix source tables and Snowflake destination tables for the company reference data pipeline.

## Table Mappings

### 1. Company Master Data
- **Source Table**: `ie_delmas:informix.agr_soc`
- **Target Table**: `company.ie_company_master`
- **Description**: Master company/society information including registration details and configuration
- **Row Count**: ~28 rows
- **Key Fields**:
  - `asoc_num`: Company number (Primary Key)
  - `asoc_lib`: Company name
  - `asoc_siret`: SIRET registration number
  - `asoc_devise`: Default currency
  - `asoc_lang`: Primary language

### 2. Company Branches
- **Source Table**: `ie_delmas:informix.agr_succ`
- **Target Table**: `company.ie_company_branches`
- **Description**: Branch/subsidiary information for each company
- **Row Count**: ~201 rows
- **Key Fields**:
  - `asuc_num`: Branch code (Primary Key component)
  - `asuc_numsoc`: Company number (Primary Key component)
  - `asuc_lib`: Branch name
  - `asuc_numcli`: Customer number
  - `asuc_numfou`: Supplier number
  - `asuc_active`: Active status

### 3. Company Users
- **Source Table**: `ie_delmas:informix.agr_usr`
- **Target Table**: `company.ie_company_users`
- **Description**: User/operator information including assignments to companies, branches, and service departments
- **Row Count**: ~7,686 rows
- **Key Fields**:
  - `ausr_num`: User number (Primary Key component)
  - `ausr_nom`: Username (Primary Key component)
  - `ausr_soc`: Company code
  - `ausr_lang`: User language preference
  - `ausr_succ`: Branch assignment
  - `ausr_serv`: Service department code
  - `ausr_ope`: Operator code
  - `ausr_multi`: Multi-company access flag (O=Yes, N=No)
  - `ausr_prio`: Priority level
  - `ausr_niveau`: User access level

## Additional Fields Added During ETL

Each table includes two additional fields added during the extraction process:

1. **`integration_date`**: Timestamp when the record was extracted from Informix
2. **`source_table_name`**: Original Informix table name for traceability

## Table Relationships

```
company_master (agr_soc)
    |
    |-- 1:N --> company_branches (agr_succ)
    |
    |-- 1:N --> company_users (agr_usr)
```

### Relationship Details:
- One company can have multiple branches/subsidiaries
- Each branch is linked to a parent company via `asuc_numsoc`
- Branches can have their own customer and supplier numbers
- One company can have multiple users/operators
- Each user is linked to a company via `ausr_soc`
- Users can be assigned to specific branches via `ausr_succ`
- Users can have multi-company access (`ausr_multi = 'O'`)

## Load Strategy

All three tables use a **merge strategy** because:
- Small to medium data volume (~7,900 rows total)
- Reference data that changes infrequently
- Critical for maintaining referential integrity
- Merge allows tracking changes over time
- Tables: agr_soc (~28 rows), agr_succ (~201 rows), agr_usr (~7,686 rows)

## Column Metadata

The DLT pipeline includes column descriptions for key fields in each table. These descriptions are defined in the `@dlt.resource` decorator and will be available in the Snowflake information schema.

## Usage Notes

1. All tables use `write_disposition="merge"` for incremental updates
2. No date-based filtering (full table scans due to small sizes)
3. Daily refresh ensures data consistency
4. Tables are loaded to the `company` schema in Snowflake (renamed from `company_operations`)
5. Parquet format is used for staging and archival
6. Arrow-based extraction for optimal performance

## Data Quality Considerations

- Monitor for inactive branches (`asuc_active = 0`)
- Validate SIRET numbers format
- Check for orphaned branches without valid company references
- Ensure currency and language codes are valid
- For agr_usr:
  - Check for orphaned users without valid company references (`ausr_soc`)
  - Validate branch assignments (`ausr_succ`) reference existing branches
  - Monitor multi-company access flags (`ausr_multi`)
  - Ensure user access levels (`ausr_niveau`) are within expected ranges
  - Check for duplicate username + user number combinations

## Migration Notes

### Changes from v1.0:
- **Schema renamed**: `company_operations` → `company` (per MDP naming standards)
- **agr_usr table added**: Third table in the domain (7,686 users)
- **Write disposition changed**: `replace` → `merge` for all tables
- **Package renamed**: `mdp_company_operations` → `mdp_company`
- **Project renamed**: `mdp-company-operations` → `mdp-company`
