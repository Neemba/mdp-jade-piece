# Changelog

All notable changes to the MDP Company Operations project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Company users table (`agr_usr`) with 7,686 user records
- User/operator information including company assignments, branches, service departments
- Complete user access level and multi-company access tracking
- Enhanced organizational analytics with user-company relationships

### Changed

- **BREAKING**: Renamed project from `mdp-company-operations` to `mdp-company` (per MDP naming standards)
- **BREAKING**: Schema renamed from `company_operations` to `company` in Snowflake
- **BREAKING**: Package renamed from `mdp_company_operations` to `mdp_company`
- Updated asset group name from `informix_company_operations_ingestion` to `informix_company_ingestion`
- Changed DATASET_NAME from `companies` to `company`
- Enhanced README with 3-table architecture (master, branches, users)
- Updated all documentation to reflect new naming conventions
- Improved table_mapping.md with complete 3-table relationships

### Fixed

- Missing agr_usr table implementation in configs.py, dlt_source.py, and assets.py
- Dockerfile CMD path updated to use `mdp_company` instead of `mdp_company_operations`
- Test imports corrected to use new package name

## [0.1.0] - 2024-09-29

### Added

- Initial project setup with Dagster orchestration for company operations data
- DLT-based data loading pipeline for Informix to Snowflake company data
- Apache Arrow integration for high-performance company data extraction
- Company operations data pipeline for comprehensive organizational management processing
- Complete company structure tracking (master data, branch relationships)
- Docker containerization with optimized multi-stage builds
- Kubernetes deployment manifests for company operations pipeline
- GitHub Actions CI/CD pipeline with automated deployment
- Comprehensive test suite with unit and integration tests
- Environment-based configuration management for company systems

### Changed

- Simplified codebase by removing 200+ lines of duplicated code through mdp_common integration
- Enhanced GitHub Actions workflow with secure token-based authentication
- Optimized Dockerfile by removing redundant JAR file operations (1.7MB+ savings)
- Consolidated company data extraction logic using mdp_common library patterns

### Fixed

- Docker build failures due to missing JAR files
- GitHub Actions authentication issues with private mdp_common repository
- Inconsistent JDBC driver versions across company pipeline components
- Function signature compatibility issues with get_incremental_date_filter
- SnowflakeStageManager initialization with required dataset_name parameter

### Security

- Implemented secure GitHub App token authentication for private repository access
- Added conditional Git credential configuration in Docker builds
- Enhanced secret management in CI/CD pipeline for company data security
- Secure handling of Informix and Snowflake credentials

### Performance

- Reduced Docker image size by eliminating duplicate JAR files (1.7MB+ reduction)
- Improved build times through mdp_common dependency consolidation
- Optimized company data extraction using Apache Arrow columnar processing
- Achieved high-performance processing for company transactions
- Efficient handling of company records with batch optimization

---

## Release Notes

### Version 0.1.0 - Company Operations Pipeline Foundation

This is the initial release of the MDP Company Operations pipeline, establishing the foundation for enterprise organizational analytics. Key achievements:

**Company Data Processing:**

- Complete company organizational structure tracking from master data to branch operations
- Processing of company master information, branch/subsidiary relationships
- Multi-entity company support with geographic and operational data
- Company segmentation and operational status tracking
- Comprehensive organizational identity management

**Code Quality Improvements:**

- Eliminated 200+ lines of duplicated code through mdp_common integration
- Established shared library pattern following mdp_params and mdp_sales best practices
- Implemented comprehensive test coverage for company data processing
- Reduced codebase from custom implementation to clean wrapper using mdp_common

**Infrastructure Modernization:**

- Containerized deployment with Kubernetes orchestration
- Automated CI/CD pipeline with GitHub Actions
- Secure authentication for private repository dependencies
- Daily scheduled execution at 2:00 AM UTC

**Performance Enhancements:**

- Apache Arrow integration for high-performance company data extraction
- Optimized Docker builds with significant size reduction
- Efficient batch processing for varying company data volumes
- High-performance processing for company transactions

**Enterprise Features:**

- Comprehensive monitoring and alerting via Dagster
- Complete data lineage from Informix source to Snowflake analytics
- SOX-compliant change tracking and deployment workflows
- ERP integration with company management, organizational structure, and operational processing

This release follows the technical foundation and best practices established in mdp_params and mdp_sales and utilizes the mdp_common library for consistency across the MDP project family (mdp-sales, mdp-customers, mdp-purchases, etc.).

---

**Maintainers**: Data Team
**Author**: Yassir ELSAYED (yassir.elsayed@neemba.com)