# MDP Service - Corporate Structure Data Pipeline

[![License](https://img.shields.io/badge/License-Proprietary-red.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.12-blue.svg)](https://python.org)
[![Dagster](https://img.shields.io/badge/Dagster-1.11.2-green.svg)](https://dagster.io)
[![Snowflake](https://img.shields.io/badge/Snowflake-Destination-blue.svg)](https://snowflake.com)

A Dagster data pipeline for extracting and processing service operations and organizational structure data from Informix databases to Snowflake. This pipeline is part of the MDP (Modern Data Platform) suite, specifically handling corporate master data and organizational hierarchy analytics.

## 🎯 Overview

The **MDP Service** pipeline is a critical component of the enterprise organizational analytics infrastructure, responsible for maintaining accurate and up-to-date service structure data across the organization. It extracts service master information, branch relationships, operational details, and organizational hierarchy from legacy Informix systems and transforms them into a modern, analytics-ready format in Snowflake.

### Key Capabilities

- **Comprehensive Corporate Processing**: Handles complete organizational structure from headquarters to subsidiaries
- **Multi-Entity Support**: Tracks service relationships across different business units and branches
- **Enterprise-Grade Reliability**: Built with Dagster for monitoring, lineage, and observability
- **Cloud-Native Architecture**: Containerized deployment with Kubernetes orchestration
- **Automated Operations**: Daily refresh cycles with comprehensive error handling

## 📊 Data Architecture

### Source System

- **Database**: Informix (ie_delmas database)
- **Primary Tables**: service organizational tables (3 tables)
  - `agr_soc` - service master information (~28 rows)
  - `agr_succ` - Branch/subsidiary operations (~201 rows)
  - `agr_usr` - User/operator information (~7,686 rows)
- **Update Frequency**: Daily merge strategy for reference data

### Destination System

- **Platform**: Snowflake Data Cloud
- **Schema**: `service`
- **Target Tables**:
  - `ie_service_master` (~28 companies)
  - `ie_service_branches` (~201 branches)
  - `ie_service_users` (~7,686 users)
- **Format**: Structured, analytics-ready with metadata enrichment

### Data Flow

```
Informix service Tables → Arrow Extraction → DLT Processing → Snowflake service Tables
                                          ↓
                                    Parquet Archive → S3/Snowflake Stage
```

## 🏗️ Architecture & Technology Stack

### Core Technologies

- **Orchestration**: [Dagster](https://dagster.io) 1.11.2 - Data orchestration and observability
- **Data Loading**: [DLT](https://dlthub.com) - Scalable data loading framework
- **Performance**: [Apache Arrow](https://arrow.apache.org) - High-performance data processing
- **Storage**: [Snowflake](https://snowflake.com) - Cloud data platform
- **Language**: Python 3.12 with modern dependency management via `uv`

### Dependencies

Built on the **mdp_common** shared library providing:

- Standardized Informix connectivity and JDBC driver management
- Optimized Arrow-based extraction patterns
- Snowflake integration and staging utilities
- Common configuration and error handling patterns

## 🏢 service Data Categories

### Master Data

- **service Information**: Corporate names, registration numbers, and legal identifiers
- **Geographic Data**: Addresses, postal codes, countries, and regional information
- **Financial Configuration**: Default currencies, payment terms, and accounting settings
- **Regulatory Compliance**: SIRET numbers, APE codes, and legal structures

### Operational Data

- **Branch Operations**: Subsidiary locations, operational status, and service areas
- **Organizational Hierarchy**: Parent-child relationships and reporting structures
- **Business Classifications**: Activity codes, sectors, and operational categories
- **Service Management**: Customer and supplier numbers, operational capabilities

## 🚀 Quick Start

### Prerequisites

- Python 3.12+
- Access to Informix (ie_delmas) database
- Snowflake account with `service_operations` schema permissions
- Java runtime for JDBC connectivity
- Docker (for containerized deployment)

### Local Development

```bash
# Clone the repository
git clone <repository-url>
cd mdp-service

# Install dependencies
uv venv && source .venv/bin/activate
uv pip install -e .

# Configure environment variables
cp .env.example .env
# Edit .env with your database credentials

# Start Dagster development server
dagster dev -f src/mdp_service/definitions.py
```

### Environment Configuration

```bash
# Informix Source Database
INFORMIX_HOST=<host>
INFORMIX_PORT=<port>
INFORMIX_DATABASE=<database>
INFORMIX_SERVER=<server>
INFORMIX_USER=<username>
INFORMIX_PASSWORD=<password>

# Snowflake Destination (configured via Dagster resources)
# See pyproject.toml for Snowflake resource configuration
```

## 📅 Schedule & Operations

### Execution Schedule

- **Daily Full Refresh**: 2:00 AM UTC - Complete organizational data validation
- **Weekly Verification**: 1:00 AM UTC Sundays - Data quality and integrity checks
- **On-Demand**: Manual execution via Dagster UI

### Operational Features

- **Reference Data Strategy**: Full refresh approach ensuring complete data integrity
- **Progress Tracking**: Real-time progress with processing metrics
- **Error Recovery**: Automatic retry logic with backoff strategies
- **Data Quality**: Built-in validation and organizational structure checks

## 🔧 Configuration

### Pipeline Configuration

```python
# Load modes
- full: Complete table refresh (default for reference data)

# Performance tuning
- Small tables: 4-6 workers, replace strategy
- Optimized for reference data consistency
```

### Resource Configuration

All database connections and cloud resources are managed through Dagster's resource system with environment-based configuration.

## 🏭 Deployment

### Containerized Deployment

```bash
# Build Docker image
docker build -t mdp-service .

# Run container
docker run -e GITHUB_TOKEN=$GITHUB_TOKEN mdp-service
```

### Kubernetes Deployment

```bash
# Apply Kubernetes manifests
kubectl apply -f k8s/

# Monitor deployment
kubectl get pods -l app=mdp-service
```

### CI/CD Pipeline

Automated deployment via GitHub Actions with:

- Secure GitHub token authentication for private dependencies
- Multi-stage Docker builds with optimization
- Kubernetes rollout with health checks

## 📈 Monitoring & Observability

### Dagster Integration

- **Asset Lineage**: Complete data lineage from source to destination
- **Run History**: Detailed execution logs and performance metrics
- **Data Quality**: Automated data validation and profiling
- **Alerting**: Configurable alerts for failures and data quality issues

### Logging

- **Structured Logging**: JSON-formatted logs with correlation IDs
- **Performance Metrics**: Extraction rates, processing times, and throughput
- **Error Tracking**: Detailed error capture with stack traces and context

## 🔒 Security & Compliance

### Data Security

- **Credential Management**: Secure handling of database credentials via environment variables
- **Network Security**: VPC-based deployment with restricted network access
- **Data Encryption**: End-to-end encryption in transit and at rest

### Compliance

- **Audit Trail**: Complete audit logs for SOX compliance
- **Data Lineage**: Full traceability from source to destination
- **Change Management**: Version-controlled deployments with approval workflows

## 🤝 Contributing

### Development Workflow

1. Create feature branch from `main`
2. Implement changes with comprehensive tests
3. Update documentation as needed
4. Submit pull request for review
5. Deploy via automated CI/CD pipeline

### Code Standards

- **Python**: Follow PEP 8 with `black` formatting and `ruff` linting
- **Testing**: Comprehensive unit and integration test coverage
- **Documentation**: Clear docstrings and updated README for significant changes

## 📚 Documentation

- **Table Mapping**: [docs/table_mapping.md](docs/table_mapping.md) - Detailed source-to-target mapping
- **API Reference**: Generated from code docstrings
- **Architecture**: System design and data flow documentation
- **Runbooks**: Operational procedures and troubleshooting guides

## 🆘 Support

### Getting Help

- **Documentation**: Check this README and linked documentation first
- **Issues**: Open GitHub issues for bugs or feature requests
- **Team Contact**: Reach out to the Data Team for urgent support

## 📝 Project Information

**Version**: 0.1.0
**Maintainers**: Data Team
**Author**: Yassir ELSAYED (yassir.elsayed@neemba.com)
**License**: Proprietary

Part of the **MDP (Modern Data Platform)** suite including:

- [mdp-params](../mdp-params) - Reference data and parameters
- [mdp-sales](../mdp-sales) - Sales order analytics
- [mdp-purchases](../mdp-purchases) - Purchase order data
- [mdp-customers](../mdp-customers) - Customer relationship data
