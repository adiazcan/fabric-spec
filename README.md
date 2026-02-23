# Microsoft 365 Copilot License Usage Analytics

Analytics solution for monitoring Copilot license utilization and identifying optimization opportunities using Microsoft Fabric, Microsoft Graph APIs, and Power BI.

## Project Overview

- Ingests user, license, usage, M365 activity, and audit data into Bronze tables
- Transforms data through Silver and Gold medallion layers with usage scoring and recommendations
- Publishes executive and operational analytics in a multi-page Power BI report
- Sends automated notifications for pipeline status and utilization threshold breaches

## Prerequisites

- Microsoft Fabric workspace with Lakehouse and Data Pipeline capabilities
- Azure Key Vault with Graph API app secrets (`graph-tenant-id`, `graph-client-id`, `graph-client-secret`)
- Microsoft Entra ID app registration with Graph application permissions:
	- `User.Read.All`
	- `Organization.Read.All`
	- `Reports.Read.All`
	- `AuditLogsQuery.Read.All`
- Python 3.10+ for deployment automation tooling

## Setup Steps

1. Configure runtime settings in `config/parameter.yml` and secret mappings in `config/key_vault_config.yml`
2. Validate deployment parameterization in root `parameter.yml`
3. Import or sync Fabric items from this repository
4. Run the daily refresh pipeline definition in `pipelines/daily_refresh_pipeline.json`
5. Connect and refresh semantic/report artifacts under `semantic-model/` and `reports/`

## Architecture Reference

- Bronze → Silver → Gold medallion flow is documented in `specs/001-copilot-license-optimization/data-model.md`
- Pipeline orchestration sequence is defined in `pipelines/daily_refresh_pipeline.json`
- Report/semantic model assets are in `reports/copilot_license_analytics.pbir` and `semantic-model/model.bim`

## Specification Links

- Feature spec: `specs/001-copilot-license-optimization/spec.md`
- Implementation plan: `specs/001-copilot-license-optimization/plan.md`
- Research decisions: `specs/001-copilot-license-optimization/research.md`
- Data model: `specs/001-copilot-license-optimization/data-model.md`
- API contracts: `specs/001-copilot-license-optimization/contracts/api-contracts.md`
- Quickstart and verification: `specs/001-copilot-license-optimization/quickstart.md`
- Task breakdown: `specs/001-copilot-license-optimization/tasks.md`