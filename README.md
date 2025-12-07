# SAP Data Vault 2.0 with dbt

A comprehensive implementation of Data Vault 2.0 methodology using dbt (data build tool) for loading SAP data into Snowflake. This project demonstrates how to build a scalable, auditable, and historized data warehouse from SAP source systems.

## 📋 Project Overview

This project implements a complete Data Vault 2.0 architecture using dbt to transform SAP data through three distinct layers:

### 1. **Staging Layer** (`models/staging/`)
- **Purpose**: Clean and standardize raw SAP data
- **Process**: 
  - Extracts data from SAP tables (KNA1, MARA, VBAK, VBAP, VBPA)
  - Applies data quality rules and standardization
  - Generates hash keys for business keys
  - Prepares data for Data Vault ingestion
- **Tables**: `stg_sap__kna1` (customers), `stg_sap__mara` (materials), `stg_sap__vbak` (order headers), `stg_sap__vbap` (order items), `stg_sap__vbpa` (partner functions)

### 2. **Raw Vault Layer** (`models/raw_vault/`)
The core Data Vault 2.0 implementation following strict architectural patterns:

#### **Hubs** - Unique Business Keys
- `hub_customer` - Customer business keys
- `hub_material` - Material business keys  
- `hub_order` - Sales order business keys
- `hub_order_item` - Order line item business keys

#### **Links** - Relationships Between Hubs
- `link_order_customer` - Order to customer relationships
- `link_order_item` - Order to order item relationships
- `link_order_material` - Order item to material relationships

#### **Satellites** - Historical Descriptive Data
- `sat_sap_customer_kna1` - Customer attributes with full history
- `sat_sap_material_mara` - Material attributes with full history
- `sat_sap_order_header_vbak` - Order header details with full history
- `sat_sap_order_item_vbap` - Order item details with full history

**Key Features**:
- Insert-only pattern (true Data Vault immutability)
- Hash keys (MD5/SHA256) for uniqueness
- Hash diffs for change detection
- Load date tracking for temporal queries
- Record source tracking for data lineage

### 3. **Business Vault Layer** (`models/business_vault/`)
Business-friendly views and aggregations:

- **Point-in-Time (PIT) Tables**: `pit_customer_daily` - Daily snapshots of customer data for time-travel queries
- **Bridge Tables**: `bridge_sales` - Denormalized sales data for analytics

## 🏗️ Data Vault 2.0 Architecture

```
SAP Source Systems
        ↓
   Staging Layer (Cleaned & Hashed)
        ↓
   Raw Vault (Hubs → Links → Satellites)
        ↓
   Business Vault (PITs & Bridges)
        ↓
   Analytics & Reporting
```

## 🎯 Key Benefits

- **Auditability**: Complete history of all changes
- **Flexibility**: Easy to add new sources and attributes
- **Scalability**: Parallel loading patterns
- **Data Quality**: Built-in validation and standardization
- **Time Travel**: Query data as it appeared at any point in time

## 🚀 Getting Started

### Prerequisites

- Python 3.12 or higher
- Snowflake account with appropriate credentials
- [uv](https://docs.astral.sh/uv/) package manager
- Git

### Installation

1. **Clone the repository**:
```powershell
git clone https://github.com/subrahmaniank/sap_datavault_dbt.git
cd sap_datavault_dbt
```

2. **Create and activate virtual environment with uv**:
```powershell
uv venv
.venv\Scripts\Activate.ps1
```

3. **Install dependencies**:
```powershell
uv pip install -e .
```

This installs:
- `dbt-core` (>=1.10.15)
- `dbt-snowflake` (>=1.8.0)
- `dbt-coverage` (>=0.4.1)
- Development tools: `pandas`, `faker`

### Configuration

1. **Set up dbt profile**: Create or update `~\.dbt\profiles.yml`:
```yaml
sap_datavault_example:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: YOUR_ACCOUNT
      user: YOUR_USERNAME
      password: YOUR_PASSWORD
      role: YOUR_ROLE
      database: SAP_DATAVAULT
      warehouse: YOUR_WAREHOUSE
      schema: DEV
      threads: 4
```

2. **Configure Snowflake schemas**: The project uses these schemas:
   - `staging_sap` - Staging layer
   - `raw_vault` - Hubs, Links, Satellites
   - `business_vault` - PIT and Bridge tables
   - `seeds` - Sample SAP data

## 📦 dbt Commands

Navigate to the sales_example directory for all dbt commands:
```powershell
cd sales_example
```

### Initial Setup

1. **Install dbt packages**:
```powershell
dbt deps
```

2. **Load sample SAP data** (CSV seeds):
```powershell
dbt seed
```
This loads sample data from:
- `seed_sap_kna1.csv` (Customers)
- `seed_sap_mara.csv` (Materials)
- `seed_sap_vbak.csv` (Order Headers)
- `seed_sap_vbap.csv` (Order Items)
- `seed_sap_vbpa.csv` (Partner Functions)

### Build the Data Vault

3. **Run all models** (staging → raw vault → business vault):
```powershell
dbt run
```

4. **Run specific layers**:
```powershell
# Staging only
dbt run --select staging

# Raw vault only
dbt run --select raw_vault

# Business vault only
dbt run --select business_vault

# Specific model
dbt run --select hub_customer
```

### Testing & Validation

5. **Run data quality tests**:
```powershell
dbt test
```

6. **Generate documentation**:
```powershell
dbt docs generate
dbt docs serve
```

### Incremental Loads

7. **Simulate incremental load** (after initial run):
```powershell
# Add new records to seeds or update them
dbt seed

# Run incremental refresh
dbt run
```

The Data Vault architecture ensures:
- Hubs: Only new business keys are inserted
- Satellites: Only changed records create new versions
- Links: Only new relationships are added

## 🔧 Custom Macros

The project includes reusable Data Vault macros in `macros/`:

- `generate_hash_key()` - Creates hash keys for business keys
- `generate_hash_diff()` - Creates hash diffs for change detection
- `hub()`, `link()`, `satellite()` - Template macros for Data Vault objects
- `business_key()` - Business key utilities
- `load_date()` - Load date handling

## 📊 Project Structure

```
sales_example/
├── dbt_project.yml          # Project configuration
├── packages.yml             # dbt package dependencies
├── seeds/                   # Sample SAP data (CSV)
├── macros/                  # Reusable Data Vault macros
├── models/
│   ├── staging/            # Staging layer (cleaned data)
│   ├── raw_vault/          # Data Vault 2.0 core
│   │   ├── hub/           # Business key tables
│   │   ├── link/          # Relationship tables
│   │   └── satellite/     # Historical attribute tables
│   └── business_vault/     # Analytics-ready views
├── tests/                   # Data quality tests
└── target/                 # Compiled SQL & artifacts
```

## 🔍 Advanced Usage

### Full Refresh

Force complete rebuild of incremental models:
```powershell
dbt run --full-refresh
```

### Run with Coverage Report

```powershell
dbt run
dbt-coverage compute doc --cov-report coverage-doc.json
dbt-coverage compute test --cov-report coverage-test.json
```

### Generate Sample Orders

Use the included Python script to generate additional sample data:
```powershell
cd sales_example
python generate_orders.py
```

## 📚 Resources

- [dbt Documentation](https://docs.getdbt.com/)
- [Data Vault 2.0 Standards](https://datavaultalliance.com/)
- [Snowflake Documentation](https://docs.snowflake.com/)
- [dbt Utils Package](https://hub.getdbt.com/dbt-labs/dbt_utils/latest/)

## 🤝 Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

## 📄 License

This project is for educational and demonstration purposes.

## 👤 Author

Subrahmanian K

## 🙏 Acknowledgments

Built using:
- dbt Core & dbt-snowflake
- Data Vault 2.0 methodology
- SAP data structures (KNA1, MARA, VBAK, VBAP)
