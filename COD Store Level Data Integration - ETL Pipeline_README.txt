# COD Store Level Data Integration - ETL Pipeline

## Overview

This Python mini-project simulates the **COD (Consumer Offtake Data) Store Level Data Integration** ETL pipeline as described in the Business Scope Statement (BSS). It demonstrates the complete data flow from raw retailer files through validation, harmonization, quality checks, and loading into the Corporate Data Model (CDM).

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         ETL PIPELINE FLOW                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  Raw CSV File (Retailer Data)                                           │
│         │                                                                │
│         ▼                                                                │
│  ┌──────────────────────────────────────────┐                          │
│  │  1. File Naming Validation                │                          │
│  │     AABBB_CCDDDDEEFFFF_GGGGG.csv         │                          │
│  └──────────────────────────────────────────┘                          │
│         │                                                                │
│         ▼                                                                │
│  ┌──────────────────────────────────────────┐                          │
│  │  2. Top Line Data Validation              │                          │
│  │     • Store ID completeness               │                          │
│  │     • New stores detection                │                          │
│  │     • Store attribute changes             │                          │
│  │     • Expected measures                   │                          │
│  │     • Volume consistency                  │                          │
│  └──────────────────────────────────────────┘                          │
│         │                                                                │
│         ▼                                                                │
│  ┌──────────────────────────────────────────┐                          │
│  │  3. Data Harmonization                    │                          │
│  │     • Time (weeks, dates)                 │                          │
│  │     • Location (unique store IDs)         │                          │
│  │     • Product (SKU mapping)               │                          │
│  │     • Measures (promo split, formats)     │                          │
│  └──────────────────────────────────────────┘                          │
│         │                                                                │
│         ▼                                                                │
│  ┌──────────────────────────────────────────┐                          │
│  │  4. Data Quality Validation               │                          │
│  │     • CRM minimum requirements            │                          │
│  │     • Volume by banner                    │                          │
│  │     • Market attributes rollup            │                          │
│  └──────────────────────────────────────────┘                          │
│         │                                                                │
│         ▼                                                                │
│  ┌──────────────────────────────────────────┐                          │
│  │  5. CDM Load (Dual-Dimension Strategy)    │                          │
│  │     • COD-specific dimensions (all)       │                          │
│  │     • Cross-functional (CRM qualified)    │                          │
│  │     • Fact tables                         │                          │
│  │     • Audit logging                       │                          │
│  └──────────────────────────────────────────┘                          │
│         │                                                                │
│         ▼                                                                │
│  SQLite Database (cod_cdm.db)                                           │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

## Key Features Implemented from BSS

### 1. **File Naming Convention Validation** (BSS 3.2.2.2)
- Validates format: `AABBB_CCDDDDEEFFFF_GGGGG[...].csv`
- Extracts metadata: country, file type, date ranges, customer ID

### 2. **Top Line Data Validation** (BSS 3.2.3)
- ✅ Store ID completeness check
- ✅ New stores identification
- ✅ Store attribute change detection
- ✅ Expected measures validation
- ✅ Volume consistency checks
- ✅ Delta load logic (file hashing)

### 3. **Data Harmonization** (BSS 3.2.4)
- **Time**: Week 53/0 mapping, date standardization
- **Location**: Unique store ID creation, COD attributes
- **Product**: Retailer SKU → RB Reference SKU mapping
- **Measures**: Thousand separator handling, promo/non-promo split

### 4. **Data Quality Validation** (BSS 3.2.5)
- CRM minimum requirements check
- Volume consistency by banner
- Market attributes rollup
- Product attributes validation

### 5. **CDM Data Model** (BSS Section on Data Model)

#### Dimensions:
- `dim_store` - COD-specific store attributes
- `dim_product` - Product master data
- `dim_time` - Time dimension
- `dim_data_provider` - External data providers

#### Facts:
- `fact_offtake` - Store-level sales transactions

#### Audit:
- `audit_file_log` - File processing history
- `audit_validation_history` - Validation results

### 6. **Dual-Dimension Strategy** (BSS Section on Dimensions)
- All stores loaded to COD-specific dimensions
- Only CRM-qualified stores available cross-functionally
- Enables flexible reporting

### 7. **KPI Implementation** (BSS Section 4)
Reports simulate KPIs SASR001050-SASR001068:
- Offtake Volume/Value (Store, Syndicated, Combined)
- Promo vs Non-Promo metrics
- Numeric Distribution
- Selling Stores (3 months)

## Installation & Setup

### Prerequisites
```bash
pip install pandas sqlite3 hashlib
```

### Quick Start
```bash
python cod_etl_pipeline.py
```

## Usage Examples

### 1. Process a Single File
```python
from cod_etl_pipeline import CODETLPipeline

# Initialize pipeline
pipeline = CODETLPipeline()

# Process file
result = pipeline.process_file('ATSOF_012025022025_REWE.csv')

# Check results
print(f"Status: {result['status']}")
print(f"Records: {result['records_processed']}")
```

### 2. Generate Sample Data
```python
pipeline = CODETLPipeline()
sample_file = pipeline.generate_sample_data('test_data.csv')
```

### 3. Query KPI Reports
```python
# Get KPI summary
kpi_report = pipeline.get_kpi_report()
print(kpi_report)

# Get store performance
store_report = pipeline.get_store_performance_report()
print(store_report)

# Get audit log
audit = pipeline.get_audit_log()
print(audit)
```

### 4. Direct Database Queries
```python
import sqlite3
import pandas as pd

conn = sqlite3.connect('cod_cdm.db')

# Query all stores
stores = pd.read_sql_query("SELECT * FROM dim_store", conn)

# Query offtake facts
offtake = pd.read_sql_query("""
    SELECT 
        s.store_name,
        s.city,
        SUM(f.volume) as total_volume
    FROM fact_offtake f
    JOIN dim_store s ON f.store_id = s.store_id
    GROUP BY s.store_name, s.city
    ORDER BY total_volume DESC
""", conn)

conn.close()
```

## File Format Requirements

### Input CSV Structure
```csv
store_id,store_name,street,house_number,zip_code,city,country,banner_name,store_code,key_account,retailer_sku,transaction_date,volume,value,promo_flag,currency
ST001,REWE Center Vienna,Hauptstrasse,12,1010,Vienna,AT,REWE,001,REWE_AT,REDBULL_250ML_RETAIL,2025-01-15,1.250,"3.125,50",1,EUR
```

### Required Fields (CRM Minimum Requirements)
- ✅ `store_id` - Unique store identifier
- ✅ `store_name` - Store name
- ✅ `street` - Street address
- ✅ `house_number` - House number
- ✅ `zip_code` - Postal code
- ✅ `city` - City
- ✅ `country` - Country code

### Optional Fields
- `banner_name` - Banner/chain name
- `region_state` - State/region
- `sales_organization` - Sales org
- `segmentation` - Local/global segments

## Data Model Schema

### dim_store
```sql
store_id TEXT PRIMARY KEY
unique_store_id TEXT
external_data_provider TEXT
store_name TEXT
street TEXT
house_number TEXT
zip_code TEXT
city TEXT
country TEXT
region_state TEXT
banner_name TEXT
sales_organization TEXT
crm_qualified INTEGER (0/1)
created_date TEXT
updated_date TEXT
```

### fact_offtake
```sql
fact_id TEXT PRIMARY KEY
store_id TEXT (FK)
product_id TEXT (FK)
date_key TEXT (FK)
data_provider_id TEXT
data_source TEXT ('STORE_LEVEL' or 'SYNDICATED')
volume REAL
value REAL
volume_promo REAL
volume_non_promo REAL
value_promo REAL
value_non_promo REAL
promo_flag INTEGER
currency TEXT
load_date TEXT
file_name TEXT
```

## Validation Rules

### Top Line Validation Checks
1. **Store ID Completeness**: All records must have store_id
2. **New Stores**: Detects and reports new store IDs
3. **Attribute Changes**: Flags when existing store attributes change
4. **Expected Measures**: Validates presence of volume, value
5. **Volume Consistency**: Compares total volume vs. previous loads (±20% threshold)

### Data Quality Checks
1. **CRM Qualification**: Validates minimum required fields
2. **Volume by Banner**: Aggregates and validates by retailer banner
3. **Market Attributes**: Ensures store dimension completeness

## Reporting Capabilities

### KPI Report
Based on BSS definitions (SASR001050-SASR001068):
- Off Take Volume/Value (Store level)
- Promo Volume/Value
- Non-Promo Volume/Value
- Numeric Distribution
- Selling Stores count

### Store Performance Report
- Store-level metrics
- SKU count per store
- Total volume/value
- Promo vs non-promo breakdown
- Promo percentage

### Audit Log
- File processing history
- Validation results
- Quality check outcomes
- Processing timestamps

## Error Handling

### File Validation Errors
```python
# Invalid file name
result = pipeline.process_file('invalid_name.csv')
# Returns: {'status': 'FAILED', 'error': 'Invalid file naming convention'}
```

### Data Quality Issues
- Non-qualified stores loaded to COD dimensions only
- CRM-qualified stores loaded to both COD and cross-functional
- All issues logged in audit tables

## Extension Points

### Add New Validation Rules
```python
class TopLineValidator:
    def check_custom_rule(self, df: pd.DataFrame) -> Dict:
        # Add your validation logic
        result = {
            'check': 'Custom Rule',
            'passed': True,
            'details': {}
        }
        self.validation_results.append(result)
        return result
```

### Add Harmonization Rules
```python
class DataHarmonizer:
    def _load_rules(self):
        rules = super()._load_rules()
        rules['custom_mappings'] = {
            # Add your mappings
        }
        return rules
```

### Add Custom KPIs
```python
def get_custom_kpi_report(self) -> pd.DataFrame:
    query = """
    SELECT 
        -- Your custom KPI logic
    FROM fact_offtake
    """
    return pd.read_sql_query(query, self.cdm_loader.conn)
```

## Testing

### Unit Test Example
```python
import unittest

class TestCODETL(unittest.TestCase):
    def setUp(self):
        self.pipeline = CODETLPipeline('test_cod.db')
    
    def test_file_naming_validation(self):
        validator = FileNamingValidator()
        valid, metadata = validator.validate_filename('ATSOF_012025022025_REWE.csv')
        self.assertTrue(valid)
        self.assertEqual(metadata['country'], 'AT')
    
    def test_harmonization(self):
        harmonizer = DataHarmonizer()
        df = pd.DataFrame({
            'volume': ['1.250', '850'],
            'week': ['WK53', 'WK01']
        })
        result = harmonizer.harmonize(df)
        self.assertEqual(result['week'].iloc[0], '52')
```

## Performance Considerations

- **Batch Processing**: Process multiple files in sequence
- **Indexing**: Database has primary keys on all dimensions
- **Delta Load**: File hashing prevents duplicate processing
- **Incremental Load**: Only new/changed data is processed

## Troubleshooting

### Issue: "Invalid file naming convention"
**Solution**: Ensure file follows pattern `AABBB_CCDDDDEEFFFF_GGGGG.csv`

### Issue: "CRM qualification rate low"
**Solution**: Check that input data contains all minimum required fields

### Issue: "Volume consistency check failed"
**Solution**: Review data for significant volume drops (>20% threshold)

## Alignment with BSS Document

| BSS Section | Implementation | Status |
|-------------|----------------|--------|
| 3.2.1 Data Collection | Sample data generation | ✅ Complete |
| 3.2.2 Data Provisioning | File naming validation | ✅ Complete |
| 3.2.3 Top Line Validation | 5 validation checks | ✅ Complete |
| 3.2.4 Data Harmonization | Time/Location/Product/Measures | ✅ Complete |
| 3.2.5 Data Quality | 3 quality checks | ✅ Complete |
| 3.2.6 CDM Load | Dual-dimension strategy | ✅ Complete |
| Section 4 KPIs | SASR001050-068 simulated | ✅ Complete |
| Audit Trail | File and validation logging | ✅ Complete |

## Future Enhancements

- [ ] CRM parallel load simulation
- [ ] MDM deduplication process
- [ ] Advanced product mapping (local SKU)
- [ ] BI Dashboard integration
- [ ] Real-time monitoring dashboard
- [ ] Automated file archiving
- [ ] Advanced error recovery
- [ ] Multi-database pilot support

## License

Educational/Demo project for BSS implementation showcase.

## Contact

For questions about the BSS document or this implementation, refer to the project documentation.