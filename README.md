# SAP Goods Movement Logic – DBT & Snowflake

This repository contains my implementation of the **SAP Goods Movement logic**, specifically for the extractors **`2LIS_03_BF`** (Goods Movements) and **`2LIS_03_BX`** (Inventory Initialization).  
It includes the **raw extractor code**, **profiling files**, **dbt models for staging, transformed, and conformed layers**, **macros**, and the **data mart** that consolidates all logic for reporting.

---

## 📌 **Project Overview**
The goal of this project is to replicate **SAP Inventory Management logic** into a **modern data stack** using **Snowflake + dbt**.  
The implementation covers:
- **Initialization of stock levels** (`2LIS_03_BX`) and **Goods movements** (`2LIS_03_BF`)
- Consolidation into a **data mart for analytics**

## 📂 **Repository Structure**
```bash
.
├── datamart/
│   ├── dm_goods_movement.sql        # Final data mart for goods movement
│   └── dm_goods_movement.yml        # Tests & documentation for the data mart
│
├── documentation/                   # SAP-related docs for business logic
│   ├── ANREICHERN_MSEG.docx
│   ├── BSTAUS_BSTTYP.docx
│   ├── BWVORG(transaction_keys).pdf
│   └── BWVORG_BWBREL_BWMNG_BWGEO_BWGVO_BWGVP.docx
│
├── macros/                          # Custom dbt macros used in transformations
│   ├── add_missing_master_data.sql
│   ├── convert_to_date.sql
│   └── handle_cdc_load.sql
│
├── models/
│   ├── conformed/
│   │   ├── fact_goods_movement.sql  # Fact table for goods movements
│   │   └── fact_goods_movement.yml  # Tests & documentation for the fact table
│   │
│   ├── staging/
│   │   └── stg_goods_movement.sql   # Staging layer for raw goods movement data
│   │
│   ├── transformed/
│   │   ├── trn_tbl_goods_movement.sql  # Transformation logic
│   │   └── trn_tbl_goods_movement.yml  # Tests & documentation for transformations
│
├── profiling/
│   └── fact_goods_movements.xlsx    # Aliases/mapping for MSEG & MKPF columns
│
├── Raw_Code_Extractor_2LIS_03_BX_&_2LIS_03_BF/  # Raw extractor logic
│
└── README.md
```

## ⚙️ **Key Components**
- **📜 Raw Extractor Code:**  
  Located in `Raw_Code_Extractor_2LIS_03_BX_&_2LIS_03_BF/`, these SQL scripts were the base that made possible to replicate SAP logic for:
  - `2LIS_03_BF` → Goods Movements
  - `2LIS_03_BX` → Stock Initialization

- **🧩 Staging Layer (`models/staging`):**  
  Cleans and standardizes raw data.

- **🔄 Transformation Layer (`models/transformed`):**  
  Applies SAP movement logic, calculates quantities, and handles special cases.

- **📦 Conformed Layer (`models/conformed`):**  
  Fact table: `fact_goods_movement.sql`  
  Includes YAML tests for:
  - **Primary key uniqueness**
  - **Foreign Key relationship**
  - **Not null constraints**

- **📊 Data Mart (`datamart/`):**  
  `dm_goods_movement.sql` aggregates goods movement data for analytics and reporting.

- **⚡ Macros (`macros/`):**  
  - `add_missing_master_data.sql` → Adds missing master data entries  
  - `convert_to_date.sql` → Standardizes date conversion  
  - `handle_cdc_load.sql` → Handles Change Data Capture (CDC) logic  

- **📑 Documentation (`documentation/`):**  
  Contains original SAP documentation for transaction keys, material movements, and enrichment logic.

- **📈 Profiling (`profiling/fact_goods_movements.xlsx`):**  
  Shows **column aliases and mappings** for **MSEG** and **MKPF** tables (main SAP tables behind goods movements).

---

## 🛠 **Technologies Used**
- **SAP ECC** (Source system)
- **Snowflake** (Cloud Data Warehouse)
- **dbt** (Data Transformation & Testing)
- **SQL** (Business logic implementation)

---

## 📖 **Use Case**

This solution is designed for:

- **SAP BW migration** projects moving extractors to Snowflake
- **Inventory analytics** without relying on SAP BW
- **Ensuring data transparency** and quality in modern data stacks
