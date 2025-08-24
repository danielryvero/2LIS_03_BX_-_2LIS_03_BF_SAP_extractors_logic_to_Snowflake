{{ config(
    materialized="incremental",
    unique_key=["material_doc_num", "material_doc_year", "doc_item"],
    pre_hook=[
        "{{warehouse_size('MEDIUM')}}",
        "{{add_missing_master_data([{'mseg':{'lgort':'storage_location','werks':'plant'}}], 'dim_storage_location', 'storage_location_key')}}",
        "{{add_missing_master_data([{'mseg':{'matnr':'sap_material_number'}}], 'dim_material', 'material_key')}}",
        "{{add_missing_master_data([{'mseg':{'aufnr':'coorder_number'}}], 'dim_coorder', 'coorder_key')}}",
        "{{add_missing_master_data([{'mseg':{'lgnum':'warehouse_number','lgtyp':'storage_type', 'lgpla':'storage_bin'}}], 'dim_storage_bins', 'storage_bin_key')}}",
        "{{add_missing_master_data([{'mseg':{'bwart':'movement_type'}}], 'dim_movement_type', 'movement_type_key')}}",
        "{{add_missing_master_data([{'mseg':{'kokrs':'co_area', 'prctr':'profit_ctr'}}], 'dim_profit_center', 'profit_center_key')}}",
        "{{add_missing_master_data([{'mseg':{'lgnum':'warehouse_number','lgtyp':'storage_type'}}], 'dim_storage_type', 'storage_type_key')}}",
        "{{add_missing_master_data([{'mseg':{'kokrs':'co_area', 'kostl':'cost_center'}}], 'dim_cost_center', 'cost_center_key')}}",
        "{{add_missing_master_data([{'mseg':{'vgart_mkpf':'transaction_event_type'}}], 'dim_transaction_event_type', 'transaction_event_type_key')}}",
        "{{add_missing_master_data([{'mseg':{'grund':'movement_reason', 'bwart':'movement_type'}}], 'dim_movement_reason', 'movement_reason_key')}}",
        "{{add_missing_master_data([{'mseg':{'lgnum':'warehouse_number', 'bwlvs':'warehouse_movement_type'}}], 'dim_warehouse_movement_type', 'warehouse_movement_type_key')}}",
        "{{add_missing_master_data([{'mseg':{'lifnr':'vendor'}}], 'dim_vendor', 'vendor_key')}}",
        "{{add_missing_master_data([{'mseg':{'kunnr':'customer_number'}}], 'dim_customer', 'customer_key')}}"
        ],
    tags=["analyticsinventory"]) }}
with
    trn_tbl_goods_movement as (
        select
            material_doc_num,
            material_doc_year,
            doc_item,
            material_key,
            plant_key,
            storage_location_key,
            movement_type_key,
            posting_date,
            user_name,
            reference_doc_number,
            reference_doc_number_mkpf,
            debit_credit_flag,
            case 
                when debit_credit_flag = 'H' then 'Credit'
                when debit_credit_flag = 'S' then 'Debit' 
                else '-'
            end as debit_credit_ind,            
            quantity,
            entry_unit_key,
            recv_issue_plant_key,
            amount_lc,
            transaction_event_type_key,
            accounting_doc_type_key,
            revaluation_doc_type_key,
            accounting_doc_entry_date,
            entry_time,
            last_changed_on,
            document_header_text,
            case 
                when gr_gi_slip_version = '1' then 'Individual Slip'
                when gr_gi_slip_version = '2' then 'Individual Slip with Inspection Text'
                when gr_gi_slip_version = '3' then 'Collective Slip'
                else 'No GR/GI Slip'
            end as gr_gi_slip_version,
            gr_gi_slip_number,
            logical_system,
            transaction_code,
            case
                when external_wms_posting = '1' then 'Execute stock change in ERP'
                when external_wms_posting = '2' then 'Create delivery and transmit to WM'
                else 'No control posting for external wms'
            end as external_wms_posting,
            goods_issue_time,
            delivery_number,
            line_id,
            parent_id,
            auto_item,
            batch_number,
            vendor_key,
            customer_key,
            currency_code_name,
            case
                when debit_credit_reval_ind = 'H' then 'Credit'
                when debit_credit_reval_ind = 'S' then 'Debit'
                else '-'
            end as debit_credit_reval_ind,
            valuation_type,
            transaction_quantity,
            base_unit_key,
            qty_price_unit,
            order_price_unit_key,
            po_doc_number,
            po_doc_item,
            ref_doc_year,
            document_no_ref_doc_num,
            reference_doc_item_num,
            sec_doc_year,
            sec_doc_num,
            sec_doc_item,
            case
                when deliv_complete_ind = 'X' then 'Yes'
                else 'No'
            end as deliv_complete_ind,
            item_text,
            goods_recipient,
            case
                when co_area = '1000' then 'Rush Enterprise, Inc.'
                else '-'
            end as co_area,
            cost_center_key,
            old_project_num,
            coorder_key,
            fiscal_year,
            case    
                when allow_backpost = 'X' then 'Yes'
                else 'No'            
            end as allow_backpost,
            company_code, 
            res_num,
            res_item_num,
            case 
                when trans_event_stat = '2' then 'Document evaluations and standard analyses (update)'
                when trans_event_stat = '3' then 'Standard analyses only (update)'
                else 'Not relevant'
            end as trans_event_stat,
            recv_issue_mat_key,
            recv_issue_storage_key,
            recv_issue_batch,
            transfer_batch_type,
            case 
                when move_ind = 'B' then  'Goods movement for purchase order'
                when move_ind = 'F' then  'Goods movement for production order'
                when move_ind = 'L' then  'Goods movement for delivery note'
                when move_ind = 'K' then  'Goods movement for kanban requirement (WM - internal only)'
                when move_ind = 'O' then  'Subsequent adjustment of material-provided consumption'
                when move_ind = 'W' then  'Subsequent adjustment of proportion/product unit material'
                else 'Goods movement w/o reference'
            end as move_ind,
            special_stock_ind,
            mvnt_type_eval_key,
            consump_posting,
            case
                when receipt_ind = '' then 'Normal receipt'
                when receipt_ind = 'X' then 'Stock transport order'
                when receipt_ind = 'L' then 'Tied empties'
                else '-'
            end as receipt_ind,
            warehouse_number_key,
            storage_type_key,
            storage_bin_key,
            warehouse_movement_type_key,
            transfer_req_num,
            transfer_req_item,
            case    
                when whs_posting_ind = 'X' then 'Yes'
                else 'No'            
            end as whs_posting_ind,
            case    
                when src_interim_posting = 'X' then 'Yes'
                else 'No'            
            end as src_interim_posting,
            case    
                when dest_interim_posting = 'X' then 'Yes'
                else 'No'            
            end as dest_interim_posting,
            case    
                when deliv_complete_ind = 'X' then 'Yes'
                else 'No'            
            end as dyn_storage_bin,
            gr_gi_slips_num,
            movement_reason_key,
            profit_center_key,
            order_item,
            gl_account_key,
            po_unit,
            supplier,
            shelf_life_date,
            partner_profit_center,
            stock_material,
            recv_issue_mat_2,
            post_str_qty,
            post_str_val,
            case    
                when qty_update = 'X' then 'Yes'
                else 'No'            
            end as qty_update,
            case    
                when val_update = 'X' then 'Yes'
                else 'No'            
            end as val_update,
            pre_post_stock,
            case 
                when price_ctrl_ind = 'S' then 'Standard price'
                when price_ctrl_ind = 'V' then 'Moving average price/periodic unit price'
                else '-'
            end as price_ctrl_ind,
            settle_ref_date,
            settle_ref_date_2,
            orig_doc_line,
            manufacture_date,
            stock_type_mod,
            case 
                when stock_type_mod = '0' then 'Restricted-Use Stock'
                when stock_type_mod = '1' then 'Unrestricted-Use Stock'
                when stock_type_mod = '2' then 'Stock in Quality Inspection'
                when stock_type_mod = '3' then 'Blocked Stock'
                when stock_type_mod = 'L' then 'Tied Empties'
                when stock_type_mod = 'M' then 'Stock Transfer of "Empties", on Mat. Basis After Val. Basis'
                when stock_type_mod = 'W' then 'Stock Transfer of "Empties", on Val. Basis After Mat. Basis'
                when stock_type_mod = 'F' then 'Unrestricted-Use Stock (T156M-ZUSTD Read with " ")'
                else '-'
            end as stock_type_mod_description,
            trans_event_type_mkpf,
            posting_date_mkpf,
            entry_date_mkpf,
            entry_time_mkpf,
            user_name_mkpf,
            trans_code_mkpf,
            delivery_num,
            delivery_item,
            edw_load_timestamp,
            current_timestamp() as dbt_run_timestamp,
            datasource_key
        from {{ ref("stg_goods_movement") }}
    )
select
    material_doc_num,
    material_doc_year,
    doc_item,
    material_key,
    plant_key,
    storage_location_key,
    movement_type_key,
    posting_date,
    user_name,
    reference_doc_number,
    reference_doc_number_mkpf,
    quantity,
    entry_unit_key,
    recv_issue_plant_key,
    amount_lc,
    debit_credit_flag,
    debit_credit_ind, 
    transaction_event_type_key,
    accounting_doc_type_key,
    revaluation_doc_type_key,
    accounting_doc_entry_date,
    entry_time,
    last_changed_on,
    document_header_text,
    gr_gi_slip_version,
    gr_gi_slip_number,
    logical_system,
    transaction_code,
    external_wms_posting,
    goods_issue_time,
    delivery_number,
    line_id,
    parent_id,
    auto_item,
    batch_number,
    vendor_key,
    customer_key,
    currency_code_name,
    debit_credit_reval_ind,
    valuation_type,
    transaction_quantity,
    base_unit_key,
    qty_price_unit,
    order_price_unit_key,
    po_doc_number,
    po_doc_item,
    ref_doc_year,
    document_no_ref_doc_num,
    reference_doc_item_num,
    sec_doc_year,
    sec_doc_num,
    sec_doc_item,
    deliv_complete_ind,
    item_text,
    goods_recipient,
    co_area,
    cost_center_key,
    old_project_num,
    coorder_key,
    fiscal_year,
    allow_backpost,
    company_code,
    res_num,
    res_item_num,
    trans_event_stat,
    recv_issue_mat_key,
    recv_issue_storage_key,
    recv_issue_batch,
    transfer_batch_type,
    move_ind,
    special_stock_ind,
    mvnt_type_eval_key,
    consump_posting,
    receipt_ind,
    warehouse_number_key,
    storage_type_key,
    storage_bin_key,
    warehouse_movement_type_key,
    transfer_req_num,
    transfer_req_item,
    whs_posting_ind,
    src_interim_posting,
    dest_interim_posting,
    dyn_storage_bin,
    gr_gi_slips_num,
    movement_reason_key,
    profit_center_key,
    order_item,
    gl_account_key,
    po_unit,
    supplier,
    shelf_life_date,
    partner_profit_center,
    stock_material,
    recv_issue_mat_2,
    post_str_qty,
    post_str_val,
    qty_update,
    val_update,
    pre_post_stock,
    price_ctrl_ind,
    settle_ref_date,
    settle_ref_date_2,
    orig_doc_line,
    manufacture_date,
    stock_type_mod,
    stock_type_mod_description,
    trans_event_type_mkpf,
    posting_date_mkpf,
    entry_date_mkpf,
    entry_time_mkpf,
    user_name_mkpf,
    trans_code_mkpf,
    delivery_num,
    delivery_item,
    edw_load_timestamp,
    dbt_run_timestamp,
    datasource_key
from trn_tbl_goods_movement