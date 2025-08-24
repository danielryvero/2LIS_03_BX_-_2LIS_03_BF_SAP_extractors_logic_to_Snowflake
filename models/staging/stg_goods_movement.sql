{{ config(
    tags=["analyticsinventory"]) 
    }}
with
    datasource as (select datasource_key, datasource from {{ ref("dim_datasources") }}),
    mkpf_raw as (
        select
            cpudt,
            spe_budat_uhr,
            awsys,
            aedat,
            mjahr as mjahr_mkpf,
            mblnr as mblnr_mkpf,
            wever,
            le_vbeln,
            usnam,
            cputm,
            blart,
            blaum,
            bfwms,
            xabln,
            xblnr,
            vgart,
            tcode2,
            budat,
            bktxt,
            source,
            edw_load_timestamp,
            opflag,
            aedattm
        from
            {{ source("facts", 'mkpf') }}
            {{ stage_incremental('trn_tbl_goods_movement') }}
    ),
    mkpf as ({{ handle_cdc_load("mkpf_raw", ["mblnr_mkpf","mjahr_mkpf"]) }}),
    dim_material as (
        select sap_material_number, material_key from {{ ref("dim_material") }}
    ),
    dim_plant as (select plant, plant_key from {{ ref("dim_plant") }}),
    dim_storage_location as (
        select storage_location, plant, storage_location_key
        from {{ ref("dim_storage_location") }}
    ),
    dim_movement_type as (
        select movement_type, movement_type_key from {{ ref("dim_movement_type") }}
    ),
    dim_movement_type_evaluation as (
        select movement_type, special_stock_ind, movement_ind, mvnt_type_eval_key from {{ ref("dim_movement_type_evaluation") }} 
    ) ,  
    dim_uom as (select uom_key, uom from {{ ref("dim_uom") }}),
    dim_transaction_event_type as (
        select transaction_event_type, transaction_event_type_key
        from {{ ref("dim_transaction_event_type") }}
    ),
    dim_accounting_doc_type as (
        select accounting_doc_type, accounting_doc_type_key
        from {{ ref("dim_accounting_doc_type") }}
    ),
    dim_vendor as (select vendor, vendor_key from {{ ref("dim_vendor") }}),
    dim_customer as (
        select customer_key, customer_number from {{ ref("dim_customer") }}
    ),
    dim_cost_center as (
        select co_area, cost_center, cost_center_key from {{ ref("dim_cost_center") }}
    ),
    dim_coorder as (select coorder_key, coorder_number from {{ ref("dim_coorder") }}),
    dim_warehouse as (
        select warehouse_number_key, warehouse_number from {{ ref("dim_warehouse") }}
    ),
    dim_storage_type as (
        select storage_type_key, storage_type, warehouse_number
        from {{ ref("dim_storage_type") }}
    ),
    dim_storage_bins as (
        select storage_type, storage_bin, warehouse_number, storage_bin_key
        from {{ ref("dim_storage_bins") }}
    ),
    dim_warehouse_movement_type as (
        select warehouse_movement_type, warehouse_number, warehouse_movement_type_key
        from {{ ref("dim_warehouse_movement_type") }}
    ),
    dim_movement_reason as (
        select movement_reason_key, movement_reason, movement_type
        from {{ ref("dim_movement_reason") }}
    ),
    dim_profit_center as (
        select co_area, profit_ctr, profit_center_key
        from {{ ref("dim_profit_center") }}
    ),
    dim_gl_account as (
        select gl_account_key, gl_account from {{ ref("dim_gl_account") }}
    ),
    goods_movement as (
        select
            mblnr,
            mjahr,
            zeile,
            matnr,
            werks,
            lgort,
            bwart,
            xblnr_mkpf,
            erfmg,
            erfme,
            umwrk,
            dmbtr,
            shkzg,
            line_id,
            parent_id,
            xauto,
            charg,
            lifnr,
            kunnr,
            waers,
            shkum,
            bwtar,
            menge,
            meins,
            bpmng,
            bprme,
            ebeln,
            ebelp,
            lfbja,
            lfbnr,
            lfpos,
            sjahr,
            smbln,
            smblp,
            elikz,
            sgtxt,
            wempf,
            kokrs,
            kostl,
            projn,
            aufnr,
            gjahr,
            xruem,
            bukrs,
            rsnum,
            rspos,
            kzstr,
            ummat,
            umlgo,
            umcha,
            umbar,
            kzbew,
            coalesce(kzbew, '') as kzbew_join,
            kzvbr,
            kzzug,
            lgnum,
            lgtyp,
            lgpla,
            tbnum,
            tbpos,
            xblvs,
            vschn,
            nschn,
            dypla,
            weanz,
            grund,
            bwlvs,
            prctr,
            aufps,
            sakto,
            bstme,
            emlif,
            vfdat,
            pprctr,
            matbf,
            ummab,
            bustm,
            bustw,
            mengu,
            wertu,
            lbkum,
            vprsv,
            dabrbz,
            dabrz,
            urzei,
            txjcd,
            hsdat,
            zustd_t156m,
            vgart_mkpf,
            budat_mkpf,
            cpudt_mkpf,
            cputm_mkpf,
            usnam_mkpf,
            tcode2_mkpf,
            vbeln_im,
            vbelp_im,
            sobkz, 
            coalesce(sobkz, '') as sobkz_join,
            source,
            edw_load_timestamp,
            opflag,
            aedattm
        from
            {{ source("facts", 'mseg') }}
            {{ stage_incremental('trn_tbl_goods_movement') }}
    ),
    macro_goods_movement as (
        {{ handle_cdc_load("goods_movement", ["mblnr","mjahr","zeile"]) }}
    ),
    stg_goods_movement as (
        select
            mblnr as material_doc_num,
            mjahr as material_doc_year,
            zeile as doc_item,
            dim_material.material_key as material_key,
            dim_plant.plant_key as plant_key,
            dim_storage_location.storage_location_key as storage_location_key,
            dim_movement_type.movement_type_key as movement_type_key,
            mkpf.budat as posting_date,
            mkpf.usnam as user_name,
            mkpf.xblnr as reference_doc_number,
            xblnr_mkpf as reference_doc_number_mkpf,
            erfmg as quantity,
            dim_uom_entry_unit.uom_key as entry_unit_key,
            dim_plant_rcv_issue.plant_key as recv_issue_plant_key,
            dmbtr as amount_lc,
            shkzg as debit_credit_flag,
            dim_transaction_event_type.transaction_event_type_key
            as transaction_event_type_key,
            dim_accounting_doc_type.accounting_doc_type_key as accounting_doc_type_key,
            dim_accounting_doc_type_reval.accounting_doc_type_key
            as revaluation_doc_type_key,
            mkpf.cpudt as accounting_doc_entry_date,
            mkpf.cputm as entry_time,
            mkpf.aedat as last_changed_on,
            mkpf.bktxt as document_header_text,
            mkpf.wever as gr_gi_slip_version,
            mkpf.xabln as gr_gi_slip_number,
            mkpf.awsys as logical_system,
            mkpf.tcode2 as transaction_code,
            mkpf.bfwms as external_wms_posting,
            mkpf.spe_budat_uhr as goods_issue_time,
            mkpf.le_vbeln as delivery_number,
            line_id as line_id,
            parent_id as parent_id,
            xauto as auto_item,
            charg as batch_number,
            dim_vendor.vendor_key as vendor_key,
            dim_customer.customer_key as customer_key,
            waers as currency_code_name,
            shkum as debit_credit_reval_ind,
            bwtar as valuation_type,
            menge as transaction_quantity,
            dim_uom_base_unit.uom_key as base_unit_key,
            bpmng as qty_price_unit,
            dim_uom_order_price.uom_key as order_price_unit_key,
            ebeln as po_doc_number,
            ebelp as po_doc_item,
            lfbja as ref_doc_year,
            lfbnr as document_no_ref_doc_num,
            lfpos as reference_doc_item_num,
            sjahr as sec_doc_year,
            smbln as sec_doc_num,
            smblp as sec_doc_item,
            elikz as deliv_complete_ind,
            sgtxt as item_text,
            wempf as goods_recipient,
            kokrs as co_area,
            kostl as cost_center,
            dim_cost_center.cost_center_key as cost_center_key,
            projn as old_project_num,
            dim_coorder.coorder_key as coorder_key,
            gjahr as fiscal_year,
            xruem as allow_backpost,
            bukrs as company_code,
            rsnum as res_num,
            rspos as res_item_num,
            kzstr as trans_event_stat,
            dim_material_recv_issue.material_key as recv_issue_mat_key,
            dim_storage_location_recv.storage_location_key as recv_issue_storage_key,
            umcha as recv_issue_batch,
            umbar as transfer_batch_type,
            kzbew as move_ind,
            kzvbr as consump_posting,
            kzzug as receipt_ind,
            lgnum as warehouse_number,
            dim_warehouse.warehouse_number_key as warehouse_number_key,
            lgtyp as storage_type,
            dim_storage_type.storage_type_key as storage_type_key,
            lgpla as storage_bin,
            dim_storage_bins.storage_bin_key as storage_bin_key,
            dim_warehouse_movement_type.warehouse_movement_type_key
            as warehouse_movement_type_key,
            tbnum as transfer_req_num,
            tbpos as transfer_req_item,
            xblvs as whs_posting_ind,
            vschn as src_interim_posting,
            nschn as dest_interim_posting,
            dypla as dyn_storage_bin,
            weanz as gr_gi_slips_num,
            dim_movement_reason.movement_reason_key as movement_reason_key,
            dim_profit_center.profit_center_key as profit_center_key,
            aufps as order_item,
            dim_gl_account.gl_account_key as gl_account_key,
            bstme as po_unit,
            emlif as supplier,
            vfdat as shelf_life_date,
            pprctr as partner_profit_center,
            matbf as stock_material,
            ummab as recv_issue_mat_2,
            bustm as post_str_qty,
            bustw as post_str_val,
            mengu as qty_update,
            wertu as val_update,
            lbkum as pre_post_stock,
            vprsv as price_ctrl_ind,
            dabrbz as settle_ref_date,
            dabrz as settle_ref_date_2,
            urzei as orig_doc_line,
            hsdat as manufacture_date,
            zustd_t156m as stock_type_mod,
            vgart_mkpf as trans_event_type_mkpf,
            budat_mkpf as posting_date_mkpf,
            cputm_mkpf as entry_time_mkpf,
            usnam_mkpf as user_name_mkpf,
            tcode2_mkpf as trans_code_mkpf,
            cpudt_mkpf as entry_date_mkpf,
            vbeln_im as delivery_num,
            vbelp_im as delivery_item,
            mseg.sobkz as special_stock_ind,
            dim_movement_type_evaluation.mvnt_type_eval_key,
            least(mkpf.edw_load_timestamp, mseg.edw_load_timestamp) as edw_load_timestamp,
            datasource.datasource_key
        from macro_goods_movement as mseg
        left join
            mkpf
            on equal_null(
                (mseg.mjahr || mseg.mblnr), (mkpf.mjahr_mkpf || mkpf.mblnr_mkpf)
            )
        left join
            dim_material on equal_null(mseg.matnr, dim_material.sap_material_number)
        left join
            dim_material as dim_material_recv_issue
            on equal_null(mseg.ummat, dim_material_recv_issue.sap_material_number)
        left join dim_plant on equal_null(mseg.werks, dim_plant.plant)
        left join
            dim_plant as dim_plant_rcv_issue
            on equal_null(mseg.umwrk, dim_plant_rcv_issue.plant)
        left join
            dim_storage_location
            on equal_null(
                (mseg.lgort || mseg.werks),
                (dim_storage_location.storage_location || dim_storage_location.plant)
            )
        left join
            dim_storage_location as dim_storage_location_recv
            on equal_null(
                (mseg.lgort || mseg.werks),
                (
                    dim_storage_location_recv.storage_location
                    || dim_storage_location_recv.plant
                )
            )
        left join
            dim_movement_type on equal_null(mseg.bwart, dim_movement_type.movement_type)
       left join 
           dim_movement_type_evaluation on equal_null((mseg.bwart || mseg.sobkz_join || kzbew_join ),
            (dim_movement_type_evaluation.movement_type || dim_movement_type_evaluation.special_stock_ind || dim_movement_type_evaluation.movement_ind))
        left join
            dim_uom as dim_uom_entry_unit
            on equal_null(mseg.erfme, dim_uom_entry_unit.uom)
        left join
            dim_uom as dim_uom_base_unit
            on equal_null(mseg.meins, dim_uom_base_unit.uom)
        left join
            dim_uom as dim_uom_order_price
            on equal_null(mseg.bprme, dim_uom_order_price.uom)
        left join
            dim_transaction_event_type
            on equal_null(
                mseg.vgart_mkpf, dim_transaction_event_type.transaction_event_type
            )
        left join
            dim_accounting_doc_type
            on equal_null(mkpf.blart, dim_accounting_doc_type.accounting_doc_type)
        left join
            dim_accounting_doc_type as dim_accounting_doc_type_reval
            on equal_null(mkpf.blaum, dim_accounting_doc_type_reval.accounting_doc_type)
        left join dim_vendor on equal_null(mseg.lifnr, dim_vendor.vendor)
        left join dim_customer on equal_null(mseg.kunnr, dim_customer.customer_number)
        left join
            dim_cost_center
            on equal_null(
                (mseg.kostl || mseg.kokrs),
                (dim_cost_center.cost_center || dim_cost_center.co_area)
            )
        left join dim_coorder on equal_null(mseg.aufnr, dim_coorder.coorder_number)
        left join
            dim_warehouse on equal_null(mseg.lgnum, dim_warehouse.warehouse_number)
        left join
            dim_storage_type
            on equal_null(
                (mseg.lgnum || mseg.lgtyp),
                (dim_storage_type.warehouse_number || dim_storage_type.storage_type)
            )
        left join
            dim_storage_bins
            on equal_null(
                (mseg.lgnum || mseg.lgtyp || mseg.lgpla),
                (
                    dim_storage_bins.warehouse_number
                    || dim_storage_bins.storage_type
                    || dim_storage_bins.storage_bin
                )
            )
        left join
            dim_warehouse_movement_type
            on equal_null(
                (mseg.lgnum || mseg.bwlvs),
                (
                    dim_warehouse_movement_type.warehouse_number
                    || dim_warehouse_movement_type.warehouse_movement_type
                )
            )
        left join
            dim_movement_reason
            on equal_null(
                (mseg.grund || mseg.bwart),
                (
                    dim_movement_reason.movement_reason
                    || dim_movement_reason.movement_type
                )
            )
        left join
            dim_profit_center
            on equal_null(
                (mseg.prctr || mseg.kokrs),
                (dim_profit_center.profit_ctr || dim_profit_center.co_area)
            )
        left join dim_gl_account on equal_null(mseg.sakto, dim_gl_account.gl_account)
        left join datasource on equal_null(mseg.source, datasource.datasource)
    )
select
    material_doc_num,
    material_doc_year,
    doc_item,
    material_key,
    plant_key,
    storage_location_key,
    movement_type_key,
    user_name,
    reference_doc_number,
    reference_doc_number_mkpf,
    quantity,
    entry_unit_key,
    recv_issue_plant_key,
    amount_lc,
    debit_credit_flag,
    transaction_event_type_key,
    accounting_doc_type_key,
    revaluation_doc_type_key,
    {{ convert_to_date("posting_date") }},
    {{ convert_to_date("accounting_doc_entry_date") }},
    entry_time,
    {{ convert_to_date("last_changed_on") }},
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
    cost_center,
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
    warehouse_number,
    warehouse_number_key,
    storage_type,
    storage_type_key,
    storage_bin,
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
    {{ convert_to_date("shelf_life_date") }},
    partner_profit_center,
    stock_material,
    recv_issue_mat_2,
    post_str_qty,
    post_str_val,
    qty_update,
    val_update,
    pre_post_stock,
    price_ctrl_ind,
    {{ convert_to_date("settle_ref_date") }},
    {{ convert_to_date("settle_ref_date_2") }},
    orig_doc_line,
    {{ convert_to_date("manufacture_date") }},
    stock_type_mod,
    {{ convert_to_date("posting_date_mkpf") }},
    entry_time_mkpf,
    user_name_mkpf,
    trans_event_type_mkpf,
    {{ convert_to_date("entry_date_mkpf") }},
    trans_code_mkpf,
    delivery_num,
    delivery_item,
    edw_load_timestamp,
    datasource_key
from stg_goods_movement
