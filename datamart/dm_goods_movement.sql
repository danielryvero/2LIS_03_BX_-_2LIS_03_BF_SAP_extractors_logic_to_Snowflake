{{
    config(
        materialized="incremental", 
        unique_key=["material_doc_num", "material_doc_year", "doc_item"],
        pre_hook=["{{warehouse_size('MEDIUM')}}"],
        tags=["analyticsinventory"]
    )
}}
with
    dim_material as (
        select 
            material_key,
            sap_material_number,
            part_number,
            product_description,
            mat_type,
            mat_type_desc,
            mat_group,
            mat_group_desc,
            c1_core,
            c2_core,
            core_or_reman,
            core_group_id,
            core,
            prodh1,
            prodh1_text,
            prodh2,
            prodh2_text,
            prodh3,
            prodh3_text,
            part_vendor_code,
            part_vendor_text
        from
            {{ ref("dim_material") }}
    ),
    dim_storage_location as (
        select
            storage_location_key,
            storage_location,
            storage_location_description,
            storage_location_type,
            storage_location_type_description,
        from
            {{ ref("dim_storage_location")}}
    ),
    dim_movement_type as (
        select
            movement_type_key,
            movement_type,
            reversal_movement_type,
            movement_type_text
        from
            {{ ref("dim_movement_type")}}
    ),
    dim_uom as (
        select
            uom_key,
            uom
        from
            {{ ref("dim_uom")}}
    ),
    dim_plant as (
        select
            plant_key,
            plant,
            profit_center_key,
            plant_name1 as plant_name
        from
            {{ ref("dim_plant")}}
    ),
    dim_transaction_event_type as (
        select
            transaction_event_type_key,
            transaction_event_type,
            transaction_event_type_text
        from
            {{ ref("dim_transaction_event_type")}}
    ),
    dim_accounting_doc_type as (
        select
            accounting_doc_type_key,
            accounting_doc_type,
            accounting_doc_type_text
        from
            {{ ref("dim_accounting_doc_type")}}
    ),
    dim_vendor as (
        select
            vendor_key,
            vendor
        from
            {{ ref("dim_vendor")}}
    ),
    dim_customer as (
        select
            customer_key,
            customer_number
        from
            {{ ref("dim_customer")}}
    ),
    dim_cost_center as (
        select
            cost_center_key,
            cost_center
        from
            {{ ref("dim_cost_center")}}
    ),
    dim_coorder as (
        select
            coorder_key,
            coorder_number
        from
            {{ ref("dim_coorder")}}
    ),
    dim_warehouse as (
        select
            warehouse_number_key,
            warehouse_number,
            warehouse_number_description
        from
            {{ ref("dim_warehouse")}}
    ),
    dim_warehouse_movement_type as (
    select 
        warehouse_movement_type_key, 
        warehouse_movement_type
    from
        {{ ref("dim_warehouse_movement_type")}}
    ),
    dim_storage_type as (
        select
            storage_type_key,
            storage_type,
            storage_type_name
        from
            {{ ref("dim_storage_type")}}
    ),
    dim_storage_bins as (
        select
            storage_bin_key,
            storage_bin
        from
            {{ ref("dim_storage_bins")}}
    ),
    dim_movement_reason as (
        select
            movement_reason_key,
            movement_reason,
            movement_reason_text
        from
            {{ ref("dim_movement_reason")}}
    ),
    dim_profit_center as (
        select
            profit_ctr,
            short_text,
            profit_center_key,
            company,
            company_text,
            country,
            currency,
            country_text,
            business_unit,
            business_unit_text,
            division,
            division_text,
            region,
            region_text,
            district,
            district_text
        from
            {{ ref("dim_profit_center")}}
    ),
    dim_gl_account as (
        select
            gl_account_key,
            gl_account
        from
            {{ ref("dim_gl_account")}}            
    ),
    dim_currency_conversion as (
        select currency_from, currency_to, conversion_date, exchange_rate
        from {{ ref("dim_currency_conversions") }}
        where exchange_rate_type = 'M'
    ),
    dim_stock_type as (
        select
            stock_type_key,
            stock_type,
            stock_movement_type,
            special_stock_ind,
            coalesce(special_stock_ind, '') as special_stock_ind_join,
            stock_unrestricted,
            stock_quality_insp,
            stock_consignment_unrestricted,
            stock_consignment_quality_insp,
            stock_blocked_returns,
            stock_transfer_storage_loc,
            stock_transfer_plant,
            stock_in_transit,
            cons_customer,
            packaging_customer,
            stock_provided_by_customer,
            stock_sales_order,
            stock_provided_to_vendor,
            cons_quality_insp,
            packaging_quality_insp,
            stock_blocked,
            stock_consignment_blocked,
            stock_sales_order_quality_insp,
            stock_sales_order_blocked,
            stock_provided_quality_insp,
            proj_stock_unrestricted,
            proj_stock_quality_insp,
            proj_stock_blocked,
            stock_restricted_use,
            proj_stock_restricted,
            stock_sales_order_restricted,
            cons_restricted_use,
            packaging_restricted_use,
            stock_provided_restricted,
            consignment_restricted,
            packaging_unrestricted,
            packaging_returnable,
            packaging_blocked,
            packaging_restricted,
            tied_empties_stock,
            stock_in_transit_1,
            stock_in_transit_2,
            stock_blocked_gr,
            subcontractor_stock_transfer,
            consignment_stock_transfer,
            stock_blocked_gr_sales,
            stock_blocked_gr_project,
            stock_blocked_gr_batch,
            stock_blocked_gr_sd_batch,
            stock_blocked_gr_project_batch
        from
            {{ ref("dim_stock_type")}}
    ),
    dim_movement_type_quantity_posting as (
        select
            mvnt_type_quantity_posting_key,
            quantity_posting_string,
            stock_type_modification,
            consecutive_counter,
            segment_string_marc,
            segment_string_mstb,
            segment_string_mard,
            segment_string_mchb,
            segment_string_mbpr,
            segment_string_mlib,
            segment_string_mkol,
            segment_string_meik,
            segment_string_mklk,
            segment_string_mkub,
            segment_string_ekpo,
            segment_string_faup,
            segment_string_mres,
            segment_string_bbst,
            segment_string_bfau,
            segment_string_iupd,
            consumption_update_indicator,
            segment_string_msku,
            segment_string_mslb,
            segment_string_mssl,
            segment_string_mska,
            segment_string_mste,
            segment_string_mssa,
            segment_string_mspr,
            segment_string_mstq,
            segment_string_mssp,
            segment_string_msca,
            segment_string_msoa,
            segment_string_mscp,
            segment_string_msop,
            movement_type_for_goods_issue, --lbbsa
            suppress_batch_split_indicator,
            segment_string_ekpk,
            segment_string_iseg,
            segment_string_ekla,
            segment_string_mcsd,
            segment_string_mcss,
            segment_string_msrd,
            segment_string_msrs,
            segment_string_msfd,
            segment_string_msfs,
            segment_string_mscd,
            segment_string_mscs,
            segment_string_msid,
            segment_string_msis,
            null as second_line_flag
        from {{ ref("dim_movement_type_quantity_posting")}}
        union all (
        select
            mvnt_type_quantity_posting_key,
            quantity_posting_string,
            stock_type_modification,
            consecutive_counter,
            segment_string_marc,
            segment_string_mstb,
            segment_string_mard,
            segment_string_mchb,
            segment_string_mbpr,
            segment_string_mlib,
            segment_string_mkol,
            segment_string_meik,
            segment_string_mklk,
            segment_string_mkub,
            segment_string_ekpo,
            segment_string_faup,
            segment_string_mres,
            segment_string_bbst,
            segment_string_bfau,
            segment_string_iupd,
            consumption_update_indicator,
            segment_string_msku,
            segment_string_mslb,
            segment_string_mssl,
            segment_string_mska,
            segment_string_mste,
            segment_string_mssa,
            segment_string_mspr,
            segment_string_mstq,
            segment_string_mssp,
            segment_string_msca,
            segment_string_msoa,
            segment_string_mscp,
            segment_string_msop,
            movement_type_for_consignment as movement_type_for_goods_issue, --kbbsa
            suppress_batch_split_indicator,
            segment_string_ekpk,
            segment_string_iseg,
            segment_string_ekla,
            segment_string_mcsd,
            segment_string_mcss,
            segment_string_msrd,
            segment_string_msrs,
            segment_string_msfd,
            segment_string_msfs,
            segment_string_mscd,
            segment_string_mscs,
            segment_string_msid,
            segment_string_msis,
            'X' as second_line_flag
        from {{ ref("dim_movement_type_quantity_posting")}}
        where movement_type_for_consignment is not null
        )
    ),
    dim_movement_type_evaluation as (
        select
            mvnt_type_eval_key,
            movement_type,
            special_stock_ind,
            movement_ind,
            group_one_eval,
            group_two_eval,
            group_three_eval,
            group_four_eval,
            stock_transfer_ind,
            returrn_ind,
            physical_inv_ind,
            correction_ind
        from {{ ref("dim_movement_type_evaluation")}}
    ),

    fact_goods_movement as (
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
            trans_event_type_mkpf,
            posting_date_mkpf,
            entry_date_mkpf,
            entry_time_mkpf,
            user_name_mkpf,
            trans_code_mkpf,
            delivery_num,
            delivery_item,
            mvnt_type_eval_key,
            special_stock_ind,
            edw_load_timestamp
        from
            {{ ref("fact_goods_movement")}}
    {% if is_incremental() %}
    where
        edw_load_timestamp > (
            select ifnull(max(edw_load_timestamp), to_timestamp('1999-12-31 00:00:00'))
            from {{ this }}
        )
        {% endif %}   
 ),
    dm_goods_movement as (
        select
            -- Currency conversion details
            dim_currency_conversion.currency_from,
            dim_currency_conversion.currency_to,
            dim_currency_conversion.conversion_date,
            dim_currency_conversion.exchange_rate,
            -- Goods movement details
            fact_goods_movement.material_doc_num,
            fact_goods_movement.material_doc_year,
            fact_goods_movement.doc_item,
            fact_goods_movement.posting_date,
            fact_goods_movement.user_name,
            fact_goods_movement.reference_doc_number,
            fact_goods_movement.reference_doc_number_mkpf,
            fact_goods_movement.quantity,
            fact_goods_movement.amount_lc,
            case 
                when dim_movement_type_quantity_posting.second_line_flag = 'X' and fact_goods_movement.debit_credit_ind = 'Debit' 
                then 'Credit'
                when dim_movement_type_quantity_posting.second_line_flag = 'X' and fact_goods_movement.debit_credit_ind = 'Credit' 
                then 'Debit'
                else fact_goods_movement.debit_credit_ind
            end as debit_credit_ind_adjusted,
            fact_goods_movement.accounting_doc_entry_date,
            fact_goods_movement.entry_time,
            fact_goods_movement.last_changed_on,
            fact_goods_movement.document_header_text,
            fact_goods_movement.gr_gi_slip_version,
            fact_goods_movement.gr_gi_slip_number,
            fact_goods_movement.logical_system,
            fact_goods_movement.transaction_code,
            fact_goods_movement.external_wms_posting,
            fact_goods_movement.goods_issue_time,
            fact_goods_movement.delivery_number,
            fact_goods_movement.line_id,
            fact_goods_movement.parent_id,
            fact_goods_movement.auto_item,
            fact_goods_movement.batch_number,
            fact_goods_movement.currency_code_name,
            fact_goods_movement.debit_credit_reval_ind,
            fact_goods_movement.valuation_type,
            fact_goods_movement.transaction_quantity,
            fact_goods_movement.qty_price_unit,
            fact_goods_movement.po_doc_number,
            fact_goods_movement.po_doc_item,
            fact_goods_movement.ref_doc_year,
            fact_goods_movement.document_no_ref_doc_num,
            fact_goods_movement.reference_doc_item_num,
            fact_goods_movement.sec_doc_year,
            fact_goods_movement.sec_doc_num,
            fact_goods_movement.sec_doc_item,
            fact_goods_movement.deliv_complete_ind,
            fact_goods_movement.item_text,
            fact_goods_movement.goods_recipient,
            fact_goods_movement.co_area,
            fact_goods_movement.old_project_num,
            fact_goods_movement.fiscal_year,
            fact_goods_movement.allow_backpost,
            fact_goods_movement.company_code,
            fact_goods_movement.res_num,
            fact_goods_movement.res_item_num,
            fact_goods_movement.trans_event_stat,
            fact_goods_movement.recv_issue_batch,
            fact_goods_movement.transfer_batch_type,
            fact_goods_movement.move_ind,
            fact_goods_movement.consump_posting,
            fact_goods_movement.receipt_ind,
            fact_goods_movement.transfer_req_num,
            fact_goods_movement.transfer_req_item,
            fact_goods_movement.whs_posting_ind,
            fact_goods_movement.src_interim_posting,
            fact_goods_movement.dest_interim_posting,
            fact_goods_movement.dyn_storage_bin,
            fact_goods_movement.gr_gi_slips_num,
            fact_goods_movement.order_item,
            fact_goods_movement.po_unit,
            fact_goods_movement.supplier,
            fact_goods_movement.shelf_life_date,
            fact_goods_movement.partner_profit_center,
            fact_goods_movement.stock_material,
            fact_goods_movement.recv_issue_mat_2,
            fact_goods_movement.post_str_qty,
            fact_goods_movement.post_str_val,
            fact_goods_movement.qty_update,
            fact_goods_movement.val_update,
            fact_goods_movement.pre_post_stock,
            fact_goods_movement.price_ctrl_ind,
            fact_goods_movement.settle_ref_date,
            fact_goods_movement.settle_ref_date_2,
            fact_goods_movement.orig_doc_line,
            fact_goods_movement.manufacture_date,
            fact_goods_movement.stock_type_mod,
            fact_goods_movement.trans_event_type_mkpf,
            fact_goods_movement.posting_date_mkpf,
            fact_goods_movement.entry_date_mkpf,
            fact_goods_movement.entry_time_mkpf,
            fact_goods_movement.user_name_mkpf,
            fact_goods_movement.trans_code_mkpf,
            fact_goods_movement.delivery_num,
            fact_goods_movement.delivery_item,
            fact_goods_movement.edw_load_timestamp,
            case 
                when fact_goods_movement.stock_type_mod = 'F'
                then ''
                else stock_type_mod 
            end as stock_type_mod_join,    
            case
                when fact_goods_movement.auto_item = 'X'
                then '02' else '01'
            end as auto_item_join,
            dim_material.sap_material_number,
            dim_material.part_number,
            dim_material.product_description,
            dim_material.mat_type,
            dim_material.mat_type_desc,
            dim_material.mat_group,
            dim_material.mat_group_desc,
            dim_material.c1_core,
            dim_material.c2_core,
            dim_material.core_or_reman,
            dim_material.core_group_id,
            dim_material.core,
            dim_material.prodh1,
            dim_material.prodh1_text,
            dim_material.prodh2,
            dim_material.prodh2_text,
            dim_material.prodh3,
            dim_material.prodh3_text,
            dim_material.part_vendor_code,
            dim_material.part_vendor_text,
            recv_issue_mat.sap_material_number as recv_issue_mat,
            dim_storage_location.storage_location,
            dim_storage_location.storage_location_description,
            dim_storage_location.storage_location_type,
            dim_storage_location.storage_location_type_description,
            dim_movement_type.movement_type,
            dim_movement_type.movement_type_text,
            entry_unit.uom as entry_unit,
            base_unit.uom as base_unit,
            order_price.uom as order_price_unit,
            recv_issue_plant.plant as recv_issue_plant,
            dim_plant.plant as plant,
            dim_plant.plant_name,
            dim_transaction_event_type.transaction_event_type,
            dim_transaction_event_type.transaction_event_type_text,
            dim_accounting_doc_type.accounting_doc_type,
            dim_accounting_doc_type.accounting_doc_type_text,
            revaluation_doc_type.accounting_doc_type as revaluation_doc_type,
            revaluation_doc_type.accounting_doc_type_text as revaluation_doc_type_text,
            dim_vendor.vendor,
            dim_customer.customer_number,
            dim_cost_center.cost_center,
            dim_coorder.coorder_number,
            dim_warehouse.warehouse_number,
            dim_warehouse.warehouse_number_description,
            dim_warehouse_movement_type.warehouse_movement_type,
            dim_storage_type.storage_type,
            dim_storage_type.storage_type_name,
            dim_storage_bins.storage_bin,
            dim_movement_reason.movement_reason,
            dim_movement_reason.movement_reason_text,
            dim_profit_center.profit_ctr,
            dim_profit_center.short_text,
            dim_profit_center.company,
            dim_profit_center.company_text,
            dim_profit_center.country,
            dim_profit_center.country_text,
            dim_profit_center.business_unit,
            dim_profit_center.business_unit_text,
            dim_profit_center.division,
            dim_profit_center.division_text,
            dim_profit_center.region,
            dim_profit_center.region_text,
            dim_profit_center.district,
            dim_profit_center.district_text,
            dim_gl_account.gl_account,
            dim_movement_type_evaluation.group_one_eval,
            dim_movement_type_evaluation.group_two_eval,
            dim_movement_type_evaluation.group_three_eval,
            dim_movement_type_evaluation.group_four_eval,
            dim_movement_type_evaluation.stock_transfer_ind,
            dim_movement_type_evaluation.returrn_ind,
            dim_movement_type_evaluation.physical_inv_ind,
            dim_movement_type_evaluation.correction_ind,
            dim_movement_type_quantity_posting.movement_type_for_goods_issue as movement_type_for_goods_issue_join,
            case when dim_stock_type.stock_type is null then 'V' else dim_stock_type.stock_type end as stock_type_trn,
            fact_goods_movement.special_stock_ind, 
            case 
                when 
                       dim_stock_type.tied_empties_stock = 'X'
                    or dim_stock_type.stock_unrestricted = 'X'
                    or dim_stock_type.stock_restricted_use = 'X'
                    or dim_stock_type.stock_quality_insp = 'X'
                    or dim_stock_type.stock_blocked = 'X'
                    or dim_stock_type.stock_transfer_storage_loc = 'X'
                    or dim_stock_type.stock_transfer_plant = 'X'
                    or dim_stock_type.stock_in_transit = 'X'
                    or dim_stock_type.cons_customer = 'X'
                    or dim_stock_type.cons_quality_insp = 'X'
                    or dim_stock_type.cons_restricted_use = 'X'
                    or dim_stock_type.packaging_customer = 'X'
                    or dim_stock_type.packaging_quality_insp = 'X'
                    or dim_stock_type.packaging_restricted_use = 'X'
                    or dim_stock_type.stock_provided_to_vendor = 'X'
                    or dim_stock_type.stock_provided_quality_insp = 'X'
                    or dim_stock_type.stock_provided_restricted = 'X'
                    or dim_stock_type.stock_blocked_gr = 'X'
                    or dim_stock_type.subcontractor_stock_transfer = 'X'
                    or dim_stock_type.consignment_stock_transfer = 'X'
                then ''
            
                when 
                       dim_stock_type.stock_consignment_unrestricted = 'X'
                    or dim_stock_type.stock_consignment_quality_insp = 'X'
                    or dim_stock_type.stock_consignment_blocked = 'X'
                    or dim_stock_type.consignment_restricted = 'X'
                then 'K'

                when 
                       dim_stock_type.stock_sales_order = 'X'
                    or dim_stock_type.stock_sales_order_quality_insp = 'X'
                    or dim_stock_type.stock_sales_order_blocked = 'X'
                    or dim_stock_type.stock_sales_order_restricted = 'X'
                    or dim_stock_type.stock_provided_by_customer = 'X'
                    or dim_stock_type.stock_in_transit_1 = 'X'
                    or dim_stock_type.stock_blocked_gr_sales = 'X'
                then 'E'

                when 
                       dim_stock_type.proj_stock_unrestricted = 'X'
                    or dim_stock_type.proj_stock_quality_insp = 'X'
                    or dim_stock_type.proj_stock_blocked = 'X'
                    or dim_stock_type.proj_stock_restricted = 'X'
                    or dim_stock_type.stock_in_transit_2 = 'X'
                    or dim_stock_type.stock_blocked_gr_project = 'X'
                then 'Q'

                when dim_stock_type.stock_blocked_returns = 'X' then 'R'
                
                when 
                       dim_stock_type.packaging_unrestricted = 'X'
                    or dim_stock_type.packaging_returnable = 'X'
                    or dim_stock_type.packaging_blocked = 'X'
                    or dim_stock_type.packaging_restricted = 'X'
                then 'M'

                when dim_stock_type.stock_sales_order_blocked = 'X' then 'T'

                when 
                         fact_goods_movement.val_update is null 
                     and fact_goods_movement.qty_update = 'X'
                     then 'U'
                
                else 'V'
            end as stock_category,
            case 
                when stock_type_trn = 'T' and stock_category = '' then 'Tied Empties Stock'
                when stock_type_trn = 'A' and stock_category = '' then 'Unrestricted'
                when stock_type_trn = 'E' and stock_category = '' then 'Restricted-use stock'
                when stock_type_trn = 'B' and stock_category = '' then 'In Qual. Inspection'
                when stock_type_trn = 'D' and stock_category = '' then 'Blocked Stock'
                when stock_type_trn = 'F' and stock_category = '' then 'Stck i.trnsf.(plant)'
                when stock_type_trn = 'H' and stock_category = '' then 'Transit'
                when stock_type_trn = 'K' and stock_category = '' then 'Consignmt at custom.'
                when stock_type_trn = 'L' and stock_category = '' then 'Consigmt at cust. QI'
                when stock_type_trn = 'M' and stock_category = '' then 'Restr. cons.at cust.'
                when stock_type_trn = 'N' and stock_category = '' then 'Return.pack.at cust.'
                when stock_type_trn = 'O' and stock_category = '' then 'Re.pck.at custom. QI'
                when stock_type_trn = 'P' and stock_category = '' then 'Rstr.cust.ret.packng'
                when stock_type_trn = 'Q' and stock_category = '' then 'Mat. prov. to Vendor'
                when stock_type_trn = 'R' and stock_category = '' then 'Mat. prov.to vend QI'
                when stock_type_trn = 'S' and stock_category = '' then 'Rstr.mat.prov.to ven'
                when stock_type_trn = 'W' and stock_category = '' then 'Valuated Goods Receipt Blocked Stock'
                when stock_type_trn = 'U' and stock_category = '' then 'SC Stock in Transfer'
                when stock_type_trn = 'X' and stock_category = '' then 'Cust.Consig. Transf.'
                when stock_type_trn = 'A' and stock_category = 'K' then 'Unr-use consign.stck'
                when stock_type_trn = 'B' and stock_category = 'K' then 'Cons.qual.insp. stck'
                when stock_type_trn = 'D' and stock_category = 'K' then 'Blocked'
                when stock_type_trn = 'E' and stock_category = 'K' then 'Restr.-use consignmt'
                when stock_type_trn = 'A' and stock_category = 'E' then 'Sales order'
                when stock_type_trn = 'B' and stock_category = 'E' then 'Sales order'
                when stock_type_trn = 'D' and stock_category = 'E' then 'Blckd sales order (if MSEG-SOBKZ <> T)'
                when stock_type_trn = 'E' and stock_category = 'E' then 'Restr. sales order'
                when stock_type_trn = 'G' and stock_category = 'E' then 'Mat. Prov. By Cust.'
                when stock_type_trn = 'H' and stock_category = 'E' then 'Transit'
                when stock_type_trn = 'A' and stock_category = 'Q' then 'Project stock'
                when stock_type_trn = 'B' and stock_category = 'Q' then 'Project in QI'
                when stock_type_trn = 'D' and stock_category = 'Q' then 'Project blocked'
                when stock_type_trn = 'E' and stock_category = 'Q' then 'Restr. project stock'
                when stock_type_trn = 'H' and stock_category = 'Q' then 'Transit'
                when stock_type_trn = 'C' and stock_category = 'R' then 'Return'
                when stock_type_trn = 'A' and stock_category = 'M' then 'Unrestricted-use RTP'
                when stock_type_trn = 'B' and stock_category = 'M' then 'Qual. RTP'
                when stock_type_trn = 'D' and stock_category = 'M' then 'Blocked RTP'
                when stock_type_trn = 'D' and stock_category = 'T' then 'Blckd sales order ( stock_category = T if MSEG-SOBKZ = T; otherwise stock_category = E )'
                when stock_type_trn = 'E' and stock_category = 'M' then 'Restricted RTP'
                when stock_type_trn = 'V' and stock_category = 'V' then ''
                else 'Non-Valuated Stock'
            end as stock_type_description,
            case 
                when dim_movement_type.movement_type = 'Z52' and debit_credit_ind_adjusted = 'Credit' then 1
                when dim_movement_type.movement_type = 'Z52' and debit_credit_ind_adjusted = 'Debit' then -1
                when dim_movement_type.movement_type = '906' and debit_credit_ind_adjusted = 'Credit' then 1
                when dim_movement_type.movement_type = '906' and debit_credit_ind_adjusted = 'Debit' then -1																						   
                when dim_movement_type.movement_type = '122' and debit_credit_ind_adjusted = 'Credit' then -1
                when dim_movement_type.movement_type = '122' and debit_credit_ind_adjusted = 'Debit' then 1
                when dim_movement_type.movement_type = '123' and debit_credit_ind_adjusted = 'Credit' then -1
                when dim_movement_type.movement_type = '123' and debit_credit_ind_adjusted = 'Debit' then 1
                when dim_movement_type.reversal_movement_type is null and debit_credit_ind_adjusted = 'Debit' then 1
                when dim_movement_type.reversal_movement_type is null and debit_credit_ind_adjusted = 'Credit' then -1
                when dim_movement_type.reversal_movement_type is not null and debit_credit_ind_adjusted = 'Debit' then -1
                when dim_movement_type.reversal_movement_type is not null and debit_credit_ind_adjusted = 'Credit' then 1
             end as departure_ind,     
            case 
                when 
                        dim_movement_type_evaluation.stock_transfer_ind = 'X' 
                    and recv_issue_mat <> dim_material.sap_material_number 
                    and departure_ind > 0
                    then '004'
                when 
                        dim_movement_type_evaluation.stock_transfer_ind = 'X' 
                    and recv_issue_mat <> dim_material.sap_material_number 
                    and departure_ind < 0 
                    then '104'
                when 
                        dim_movement_type_evaluation.stock_transfer_ind = 'X' 
                    and recv_issue_plant <> dim_plant.plant 
                    and departure_ind > 0 
                    then '010'   
                when 
                        dim_movement_type_evaluation.stock_transfer_ind = 'X' 
                    and recv_issue_plant <> dim_plant.plant 
                    and departure_ind < 0 
                    then '110'                    
                when 
                        dim_movement_type_evaluation.physical_inv_ind = 'X'
                    and departure_ind > 0 
                    then '005'
                when 
                        dim_movement_type_evaluation.physical_inv_ind = 'X' 
                    and departure_ind < 0  
                    then '105'
                when 
                        dim_movement_type_evaluation.correction_ind = 'X'
                    and departure_ind > 0 
                    then '006'
                when 
                        dim_movement_type_evaluation.correction_ind = 'X' 
                    and departure_ind < 0  
                    then '106'
                when 
                        dim_vendor.vendor is not null 
                    and dim_movement_type_evaluation.correction_ind is null 
                    and dim_movement_type_evaluation.physical_inv_ind is null
                    and dim_movement_type_evaluation.stock_transfer_ind is null 
                    and departure_ind > 0
                    then '001'
                when 
                        dim_vendor.vendor is not null 
                    and dim_movement_type_evaluation.correction_ind is null 
                    and dim_movement_type_evaluation.physical_inv_ind is null
                    and dim_movement_type_evaluation.stock_transfer_ind is null 
                    and departure_ind < 0  
                    then '101'  
                when departure_ind > 0 
                    then '000'
                else '100'
            end as process_key,  
            case 
                    when stock_type_trn = 'V' and stock_category = 'V' then 2
                    when dim_material.sap_material_number = fact_goods_movement.stock_material or fact_goods_movement.stock_material is null
                        then 1
                    else 2
            end as stock_relev, --BWBREL
            case 
                when dim_movement_type.movement_type in ('102', '162', '262', '642', '906', '562', '302', '312', '314', 
                '310', '604', '672', '108', '344', '512', 'Z02', 'Z52', '110', '602', '123', '304', '316', '542', '202') then 'X'
                else ''
            end as rocancel,
            case
                when rocancel = 'X' then fact_goods_movement.transaction_quantity*-1
                else fact_goods_movement.transaction_quantity
            end as transaction_quantity_adjusted, --0CPQUABU maps to BWMNG  -> may need to adjust for conversion logic
            case 
                when rocancel = 'X' then fact_goods_movement.amount_lc*-1
                else fact_goods_movement.amount_lc --0CPPVLC maps to BWGEO
            end as amount_lc_adjusted,
            case 
                when rocancel = 'X' then fact_goods_movement.transaction_quantity*-1
                else fact_goods_movement.quantity 
            end as quantity_adjusted,
        -- Curency conversion details
        fact_goods_movement.amount_lc as amount_lc_usd,
        amount_lc_adjusted as amount_lc_adjusted_usd
        from
            fact_goods_movement
        left join
            dim_material
        on
            equal_null(fact_goods_movement.material_key, dim_material.material_key)
        left join
            dim_material recv_issue_mat
        on
            equal_null(fact_goods_movement.recv_issue_mat_key, recv_issue_mat.material_key)
        left join
            dim_storage_location
        on
            equal_null(fact_goods_movement.storage_location_key, dim_storage_location.storage_location_key)
        left join
            dim_movement_type
        on
            equal_null(fact_goods_movement.movement_type_key, dim_movement_type.movement_type_key)
        left join
            dim_uom entry_unit
        on
            equal_null(fact_goods_movement.entry_unit_key, entry_unit.uom_key)
        left join
            dim_uom base_unit
        on
            equal_null(fact_goods_movement.base_unit_key, base_unit.uom_key)
        left join
            dim_uom order_price
        on
            equal_null(fact_goods_movement.order_price_unit_key, order_price.uom_key)
        left join
            dim_plant recv_issue_plant
        on
            equal_null(fact_goods_movement.recv_issue_plant_key, recv_issue_plant.plant_key)
        left join
            dim_plant
        on 
            equal_null(fact_goods_movement.plant_key, dim_plant.plant_key)
        left join
            dim_transaction_event_type
        on
            equal_null(fact_goods_movement.transaction_event_type_key, dim_transaction_event_type.transaction_event_type_key)
        left join
            dim_accounting_doc_type
        on
            equal_null(fact_goods_movement.accounting_doc_type_key, dim_accounting_doc_type.accounting_doc_type_key)
        left join
            dim_accounting_doc_type revaluation_doc_type
        on
            equal_null(fact_goods_movement.revaluation_doc_type_key, dim_accounting_doc_type.accounting_doc_type_key)
        left join
            dim_vendor
        on
            equal_null(fact_goods_movement.vendor_key, dim_vendor.vendor_key)
        left join
            dim_customer
        on
            equal_null(fact_goods_movement.customer_key, dim_customer.customer_key)
        left join
            dim_cost_center
        on
            equal_null(fact_goods_movement.cost_center_key, dim_cost_center.cost_center_key)
        left join
            dim_coorder
        on
            equal_null(fact_goods_movement.coorder_key, dim_coorder.coorder_key)
        left join
            dim_warehouse
        on
            equal_null(fact_goods_movement.warehouse_number_key, dim_warehouse.warehouse_number_key)
        left join
            dim_warehouse_movement_type
        on
            equal_null(fact_goods_movement.warehouse_movement_type_key, dim_warehouse_movement_type.warehouse_movement_type_key)
        left join
            dim_storage_type
        on
            equal_null(fact_goods_movement.storage_type_key, dim_storage_type.storage_type_key)
        left join
            dim_storage_bins
        on
            equal_null(fact_goods_movement.storage_bin_key, dim_storage_bins.storage_bin_key)
        left join
            dim_movement_reason
        on
            equal_null(fact_goods_movement.movement_reason_key, dim_movement_reason.movement_reason_key)
        left join
            dim_profit_center
        on
            equal_null(dim_plant.profit_center_key, dim_profit_center.profit_center_key)
        left join
            dim_gl_account
        on
            equal_null(fact_goods_movement.gl_account_key, dim_gl_account.gl_account_key)
        left join
            dim_movement_type_evaluation
        on
            equal_null(dim_movement_type_evaluation.mvnt_type_eval_key, fact_goods_movement.mvnt_type_eval_key)
        left join
            dim_movement_type_quantity_posting
        on
            equal_null(dim_movement_type_quantity_posting.quantity_posting_string , fact_goods_movement.post_str_qty)
            and equal_null(coalesce(dim_movement_type_quantity_posting.stock_type_modification, '') , stock_type_mod_join)
            and equal_null(dim_movement_type_quantity_posting.consecutive_counter , auto_item_join)             
        left join
            dim_stock_type
        on
            equal_null(coalesce(fact_goods_movement.special_stock_ind, ''), coalesce(dim_stock_type.special_stock_ind_join, ''))
            and equal_null(movement_type_for_goods_issue_join, dim_stock_type.stock_movement_type)
        left join
            dim_currency_conversion
        on 
            equal_null(fact_goods_movement.posting_date_mkpf, dim_currency_conversion.conversion_date)
            and equal_null(dim_profit_center.currency, dim_currency_conversion.currency_from)
            and equal_null(dim_currency_conversion.currency_to, 'USD')

    )
    
select
    material_doc_num,
    material_doc_year,
    doc_item,
    posting_date,
    profit_ctr,
    short_text,
    company,
    company_text,
    country,
    country_text,
    business_unit,
    business_unit_text,
    division,
    division_text,
    region,
    region_text,
    district,
    district_text,
    user_name,
    reference_doc_number,
    reference_doc_number_mkpf,
    quantity,
    quantity_adjusted,
    amount_lc,
    amount_lc_adjusted,
    -- Currency conversion details
    {{ currency_template("amount_lc_usd") }},
    {{ currency_template("amount_lc_adjusted_usd") }},
    debit_credit_ind_adjusted as debit_credit_ind,
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
    currency_code_name,
    debit_credit_reval_ind,
    valuation_type,
    transaction_quantity,
    transaction_quantity_adjusted as transaction_quantity_adjusted,
    qty_price_unit,
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
    old_project_num,
    fiscal_year,
    allow_backpost,
    company_code,
    res_num,
    res_item_num,
    trans_event_stat,
    recv_issue_batch,
    transfer_batch_type,
    move_ind,
    consump_posting,
    receipt_ind,
    transfer_req_num,
    transfer_req_item,
    whs_posting_ind,
    src_interim_posting,
    dest_interim_posting,
    dyn_storage_bin,
    gr_gi_slips_num,
    order_item,
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
    trans_event_type_mkpf,
    posting_date_mkpf,
    entry_date_mkpf,
    entry_time_mkpf,
    user_name_mkpf,
    trans_code_mkpf,
    delivery_num,
    delivery_item,
    sap_material_number,
    mat_type,
    mat_type_desc,
    mat_group,
    mat_group_desc,
    part_number,
    product_description,
    c1_core,
    c2_core,
    core_or_reman,
    core_group_id,
    core,
    prodh1,
    prodh1_text,
    prodh2,
    prodh2_text,
    prodh3,
    prodh3_text,
    part_vendor_code,
    part_vendor_text,
    recv_issue_mat,
    storage_location,
    storage_location_description,
    storage_location_type,
    storage_location_type_description,
    movement_type,
    movement_type_text,
    entry_unit,
    base_unit,
    order_price_unit,
    plant,
    plant_name,
    recv_issue_plant,
    transaction_event_type,
    transaction_event_type_text,
    accounting_doc_type,
    accounting_doc_type_text,
    revaluation_doc_type,
    revaluation_doc_type_text,
    vendor,
    customer_number,
    cost_center,
    coorder_number,
    warehouse_number,
    warehouse_number_description,
    warehouse_movement_type,
    storage_type,
    storage_type_name,
    storage_bin,
    movement_reason,
    movement_reason_text,
    gl_account,
    stock_type_trn as stock_type,
    stock_category,
    stock_type_description,
    process_key,
    special_stock_ind, 
    stock_relev, 
    edw_load_timestamp,
    current_timestamp as dbt_run_timestamp
from dm_goods_movement