--------------------------------------------------------
--  DDL for View XXFT_RPRO_OE_ORDER_DETAILS_V
--------------------------------------------------------
/* +===================================================================+
-- |                         Fortinet, Inc.
-- |                         Sunnyvale, CA
-- +===================================================================
-- |
-- |Object Name     : XXFT_RPRO_OE_ORDER_DETAILS_V.sql
-- |
-- |Description      : View to get the sales order line details.
-- |                   This is used by Revpro Integration module to get the SO details.
__ |
-- |
-- |Change Record:
-- |===============
-- |Version   Date        Author             Remarks
-- |=======   =========== ==============     ============================
-- |1.0       12-FEB-2016 (NTT)              Initial code version
-- |1.1       10-MAY-2016 (NTT)              Modified to include the condition to get attribute1 as service ref_line Id.
-- |1.2       16-MAY-2016 (NTT)              Modified to fetch FOB_POINT_CODE for shipping terms per defect #2703.
-- |1.3       14-JUN-2016 (NTT)              Modifed to provide N for Unbilled Accounting Flag for Professional Service Items. #3292.
-- |1.4       14-JUN-2016 (NTT)              Modified the embedded support deriviation for Co-TERM skus'.#3306.
-- |1.5       22-JUN-2016 (NTT)              Modified the get_list_price function to pass line id as parameter. Defect #3367
-- |1.6       23-JUN-2016 (NTT)              Modified to send the attribute28 as Y in case of Coterm with Quote.#3367
-- |1.7       16-AUG-2016 (NTT)              Modified the derivation of pcs_flag from product category instead of Product line #528353.
-- |1.8       13-SEP-2016 (NTT)              Modified to include the start date and end date.ITS#553115
-- |1.9       26-OCT-2016 (NTT)              Modified to retreive the attribute47 based on the get_order_classification.ITS#575976
-- +===================================================================+*/
CREATE OR REPLACE FORCE VIEW "APPS"."XXFT_RPRO_OE_ORDER_DETAILS_V" (
  "TRAN_TYPE"
  , "ITEM_ID"
  , "ITEM_NUMBER"
  , "ITEM_DESC"
  , "PRODUCT_FAMILY"
  , "PRODUCT_CATEGORY"
  , "PRODUCT_LINE"
  , "PRODUCT_CLASS"
  , "PRICE_LIST_NAME"
  , "UNIT_LIST_PRICE"
  , "UNIT_SELL_PRICE"
  , "EXT_SELL_PRICE"
  , "EXT_LIST_PRICE"
  , "REC_AMT"
  , "DEF_AMT"
  , "COST_AMOUNT"
  , "COST_REC_AMT"
  , "COST_DEF_AMT"
  , "TRANS_CURR_CODE"
  , "EX_RATE"
  , "BASE_CURR_CODE"
  , "COST_CURR_CODE"
  , "COST_EX_RATE"
  , "RCURR_EX_RATE"
  , "ACCOUNTING_PERIOD"
  , "ACCOUNTING_RULE"
  , "RULE_START_DATE"
  , "RULE_END_DATE"
  , "VAR_RULE_ID"
  , "PO_NUM"
  , "QUOTE_NUM"
  , "SALES_ORDER"
  , "SALES_ORDER_LINE"
  , "SALES_ORDER_ID"
  , "SALES_ORDER_LINE_ID"
  , "SHIP_DATE"
  , "SO_BOOK_DATE"
  , "TRANS_DATE"
  , "SCHEDULE_SHIP_DATE"
  , "QUANTITY_SHIPPED"
  , "QUANTITY_ORDERED"
  , "QUANTITY_CANCELED"
  , "SALESREP_NAME"
  , "SALES_REP_ID"
  , "ORDER_TYPE"
  , "ORDER_LINE_TYPE"
  , "SERVICE_REFERENCE_LINE_ID"
  , "CUSTOMER_ID"
  , "CUSTOMER_NAME"
  , "CUSTOMER_CLASS"
  , "BILL_TO_ID"
  , "BILL_TO_CUSTOMER_NAME"
  , "BILL_TO_CUSTOMER_NUMBER"
  , "BILL_TO_COUNTRY"
  ,"SHIP_TO_ID"
  , "SHIP_TO_CUSTOMER_NAME"
  , "SHIP_TO_CUSTOMER_NUMBER"
  , "SHIP_TO_COUNTRY"
  , "BUSINESS_UNIT"
  , "ORG_ID"
  , "SOB_ID"
  , "SEC_ATTR_VALUE"
  , "RETURN_FLAG"
  , "CANCELLED_FLAG"
  , "FLAG_97_2"
  , "PCS_FLAG"
  , "UNDELIVERED_FLAG"
  , "STATED_FLAG"
  , "ELIGIBLE_FOR_CV"
  , "ELIGIBLE_FOR_FV"
  , "DEFERRED_REVENUE_FLAG"
  , "NON_CONTINGENT_FLAG"
  , "UNBILLED_ACCOUNTING_FLAG"
  , "DEAL_ID"
  , "LAG_DAYS"
  , "ATTRIBUTE1"
  , "ATTRIBUTE2"
  , "ATTRIBUTE3"
  , "ATTRIBUTE4"
  , "ATTRIBUTE5"
  , "ATTRIBUTE6"
  , "ATTRIBUTE7"
  , "ATTRIBUTE8"
  , "ATTRIBUTE9"
  , "ATTRIBUTE10"
  , "ATTRIBUTE11"
  , "ATTRIBUTE12"
  , "ATTRIBUTE13"
  , "ATTRIBUTE14"
  , "ATTRIBUTE15"
  , "ATTRIBUTE16"
  , "ATTRIBUTE17"
  , "ATTRIBUTE18"
  , "ATTRIBUTE19"
  , "ATTRIBUTE20"
  , "ATTRIBUTE21"
  , "ATTRIBUTE22"
  , "ATTRIBUTE23"
  , "ATTRIBUTE24"
  , "ATTRIBUTE25"
  , "ATTRIBUTE26"
  , "ATTRIBUTE27"
  , "ATTRIBUTE28"
  , "ATTRIBUTE29"
  , "ATTRIBUTE30"
  , "ATTRIBUTE31"
  , "ATTRIBUTE32"
  , "ATTRIBUTE33"
  , "ATTRIBUTE34"
  , "ATTRIBUTE35"
  , "ATTRIBUTE36"
  , "ATTRIBUTE37"
  , "ATTRIBUTE38"
  , "ATTRIBUTE39"
  , "ATTRIBUTE40"
  , "ATTRIBUTE41"
  , "ATTRIBUTE42"
  , "ATTRIBUTE43"
  , "ATTRIBUTE44"
  , "ATTRIBUTE45"
  , "ATTRIBUTE46"
  , "ATTRIBUTE47"
  , "ATTRIBUTE48"
  , "ATTRIBUTE49"
  , "ATTRIBUTE50"
  , "ATTRIBUTE51"
  , "ATTRIBUTE52"
  , "ATTRIBUTE53"
  , "ATTRIBUTE54"
  , "ATTRIBUTE55"
  , "ATTRIBUTE56"
  , "ATTRIBUTE57"
  , "ATTRIBUTE58"
  , "ATTRIBUTE59"
  , "ATTRIBUTE60"
  , "DATE1"
  , "DATE2"
  , "DATE3"
  , "DATE4"
  , "DATE5"
  , "NUMBER1"
  , "NUMBER2"
  , "NUMBER3"
  , "NUMBER4"
  , "NUMBER5"
  , "NUMBER6"
  , "NUMBER7"
  , "NUMBER8"
  , "NUMBER9"
  , "NUMBER10"
  , "NUMBER11"
  , "NUMBER12"
  , "NUMBER13"
  , "NUMBER14"
  , "NUMBER15"
  , "REV_ACCTG_SEG1"
  , "REV_ACCTG_SEG2", "REV_ACCTG_SEG3", "REV_ACCTG_SEG4", "REV_ACCTG_SEG5", "REV_ACCTG_SEG6", "REV_ACCTG_SEG7", "REV_ACCTG_SEG8", "REV_ACCTG_SEG9", "REV_ACCTG_SEG10", "DEF_ACCTG_SEG1", "DEF_ACCTG_SEG2", "DEF_ACCTG_SEG3", "DEF_ACCTG_SEG4", "DEF_ACCTG_SEG5", "DEF_ACCTG_SEG6", "DEF_ACCTG_SEG7", "DEF_ACCTG_SEG8", "DEF_ACCTG_SEG9", "DEF_ACCTG_SEG10", "COGS_R_SEG1", "COGS_R_SEG2", "COGS_R_SEG3", "COGS_R_SEG4", "COGS_R_SEG5", "COGS_R_SEG6", "COGS_R_SEG7", "COGS_R_SEG8", "COGS_R_SEG9",
  "COGS_R_SEG10", "COGS_D_SEG1", "COGS_D_SEG2", "COGS_D_SEG3", "COGS_D_SEG4", "COGS_D_SEG5", "COGS_D_SEG6", "COGS_D_SEG7", "COGS_D_SEG8", "COGS_D_SEG9", "COGS_D_SEG10", "LT_DEFERRED_ACCOUNT", "LT_DCOGS_ACCOUNT", "BOOK_ID", "BNDL_CONFIG_ID", "SO_LAST_UPDATE_DATE", "SO_LINE_CREATION_DATE", "ORG_NAME", "SERVICE_DURATION", "BILL_TO_SITE_USE_ID", "BILL_TO_ADDRESS_ID", "BILL_TO_ADDRESS1", "BILL_TO_ADDRESS2", "BILL_TO_CITY", "BILL_TO_STATE", "BILL_TO_POSTAL_CODE", "SHIP_TO_SITE_USE_ID", "SHIP_TO_ADDRESS_ID", "SHIP_TO_ADDRESS1", "SHIP_TO_ADDRESS2", "SHIP_TO_CITY", "SHIP_TO_STATE", "SHIP_TO_POSTAL_CODE", "SOLD_TO_CUSTOMER_NUMBER", "SHIP_FROM_ORG_ID", "SET_OF_BOOKS_NAME", "UOM_CODE", "HEADER_TYPE_ID", "LINE_TYPE_ID", "END_CUSTOMER_NUMBER", "END_CUSTOMER_NAME", "SHIPPING_TERMS", "SALES_CHANNEL", "FOB_POINT", "ACTUAL_SHIPMENT_DATE", "ACTUAL_DELIVERY_DATE", "FULFILLMENT_DATE", "ACTUAL_FULFILLMENT_DATE")
AS
  SELECT
    /*+ PARALLEL(oel,DEFAULT) */
    'SO' tran_type,
    TO_CHAR (oel.inventory_item_id) item_id,
    msi.segment1 item_number,
    msi.description item_desc,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_product_family(msi.inventory_item_id,msi.organization_id) product_family,    --micv.segment2 product_family,    --     NULL product_family,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_product_category(msi.inventory_item_id,msi.organization_id) product_category,--micv.segment1 product_category,  --     NULL product_category,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_product_line(msi.inventory_item_id,msi.organization_id) product_line,        --micv.segment2 product_line,               --     micv.segment3 product_line,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_product_class(msi.inventory_item_id,msi.organization_id) product_class,      --micv.segment2 product_class,     --     null PRODUCT_CLASS,
    qh.name PRICE_LIST_NAME,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_list_price(oel.line_id,NVL(oel.price_list_id,oeh.price_list_id),oel.inventory_item_id,oel.pricing_date), -- ROUND (oel.unit_list_price, 2) unit_list_price,  -- Bala  --ITS#544414
    ROUND (oel.unit_selling_price, 2) unit_sell_price,
    ROUND ( (oel.unit_selling_price ), 2) ext_sell_price,
    ROUND ( (oel.unit_list_price ), 2) ext_list_price,
    NULL rec_amt,
    NULL def_amt, --for Cancelled lines the def_amt should be 0
    ROUND ((XXFT_RPRO_DATA_EXTRACT_PKG.GET_COST_AMOUNT(oel.inventory_item_id,oel.line_id,oel.ship_from_org_id,'Y',oel.ato_line_id)/ordered_quantity),2) cost_amount,
    NULL cost_rec_amt,
    NULL cost_def_amt,
    oeh.transactional_curr_code trans_curr_code,
    XXFT_RPRO_DATA_EXTRACT_PKG.GET_CONVERSION_RATE(oeh.transactional_curr_code,sob.currency_code,'FUNCTIONAL',TRUNC (oeh.booked_date)) ex_rate,
    sob.currency_code base_curr_code,
    NULL cost_curr_code,
    NULL cost_ex_rate,
    XXFT_RPRO_DATA_EXTRACT_PKG.GET_CONVERSION_RATE(sob.currency_code,NULL,'REPORTING',TRUNC (oeh.booked_date)) rcurr_ex_rate,
    NULL accounting_period,
    NULL accounting_rule,
    CASE
      WHEN oel.item_type_code     ='INCLUDED'
      AND oel.ORDER_QUANTITY_UOM <> 'EA'
      THEN NVL(oel.service_start_date,NVL(oel.attribute2,XXFT_RPRO_DATA_EXTRACT_PKG.get_line_contract_date(oel.line_id,'START'))) /* ITS#553115*/
      ELSE oel.service_start_date
    END rule_start_date,
    CASE
      WHEN oel.item_type_code     ='INCLUDED'
      AND oel.ORDER_QUANTITY_UOM <> 'EA'
      THEN NVL(oel.service_end_date,NVL(oel.attribute19,XXFT_RPRO_DATA_EXTRACT_PKG.get_line_contract_date(oel.line_id,'END')))  /* ITS#553115*/      
      ELSE oel.service_end_date
    END rule_end_date,
    --oel.service_start_date rule_start_date,
    /* CASE
    WHEN UPPER (oel.service_period) = 'YR'
    AND oel.service_start_date     IS NOT NULL
    THEN ADD_MONTHS (Oel.Service_Start_Date - 1, Oel.Service_Duration * 12)
    WHEN UPPER (oel.service_period) = 'MTH'
    AND service_start_date         IS NOT NULL
    THEN ADD_MONTHS (Oel.Service_Start_Date - 1, Oel.Service_Duration)
    ELSE Oel.Service_End_Date
    END rule_end_date, */
    NULL var_rule_id,
    oeh.cust_po_number po_num,
    NULL quote_num,
    TO_CHAR (oeh.order_number) sales_order,
    TO_CHAR (oel.line_number) sales_order_line,
    TO_CHAR (oeh.header_id) sales_order_id,
    TO_CHAR (oel.line_id) sales_order_line_id,
    NVL(oel.actual_shipment_date,NVL(oel.actual_fulfillment_date, oel.fulfillment_date)) ship_date,
    TRUNC (oeh.booked_date) so_book_date,
    TRUNC (oeh.booked_date) trans_date,
    oel.schedule_ship_date,
    OEL.SHIPPED_QUANTITY quantity_shipped,
    oel.ordered_quantity QUANTITY_ORDERED,
    oel.cancelled_quantity QUANTITY_CANCELLED,
    (SELECT jrs.name
    FROM apps.JTF_RS_SALESREPS jrs
    WHERE jrs.salesrep_id=oeh.salesrep_id
    AND jrs.org_id       =oeh.org_id
    ) SALESREP_NAME,
    oeh.salesrep_id sales_rep_id,
    (SELECT DISTINCT name
    FROM apps.oe_transaction_types_tl
    WHERE transaction_type_id = oeh.order_type_id
    AND language              ='US'
    ) Order_Type,
    ottl.name order_line_type,
    NVL(oel.service_reference_line_id,oel.attribute1) service_reference_line_id,
    oeh.sold_to_org_id customer_id,
    hp_sold.party_name customer_name,
    bill_cust.CUSTOMER_CLASS_CODE customer_class,
    TO_CHAR (bill_cust.cust_account_id) bill_to_id,
    bill_cust.party_name bill_to_customer_name,
    bill_cust.customer_number bill_to_customer_number,
    bill_cust.country bill_to_country,
    TO_CHAR (ship_cust.cust_account_id) ship_to_id,
    ship_cust.party_name ship_to_customer_name,
    ship_cust.customer_number ship_to_customer_number,
    ship_cust.country ship_to_country,
    (SELECT name
    FROM apps.HR_ALL_ORGANIZATION_UNITS_TL
    WHERE organization_id = oel.org_id
    ) business_unit,
    oel.org_id org_id,
    TO_CHAR (hou.set_of_books_id) sob_id,
    NULL sec_attr_value,
    DECODE (oel.flow_status_code, 'CANCELLED', 'Y', 'N') return_flag,
    DECODE (oel.flow_status_code, 'CANCELLED', 'Y', 'N') cancelled_flag,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_flag_97_2(msi.inventory_item_id,msi.organization_id) flag_97_2,
    /*DECODE(XXFT_RPRO_DATA_EXTRACT_PKG.get_product_class(msi.inventory_item_id,msi.organization_id),'SUPPORT','Y','SUBSCRIPTION','Y','N') pcs_flag,   ITS Ticket #528353*/
    DECODE(XXFT_RPRO_DATA_EXTRACT_PKG.get_product_category(msi.inventory_item_id,msi.organization_id),'SUPPORT','Y','SUBSCRIPTION','Y','SERVICE BUNDLE','Y','N') pcs_flag,
    --DECODE(XXFT_RPRO_DATA_EXTRACT_PKG.get_product_class(msi.inventory_item_id,msi.organization_id),'SUPPORT','Y','SOFTWARE','Y','SUBSCRIPTION','Y','PROFESSIONAL SERVICES','Y','N') undelivered_flag,
    --DECODE(XXFT_RPRO_DATA_EXTRACT_PKG.get_product_class(msi.inventory_item_id,msi.organization_id),'SUPPORT','Y','SOFTWARE','Y','PROFESSIONAL SERVICES','Y','N') undelivered_flag,
    --DECODE(XXFT_RPRO_DATA_EXTRACT_PKG.get_product_line(msi.inventory_item_id,msi.organization_id),'REGISTERABLE PROFESSIONAL SERVICES','Y','SOW PROFESSIONAL SERVICES','Y','TIME BASED SOFTWARE (REG)','Y','TIME BASED SOFTWARE (NONREG)','Y','HARDWARE PCS','Y','SOFTWARE PCS','Y','TRAINING','Y','N') undelivered_flag,
    DECODE(XXFT_RPRO_DATA_EXTRACT_PKG.get_product_line(msi.inventory_item_id,msi.organization_id),'REGISTERABLE PROFESSIONAL SERVICES','Y','SOW PROFESSIONAL SERVICES','Y','TIME BASED SOFTWARE (REG)','Y','TIME BASED SOFTWARE (NONREG)','Y','HARDWARE PCS','Y','SOFTWARE PCS','Y','TRAINING','Y','N') undelivered_flag,
    NULL stated_flag,
    'Y' eligible_for_cv,
    'Y' eligible_for_fv,
    'Y' deferred_revenue_flag,
    'Y' NON_CONTINGENT_FLAG,
    --DECODE(XXFT_RPRO_DATA_EXTRACT_PKG.get_product_family(msi.inventory_item_id,msi.organization_id),'PROFESSIONAL SERVICES','Y','N') unbilled_accounting_flag,
    'N' unbilled_accounting_flag, -- Modifie for the Defect #3292.
    NULL deal_id,
    NULL lag_days,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_region(oel.line_id) ATTRIBUTE1,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_bill_to_territory(oel.line_id) ATTRIBUTE2, --null ATTRIBUTE2,                             --interface_line_context
    XXFT_RPRO_DATA_EXTRACT_PKG.get_bill_to_geo(oel.line_id) ATTRIBUTE3,       --NULL ATTRIBUTE3,                             --CONTRACT_MODIFIER
    XXFT_RPRO_DATA_EXTRACT_PKG.get_bill_to_state(oel.line_id) ATTRIBUTE4,     --NULL ATTRIBUTE4,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_ship_to_territory(oel.line_id) ATTRIBUTE5, --NULL ATTRIBUTE5,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_ship_to_geo(oel.line_id) ATTRIBUTE6,       --NULL ATTRIBUTE6,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_ship_to_state(oel.line_id) ATTRIBUTE7,     --NULL ATTRIBUTE7,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_end_cust_name(oel.header_id) ATTRIBUTE8,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_end_cust_number(oel.header_id) ATTRIBUTE9,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_end_cust_state(oel.header_id) ATTRIBUTE10,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_end_cust_zip(oel.header_id) ATTRIBUTE11,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_other_element_type(msi.inventory_item_id,msi.organization_id) ATTRIBUTE12,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_soho_type(msi.inventory_item_id,msi.organization_id) ATTRIBUTE13,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_report_family(msi.inventory_item_id,msi.organization_id) ATTRIBUTE14, --Reporting family
    XXFT_RPRO_DATA_EXTRACT_PKG.get_product_group(msi.inventory_item_id,msi.organization_id) ATTRIBUTE15, --micv.segment3
    -- oeh.freight_terms_code ATTRIBUTE16, -- commented on 5/16/2016 per defect 2703
    oeh.fob_point_code ATTRIBUTE16,
    oeh.flow_status_code ATTRIBUTE17,
    NULL ATTRIBUTE18,
    NULL ATTRIBUTE19,
    NULL ATTRIBUTE20,
    NULL ATTRIBUTE21,
    NULL ATTRIBUTE22,
    NULL ATTRIBUTE23,
    CASE
      WHEN oel.top_model_line_id = oel.line_id
      THEN TO_CHAR(1)
      ELSE REPLACE(oel.sort_order,'0','')
    END ATTRIBUTE24,
    NULL ATTRIBUTE25,
    NULL ATTRIBUTE26,
    NULL ATTRIBUTE27,
    (SELECT CASE WHEN COUNT(1) > 0 THEN 'Y' ELSE 'N' END       
       FROM oe_order_sources os
      WHERE 1=1 
        and os.order_source_id   = oel.order_source_id
        AND name                = 'Co-Term with Quote') ATTRIBUTE28, /* Defect # 3367 */
    NULL ATTRIBUTE29,
    NULL ATTRIBUTE30,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_opportunity(oel.header_id,oel.line_id) ATTRIBUTE31,
    NULL ATTRIBUTE32,
    NULL ATTRIBUTE33,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_spr_number(oel.header_id,oel.line_id) ATTRIBUTE34,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_customer_flag(oel.line_id) ATTRIBUTE35,
    NULL ATTRIBUTE36,
    (
    CASE
      WHEN UPPER (hca_sold.sales_channel_code) = 'CHANNEL_PARTNER'
      THEN 'P'
      ELSE 'N'
    END) ATTRIBUTE37,
    NULL ATTRIBUTE38,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_customer_type(hca_sold.cust_account_id),
    /*--hp_sold.category_code ATTRIBUTE39,*/
    NULL ATTRIBUTE40,
    oel.link_to_line_id ATTRIBUTE41,
    NULL ATTRIBUTE42,
    NULL ATTRIBUTE43,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_embedded_service(oel.service_reference_type_code,oel.line_id,NVL(oel.service_reference_line_id,oel.attribute1), msi.inventory_item_id,msi.organization_id) ATTRIBUTE44,
    /*Defect#3306*/
    /*--XXFT_RPRO_DATA_EXTRACT_PKG.get_embedded_service(oel.servi,nvl(oel.service_reference_line_id,oel.attribute1), msi.inventory_item_id,msi.organization_id) ATTRIBUTE44,
    --XXFT_RPRO_DATA_EXTRACT_PKG.get_embedded_service(oel.service_reference_line_id,XXFT_RPRO_DATA_EXTRACT_PKG.get_product_category(msi.inventory_item_id,msi.organization_id)) ATTRIBUTE44, */
    NULL ATTRIBUTE45,
    NULL ATTRIBUTE46,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_order_classification(oel.line_id) ATTRIBUTE47,
    NULL ATTRIBUTE48,
    NULL ATTRIBUTE49,
    NULL ATTRIBUTE50,
    /* ATTRIBUTE51 TO 60 REPORTING COLUMNS */
    NULL ATTRIBUTE51,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_line_source(oel.line_id) ATTRIBUTE52,
    NULL ATTRIBUTE53,
    NULL ATTRIBUTE54,
    NULL ATTRIBUTE55,
    NULL ATTRIBUTE56,
    NULL ATTRIBUTE57,
    NULL ATTRIBUTE58,
    NULL ATTRIBUTE59,
    NULL ATTRIBUTE60,
    NULL DATE1,
    /*-- so_last_update_date */
    NULL DATE2,
    /*-- so_line_creation_date */
    NULL DATE3,
    /*-- last_update_date specific to inv*/
    NULL DATE4,
    NULL DATE5,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_no_days(oel.line_id) NUMBER1,
    /*--quantity_cancelled */
    oel.line_id NUMBER2,
    NULL NUMBER3,
    /*--DECODE(XXFT_RPRO_DATA_EXTRACT_PKG.get_product_category(msi.inventory_item_id,msi.organization_id),'SUPPORT',XXFT_RPRO_DATA_EXTRACT_PKG.get_grace_period(oel.line_id),'SUBSCRIPTION',XXFT_RPRO_DATA_EXTRACT_PKG.get_grace_period(oel.line_id),0) NUMBER3,*/
    (SELECT tag
    FROM apps.fnd_lookup_values
    WHERE lookup_type ='FTNT_REVPRO_CASH_PRIORITY'
    AND lookup_code   =(XXFT_RPRO_DATA_EXTRACT_PKG.get_product_class(msi.inventory_item_id,msi.organization_id))
    ) NUMBER4,
    NULL NUMBER5,
    NULL NUMBER6,
    NULL NUMBER7,
    NULL NUMBER8,
    NULL NUMBER9,
    NULL NUMBER10,
    NULL NUMBER11,
    NULL NUMBER12,
    NULL NUMBER13,
    NULL NUMBER14,
    NULL NUMBER15,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_ac_segment1('REV',oel.line_id) rev_acctg_seg1,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_ac_segment2('REV',oel.line_id) rev_acctg_seg2,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_ac_segment3('REV',oel.line_id) rev_acctg_seg3,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_ac_segment4('REV',oel.line_id) rev_acctg_seg4,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_ac_segment5('REV',oel.line_id) rev_acctg_seg5,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_ac_segment6('REV',oel.line_id) rev_acctg_seg6,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_ac_segment7('REV',oel.line_id) rev_acctg_seg7,
    NULL rev_acctg_seg8,
    NULL rev_acctg_seg9,
    NULL rev_acctg_seg10,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_ac_segment1('UNEARN',oel.line_id) def_acctg_seg1,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_ac_segment2('UNEARN',oel.line_id) def_acctg_seg2,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_ac_segment3('UNEARN',oel.line_id) def_acctg_seg3,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_ac_segment4('UNEARN',oel.line_id) def_acctg_seg4,
    --XXFT_RPRO_DATA_EXTRACT_PKG.get_ac_segment5('UNEARN',oel.line_id) def_acctg_seg5,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_def_account(XXFT_RPRO_DATA_EXTRACT_PKG.get_product_category(msi.inventory_item_id,msi.organization_id)) def_acctg_seg5,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_ac_segment6('UNEARN',oel.line_id) def_acctg_seg6,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_ac_segment7('UNEARN',oel.line_id) def_acctg_seg7,
    NULL def_acctg_seg8,
    NULL def_acctg_seg9,
    NULL def_acctg_seg10,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_ac_segment1('COGS',oel.line_id) COGS_R_SEG1,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_ac_segment2('COGS',oel.line_id) COGS_R_SEG2,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_ac_segment3('COGS',oel.line_id) COGS_R_SEG3,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_ac_segment4('COGS',oel.line_id) COGS_R_SEG4,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_ac_segment5('COGS',oel.line_id) COGS_R_SEG5,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_ac_segment6('COGS',oel.line_id) COGS_R_SEG6,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_ac_segment7('COGS',oel.line_id) COGS_R_SEG7,
    NULL COGS_R_SEG8,
    NULL COGS_R_SEG9,
    NULL COGS_R_SEG10,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_ac_segment1('DEF',oel.line_id) COGS_D_SEG1,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_ac_segment2('DEF',oel.line_id) COGS_D_SEG2,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_ac_segment3('DEF',oel.line_id) COGS_D_SEG3,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_ac_segment4('DEF',oel.line_id) COGS_D_SEG4,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_ac_segment5('DEF',oel.line_id) COGS_D_SEG5,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_ac_segment6('DEF',oel.line_id) COGS_D_SEG6,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_ac_segment7('DEF',oel.line_id) COGS_D_SEG7,
    NULL COGS_D_SEG8,
    NULL COGS_D_SEG9,
    NULL COGS_D_SEG10,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_lt_deff_rev,
    /*-- LT_DEFERRED_ACCOUNT, */
    XXFT_RPRO_DATA_EXTRACT_PKG.get_lt_deff_cogs,
    /*--LT_DCOGS_ACCOUNT, */
    NULL BOOK_ID,
    NULL BNDL_CONFIG_ID,
    oel.last_update_date so_last_update_date,
    /*--this column is mapped to date1 as well to get the value in rpro_arr_transactions */
    oel.creation_date so_line_creation_date,
    /*--this column is mapped to date2 as well to get the value in rpro_arr_transactions */
    /*Begin Client specific columns if required map it to attribute or date or number columns*/
    hou.name org_name,
    oel.service_duration service_duration,
    oeh.invoice_to_org_id bill_to_site_use_id,
    bill_cust.cust_acct_site_id bill_to_address_id,
    bill_cust.address1 bill_to_address1,
    bill_cust.address2 bill_to_address2,
    bill_cust.city bill_to_city,
    bill_cust.state bill_to_state,
    bill_cust.postal_code bill_to_postal_code,
    NVL (oel.ship_to_org_id, oeh.ship_to_org_id) ship_to_site_use_id,
    ship_cust.cust_acct_site_id ship_to_address_id,
    ship_cust.address1 ship_to_address1,
    ship_cust.address2 ship_to_address2,
    ship_cust.city ship_to_city,
    ship_cust.state ship_to_state,
    ship_cust.postal_code ship_to_postal_code,
    hca_sold.account_number sold_to_customer_number,
    oel.ship_from_org_id ship_from_org_id,
    sob.name set_of_books_name,
    oel.order_quantity_uom uom_code,
    oeh.order_type_id header_type_id,
    oel.line_type_id line_type_id,
    NULL END_CUST_NUMBER,
    NULL END_CUSTOMER_NAME,
    /*-- oeh.freight_terms_code shipping_terms, -- commented on 5/16 per defect #2703 */
    oeh.fob_point_code shipping_terms,
    /*-- per defect #2703. */
    oeh.sales_channel_code sales_channel,
    oel.fob_point_code fob_point,
    oel.actual_shipment_date,
    oel.actual_arrival_date actual_delivery_date,
    oel.fulfillment_date,
    oel.actual_fulfillment_date
    /*End Client specific columns if required map it to attribute or date or number columns*/
  FROM apps.oe_order_headers_all oeh,
    apps.oe_order_lines_all oel,
    apps.oe_transaction_types_all ott,
    apps.oe_transaction_types_tl ottl,
    apps.hr_operating_units hou,
    apps.gl_sets_of_books sob,
    apps.mtl_system_items_b msi,
    xxft_rpro_customers_v ship_cust,
    xxft_rpro_customers_v bill_cust,
    apps.hz_cust_accounts hca_sold,
    apps.hz_parties hp_sold,
    apps.qp_list_headers_all qh,
    apps.mtl_parameters mp
    -- apps.MTL_ITEM_CATEGORIES_V micv
  WHERE 1                       = 1
  AND oeh.booked_flag           = 'Y'
  AND oeh.header_id             = oel.header_id
  AND oel.line_type_id          = ott.transaction_type_id
  AND oel.org_id                = ott.org_id
  AND ott.order_category_code  <> 'RETURN'
  AND ott.transaction_type_id   = ottl.transaction_type_id
  AND ottl.LANGUAGE             = 'US'
  AND oeh.org_id                = hou.organization_id
  AND oel.org_id                = hou.organization_id
  AND hou.set_of_books_id       = sob.set_of_books_id
  AND oel.inventory_item_id     = msi.inventory_item_id(+)
  AND mp.master_organization_id = mp.organization_id
    /*-- AND oel.ship_from_org_id     = msi.organization_id */
  AND msi.organization_id     = mp.organization_id
  AND ship_cust.site_use_id   = NVL (oel.ship_to_org_id, oeh.ship_to_org_id)
  AND ship_cust.site_use_code = 'SHIP_TO'
  AND bill_cust.site_use_id   = oeh.invoice_to_org_id
  AND bill_cust.site_use_code = 'BILL_TO'
  AND oeh.sold_to_org_id      = hca_sold.cust_account_id
  AND hca_sold.party_id       = hp_sold.party_id
  AND qh.list_header_id(+)    = oel.price_list_id;
  /
  show errors;