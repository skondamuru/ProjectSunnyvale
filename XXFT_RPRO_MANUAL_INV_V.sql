--------------------------------------------------------
--  DDL for View XXFT_RPRO_MANUAL_INV_V
--------------------------------------------------------
/* +===================================================================+
-- |                         Fortinet, Inc.
-- |                         Sunnyvale, CA
-- +===================================================================
-- |
-- |Object Name     : XXFT_RPRO_MANUAL_INV_V.sql
-- |
-- |Description      : View to extract Manual Invoices, Credit Memos, 
-- |                   Historical SPR Credit Memos.
-- |                   
__ |
-- |
-- |Change Record:
-- |===============
-- |Version   Date        Author             Remarks
-- |=======   =========== ==============     ============================
-- |1.0       12-FEB-2016 (NTT)              Initial code version
-- |1.3       17-JUN-2016 (NTT)              Added the Sales Order as Credit Memo Number 
-- |                                         for manual Credit Memos.
-- |1.4       04-JUL-2016 (NTT)              Modified to provide the ext list price as quantity credited multiplied by unit standard price. Defect#3396.
-- |1.5       13-JUL-2016 (NTT)              Pulled the Unit List Price from ozf_sales_transactions_all.attribute10 for the defect #3487.
-- |1.6       10-Aug-2016 (NTT)              Modified to add the list price,FLAG_97_2 and Unit List Price derivation as per the Ticket# 521951
-- |1.7       09-Sep-2016 (NTT)              AR Transaction types condition added ITS#543505
-- |1.8       07-OCT-2016 (NTT)              Modified for the ticket ITS#576718
-- +===================================================================+*/
--------------------------------------------------------
--  DDL for View XXFT_RPRO_MANUAL_INV_V
--------------------------------------------------------
CREATE OR REPLACE FORCE VIEW "APPS"."XXFT_RPRO_MANUAL_INV_V" ("INVOICE_TYPE", "INVOICE_NAME", "INVOICE_NUMBER", "INVOICE_ID", "INVOICE_LINE_ID", "INVOICE_DATE", "INVOICE_LINE", "ITEM_ID", "ITEM_NUMBER", "ITEM_DESC", "PRODUCT_FAMILY", "PRODUCT_CATEGORY", "PRODUCT_LINE", "PRODUCT_CLASS", "PRODUCT_GROUP", "ELEMENT_TYPE", "SOHO_TYPE", "QUANTITY_ORDERED", "QUANTITY_INVOICED", "QUANTITY_CREDITED", "UNIT_LIST_PRICE", "UNIT_SELL_PRICE", "EXTENDED_AMOUNT", "EXT_LIST_PRICE", "COST_AMOUNT", "INVOICE_CURRENCY_CODE", "EXCHANGE_RATE", "SOB_ID", "ORG_ID", "DUE_DATE", "PO_NUM", "SALES_ORDER", "UOM_CODE", "SALES_ORDER_LINE", "INTERFACE_LINE_CONTEXT", "INTERFACE_LINE_ATTRIBUTE6", "BILL_TO_ID", "BILL_TO_SITE_USE_ID", "BILL_TO_CUSTOMER_NAME", "BILL_TO_CUSTOMER_NUMBER", "BILL_TO_COUNTRY", "BILL_TO_ADDRESS_ID", "BILL_TO_ADDRESS1", "BILL_TO_ADDRESS2", "BILL_TO_CITY", "BILL_TO_STATE", "BILL_TO_POSTAL_CODE", "SHIP_TO_ID", "SHIP_TO_SITE_USE_ID", "SHIP_TO_CUSTOMER_NAME", "SHIP_TO_CUSTOMER_NUMBER",
  "SHIP_TO_COUNTRY", "SHIP_TO_ADDRESS_ID", "SHIP_TO_ADDRESS1", "SHIP_TO_ADDRESS2", "SHIP_TO_CITY", "SHIP_TO_STATE", "SHIP_TO_POSTAL_CODE", "CUSTOMER_ID", "CUSTOMER_NAME", "CUSTOMER_NUMBER", "REV_ACCTG_SEG1", "REV_ACCTG_SEG2", "REV_ACCTG_SEG3", "REV_ACCTG_SEG4", "REV_ACCTG_SEG5", "REV_ACCTG_SEG6", "REV_ACCTG_SEG7", "DEF_ACCTG_SEG1", "DEF_ACCTG_SEG2", "DEF_ACCTG_SEG3", "DEF_ACCTG_SEG4", "DEF_ACCTG_SEG5", "DEF_ACCTG_SEG6", "DEF_ACCTG_SEG7", "COGS_ACCTG_SEG1", "COGS_ACCTG_SEG2", "COGS_ACCTG_SEG3", "COGS_ACCTG_SEG4", "COGS_ACCTG_SEG5", "COGS_ACCTG_SEG6", "COGS_ACCTG_SEG7"
  ,"SERVICE_START_DATE","SERVICE_END_DATE","CONTRACT_NUMBER","GP_INVOICE_NUMBER","FLAG_97_2")
AS
  (
  SELECT trxt.type trx_type,
    trxt.name trx_name,
    trx.trx_number trx_number,
    trx.customer_trx_id,
    TO_CHAR(trxl.customer_trx_line_id),
    trx.trx_date,
    trxl.line_number,
    trxl.inventory_item_id,
    (SELECT a.segment1
    FROM mtl_system_items_b a
    WHERE a.inventory_item_id = trxl.inventory_item_id
    AND rownum                = 1
    ) item_number,
    trxl.description,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_product_family(trxl.inventory_item_id,fnd_profile.value('AMS_ITEM_ORGANIZATION_ID')) product_family,     --micv.segment2 product_family,    --        NULL product_family,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_product_category(trxl.inventory_item_id,fnd_profile.value('AMS_ITEM_ORGANIZATION_ID')) product_category, --micv.segment1 product_category,  --     NULL product_category,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_product_line(trxl.inventory_item_id,fnd_profile.value('AMS_ITEM_ORGANIZATION_ID')) product_line,         --micv.segment2 product_line,               --     micv.segment3 product_line,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_product_class(trxl.inventory_item_id,fnd_profile.value('AMS_ITEM_ORGANIZATION_ID')) product_class,       --micv.segment2 product_class,     --     null PRODUCT_CLASS,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_product_group(trxl.inventory_item_id,fnd_profile.value('AMS_ITEM_ORGANIZATION_ID')) product_group,       --micv.segment2 product_group,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_other_element_type(trxl.inventory_item_id,fnd_profile.value('AMS_ITEM_ORGANIZATION_ID')) element_type,
    XXFT_RPRO_DATA_EXTRACT_PKG.get_soho_type(trxl.inventory_item_id,fnd_profile.value('AMS_ITEM_ORGANIZATION_ID')) soho_type,
    trxl.quantity_ordered,
    trxl.quantity_invoiced,
    trxl.quantity_credited,
    /*(CASE WHEN trxl.unit_standard_price IS NULL THEN trxl.unit_selling_price ELSE trxl.unit_standard_price END ) Unit_List_Price, */
    trxl.unit_standard_price,
    trxl.unit_selling_price,
    trxl.extended_amount,
    /*(NVL(trxl.unit_standard_price,trxl.unit_selling_price) * trxl.quantity_invoiced) ext_list_price, */
    (trxl.unit_standard_price * trxl.quantity_invoiced) ext_list_price, 
    cst_cost_api.get_item_cost(1,trxl.inventory_item_id,fnd_profile.value('AMS_ITEM_ORGANIZATION_ID'),NULL,NULL,2),
    trx.invoice_currency_code,
    trx.exchange_rate,
    trx.set_of_books_id,
    trx.org_id,
    trx.term_due_date,
    trx.purchase_order,
    trxl.sales_order,
    trxl.uom_code,
    trxl.SALES_ORDER_LINE,
    trxl.interface_line_context,
    trxl.interface_line_attribute6,
    trx.bill_to_customer_id,
    trx.bill_to_site_use_id,
    bill_cust.party_name bill_to_customer_name,
    bill_cust.customer_number bill_to_customer_number,
    bill_cust.country bill_to_country,
    bill_cust.cust_acct_site_id bill_to_address_id,
    bill_cust.address1 bill_to_address1,
    bill_cust.address1 bill_to_address2,
    bill_cust.city bill_to_city,
    bill_cust.state bill_to_state,
    bill_cust.postal_code bill_to_postal_code,
    trx.ship_to_customer_id,
    trx.ship_to_site_use_id,
    ship_cust.party_name ship_to_customer_name,
    ship_cust.customer_number ship_to_customer_number,
    ship_cust.country ship_to_country,
    ship_cust.cust_acct_site_id ship_to_address_id,
    ship_cust.address1 ship_to_address1,
    ship_cust.address2 ship_to_address2,
    ship_cust.city ship_to_city,
    ship_cust.state ship_to_state,
    ship_cust.postal_code ship_to_postal_code,
    trx.sold_to_customer_id,
    hp_sold.party_name customer_name,
    hca_sold.account_number sold_to_customer_number,
    gcc1.Segment1 Rev_Acctg_Seg1,
    gcc1.Segment2 Rev_Acctg_Seg2,
    gcc1.Segment3 Rev_Acctg_Seg3,
    gcc1.Segment4 Rev_Acctg_Seg4,
    gcc1.Segment5 Rev_Acctg_Seg5,
    gcc1.Segment6 Rev_Acctg_Seg6,
    gcc1.Segment7 Rev_Acctg_Seg7,
    gcc2.segment1 def_acctg_seg1,
    gcc2.segment2 def_acctg_seg2,
    gcc2.segment3 def_acctg_seg3,
    gcc2.segment4 def_acctg_seg4,
    gcc2.segment5 def_acctg_seg5,
    gcc2.segment6 def_acctg_seg6,
    gcc2.segment7 def_acctg_seg7,
    gcc3.segment1 cogs_acctg_seg1,
    gcc3.segment2 cogs_acctg_seg2,
    gcc3.segment3 cogs_acctg_seg3,
    gcc3.segment4 cogs_acctg_seg4,
    gcc3.segment5 cogs_acctg_seg5,
    gcc3.segment6 cogs_acctg_seg6,
    gcc3.segment7 cogs_acctg_seg7,
    NULL "Service Start Date",
    NULL "Service End Date",
    NULL "Contract Number",
    NULL  "GP_INVOICE_NUMBER",
    XXFT_RPRO_DATA_EXTRACT_PKG.get_flag_97_2(msi.inventory_item_id,msi.organization_id) "flag_97_2"
  FROM ra_customer_trx_all trx,
    ra_cust_trx_types_all trxt,
    ra_customer_trx_lines_all trxl,
    xxft_rpro_customers_v ship_cust,
    xxft_rpro_customers_v bill_cust,
    hz_cust_accounts hca_sold,
    hz_parties hp_sold,
    ra_cust_trx_line_gl_dist_all rctgl1,
    ra_cust_trx_line_gl_dist_all rctgl2,
    mtl_system_items_b msi,
    gl_code_combinations gcc1,
    gl_code_combinations gcc2,
    gl_code_combinations gcc3
  WHERE trx.cust_trx_type_id = trxt.cust_trx_type_id
  AND trx.CUSTOMER_TRX_ID    = trxl.CUSTOMER_TRX_ID
  AND trx.org_id             = trxt.org_id
  AND trx.batch_source_id    = -1 
    --and trxl.PREVIOUS_CUSTOMER_TRX_LINE_ID IS NULL
  AND trxl.line_type             = 'LINE'
  --  AND trxt.type                  = 'INV' --Commented by Obi on 9-Sep-16 ITS#543505
    AND trxt.name IN (SELECT meaning 
                          FROM fnd_lookup_values
                         WHERE lookup_type='XXFT_REVPRO_AR_TRX_TYPES'
                           AND TRUNC(SYSDATE) BETWEEN NVL(START_DATE_ACTIVE,TRUNC(SYSDATE)) 
                                                AND NVL(END_DATE_ACTIVE,TRUNC(SYSDATE)) 
                           AND ENABLED_FLAG='Y'
                     ) -- Added by obi on 9-Sep-2016 ITS#543505
  AND trx.ship_to_site_use_id    = ship_cust.site_use_id(+)
  AND ship_cust.site_use_code(+) = 'SHIP_TO'
  AND trx.bill_to_site_use_id    = bill_cust.site_use_id
  AND BILL_CUST.SITE_USE_CODE    = 'BILL_TO'
  AND trx.sold_to_customer_id    = hca_sold.cust_account_id(+)
  AND hca_sold.party_id          = hp_sold.party_id(+)
  AND trx.customer_trx_id        = rctgl1.customer_trx_id
  AND trxl.customer_trx_line_id  = rctgl1.customer_trx_line_id
  AND rctgl1.account_class       = 'REV'
  AND gcc1.code_combination_id   = rctgl1.code_combination_id
  AND trxl.customer_trx_line_id  = rctgl2.customer_trx_line_id(+)
  AND rctgl2.account_class(+)    = 'UNEARN'  
  AND gcc2.code_combination_id   = rctgl1.code_combination_id
  AND msi.inventory_item_id(+)      = trxl.inventory_item_id
  AND msi.organization_id(+)        = fnd_profile.value('AMS_ITEM_ORGANIZATION_ID')
  AND msi.cost_of_sales_account  = gcc3.code_combination_id(+)

  )
UNION ALL
SELECT -- SPR Historical Credit Memo
  trxt.type trx_type,
  trxt.name trx_name,
  trx.trx_number ,
  trx.customer_trx_id,
  TO_CHAR(trxl.customer_trx_line_id),
  trx.trx_date,
  trxl.line_number,
  trxl.inventory_item_id,
  (SELECT a.segment1
  FROM mtl_system_items_b a
  WHERE a.inventory_item_id = trxl.inventory_item_id
  AND rownum                = 1
  ) item_number,
  trxl.description,
  XXFT_RPRO_DATA_EXTRACT_PKG.get_product_family(trxl.inventory_item_id,fnd_profile.value('AMS_ITEM_ORGANIZATION_ID')) product_family,     --micv.segment2 product_family,    --        NULL product_family,
  XXFT_RPRO_DATA_EXTRACT_PKG.get_product_category(trxl.inventory_item_id,fnd_profile.value('AMS_ITEM_ORGANIZATION_ID')) product_category, --micv.segment1 product_category,  --     NULL product_category,
  XXFT_RPRO_DATA_EXTRACT_PKG.get_product_line(trxl.inventory_item_id,fnd_profile.value('AMS_ITEM_ORGANIZATION_ID')) product_line,         --micv.segment2 product_line,               --     micv.segment3 product_line,
  XXFT_RPRO_DATA_EXTRACT_PKG.get_product_class(trxl.inventory_item_id,fnd_profile.value('AMS_ITEM_ORGANIZATION_ID')) product_class,       --micv.segment2 product_class,     --     null PRODUCT_CLASS,
  XXFT_RPRO_DATA_EXTRACT_PKG.get_product_group(trxl.inventory_item_id,fnd_profile.value('AMS_ITEM_ORGANIZATION_ID')) product_group,       --micv.segment2 product_group,
  XXFT_RPRO_DATA_EXTRACT_PKG.get_other_element_type(trxl.inventory_item_id,fnd_profile.value('AMS_ITEM_ORGANIZATION_ID')) element_type,
  XXFT_RPRO_DATA_EXTRACT_PKG.get_soho_type(trxl.inventory_item_id,fnd_profile.value('AMS_ITEM_ORGANIZATION_ID')) soho_type,
  trxl.quantity_ordered,
  ABS(trxl.quantity_credited),
  trxl.QUANTITY_CREDITED quantity_credited,
  (CASE WHEN trxl.unit_standard_price IS NULL THEN to_number(ozfs.attribute10) ELSE trxl.unit_standard_price END ) Unit_List_Price, /* Defect #3487 */
  trxl.unit_selling_price,
  trxl.extended_amount,
    (NVL(trxl.unit_standard_price,TO_NUMBER(ozfs.attribute10)) * trxl.quantity_credited) ext_list_price,/* Defect #3487 */
  cst_cost_api.get_item_cost(1,trxl.inventory_item_id,fnd_profile.value('AMS_ITEM_ORGANIZATION_ID'),NULL,NULL,2),
  trx.invoice_currency_code,
  trx.exchange_rate,
  trx.set_of_books_id,
  trx.org_id,
  trx.term_due_date,
  trx.purchase_order,
  nvl(trxl.sales_order,trx.trx_number),/* added trx_number as the sales order number in case of manual credit memos*/
  trxl.uom_code,
  trxl.SALES_ORDER_LINE,
  trxl.interface_line_context,
  trxl.interface_line_attribute6,
  trx.bill_to_customer_id,
  trx.bill_to_site_use_id,
  bill_cust.party_name bill_to_customer_name,
  bill_cust.customer_number bill_to_customer_number,
  bill_cust.country bill_to_country,
  bill_cust.cust_acct_site_id bill_to_address_id,
  bill_cust.address1 bill_to_address1,
  bill_cust.address2 bill_to_address2,
  bill_cust.city bill_to_city,
  bill_cust.state bill_to_state,
  bill_cust.postal_code bill_to_postal_code,
  trx.ship_to_customer_id,
  trx.ship_to_site_use_id,
  ship_cust.party_name ship_to_customer_name,
  ship_cust.customer_number ship_to_customer_number,
  ship_cust.country ship_to_country,
  ship_cust.cust_acct_site_id ship_to_address_id,
  ship_cust.address1 ship_to_address1,
  ship_cust.address2 ship_to_address2,
  ship_cust.city ship_to_city,
  ship_cust.state ship_to_state,
  ship_cust.postal_code ship_to_postal_code,
  trx.sold_to_customer_id,
  hp_sold.party_name customer_name,
  hca_sold.account_number sold_to_customer_number,
  gcc1.Segment1 Rev_Acctg_Seg1,
  gcc1.Segment2 Rev_Acctg_Seg2,
 -- gcc1.Segment3 Rev_Acctg_Seg3,
 (SELECT flv.attribute14     
  FROM  apps.fnd_lookup_values flv
 WHERE 1 =1
   AND flv.lookup_code = bill_cust.country
   AND flv.lookup_type = 'FTNT_REVPRO_REGION') Rev_Acctg_Seg3,
  gcc1.Segment4 Rev_Acctg_Seg4,
  gcc1.Segment5 Rev_Acctg_Seg5,
  gcc1.Segment6 Rev_Acctg_Seg6,
  gcc1.Segment7 Rev_Acctg_Seg7,
  gcc2.segment1 def_acctg_seg1,
  gcc2.segment2 def_acctg_seg2,
  gcc2.segment3 def_acctg_seg3,
  gcc2.segment4 def_acctg_seg4,
  gcc2.segment5 def_acctg_seg5,
  gcc2.segment6 def_acctg_seg6,
  gcc2.segment7 def_acctg_seg7,
  gcc3.segment1 cogs_acctg_seg1,
  gcc3.segment2 cogs_acctg_seg2,
  gcc3.segment3 cogs_acctg_seg3,
  gcc3.segment4 cogs_acctg_seg4,
  gcc3.segment5 cogs_acctg_seg5,
  gcc3.segment6 cogs_acctg_seg6,
  gcc3.segment7 cogs_acctg_seg,
  NULL "Service Start Date",
  NULL "Service End Date",
  NULL "Contract Number",
  NULL  "GP_INVOICE_NUMBER",
  XXFT_RPRO_DATA_EXTRACT_PKG.get_flag_97_2(msi.inventory_item_id,msi.organization_id) "flag_97_2"
FROM ra_customer_trx_all trx,
  ra_cust_trx_types_all trxt,
  ra_customer_trx_lines_all trxl,
  xxft_rpro_customers_v ship_cust,
  xxft_rpro_customers_v bill_cust,
  hz_cust_accounts hca_sold,
  hz_parties hp_sold,
  mtl_system_items_b msi,
  ra_cust_trx_line_gl_dist_all rctgl1,
  ra_cust_trx_line_gl_dist_all rctgl2,
  gl_code_combinations gcc1,
  gl_code_combinations gcc2,
  gl_code_combinations gcc3,
  ozf_claim_lines_all cl,
  apps.ozf_sales_transactions_all ozfs
WHERE trx.cust_trx_type_id              = trxt.cust_trx_type_id
AND trx.CUSTOMER_TRX_ID                 = trxl.CUSTOMER_TRX_ID
AND trx.org_id                          = trxt.org_id
AND trxl.interface_line_context         = 'CLAIM'
AND trxl.PREVIOUS_CUSTOMER_TRX_LINE_ID IS NULL
AND trxl.line_type                      = 'LINE'
AND trxt.type                           = 'CM'
AND trx.ship_to_site_use_id             = ship_cust.site_use_id(+)
AND ship_cust.site_use_code(+)          = 'SHIP_TO'
AND trx.bill_to_site_use_id             = bill_cust.site_use_id
AND BILL_CUST.SITE_USE_CODE             = 'BILL_TO'
AND trx.sold_to_customer_id             = hca_sold.cust_account_id(+)
AND hca_sold.party_id                   = hp_sold.party_id(+)
AND trx.customer_trx_id                 = rctgl1.customer_trx_id
AND trxl.customer_trx_line_id           = rctgl1.customer_trx_line_id
AND rctgl1.account_class                = 'REV'
AND gcc1.code_combination_id            = rctgl1.code_combination_id
AND trxl.customer_trx_line_id           = rctgl2.customer_trx_line_id(+)
AND rctgl2.account_class(+)             = 'UNEARN'
AND gcc2.code_combination_id            = rctgl1.code_combination_id
AND trxt.name                           = 'FTNT SPR Claim CM'
AND trxl.INTERFACE_LINE_ATTRIBUTE2      = cl.claim_id
AND trxl.INTERFACE_LINE_ATTRIBUTE3      = cl.claim_line_id
AND ozfs.attribute15                    = cl.attribute12
AND ozfs.attribute1                     = cl.attribute11
AND msi.inventory_item_id               = trxl.inventory_item_id
AND msi.organization_id                 = fnd_profile.value('AMS_ITEM_ORGANIZATION_ID')
AND msi.cost_of_sales_account           = gcc3.code_combination_id
--AND 1=2
UNION ALL
SELECT -- Unlinked Claims ITS#576718
  trxt.type trx_type,
  trxt.name trx_name,
  trx.trx_number ,
  trx.customer_trx_id,
  TO_CHAR(trxl.customer_trx_line_id),
  trx.trx_date,
  trxl.line_number,
  trxl.inventory_item_id,
  (SELECT a.segment1
  FROM mtl_system_items_b a
  WHERE a.inventory_item_id = trxl.inventory_item_id
  AND rownum                = 1
  ) item_number,
  trxl.description,
  XXFT_RPRO_DATA_EXTRACT_PKG.get_product_family(trxl.inventory_item_id,fnd_profile.value('AMS_ITEM_ORGANIZATION_ID')) product_family,     --micv.segment2 product_family,    --        NULL product_family,
  XXFT_RPRO_DATA_EXTRACT_PKG.get_product_category(trxl.inventory_item_id,fnd_profile.value('AMS_ITEM_ORGANIZATION_ID')) product_category, --micv.segment1 product_category,  --     NULL product_category,
  XXFT_RPRO_DATA_EXTRACT_PKG.get_product_line(trxl.inventory_item_id,fnd_profile.value('AMS_ITEM_ORGANIZATION_ID')) product_line,         --micv.segment2 product_line,               --     micv.segment3 product_line,
  XXFT_RPRO_DATA_EXTRACT_PKG.get_product_class(trxl.inventory_item_id,fnd_profile.value('AMS_ITEM_ORGANIZATION_ID')) product_class,       --micv.segment2 product_class,     --     null PRODUCT_CLASS,
  XXFT_RPRO_DATA_EXTRACT_PKG.get_product_group(trxl.inventory_item_id,fnd_profile.value('AMS_ITEM_ORGANIZATION_ID')) product_group,       --micv.segment2 product_group,
  XXFT_RPRO_DATA_EXTRACT_PKG.get_other_element_type(trxl.inventory_item_id,fnd_profile.value('AMS_ITEM_ORGANIZATION_ID')) element_type,
  XXFT_RPRO_DATA_EXTRACT_PKG.get_soho_type(trxl.inventory_item_id,fnd_profile.value('AMS_ITEM_ORGANIZATION_ID')) soho_type,
  trxl.quantity_ordered,
  ABS(trxl.quantity_credited),
  trxl.QUANTITY_CREDITED quantity_credited,
--  (CASE WHEN trxl.unit_standard_price IS NULL THEN to_number(ozfs.attribute10) ELSE trxl.unit_standard_price END ) Unit_List_Price, /* Defect #3487 */
  trxl.unit_standard_price Unit_List_Price, 
  trxl.unit_selling_price,
  trxl.extended_amount,
    (trxl.unit_standard_price * trxl.quantity_credited) ext_list_price,/* Defect #3487 */
  cst_cost_api.get_item_cost(1,trxl.inventory_item_id,fnd_profile.value('AMS_ITEM_ORGANIZATION_ID'),NULL,NULL,2),
  trx.invoice_currency_code,
  trx.exchange_rate,
  trx.set_of_books_id,
  trx.org_id,
  trx.term_due_date,
  trx.purchase_order,
  nvl(trxl.sales_order,trx.trx_number),/* added trx_number as the sales order number in case of manual credit memos*/
  trxl.uom_code,
  trxl.SALES_ORDER_LINE,
  trxl.interface_line_context,
  trxl.interface_line_attribute6,
  trx.bill_to_customer_id,
  trx.bill_to_site_use_id,
  bill_cust.party_name bill_to_customer_name,
  bill_cust.customer_number bill_to_customer_number,
  bill_cust.country bill_to_country,
  bill_cust.cust_acct_site_id bill_to_address_id,
  bill_cust.address1 bill_to_address1,
  bill_cust.address2 bill_to_address2,
  bill_cust.city bill_to_city,
  bill_cust.state bill_to_state,
  bill_cust.postal_code bill_to_postal_code,
  trx.ship_to_customer_id,
  trx.ship_to_site_use_id,
  ship_cust.party_name ship_to_customer_name,
  ship_cust.customer_number ship_to_customer_number,
  ship_cust.country ship_to_country,
  ship_cust.cust_acct_site_id ship_to_address_id,
  ship_cust.address1 ship_to_address1,
  ship_cust.address2 ship_to_address2,
  ship_cust.city ship_to_city,
  ship_cust.state ship_to_state,
  ship_cust.postal_code ship_to_postal_code,
  trx.sold_to_customer_id,
  hp_sold.party_name customer_name,
  hca_sold.account_number sold_to_customer_number,
  gcc1.Segment1 Rev_Acctg_Seg1,
  gcc1.Segment2 Rev_Acctg_Seg2,
 -- gcc1.Segment3 Rev_Acctg_Seg3,
 (SELECT flv.attribute14     
  FROM  apps.fnd_lookup_values flv
 WHERE 1 =1
   AND flv.lookup_code = bill_cust.country
   AND flv.lookup_type = 'FTNT_REVPRO_REGION') Rev_Acctg_Seg3,
  gcc1.Segment4 Rev_Acctg_Seg4,
  gcc1.Segment5 Rev_Acctg_Seg5,
  gcc1.Segment6 Rev_Acctg_Seg6,
  gcc1.Segment7 Rev_Acctg_Seg7,
  gcc2.segment1 def_acctg_seg1,
  gcc2.segment2 def_acctg_seg2,
  gcc2.segment3 def_acctg_seg3,
  gcc2.segment4 def_acctg_seg4,
  gcc2.segment5 def_acctg_seg5,
  gcc2.segment6 def_acctg_seg6,
  gcc2.segment7 def_acctg_seg7,
  gcc3.segment1 cogs_acctg_seg1,
  gcc3.segment2 cogs_acctg_seg2,
  gcc3.segment3 cogs_acctg_seg3,
  gcc3.segment4 cogs_acctg_seg4,
  gcc3.segment5 cogs_acctg_seg5,
  gcc3.segment6 cogs_acctg_seg6,
  gcc3.segment7 cogs_acctg_seg,
  NULL "Service Start Date",
  NULL "Service End Date",
  NULL "Contract Number",
  NULL  "GP_INVOICE_NUMBER",
  XXFT_RPRO_DATA_EXTRACT_PKG.get_flag_97_2(msi.inventory_item_id,msi.organization_id) "flag_97_2"
FROM ra_customer_trx_all trx,
  ra_cust_trx_types_all trxt,
  ra_customer_trx_lines_all trxl,
  xxft_rpro_customers_v ship_cust,
  xxft_rpro_customers_v bill_cust,
  hz_cust_accounts hca_sold,
  hz_parties hp_sold,
  mtl_system_items_b msi,
  ra_cust_trx_line_gl_dist_all rctgl1,
  ra_cust_trx_line_gl_dist_all rctgl2,
  gl_code_combinations gcc1,
  gl_code_combinations gcc2,
  gl_code_combinations gcc3,
  ozf_claim_lines_all cl--,
--  apps.ozf_sales_transactions_all ozfs
WHERE trx.cust_trx_type_id              = trxt.cust_trx_type_id
AND trx.CUSTOMER_TRX_ID                 = trxl.CUSTOMER_TRX_ID
AND trx.org_id                          = trxt.org_id
AND trxl.interface_line_context         = 'CLAIM'
AND trxl.PREVIOUS_CUSTOMER_TRX_LINE_ID IS NULL
AND trxl.line_type                      = 'LINE'
AND trxt.type                           = 'CM'
AND trx.ship_to_site_use_id             = ship_cust.site_use_id(+)
AND ship_cust.site_use_code(+)          = 'SHIP_TO'
AND trx.bill_to_site_use_id             = bill_cust.site_use_id
AND BILL_CUST.SITE_USE_CODE             = 'BILL_TO'
AND trx.sold_to_customer_id             = hca_sold.cust_account_id(+)
AND hca_sold.party_id                   = hp_sold.party_id(+)
AND trx.customer_trx_id                 = rctgl1.customer_trx_id
AND trxl.customer_trx_line_id           = rctgl1.customer_trx_line_id
AND rctgl1.account_class                = 'REV'
AND gcc1.code_combination_id            = rctgl1.code_combination_id
AND trxl.customer_trx_line_id           = rctgl2.customer_trx_line_id(+)
AND rctgl2.account_class(+)             = 'UNEARN'
AND gcc2.code_combination_id            = rctgl1.code_combination_id
AND trxt.name                           = 'FTNT SPR Claim CM'
AND trxl.INTERFACE_LINE_ATTRIBUTE2      = cl.claim_id
AND trxl.INTERFACE_LINE_ATTRIBUTE3      = cl.claim_line_id
and cl.attribute12 is null
and cl.attribute11 is null
--AND ozfs.attribute15                    = cl.attribute12
--AND ozfs.attribute1                     = cl.attribute11
AND msi.inventory_item_id               = trxl.inventory_item_id
AND msi.organization_id                 = fnd_profile.value('AMS_ITEM_ORGANIZATION_ID')
AND msi.cost_of_sales_account           = gcc3.code_combination_id
;

/
 show errors;