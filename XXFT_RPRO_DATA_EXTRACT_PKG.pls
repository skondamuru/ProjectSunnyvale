create or replace PACKAGE XXFT_RPRO_DATA_EXTRACT_PKG
IS
  /* +===================================================================+
  -- |                         Fortinet, Inc.
  -- |                         Sunnyvale, CA
  -- +===================================================================
  -- |
  -- |Object Name     : XXFT_RPRO_DATA_EXTRACT_PKG.pks
  -- |
  -- |Description      : Package to get the function derivations.
  -- |
  -- |
  __ |
  -- |
  -- |Change Record:
  -- |===============
  -- |Version   Date        Author             Remarks
  -- |=======   =========== ==============     ============================
  -- |1.0       12-FEB-2016 (NTT)              Initial code version
  -- |1.1       19-MAY-2016 (NTT)              Added List Price validation pertaining to the defect#2626.
  -- |1.2       07-Jun-2016 (NTT)              Modified for the CR #49.
  -- |1.3       15-Jun-2016 (NTT)              Modified for the defect #3306. Embedded support will be derived
  -- |                                         for service items related with sales order line.
  -- |1.4       22-Jun-2016 (NTT)              Modified for the defect#3345. get_list_price.
  -- |1.5       10-AUG-2016 (NTT)              Modified to add the get_list_price_for_cm for ITS #521951
  -- |1.6       13-SEP-2016 (NTT)              Modified to include the contract start dates.ITS#553115
  -- |1.15      26-OCT-2016 (NTT)              Modified to add a function get_order_classification ITS#575976
  -- +===================================================================+*/
  --
  --
  --
  FUNCTION get_conversion_rate(
      p_from_currency   VARCHAR2,
      p_to_currency     VARCHAR2,
      p_type            VARCHAR2,
      p_conversion_date DATE)
    RETURN NUMBER RESULT_CACHE ;
  FUNCTION get_cost_amount(
      P_INV_ITEM_ID      NUMBER,
      P_OE_LINE_ID       VARCHAR2,
      P_SHIP_FROM_ORG_ID NUMBER,
      P_CST_FLAG         VARCHAR2,
      P_ATO_LINE_ID      NUMBER DEFAULT NULL)
    RETURN NUMBER;
  FUNCTION get_region(
      p_line_id IN NUMBER)
    RETURN VARCHAR2;
  FUNCTION get_grace_period(
      p_line_id IN NUMBER)
    RETURN NUMBER;
  FUNCTION get_e_grace_period(
      p_line_id IN NUMBER)
    RETURN NUMBER;
  --
  -- Function to get the grace period.
  --
  FUNCTION get_grace_period(
      p_attribute44  IN VARCHAR2 ,
      p_product_line IN VARCHAR2 ,
      p_line_id      IN NUMBER)
    RETURN NUMBER;
  --
  -- Function to get the product category for service items. #3306
  --
  FUNCTION get_embedded_service(
      p_ser_ref_type_code IN VARCHAR2,
      p_line_id           IN NUMBER,
      p_ser_line_id       IN NUMBER ,
      p_inventory_item_id IN NUMBER ,
      p_org_id            IN NUMBER)
    RETURN VARCHAR2 RESULT_CACHE;
  FUNCTION get_s_grace_period(
      p_line_id IN NUMBER)
    RETURN NUMBER;
  ----added---
  FUNCTION get_bill_to_territory(
      p_line_id IN NUMBER)
    RETURN VARCHAR2;
  FUNCTION get_bill_to_geo(
      p_line_id IN NUMBER)
    RETURN VARCHAR2;
  FUNCTION get_bill_to_state(
      p_line_id IN NUMBER)
    RETURN VARCHAR2;
  FUNCTION get_ship_to_territory(
      p_line_id IN NUMBER)
    RETURN VARCHAR2;
  FUNCTION get_ship_to_geo(
      p_line_id IN NUMBER)
    RETURN VARCHAR2;
  FUNCTION get_ship_to_state(
      p_line_id IN NUMBER)
    RETURN VARCHAR2;
  ------------
  FUNCTION get_customer_flag(
      p_line_id IN NUMBER)
    RETURN VARCHAR2;
  FUNCTION get_spr_number(
      p_header_id IN NUMBER,
      p_line_id   IN NUMBER)
    RETURN VARCHAR2;
  FUNCTION get_opportunity(
      p_header_id IN NUMBER,
      p_line_id   IN NUMBER)
    RETURN VARCHAR2;
  FUNCTION get_no_days(
      p_line_id IN NUMBER)
    RETURN NUMBER ;
  --End customer
  FUNCTION get_customer_type(
      p_cust_account_id IN NUMBER)
    RETURN VARCHAR2 RESULT_CACHE ;
  FUNCTION get_end_cust_name(
      p_header_id IN NUMBER)
    RETURN VARCHAR2;
  FUNCTION get_end_cust_number(
      p_header_id IN NUMBER)
    RETURN VARCHAR2;
  FUNCTION get_end_cust_state(
      p_header_id IN NUMBER)
    RETURN VARCHAR2;
  FUNCTION get_end_cust_zip(
      p_header_id IN NUMBER)
    RETURN VARCHAR2;
  -- End Customer
  -- MTL Categories
  FUNCTION get_product_family(
      p_inventory_item_id IN NUMBER,
      p_org_id            IN NUMBER)
    RETURN VARCHAR2 RESULT_CACHE ;
  FUNCTION get_product_category(
      p_inventory_item_id IN NUMBER,
      p_org_id            IN NUMBER)
    RETURN VARCHAR2 RESULT_CACHE ;
  FUNCTION get_product_line(
      p_inventory_item_id IN NUMBER,
      p_org_id            IN NUMBER)
    RETURN VARCHAR2 RESULT_CACHE ;
  FUNCTION get_product_class(
      p_inventory_item_id IN NUMBER,
      p_org_id            IN NUMBER)
    RETURN VARCHAR2 RESULT_CACHE ;
  FUNCTION get_product_group(
      p_inventory_item_id IN NUMBER,
      p_org_id            IN NUMBER)
    RETURN VARCHAR2 RESULT_CACHE;
  FUNCTION get_other_element_type(
      p_inventory_item_id IN NUMBER,
      p_org_id            IN NUMBER)
    RETURN VARCHAR2 RESULT_CACHE ;
  FUNCTION get_soho_type(
      p_inventory_item_id IN NUMBER,
      p_org_id            IN NUMBER)
    RETURN VARCHAR2 RESULT_CACHE ;
  FUNCTION get_report_family(
      p_inventory_item_id IN NUMBER,
      p_org_id            IN NUMBER)
    RETURN VARCHAR2 RESULT_CACHE ;
  FUNCTION get_flag_97_2(
      p_inventory_item_id IN NUMBER,
      p_org_id            IN NUMBER)
    RETURN VARCHAR2 RESULT_CACHE ;
  -- MTL Categories
  --Defect#3345 - adding Line if as another parameter.
  FUNCTION get_list_price(
      p_line_id           IN NUMBER,
      p_price_list_id     IN NUMBER,
      p_inventory_item_id IN NUMBER,
      p_price_date DATE)
    RETURN NUMBER RESULT_CACHE ;
  FUNCTION get_ac_segment1(
      p_ac_type IN VARCHAR2,
      p_line_id IN NUMBER)
    RETURN VARCHAR2;
  FUNCTION get_ac_segment2(
      p_ac_type IN VARCHAR2,
      p_line_id IN NUMBER)
    RETURN VARCHAR2;
  FUNCTION get_ac_segment3(
      p_ac_type IN VARCHAR2,
      p_line_id IN NUMBER)
    RETURN VARCHAR2;
  FUNCTION get_ac_segment4(
      p_ac_type IN VARCHAR2,
      p_line_id IN NUMBER)
    RETURN VARCHAR2;
  FUNCTION get_ac_segment5(
      p_ac_type IN VARCHAR2,
      p_line_id IN NUMBER)
    RETURN VARCHAR2;
  FUNCTION get_ac_segment6(
      p_ac_type IN VARCHAR2,
      p_line_id IN NUMBER)
    RETURN VARCHAR2;
  FUNCTION get_ac_segment7(
      p_ac_type IN VARCHAR2,
      p_line_id IN NUMBER)
    RETURN VARCHAR2;
  FUNCTION get_lt_deff_rev
    RETURN VARCHAR2;
  FUNCTION get_lt_deff_cogs
    RETURN VARCHAR2;
  FUNCTION un_billed_act
    RETURN VARCHAR2;
  FUNCTION get_embedded_service(
      p_ser_line_id      IN NUMBER,
      p_product_category IN VARCHAR2)
    RETURN VARCHAR2 ;
  FUNCTION implied_pcs_days(
      p_cust_type    IN VARCHAR2,
      p_bill_country IN VARCHAR2,
      p_type         IN VARCHAR2)
    RETURN VARCHAR2 RESULT_CACHE ;
  FUNCTION check_line_qty_change(
      p_line_id          NUMBER,
      p_ordered_quantity NUMBER)
    RETURN VARCHAR2;
  FUNCTION get_def_account(
      p_rev_category IN VARCHAR2)
    RETURN VARCHAR2;
  FUNCTION get_unit_list_price(
      p_list_hdr_id IN NUMBER ,
      p_item_id     IN NUMBER)
    RETURN NUMBER;
  FUNCTION get_partner_type(
      p_header_id IN NUMBER )
    RETURN NUMBER RESULT_CACHE ;
  --
  -- Check for Non Serialized and Non Channel Partner order transaction. #CR49
  --
  FUNCTION check_non_serial_item(
      p_header_id IN NUMBER ,
      p_line_id   IN NUMBER )
    RETURN VARCHAR2 RESULT_CACHE;
  FUNCTION get_country_grace_period(
      p_country IN VARCHAR2)
    RETURN VARCHAR2 RESULT_CACHE;
  FUNCTION get_country_s_grace_period(
      p_country IN VARCHAR2)
    RETURN VARCHAR2 RESULT_CACHE;
  FUNCTION get_rev_cat_set_id
    RETURN NUMBER RESULT_CACHE;
  FUNCTION get_rep_cat_set_id
    RETURN NUMBER RESULT_CACHE;
  FUNCTION get_contry_region(
      p_country IN VARCHAR2)
    RETURN VARCHAR2 RESULT_CACHE;
  FUNCTION get_list_price_for_cm(
      p_price_list_id     IN NUMBER,
      p_inventory_item_id IN NUMBER,
      p_price_date DATE)
    RETURN NUMBER RESULT_CACHE;
  FUNCTION get_embedded_service_cm(
      p_line_id           IN NUMBER,
      p_inventory_item_id IN NUMBER,
      p_org_id            IN NUMBER )
    RETURN VARCHAR2;
  FUNCTION get_rma_cost(
      p_rma_line_id IN NUMBER)
    RETURN NUMBER;
  PROCEDURE get_contract_dates(
      p_contract_num IN VARCHAR2 ,
      x_start_date   OUT DATE ,
      x_end_date     OUT DATE
      -- ,x_reg_date        OUT DATE
      -- ,x_auto_start_date OUT DATE
    );
FUNCTION get_contract_end_date(p_contract_num    IN VARCHAR2)
RETURN DATE;
FUNCTION get_contract_start_date(p_contract_num    IN VARCHAR2)
RETURN DATE;
--ITS#553115
FUNCTION get_line_contract_date(
    p_line_id IN NUMBER,
    P_type    IN VARCHAR2)
  RETURN VARCHAR2;
FUNCTION get_line_grace_period(
    P_LINE_ID IN NUMBER)
  RETURN NUMBER;
--ITS#553115 --

--ITS#575976 --
--
-- Get Order Classification of the order- Drop Ship or Coterm Or Online
--
FUNCTION get_order_classification(p_line_id IN NUMBER)
RETURN VARCHAR2;
FUNCTION get_line_source(p_line_id IN NUMBER)
RETURN VARCHAR2;
--ITS#575976 --
END XXFT_RPRO_DATA_EXTRACT_PKG;
