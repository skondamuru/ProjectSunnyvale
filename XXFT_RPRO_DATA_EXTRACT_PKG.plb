create or replace PACKAGE BODY XXFT_RPRO_DATA_EXTRACT_PKG
AS
  /* +===================================================================+
  -- |                         Fortinet, Inc.
  -- |                         Sunnyvale, CA
  -- +===================================================================
  -- |
  -- |Object Name     : XXFT_RPRO_DATA_EXTRACT_PKG.pkb
  -- |
  -- |Description      : Package to get the function derivations.
  -- |c
  -- |
  __ |
  -- |
  -- |Change Record:
  -- |===============
  -- |Version   Date        Author             Remarks
  -- |=======   =========== ==============     ============================
  -- |1.0       12-FEB-2016 (NTT)              Initial code version
  -- |1.1       19-MAY-2016 (NTT)              Added List Price validation pertaining to the defect#2626.
  -- |1.2       03-JUN-2016 (NTT)              Modified the price list validation for performance tuning #2626..
  -- |1.3       06-Jun-2016 (NTT)              Modified to get the ftk item cost if it is standard as well. Defect #2949.
  -- |1.4       07-Jun-2016 (NTT)              Modified to add the result cache function to improve performance and added
  -- |                                         Check_Non_Serial_Item function to determine non Channel and Non SERIALIZABLE
  -- |                                         tranasctions.#CR49.
  -- |1.5       14-Jun-2016 (NTT)              Modified for the defect #3306. Embedded support will be derived 
  -- |                                         for service items related with sales order line. 
  -- |1.6       20-Jun-2016 (NTT)              For Bundle Item, the service reference type will not be passed to the function,
  -- |                                         hence added the condition to check Top Model Line id to decide the Bundle item.
  -- |1.7       22-Jun-2016 (NTT)              Deferred COGS account #3302 - Derive the Deferred COGS from Selling OU.
  -- |1.8       22-Jun-2016 (NTT)              Get the list price from Oracle in case of FTNT_Seat Pricing attribute.Defect #3345.
  -- |
  -- |1.9       22-Jun-2016 (NTT)              Get the List Price from Oracle Unit List price 
  -- |1.10      23-Jun-2016 (NTT)              Get tht elist Price from Oracle unit List Price if the order is Coterm with quote.#3367
  -- |1.11      10-AUG-2016 (NTT)              Added get_list_price_for_cm for the request - #521951
  -- |1.12      23-AUG-2016 (NTT)              Modified to get the list price from the pricelist in case of zero price lines in Coterm Order.ITS#530643   
  -- |
  -- |1.13      13-SEP-2016 (NTT)              Modified to add the new function for the ITS#553115.
  -- |1.14      14-SEP-2016 (NTT)              Modified the function get_customer_flag ITS#540806
  -- |1.15      26-OCT-2016 (NTT)              Modified to add a function get_order_classification ITS#575976
  -- +===================================================================+*/
  gn_user_id      NUMBER := apps.FND_GLOBAL.USER_ID;
  gn_login_id     NUMBER := APPS.FND_GLOBAL.LOGIN_ID;
  g_master_org_id NUMBER := fnd_profile.value('AMS_ITEM_ORGANIZATION_ID');
  --gn_request_id NUMBER := fnd_profile.VALUE('CONC_REQUEST_ID');
  lc_r_return_status VARCHAR2(1) := NULL;
  ln_sqlcode2        NUMBER;
  lc_sqlerrm2        VARCHAR2(2000);
  --
  -- Get Category Set Id.
  --
  FUNCTION get_rev_cat_set_id
    RETURN NUMBER RESULT_CACHE RELIES_ON(
      mtl_category_sets)
  IS
    l_cat_set_id NUMBER;
    CURSOR cur_get_cat_set_id
    IS
      SELECT category_set_id
      FROM mtl_category_sets mic
      WHERE mic.category_set_name = 'FTNT_REVENUE_CATEGORY';
  BEGIN
    OPEN cur_get_cat_set_id;
    FETCH cur_get_cat_set_id INTO l_cat_set_id;
    CLOSE cur_get_cat_set_id;
    RETURN l_cat_set_id;
  END;
  FUNCTION get_rep_cat_set_id
    RETURN NUMBER RESULT_CACHE RELIES_ON(
      mtl_category_sets)
  IS
    l_cat_set_id NUMBER;
    CURSOR cur_get_cat_set_id
    IS
      SELECT category_set_id
      FROM mtl_category_sets mic
      WHERE mic.category_set_name = 'FTNT_REPORTING_CATEGORY';
  BEGIN
    OPEN cur_get_cat_set_id;
    FETCH cur_get_cat_set_id INTO l_cat_set_id;
    CLOSE cur_get_cat_set_id;
    RETURN l_cat_set_id;
  END;
  FUNCTION get_conversion_rate(
      p_from_currency   VARCHAR2,
      p_to_currency     VARCHAR2,
      p_type            VARCHAR2,
      p_conversion_date DATE)
    RETURN NUMBER RESULT_CACHE RELIES_ON(
      gl_daily_rates)
  AS
    l_conversion_rate NUMBER;
    CURSOR c_exact_rates
    IS
      SELECT conversion_rate
      FROM apps.gl_daily_rates gdr
      WHERE conversion_type = 'Corporate'
      AND to_currency       = DECODE(p_type,'REPORTING','USD',p_to_currency)
      AND from_currency     = p_from_currency
      AND conversion_date   = TRUNC (p_conversion_date);
  BEGIN
    BEGIN
      FOR r_exact_rates IN c_exact_rates
      LOOP
        RETURN r_exact_rates.conversion_rate;
      END LOOP;
      RETURN 1;
    EXCEPTION
    WHEN OTHERS THEN
      RETURN 1;
    END;
    RETURN l_conversion_rate;
  END;
  FUNCTION get_cost_amount(
      P_INV_ITEM_ID      NUMBER,
      P_OE_LINE_ID       VARCHAR2,
      P_SHIP_FROM_ORG_ID NUMBER,
      P_CST_FLAG         VARCHAR2,
      P_ATO_LINE_ID      NUMBER DEFAULT NULL)
    RETURN NUMBER
  AS
    l_cost_amt NUMBER;
    l_item_type mtl_system_items_b.item_type%TYPE;
    l_segment1 mtl_system_items_b.segment1%TYPE;
    CURSOR cur_get_item_type
    IS
      SELECT item_type ,
        segment1
      FROM mtl_system_items_b msi ,
        mtl_parameters mp
      WHERE mp.master_organization_id = mp.organization_id
      AND msi.organization_id         = mp.organization_id
      AND msi.inventory_item_id       = p_inv_item_id;
    CURSOR cur_get_child_lines
    IS
      SELECT line_id FROM oe_order_lines_all WHERE top_model_line_id = P_OE_LINE_ID;
    CURSOR cur_get_ftk_child_cost
    IS
      SELECT SUM(actual_cost)
        --  INTO l_cost_amt
      FROM
        (SELECT (mmt.actual_cost * ABS(transaction_quantity)) actual_cost
        FROM apps.mtl_material_transactions mmt,
          oe_order_lines_all oola
        WHERE oola.top_model_line_id = P_OE_LINE_ID
        AND mmt.organization_id      = oola.ship_from_org_id
        AND mmt.inventory_item_id    = oola.inventory_item_id
        AND mmt.source_line_id       = oola.line_id
        AND mmt.trx_source_line_id   = oola.line_id
          --AND mmt.transaction_reference =TO_CHAR(r_cogs.sales_order_id)
        AND mmt.source_code = 'ORDER ENTRY'
        );
  BEGIN
    OPEN cur_get_item_type;
    FETCH cur_get_item_type INTO l_item_type,l_segment1;
    CLOSE cur_get_item_type;
    IF l_segment1 LIKE 'FTK%' AND l_item_type ='PTO' THEN
      OPEN cur_get_ftk_child_cost;
      FETCH cur_get_ftk_child_cost INTO l_cost_amt;
      CLOSE cur_get_ftk_child_cost;
      RETURN l_cost_amt;
      -- Defect #2949.
    ELSIF P_CST_FLAG ='Y' AND P_ATO_LINE_ID IS NULL THEN
      BEGIN
        SELECT SUM(actual_cost)
        INTO l_cost_amt
        FROM
          (SELECT (mmt.actual_cost * ABS(transaction_quantity)) actual_cost
          FROM apps.mtl_material_transactions mmt
          WHERE mmt.organization_id  =P_SHIP_FROM_ORG_ID
          AND mmt.inventory_item_id  =P_INV_ITEM_ID
          AND mmt.source_line_id     =P_OE_LINE_ID
          AND mmt.trx_source_line_id = P_OE_LINE_ID
            --AND mmt.transaction_reference =TO_CHAR(r_cogs.sales_order_id)
          AND mmt.source_code = 'ORDER ENTRY'
          );
      EXCEPTION
      WHEN OTHERS THEN
        l_cost_amt := 0;
      END;
    ELSIF P_CST_FLAG='Y' AND P_ATO_LINE_ID IS NOT NULL THEN
      BEGIN
        SELECT cic.item_cost
        INTO l_cost_amt
        FROM apps.cst_cost_types cct,
          apps.cst_item_costs cic,
          apps.mtl_system_items_b msi,
          apps.mtl_parameters mp
        WHERE cct.cost_type_id    = cic.cost_type_id
        AND cic.inventory_item_id = msi.inventory_item_id
        AND cic.organization_id   = msi.organization_id
        AND msi.organization_id   = mp.organization_id
        AND msi.inventory_item_id =P_INV_ITEM_ID
        AND mp.organization_id    =P_SHIP_FROM_ORG_ID
        AND CCT.COST_TYPE         ='FIFO';
      EXCEPTION
      WHEN OTHERS THEN
        l_cost_amt := 0;
      END;
    END IF;
    RETURN l_cost_amt;
  EXCEPTION
  WHEN OTHERS THEN
    NULL;
  END;
  FUNCTION get_list_price(
      p_line_id           IN NUMBER,
      p_price_list_id     IN NUMBER,
      p_inventory_item_id IN NUMBER,
      p_price_date DATE)
    RETURN NUMBER RESULT_CACHE RELIES_ON(
      QP_LIST_LINES)
  IS
    lv_list_price              VARCHAR2(100) := NULL;
    lv_price_list_id           NUMBER        := 0;
    lv_inventory_item_id       NUMBER        := 0;
    lv_secondary_price_list_id NUMBER        := 0;
    lv_count                   NUMBER        := 0;
    --
    -- Added on 6/3 to fix performance issue.
    --
    CURSOR cur_get_list_price
    IS
      SELECT QL.OPERAND
      FROM APPS.QP_LIST_LINES QL,
        APPS.QP_PRICING_ATTRIBUTES QP
      WHERE QL.LIST_HEADER_ID          = p_price_list_id
      AND QL.LIST_LINE_ID              = QP.LIST_LINE_ID
      AND QP.PRODUCT_ATTRIBUTE_CONTEXT = 'ITEM'
      AND QP.PRODUCT_ATTRIBUTE         = 'PRICING_ATTRIBUTE1'
      AND QP.PRODUCT_ATTR_VALUE        = TO_CHAR(p_inventory_item_id)
      AND trunc(p_price_date) BETWEEN NVL(trunc(QL.START_DATE_ACTIVE),trunc(p_price_date)) AND NVL(trunc(QL.END_DATE_ACTIVE),trunc(p_price_date));
    CURSOR cur_get_second_price
    IS
      SELECT QL.OPERAND
      FROM APPS.QP_LIST_LINES QL,
        APPS.QP_PRICING_ATTRIBUTES QP
      WHERE QL.LIST_HEADER_ID = ANY
        (SELECT list_header_id
        FROM apps.QP_SECONDARY_PRICE_LISTS_V
        WHERE parent_price_list_id = p_price_list_id
        )
    AND QL.LIST_LINE_ID              = QP.LIST_LINE_ID
    AND QP.PRODUCT_ATTRIBUTE_CONTEXT = 'ITEM'
    AND QP.PRODUCT_ATTRIBUTE         = 'PRICING_ATTRIBUTE1'
    AND QP.PRODUCT_ATTR_VALUE        = TO_CHAR(p_inventory_item_id)
    AND trunc(p_price_date) BETWEEN NVL(trunc(QL.START_DATE_ACTIVE),trunc(p_price_date)) AND NVL(trunc(QL.END_DATE_ACTIVE),trunc(p_price_date));
    
    CURSOR cur_seat_pricing
    IS
    SELECT unit_list_price
      FROM oe_order_lines_all oola
     WHERE oola.line_id = p_line_id
       AND EXISTS
        (SELECT 1
           FROM oe_order_price_attribs_v
          WHERE header_id         = oola.header_id
            AND line_id             = oola.line_id
            AND PRICING_CONTEXT     ='FTNT_SEAT_PRICING'
            AND pricing_attribute1 IS NOT NULL
    );
    CURSOR cur_coterm_pricing
    IS
     SELECT oola.unit_list_price       
       FROM oe_order_lines_all oola,
            oe_order_sources os
      WHERE 1=1 
        AND oola.line_id         = p_line_id
        and os.order_source_id   = oola.order_source_id
        AND name                = any('Co-Term with Quote'--,'Renew with Quote'
        );
  BEGIN
    --
    -- Check if the line has pricing Context as FTNT_SEAT_PRICING. #Defect 3345.
    --
    OPEN cur_seat_pricing;
    FETCH cur_seat_pricing INTO lv_list_price;
    CLOSE cur_seat_pricing;
    IF lv_list_price is not null and lv_list_price > 0
    THEN
      RETURN lv_list_price;
    END IF;
    -- #3367
    -- In case of Coterm Pricing, Return the List Price from OM unit Seling Price.
    --    
    OPEN cur_coterm_pricing;
    FETCH cur_coterm_pricing INTO lv_list_price;
    CLOSE cur_coterm_pricing;
    IF lv_list_price is not null and lv_list_price> 0 -- ITS 530643
    THEN
       RETURN lv_list_price;
    END IF;
    -- Modified on 5/31 to alter the conditions.
    OPEN cur_get_list_price;
    FETCH cur_get_list_price INTO lv_list_price;
    CLOSE cur_get_list_price;
    IF lv_list_price IS NULL or lv_list_price = 0  THEN
      OPEN cur_get_second_price;
      FETCH cur_get_second_price INTO lv_list_price;
      CLOSE cur_get_second_price;
    END IF;
    
    RETURN lv_list_price;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN lv_list_price;
  END get_list_price;
  
  FUNCTION get_contry_region(
      p_country IN VARCHAR2)
    RETURN VARCHAR2 RESULT_CACHE RELIES_ON(
      fnd_lookup_values)
  IS
    lv_tag VARCHAR2(100) := NULL;
    CURSOR cur_get_region
    IS
      SELECT flv.attribute15
      FROM apps.fnd_lookup_values flv
      WHERE 1             =1
      AND flv.lookup_type = 'FTNT_REVPRO_REGION'
      AND flv.lookup_code = p_country
      AND enabled_flag    ='Y'
      AND sysdate BETWEEN NVL(start_date_active, sysdate) AND NVL(end_date_active, sysdate+1);
  BEGIN
    OPEN cur_get_region;
    FETCH cur_get_region INTO lv_tag;
    CLOSE cur_get_region;
    RETURN lv_tag;
  END;
  FUNCTION get_region(
      p_line_id IN NUMBER)
    RETURN VARCHAR2
  IS
    lv_tag VARCHAR2(100) := NULL;
    CURSOR cur_get_region
    IS
      SELECT get_contry_region (rcv.country)
      FROM apps.oe_order_lines_all ol,
        XXFT_RPRO_CUSTOMERS_V rcv
      WHERE 1                  =1
      AND ol.line_id           = p_line_id
      AND ol.invoice_to_org_id = rcv.site_use_id
      AND rcv.site_use_code    = 'BILL_TO';
  BEGIN
    /*  select flv.attribute15
    INTO lv_tag
    from apps.oe_order_lines_all ol,
    XXFT_RPRO_CUSTOMERS_V   rcv,
    apps.fnd_lookup_values  flv
    where 1=1
    and ol.line_id= p_line_id
    and ol.invoice_to_org_id = rcv.site_use_id
    and rcv.site_use_code = 'BILL_TO'
    and rcv.country = flv.lookup_code
    and flv.lookup_type = 'FTNT_REVPRO_REGION';  */
    OPEN cur_get_region;
    FETCH cur_get_region INTO lv_tag;
    CLOSE cur_get_region;
    RETURN lv_tag;
  END get_region;
  FUNCTION get_country_grace_period(
      p_country IN VARCHAR2)
    RETURN VARCHAR2 RESULT_CACHE RELIES_ON(
      FND_LOOKUP_VALUES)
  IS
    lv_description VARCHAR2(100) := NULL;
    CURSOR cur_get_grace_period
    IS
      SELECT flv.attribute13
      FROM apps.fnd_lookup_values flv
      WHERE 1             =1
      AND flv.lookup_type = 'FTNT_REVPRO_REGION'
      AND flv.lookup_code = p_country
      AND enabled_flag    ='Y'
      AND sysdate BETWEEN NVL(start_date_active, sysdate) AND NVL(end_date_active, sysdate+1);
  BEGIN
    OPEN cur_get_grace_period;
    FETCH cur_get_grace_period INTO lv_description;
    CLOSE cur_get_grace_period;
    RETURN lv_description;
  END;
  FUNCTION get_country_s_grace_period(
      p_country IN VARCHAR2)
    RETURN VARCHAR2 RESULT_CACHE RELIES_ON(
      FND_LOOKUP_VALUES)
  IS
    lv_description VARCHAR2(100) := NULL;
    CURSOR cur_get_grace_period
    IS
      SELECT flv.attribute12
      FROM apps.fnd_lookup_values flv
      WHERE 1             =1
      AND flv.lookup_type = 'FTNT_REVPRO_REGION'
      AND flv.lookup_code = p_country
      AND enabled_flag    ='Y'
      AND sysdate BETWEEN NVL(start_date_active, sysdate) AND NVL(end_date_active, sysdate+1);
  BEGIN
    OPEN cur_get_grace_period;
    FETCH cur_get_grace_period INTO lv_description;
    CLOSE cur_get_grace_period;
    RETURN lv_description;
  END;
  FUNCTION get_grace_period(
      p_line_id IN NUMBER)
    RETURN NUMBER
  IS
    lv_description VARCHAR2(100) := NULL;
    lv_tag         VARCHAR2(100) := NULL;
    lv_territory   VARCHAR2(100) := NULL;
    lv_geography   VARCHAR2(100) := NULL;
    CURSOR cur_get_grace_pd
    IS
    ---get_country_grace_period
    SELECT get_country_grace_period(rcv.country)
      FROM apps.oe_order_lines_all ol,
           XXFT_RPRO_CUSTOMERS_V rcv
     WHERE 1                  =1
       AND ol.line_id           = p_line_id
       AND ol.invoice_to_org_id = rcv.site_use_id
       AND rcv.site_use_code    = 'BILL_TO';

  BEGIN
    /*select flv.attribute13
    INTO lv_description
    from apps.oe_order_lines_all ol,
    XXFT_RPRO_CUSTOMERS_V   rcv,
    apps.fnd_lookup_values  flv
    where 1=1
    and ol.line_id= p_line_id
    and ol.invoice_to_org_id = rcv.site_use_id
    and rcv.site_use_code = 'BILL_TO'
    and rcv.country = flv.lookup_code
    and flv.lookup_type = 'FTNT_REVPRO_REGION'; */
    OPEN cur_get_grace_pd;
    FETCH cur_get_grace_pd INTO lv_description;
    CLOSE cur_get_grace_pd;
    RETURN lv_description;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN lv_description;
  END get_grace_period;
  FUNCTION get_e_grace_period(
      p_line_id IN NUMBER)
    RETURN NUMBER
  IS
    lv_grace_period NUMBER := 0;
  BEGIN
    SELECT to_date(attribute11,'DD-MON-YY')-NVL(service_start_date,to_date(attribute2,'DD-MON-YY'))
    INTO lv_grace_period
    FROM oe_order_lines_all
    WHERE line_id = p_line_id;
    --          select flv.attribute13
    --          INTO lv_description
    --          from apps.oe_order_lines_all ol,
    --               XXFT_RPRO_CUSTOMERS_V   rcv,
    --               apps.fnd_lookup_values  flv
    --          where 1=1
    --           and ol.line_id= p_line_id
    --           and ol.invoice_to_org_id = rcv.site_use_id
    --           and rcv.site_use_code = 'BILL_TO'
    --           and rcv.country = flv.lookup_code
    --           and flv.lookup_type = 'FTNT_REVPRO_REGION';
    RETURN lv_grace_period;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN lv_grace_period;
  END get_e_grace_period;
  FUNCTION get_s_grace_period(
      p_line_id IN NUMBER)
    RETURN NUMBER
  IS
    lv_description VARCHAR2(100) := NULL;
    lv_tag         VARCHAR2(100) := NULL;
    lv_territory   VARCHAR2(100) := NULL;
    lv_geography   VARCHAR2(100) := NULL;
  BEGIN
    SELECT flv.attribute12
    INTO lv_description
    FROM apps.oe_order_lines_all ol,
      XXFT_RPRO_CUSTOMERS_V rcv,
      apps.fnd_lookup_values flv
    WHERE 1                  =1
    AND ol.line_id           = p_line_id
    AND ol.invoice_to_org_id = rcv.site_use_id
    AND rcv.site_use_code    = 'BILL_TO'
    AND rcv.country          = flv.lookup_code
    AND flv.lookup_type      = 'FTNT_REVPRO_REGION';
    RETURN lv_description;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN lv_description;
  END get_s_grace_period;
  FUNCTION get_grace_period(
      p_attribute44  IN VARCHAR2 ,
      p_product_line IN VARCHAR2 ,
      p_line_id      IN NUMBER)
    RETURN NUMBER
  IS
    lv_description  VARCHAR2(100) := NULL;
    lv_grace_period NUMBER        := 0;
    CURSOR cur_get_s_grace_period
    IS
      SELECT get_country_s_grace_period(rcv.country) --flv.attribute12
      FROM apps.oe_order_lines_all ol,
        XXFT_RPRO_CUSTOMERS_V rcv--,
        --apps.fnd_lookup_values  flv
      WHERE 1                  =1
      AND ol.line_id           = p_line_id
      AND ol.invoice_to_org_id = rcv.site_use_id
      AND rcv.site_use_code    = 'BILL_TO'
        --and rcv.country = flv.lookup_code
        --and flv.lookup_type = 'FTNT_REVPRO_REGION'
        ;
    --
    CURSOR cur_e_grace_period
    IS
    SELECT CASE
            WHEN attribute11 IS NULL
            THEN xxft_rpro_data_extract_pkg.get_line_grace_period(line_id) --/*ITS#553115*/
            ELSE to_date(attribute11,'DD-MON-YY')-NVL(service_start_date,to_date(attribute2,'DD-MON-YY'))
           END grace_period
      FROM oe_order_lines_all
     WHERE line_id = p_line_id;
  BEGIN
    IF p_attribute44 IN ('S_SUPPORT' ,'S_SUBSCRIPTION' ) THEN
      OPEN cur_get_s_grace_period;
      FETCH cur_get_s_grace_period INTO lv_description;
      CLOSE cur_get_s_grace_period;
      RETURN lv_description;
    ELSIF p_attribute44 = 'E_SUPPORT' THEN
      RETURN 0;
    ELSIF p_attribute44 IS NULL AND p_product_line IN ('TIME BASED SOFTWARE (REG)','SAAS SOFTWARE','REGISTERABLE PROFESSIONAL SERVICES') THEN
      OPEN cur_get_s_grace_period;
      FETCH cur_get_s_grace_period INTO lv_description;
      CLOSE cur_get_s_grace_period;
      RETURN lv_description;
    ELSIF p_attribute44 = 'E_SUBSCRIPTION' THEN
      OPEN cur_e_grace_period;
      FETCH cur_e_grace_period INTO lv_grace_period;
      CLOSE cur_e_grace_period;
      RETURN lv_grace_period;
    ELSIF p_product_line IN ('TIME BASED SOFTWARE (REG)','SAAS SOFTWARE','REGISTERABLE PROFESSIONAL SERVICES') THEN
      OPEN cur_get_s_grace_period;
      FETCH cur_get_s_grace_period INTO lv_description;
      CLOSE cur_get_s_grace_period;
      RETURN lv_description;
    END IF;
    RETURN 0;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN 0;
  END get_grace_period;
--Start Bill To
-------------------------------------------------
-- Customer type
  FUNCTION get_customer_type(
      p_cust_account_id IN NUMBER)
    RETURN VARCHAR2 RESULT_CACHE RELIES_ON(
      hz_customer_profiles)
  IS
    lv_customer_type VARCHAR2(100) := NULL;
  BEGIN
    SELECT hcpc.name
    INTO lv_customer_type
    FROM apps.hz_customer_profiles hcp,
      apps.hz_cust_profile_classes hcpc
    WHERE hcp.cust_account_id = p_cust_account_id
    AND hcp.profile_class_id  = hcpc.profile_class_id
    AND rownum                < 2;
    RETURN lv_customer_type;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN lv_customer_type;
  END get_customer_type;
--End customer
  FUNCTION get_end_cust_name(
      p_header_id IN NUMBER)
    RETURN VARCHAR2
  IS
    lv_party_name VARCHAR2(100) := NULL;
  BEGIN
    SELECT hp.party_name
    INTO lv_party_name
    FROM apps.oe_order_headers_all oe,
      apps.hz_cust_accounts hca,
      apps.hz_parties hp
    WHERE oe.header_id     = p_header_id
    AND oe.end_customer_id = hca.cust_account_id
    AND hca.party_id       = hp.party_id;
    RETURN lv_party_name;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN lv_party_name;
  END get_end_cust_name;
  FUNCTION get_end_cust_number(
      p_header_id IN NUMBER)
    RETURN VARCHAR2
  IS
    lv_party_number VARCHAR2(100) := NULL;
  BEGIN
    SELECT hca.account_number
    INTO lv_party_number
    FROM apps.oe_order_headers_all oe,
      apps.hz_cust_accounts hca,
      apps.hz_parties hp
    WHERE oe.header_id     = p_header_id
    AND oe.end_customer_id = hca.cust_account_id
    AND hca.party_id       = hp.party_id;
    RETURN lv_party_number;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN lv_party_number;
  END get_end_cust_number;
  FUNCTION get_end_cust_state(
      p_header_id IN NUMBER)
    RETURN VARCHAR2
  IS
    lv_party_state VARCHAR2(100) := NULL;
  BEGIN
    SELECT hp.state
    INTO lv_party_state
    FROM apps.oe_order_headers_all oe,
      apps.hz_cust_accounts hca,
      apps.hz_parties hp
    WHERE oe.header_id     = p_header_id
    AND oe.end_customer_id = hca.cust_account_id
    AND hca.party_id       = hp.party_id;
    RETURN lv_party_state;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN lv_party_state;
  END get_end_cust_state;
  FUNCTION get_end_cust_zip(
      p_header_id IN NUMBER)
    RETURN VARCHAR2
  IS
    lv_party_zip VARCHAR2(100) := NULL;
  BEGIN
    SELECT hp.postal_code
    INTO lv_party_zip
    FROM apps.oe_order_headers_all oe,
      apps.hz_cust_accounts hca,
      apps.hz_parties hp
    WHERE oe.header_id     = p_header_id
    AND oe.end_customer_id = hca.cust_account_id
    AND hca.party_id       = hp.party_id;
    RETURN lv_party_zip;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN lv_party_zip;
  END get_end_cust_zip;
--End Customer
  FUNCTION get_customer_flag(
      p_line_id IN NUMBER)
    RETURN VARCHAR2
  IS
    lv_count NUMBER      := 0;
    l_status VARCHAR2(1) := 'N';
  BEGIN
   /* SELECT COUNT(*)
    INTO lv_count
    FROM apps.oe_order_lines_all ol,
      apps.hz_cust_site_uses_all hcsu,
      apps.ra_terms_b tb,
      apps.ra_terms_lines tl
    WHERE ol.line_id         = p_line_id
    AND ol.invoice_to_org_id = hcsu.site_use_id
    AND hcsu.payment_term_id = tb.term_id
    AND tb.term_id           = tl.term_id
    AND tl.due_days          = '0';*/ --Commented by NTT ITS#540806
	SELECT count(*)
    INTO lv_count
    FROM apps.oe_order_lines_all ol,
      apps.hz_cust_site_uses_all hcsua,
      apps.hz_cust_acct_sites_all hcasa,
      apps.hz_cust_accounts_all hca
    WHERE ol.line_id         = p_line_id
    AND ol.invoice_to_org_id = hcsua.site_use_id
    AND hcsua.cust_acct_site_id = hcasa.cust_acct_site_id
    AND hcasa.cust_account_id = hca.cust_account_id
    AND UPPER(NVL(hca.attribute6,'N')) ='Y'; --Added by NTT ITS#540806
	
    IF lv_count              > 0 THEN
      l_status              :='Y';
    END IF ;
    RETURN l_status;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN l_status;
  END get_customer_flag;
  FUNCTION get_no_days(
      p_line_id IN NUMBER)
    RETURN NUMBER
  IS
    lv_days NUMBER := 0;
  BEGIN
    SELECT tl.DUE_DAYS
    INTO lv_days
    FROM apps.oe_order_lines_all ol,
      apps.ra_terms_b tb,
      apps.ra_terms_lines tl
    WHERE 1                =1
    AND ol.line_id         = p_line_id
    AND ol.payment_term_id = tb.term_id
    AND tb.term_id         = tl.term_id;
    RETURN lv_days;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN lv_days;
  END get_no_days;
  FUNCTION get_spr_number(
      p_header_id IN NUMBER,
      p_line_id   IN NUMBER)
    RETURN VARCHAR2
  IS
    l_spr_name VARCHAR2(100) := NULL;
  BEGIN
    SELECT TL.DESCRIPTION
    INTO l_spr_name
    FROM apps.OE_PRICE_ADJUSTMENTS OE,
      apps.QP_LIST_HEADERS_ALL_B QP,
      apps.QP_LIST_HEADERS_TL TL
    WHERE 1               =1
    AND OE.header_id      = p_header_id
    AND OE.LINE_ID        = p_line_id
    AND QP.LIST_HEADER_ID = OE.LIST_HEADER_ID
    AND TL.LIST_HEADER_ID = QP.LIST_HEADER_ID
    AND QP.CONTEXT        = 'SPR'
    AND TL.LANGUAGE       = 'US'
    AND TL.SOURCE_LANG    = 'US';
    /*SELECT TL.DESCRIPTION
    INTO l_spr_name
    FROM apps.OE_PRICE_ADJUSTMENTS OE,
    apps.QP_LIST_HEADERS_ALL_B QP,
    apps.QP_LIST_HEADERS_TL TL,
    apps.OZF_OFFERS OZF
    WHERE QP.LIST_HEADER_ID = OE.LIST_HEADER_ID
    and QP.LIST_HEADER_ID = OZF.QP_LIST_HEADER_ID
    and QP.LIST_HEADER_ID = TL.LIST_HEADER_ID
    and QP.CONTEXT = 'SPR'
    and TL.LANGUAGE = 'US'
    and TL.SOURCE_LANG = 'US'
    and OE.header_id = p_header_id
    AND OE.LINE_ID   = p_line_id; */
    RETURN l_spr_name;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN l_spr_name;
  END get_spr_number;
  FUNCTION get_opportunity(
      p_header_id IN NUMBER,
      p_line_id   IN NUMBER)
    RETURN VARCHAR2
  IS
    l_opportunity VARCHAR2(100) := NULL;
  BEGIN
    SELECT QP.ATTRIBUTE1
    INTO l_opportunity
    FROM apps.OE_PRICE_ADJUSTMENTS OE,
      apps.QP_LIST_HEADERS_ALL_B QP,
      apps.QP_LIST_HEADERS_TL TL,
      apps.OZF_OFFERS OZF
    WHERE QP.LIST_HEADER_ID = OE.LIST_HEADER_ID
    AND QP.LIST_HEADER_ID   = OZF.QP_LIST_HEADER_ID
    AND QP.LIST_HEADER_ID   = TL.LIST_HEADER_ID
    AND QP.CONTEXT          = 'SPR'
    AND TL.LANGUAGE         = 'US'
    AND TL.SOURCE_LANG      = 'US'
    AND OE.header_id        = p_header_id
    AND OE.LINE_ID          = p_line_id;
    RETURN l_opportunity;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN l_opportunity;
  END get_opportunity;
  FUNCTION get_bill_to_territory(
      p_line_id IN NUMBER)
    RETURN VARCHAR2
  IS
    lv_territory VARCHAR2(100) := NULL;
  BEGIN
    SELECT ftt.territory_short_name
    INTO lv_territory
    FROM apps.oe_order_lines_all ol,
      XXFT_RPRO_CUSTOMERS_V rcv,
      apps.fnd_territories_tl ftt
    WHERE 1                  =1
    AND ol.line_id           = p_line_id
    AND ol.invoice_to_org_id = rcv.site_use_id
    AND rcv.site_use_code    = 'BILL_TO'
    AND rcv.country          = ftt.territory_code;
    RETURN lv_territory;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN lv_territory;
  END get_bill_to_territory;
---------------------------------
  FUNCTION get_bill_to_geo(
      p_line_id IN NUMBER)
    RETURN VARCHAR2
  IS
    lv_geography VARCHAR2(100) := NULL;
  BEGIN
    SELECT hg.geography_name
    INTO lv_geography
    FROM apps.oe_order_lines_all ol,
      XXFT_RPRO_CUSTOMERS_V rcv,
      apps.hz_geographies hg
    WHERE 1                  =1
    AND ol.line_id           = p_line_id
    AND ol.invoice_to_org_id = rcv.site_use_id
    AND rcv.site_use_code    = 'BILL_TO'
    AND rcv.country          = hg.geography_code
    AND hg.geography_type    = 'COUNTRY';
    RETURN lv_geography;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN lv_geography;
  END get_bill_to_geo;
------------------------------------------------------
  FUNCTION get_bill_to_state(
      p_line_id IN NUMBER)
    RETURN VARCHAR2
  IS
    lv_state VARCHAR2(100) := NULL;
  BEGIN
    SELECT rcv.state
    INTO lv_state
    FROM apps.oe_order_lines_all ol,
      XXFT_RPRO_CUSTOMERS_V rcv
    WHERE 1                  =1
    AND ol.line_id           = p_line_id
    AND ol.invoice_to_org_id = rcv.site_use_id
    AND rcv.site_use_code    = 'BILL_TO';
    RETURN lv_state;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN lv_state;
  END get_bill_to_state;
------Ended Bill To---------------------------------
------------------------------------------------------
--Start Ship To
-------------------------------------------------
  FUNCTION get_ship_to_territory(
      p_line_id IN NUMBER)
    RETURN VARCHAR2
  IS
    lv_ship_territory VARCHAR2(100) := NULL;
  BEGIN
    SELECT ftt.territory_short_name
    INTO lv_ship_territory
    FROM apps.oe_order_lines_all ol,
      XXFT_RPRO_CUSTOMERS_V rcv,
      apps.fnd_territories_tl ftt
    WHERE 1               =1
    AND ol.line_id        = p_line_id
    AND ol.ship_to_org_id = rcv.site_use_id
    AND rcv.site_use_code = 'SHIP_TO'
    AND rcv.country       = ftt.territory_code;
    RETURN lv_ship_territory;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN lv_ship_territory;
  END get_ship_to_territory;
---------------------------------
  FUNCTION get_ship_to_geo(
      p_line_id IN NUMBER)
    RETURN VARCHAR2
  IS
    lv_ship_geography VARCHAR2(100) := NULL;
  BEGIN
    SELECT hg.geography_name
    INTO lv_ship_geography
    FROM apps.oe_order_lines_all ol,
      XXFT_RPRO_CUSTOMERS_V rcv,
      apps.hz_geographies hg
    WHERE 1               =1
    AND ol.line_id        = p_line_id
    AND ol.ship_to_org_id = rcv.site_use_id
    AND rcv.site_use_code = 'SHIP_TO'
    AND rcv.country       = hg.geography_code
    AND hg.geography_type = 'COUNTRY';
    RETURN lv_ship_geography;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN lv_ship_geography;
  END get_ship_to_geo;
------------------------------------------------------
  FUNCTION get_ship_to_state(
      p_line_id IN NUMBER)
    RETURN VARCHAR2
  IS
    lv_ship_state VARCHAR2(100) := NULL;
  BEGIN
    SELECT rcv.state
    INTO lv_ship_state
    FROM apps.oe_order_lines_all ol,
      XXFT_RPRO_CUSTOMERS_V rcv
    WHERE 1               =1
    AND ol.line_id        = p_line_id
    AND ol.ship_to_org_id = rcv.site_use_id
    AND rcv.site_use_code = 'SHIP_TO';
    RETURN lv_ship_state;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN lv_ship_state;
  END get_ship_to_state;
------------------------------------------------------End ship To------
  FUNCTION get_ac_segment1(
      p_ac_type IN VARCHAR2 ,
      p_line_id IN NUMBER)
    RETURN VARCHAR2
  IS
    lv_segement1 VARCHAR2(100) := NULL;
    lv_ac_type   VARCHAR2(100) := NULL;
  BEGIN
    lv_ac_type := p_ac_type;
    IF lv_ac_type IN ('REV','COGS') THEN
      SELECT GL.segment1
      INTO lv_segement1
      FROM apps.OE_ORDER_HEADERS_ALL OE,
        apps.OE_ORDER_LINES_ALL OL ,
        apps.OE_TRANSACTION_TYPES_ALL OET,
        apps.RA_CUST_TRX_TYPES_ALL RA,
        apps.GL_CODE_COMBINATIONS GL
      WHERE 1                  =1
      AND ol.line_id           = p_line_id
      AND ol.header_id         = OE.header_id
      AND OE.order_type_id     = OET.TRANSACTION_TYPE_ID
      AND OET.cust_trx_type_id = RA.CUST_TRX_TYPE_ID
      AND RA.GL_ID_REV         = Gl.CODE_COMBINATION_ID;
    ELSIF lv_ac_type           = 'UNEARN' THEN
      SELECT GL.segment1
      INTO lv_segement1
      FROM apps.OE_ORDER_HEADERS_ALL OE,
        apps.OE_ORDER_LINES_ALL OL ,
        apps.OE_TRANSACTION_TYPES_ALL OET,
        apps.RA_CUST_TRX_TYPES_ALL RA,
        apps.GL_CODE_COMBINATIONS GL
      WHERE 1                  =1
      AND ol.line_id           = p_line_id
      AND ol.header_id         = OE.header_id
      AND OE.order_type_id     = OET.TRANSACTION_TYPE_ID
      AND OET.cust_trx_type_id = RA.CUST_TRX_TYPE_ID
      AND RA.GL_ID_UNEARNED    = Gl.CODE_COMBINATION_ID;
    ELSIF lv_ac_type           = 'DEF' THEN
      /* Defect #3302 to pick the balancing segment from the selling ou default warehouse  */
      SELECT GCC.SEGMENT1
      INTO lv_segement1
      FROM apps.OE_ORDER_LINES_ALL OL,
           apps.oe_order_headers_all oh,
           apps.oe_transaction_types_all ott,
           apps.MTL_PARAMETERS MP,
           apps.GL_CODE_COMBINATIONS GCC
    WHERE OL.line_id             = p_line_id
      AND oh.header_id             = ol.header_id
      AND ott.transaction_type_id  = oh.order_type_id
      AND MP.ORGANIZATION_ID       = ott.warehouse_id
      AND MP.deferred_cogs_account = GCC.CODE_COMBINATION_ID;
      
      /* -- Defect #3302
      SELECT GCC.SEGMENT1
      INTO lv_segement1
      FROM apps.OE_ORDER_LINES_ALL OL,
        apps.MTL_PARAMETERS MP,
        apps.GL_CODE_COMBINATIONS GCC
      WHERE OL.LINE_ID             = p_line_id
      AND OL.ship_from_org_id      = MP.ORGANIZATION_ID
      AND MP.deferred_cogs_account = GCC.CODE_COMBINATION_ID; */
      
    END IF;
    RETURN lv_segement1;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN lv_segement1;
  END get_ac_segment1;
  FUNCTION get_ac_segment2(
      p_ac_type IN VARCHAR2 ,
      p_line_id IN NUMBER)
    RETURN VARCHAR2
  IS
    lv_segement2 VARCHAR2(100) := NULL;
    lv_ac_type   VARCHAR2(100) := NULL;
  BEGIN
    lv_ac_type := p_ac_type;
    IF lv_ac_type IN ('REV','COGS') THEN
      SELECT GL.segment2
      INTO lv_segement2
      FROM apps.OE_ORDER_HEADERS_ALL OE,
        apps.OE_ORDER_LINES_ALL OL ,
        apps.OE_TRANSACTION_TYPES_ALL OET,
        apps.RA_CUST_TRX_TYPES_ALL RA,
        apps.GL_CODE_COMBINATIONS GL
      WHERE 1                  =1
      AND ol.line_id           = p_line_id
      AND ol.header_id         = OE.header_id
      AND OE.order_type_id     = OET.TRANSACTION_TYPE_ID
      AND OET.cust_trx_type_id = RA.CUST_TRX_TYPE_ID
      AND RA.GL_ID_REV         = Gl.CODE_COMBINATION_ID;
    ELSIF lv_ac_type           = 'UNEARN' THEN
      SELECT GL.segment2
      INTO lv_segement2
      FROM apps.OE_ORDER_HEADERS_ALL OE,
        apps.OE_ORDER_LINES_ALL OL ,
        apps.OE_TRANSACTION_TYPES_ALL OET,
        apps.RA_CUST_TRX_TYPES_ALL RA,
        apps.GL_CODE_COMBINATIONS GL
      WHERE 1                  =1
      AND ol.line_id           = p_line_id
      AND ol.header_id         = OE.header_id
      AND OE.order_type_id     = OET.TRANSACTION_TYPE_ID
      AND OET.cust_trx_type_id = RA.CUST_TRX_TYPE_ID
      AND RA.GL_ID_UNEARNED    = Gl.CODE_COMBINATION_ID;
    ELSIF lv_ac_type           = 'DEF' THEN
    
      SELECT GCC.SEGMENT2
      INTO lv_segement2
      FROM apps.OE_ORDER_LINES_ALL OL,
        apps.MTL_PARAMETERS MP,
        apps.GL_CODE_COMBINATIONS GCC
      WHERE OL.LINE_ID             = p_line_id
      AND OL.ship_from_org_id      = MP.ORGANIZATION_ID
      AND MP.deferred_cogs_account = GCC.CODE_COMBINATION_ID;
      
    ELSIF lv_ac_type               = 'UNEARN' THEN
      SELECT GL.segment2
      INTO lv_segement2
      FROM apps.OE_ORDER_HEADERS_ALL OE,
        apps.OE_ORDER_LINES_ALL OL ,
        apps.OE_TRANSACTION_TYPES_ALL OET,
        apps.RA_CUST_TRX_TYPES_ALL RA,
        apps.GL_CODE_COMBINATIONS GL
      WHERE 1                  =1
      AND ol.line_id           = p_line_id
      AND ol.header_id         = OE.header_id
      AND OE.order_type_id     = OET.TRANSACTION_TYPE_ID
      AND OET.cust_trx_type_id = RA.CUST_TRX_TYPE_ID
      AND RA.GL_ID_UNEARNED    = Gl.CODE_COMBINATION_ID;
    END IF;
    RETURN lv_segement2;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN lv_segement2;
  END get_ac_segment2;
  FUNCTION get_ac_segment3(
      p_ac_type IN VARCHAR2 ,
      p_line_id IN NUMBER)
    RETURN VARCHAR2
  IS
    lv_segement3 VARCHAR2(100) := NULL;
    lv_ac_type   VARCHAR2(100) := NULL;
  BEGIN
    lv_ac_type := p_ac_type;
    IF lv_ac_type IN ('REV','COGS') THEN
      SELECT flv.attribute14
      INTO lv_segement3
      FROM apps.oe_order_lines_all ol,
        XXFT_RPRO_CUSTOMERS_V rcv,
        apps.fnd_lookup_values flv
      WHERE 1                  =1
      AND ol.line_id           = p_line_id
      AND ol.invoice_to_org_id = rcv.site_use_id
      AND rcv.site_use_code    = 'BILL_TO'
      AND rcv.country          = flv.lookup_code
      AND flv.lookup_type      = 'FTNT_REVPRO_REGION';
    ELSIF lv_ac_type           = 'DEF' THEN
      SELECT GCC.SEGMENT3
      INTO lv_segement3
      FROM apps.OE_ORDER_LINES_ALL OL,
        apps.MTL_PARAMETERS MP,
        apps.GL_CODE_COMBINATIONS GCC
      WHERE OL.LINE_ID             = p_line_id
      AND OL.ship_from_org_id      = MP.ORGANIZATION_ID
      AND MP.deferred_cogs_account = GCC.CODE_COMBINATION_ID;
    ELSIF lv_ac_type               = 'UNEARN' THEN
      SELECT GL.segment3
      INTO lv_segement3
      FROM apps.OE_ORDER_HEADERS_ALL OE,
        apps.OE_ORDER_LINES_ALL OL ,
        apps.OE_TRANSACTION_TYPES_ALL OET,
        apps.RA_CUST_TRX_TYPES_ALL RA,
        apps.GL_CODE_COMBINATIONS GL
      WHERE 1                  =1
      AND ol.line_id           = p_line_id
      AND ol.header_id         = OE.header_id
      AND OE.order_type_id     = OET.TRANSACTION_TYPE_ID
      AND OET.cust_trx_type_id = RA.CUST_TRX_TYPE_ID
      AND RA.GL_ID_UNEARNED    = Gl.CODE_COMBINATION_ID;
    END IF;
    RETURN lv_segement3;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN lv_segement3;
  END get_ac_segment3;
  FUNCTION get_ac_segment4(
      p_ac_type IN VARCHAR2 ,
      p_line_id IN NUMBER)
    RETURN VARCHAR2
  IS
    lv_segement4 VARCHAR2(100) := NULL;
    lv_ac_type   VARCHAR2(100) := NULL;
  BEGIN
    lv_ac_type := p_ac_type;
    IF lv_ac_type IN ('REV','COGS') THEN
      SELECT GL.segment4
      INTO lv_segement4
      FROM apps.OE_ORDER_HEADERS_ALL OE,
        apps.OE_ORDER_LINES_ALL OL ,
        apps.OE_TRANSACTION_TYPES_ALL OET,
        apps.RA_CUST_TRX_TYPES_ALL RA,
        apps.GL_CODE_COMBINATIONS GL
      WHERE 1                  =1
      AND ol.line_id           = p_line_id
      AND ol.header_id         = OE.header_id
      AND OE.order_type_id     = OET.TRANSACTION_TYPE_ID
      AND OET.cust_trx_type_id = RA.CUST_TRX_TYPE_ID
      AND RA.GL_ID_REV         = Gl.CODE_COMBINATION_ID;
    ELSIF lv_ac_type           = 'UNEARN' THEN
      SELECT GL.segment4
      INTO lv_segement4
      FROM apps.OE_ORDER_HEADERS_ALL OE,
        apps.OE_ORDER_LINES_ALL OL ,
        apps.OE_TRANSACTION_TYPES_ALL OET,
        apps.RA_CUST_TRX_TYPES_ALL RA,
        apps.GL_CODE_COMBINATIONS GL
      WHERE 1                  =1
      AND ol.line_id           = p_line_id
      AND ol.header_id         = OE.header_id
      AND OE.order_type_id     = OET.TRANSACTION_TYPE_ID
      AND OET.cust_trx_type_id = RA.CUST_TRX_TYPE_ID
      AND RA.GL_ID_UNEARNED    = Gl.CODE_COMBINATION_ID;
    ELSIF lv_ac_type           = 'DEF' THEN
      SELECT GCC.SEGMENT4
      INTO lv_segement4
      FROM apps.OE_ORDER_LINES_ALL OL,
        apps.MTL_PARAMETERS MP,
        apps.GL_CODE_COMBINATIONS GCC
      WHERE OL.LINE_ID             = p_line_id
      AND OL.ship_from_org_id      = MP.ORGANIZATION_ID
      AND MP.deferred_cogs_account = GCC.CODE_COMBINATION_ID;
    END IF;
    RETURN lv_segement4;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN lv_segement4;
  END get_ac_segment4;
  FUNCTION get_ac_segment5(
      p_ac_type IN VARCHAR2 ,
      p_line_id IN NUMBER)
    RETURN VARCHAR2
  IS
    lv_segement5 VARCHAR2(100) := NULL;
    lv_ac_type   VARCHAR2(100) := NULL;
  BEGIN
    lv_ac_type   := p_ac_type;
    IF lv_ac_type = 'REV' THEN
      SELECT gl.segment5
      INTO lv_segement5
      FROM apps.oe_order_lines_all ol,
        apps.mtl_system_items_b msi,
        apps.gl_code_combinations gl
      WHERE ol.line_id         = p_line_id
      AND ol.inventory_item_id = msi.inventory_item_id
      AND ol.ship_from_org_id  = msi.organization_id
      AND msi.sales_account    = gl.code_combination_id;
    ELSIF lv_ac_type           ='COGS' THEN
      SELECT gl.segment5
      INTO lv_segement5
      FROM apps.oe_order_lines_all ol,
        apps.mtl_system_items_b msi,
        apps.gl_code_combinations gl
      WHERE ol.line_id              = p_line_id
      AND ol.inventory_item_id      = msi.inventory_item_id
      AND ol.ship_from_org_id       = msi.organization_id
      AND msi.cost_of_sales_account = gl.code_combination_id;
    ELSIF lv_ac_type                = 'UNEARN' THEN
      SELECT GL.segment5
      INTO lv_segement5
      FROM apps.OE_ORDER_HEADERS_ALL OE,
        apps.OE_ORDER_LINES_ALL OL ,
        apps.OE_TRANSACTION_TYPES_ALL OET,
        apps.RA_CUST_TRX_TYPES_ALL RA,
        apps.GL_CODE_COMBINATIONS GL
      WHERE 1                  =1
      AND ol.line_id           = p_line_id
      AND ol.header_id         = OE.header_id
      AND OE.order_type_id     = OET.TRANSACTION_TYPE_ID
      AND OET.cust_trx_type_id = RA.CUST_TRX_TYPE_ID
      AND RA.GL_ID_UNEARNED    = Gl.CODE_COMBINATION_ID;
    ELSIF lv_ac_type           = 'DEF' THEN
      SELECT GCC.SEGMENT5
      INTO lv_segement5
      FROM apps.OE_ORDER_LINES_ALL OL,
        apps.MTL_PARAMETERS MP,
        apps.GL_CODE_COMBINATIONS GCC
      WHERE OL.LINE_ID             = p_line_id
      AND OL.ship_from_org_id      = MP.ORGANIZATION_ID
      AND MP.deferred_cogs_account = GCC.CODE_COMBINATION_ID;
    END IF;
    RETURN lv_segement5;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN lv_segement5;
  END get_ac_segment5;
  FUNCTION get_ac_segment6(
      p_ac_type IN VARCHAR2 ,
      p_line_id IN NUMBER)
    RETURN VARCHAR2
  IS
    lv_segement6 VARCHAR2(100) := NULL;
    lv_ac_type   VARCHAR2(100) := NULL;
  BEGIN
    lv_ac_type := p_ac_type;
    IF lv_ac_type IN ('REV','COGS') THEN
      SELECT GL.segment6
      INTO lv_segement6
      FROM apps.OE_ORDER_HEADERS_ALL OE,
        apps.OE_ORDER_LINES_ALL OL ,
        apps.OE_TRANSACTION_TYPES_ALL OET,
        apps.RA_CUST_TRX_TYPES_ALL RA,
        apps.GL_CODE_COMBINATIONS GL
      WHERE 1                  =1
      AND ol.line_id           = p_line_id
      AND ol.header_id         = OE.header_id
      AND OE.order_type_id     = OET.TRANSACTION_TYPE_ID
      AND OET.cust_trx_type_id = RA.CUST_TRX_TYPE_ID
      AND RA.GL_ID_REV         = Gl.CODE_COMBINATION_ID;
    ELSIF lv_ac_type           = 'UNEARN' THEN
      SELECT GL.segment6
      INTO lv_segement6
      FROM apps.OE_ORDER_HEADERS_ALL OE,
        apps.OE_ORDER_LINES_ALL OL ,
        apps.OE_TRANSACTION_TYPES_ALL OET,
        apps.RA_CUST_TRX_TYPES_ALL RA,
        apps.GL_CODE_COMBINATIONS GL
      WHERE 1                  =1
      AND ol.line_id           = p_line_id
      AND ol.header_id         = OE.header_id
      AND OE.order_type_id     = OET.TRANSACTION_TYPE_ID
      AND OET.cust_trx_type_id = RA.CUST_TRX_TYPE_ID
      AND RA.GL_ID_UNEARNED    = Gl.CODE_COMBINATION_ID;
    ELSIF lv_ac_type           = 'DEF' THEN
      SELECT GCC.SEGMENT6
      INTO lv_segement6
      FROM apps.OE_ORDER_LINES_ALL OL,
        apps.MTL_PARAMETERS MP,
        apps.GL_CODE_COMBINATIONS GCC
      WHERE OL.LINE_ID             = p_line_id
      AND OL.ship_from_org_id      = MP.ORGANIZATION_ID
      AND MP.deferred_cogs_account = GCC.CODE_COMBINATION_ID;
    END IF;
    RETURN lv_segement6;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN lv_segement6;
  END get_ac_segment6;
  FUNCTION get_ac_segment7(
      p_ac_type IN VARCHAR2 ,
      p_line_id IN NUMBER)
    RETURN VARCHAR2
  IS
    lv_segement7 VARCHAR2(100) := NULL;
    lv_ac_type   VARCHAR2(100) := NULL;
  BEGIN
    lv_ac_type := p_ac_type;
    IF lv_ac_type IN ('REV','COGS') THEN
      SELECT GL.segment7
      INTO lv_segement7
      FROM apps.OE_ORDER_HEADERS_ALL OE,
        apps.OE_ORDER_LINES_ALL OL ,
        apps.OE_TRANSACTION_TYPES_ALL OET,
        apps.RA_CUST_TRX_TYPES_ALL RA,
        apps.GL_CODE_COMBINATIONS GL
      WHERE 1                  =1
      AND ol.line_id           = p_line_id
      AND ol.header_id         = OE.header_id
      AND OE.order_type_id     = OET.TRANSACTION_TYPE_ID
      AND OET.cust_trx_type_id = RA.CUST_TRX_TYPE_ID
      AND RA.GL_ID_REV         = Gl.CODE_COMBINATION_ID;
    ELSIF lv_ac_type           = 'UNEARN' THEN
      SELECT GL.segment7
      INTO lv_segement7
      FROM apps.OE_ORDER_HEADERS_ALL OE,
        apps.OE_ORDER_LINES_ALL OL ,
        apps.OE_TRANSACTION_TYPES_ALL OET,
        apps.RA_CUST_TRX_TYPES_ALL RA,
        apps.GL_CODE_COMBINATIONS GL
      WHERE 1                  =1
      AND ol.line_id           = p_line_id
      AND ol.header_id         = OE.header_id
      AND OE.order_type_id     = OET.TRANSACTION_TYPE_ID
      AND OET.cust_trx_type_id = RA.CUST_TRX_TYPE_ID
      AND RA.GL_ID_UNEARNED    = Gl.CODE_COMBINATION_ID;
    ELSIF lv_ac_type           = 'DEF' THEN
      SELECT GCC.SEGMENT7
      INTO lv_segement7
      FROM apps.OE_ORDER_LINES_ALL OL,
        apps.MTL_PARAMETERS MP,
        apps.GL_CODE_COMBINATIONS GCC
      WHERE OL.LINE_ID             = p_line_id
      AND OL.ship_from_org_id      = MP.ORGANIZATION_ID
      AND MP.deferred_cogs_account = GCC.CODE_COMBINATION_ID;
    END IF;
    RETURN lv_segement7;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN lv_segement7;
  END get_ac_segment7;
--- Get long term accounts
  FUNCTION get_lt_deff_rev
    RETURN VARCHAR2
  IS
    lv_lt_deff_rev VARCHAR2(100) := NULL;
  BEGIN
    SELECT flv.meaning
    INTO lv_lt_deff_rev
    FROM apps.fnd_lookup_values flv
    WHERE 1             =1
    AND flv.lookup_type = 'FTNT_REVPRO_LT_ACCOUNTS'
    AND flv.lookup_code = 'LT_DEFF_REV';
    RETURN lv_lt_deff_rev;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN lv_lt_deff_rev;
  END get_lt_deff_rev;
  FUNCTION get_lt_deff_cogs
    RETURN VARCHAR2
  IS
    lv_lt_deff_cogs VARCHAR2(100) := NULL;
  BEGIN
    SELECT flv.meaning
    INTO lv_lt_deff_cogs
    FROM apps.fnd_lookup_values flv
    WHERE 1             =1
    AND flv.lookup_type = 'FTNT_REVPRO_LT_ACCOUNTS'
    AND flv.lookup_code = 'LT_DEFF_COGS';
    RETURN lv_lt_deff_cogs;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN lv_lt_deff_cogs;
  END get_lt_deff_cogs;
--- Get long term accounts
  FUNCTION un_billed_act
    RETURN VARCHAR2
  IS
    lv_un_billed_act VARCHAR2(100) := NULL;
  BEGIN
    SELECT flv.meaning
    INTO lv_un_billed_act
    FROM apps.fnd_lookup_values flv
    WHERE 1             =1
    AND flv.lookup_type = 'FTNT_REVPRO_LT_ACCOUNTS'
    AND flv.lookup_code = 'UN_BILLED_ACT';
    RETURN lv_un_billed_act;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN lv_un_billed_act;
  END un_billed_act;
  FUNCTION get_product_family(
      p_inventory_item_id IN NUMBER,
      p_org_id            IN NUMBER)
    RETURN VARCHAR2 RESULT_CACHE RELIES_ON(
      MTL_ITEM_CATEGORIES)
  IS
    lv_segment VARCHAR2(100) := NULL;
    CURSOR cur_get_segment4
    IS
      SELECT mc.segment4
      FROM INV.MTL_ITEM_CATEGORIES mic,
        mtl_categories mc
      WHERE mic.inventory_item_id = p_inventory_item_id
      AND mic.organization_id     = p_org_id
      AND mic.category_set_id     = get_rev_cat_set_id
      AND mc.category_id          = MIC.CATEGORY_ID;
  BEGIN
    OPEN cur_get_segment4;
    FETCH cur_get_segment4 INTO lv_segment;
    CLOSE cur_get_segment4;
    RETURN lv_segment;
    /*  select mic.segment4
    into lv_segment
    from apps.MTL_ITEM_CATEGORIES_V mic
    where 1=1
    and mic.organization_id   = p_org_id
    and mic.inventory_item_id = p_inventory_item_id
    and mic.category_set_name = 'FTNT_REVENUE_CATEGORY';
    RETURN lv_segment; */
  END get_product_family;
  FUNCTION get_product_category(
      p_inventory_item_id IN NUMBER,
      p_org_id            IN NUMBER)
    RETURN VARCHAR2 RESULT_CACHE RELIES_ON(
      MTL_ITEM_CATEGORIES)
  IS
    lv_segment VARCHAR2(100) := NULL;
    CURSOR cur_get_segment6
    IS
      SELECT mc.segment6
      FROM INV.MTL_ITEM_CATEGORIES mic,
        mtl_categories mc
      WHERE mic.inventory_item_id = p_inventory_item_id
      AND mic.organization_id     = p_org_id
      AND mic.category_set_id     = get_rev_cat_set_id
      AND mc.category_id          = MIC.CATEGORY_ID;
  BEGIN
    OPEN cur_get_segment6;
    FETCH cur_get_segment6 INTO lv_segment;
    CLOSE cur_get_segment6;
    RETURN lv_segment;
    /* select mic.segment6
    into lv_segment
    from apps.MTL_ITEM_CATEGORIES_V mic
    where 1=1
    and mic.organization_id   = p_org_id
    and mic.inventory_item_id = p_inventory_item_id
    and mic.category_set_name = 'FTNT_REVENUE_CATEGORY'; */
  END get_product_category;
/*
FUNCTION get_product_line(p_inventory_item_id IN NUMBER,p_org_id IN NUMBER)
RETURN VARCHAR2 IS
lv_segment VARCHAR2(100) := Null;
BEGIN
select mic.segment1
into lv_segment
from apps.MTL_ITEM_CATEGORIES_V mic
where 1=1
and mic.organization_id   = p_org_id
and mic.inventory_item_id = p_inventory_item_id
and mic.category_set_name = 'FTNT_REVENUE_CATEGORY';
RETURN lv_segment;
EXCEPTION
WHEN OTHERS THEN
RETURN  lv_segment;
END get_product_line;*/
  FUNCTION get_product_class(
      p_inventory_item_id IN NUMBER,
      p_org_id            IN NUMBER)
    RETURN VARCHAR2 RESULT_CACHE RELIES_ON(
      MTL_ITEM_CATEGORIES)
  IS
    lv_segment VARCHAR2(100) := NULL;
    CURSOR cur_get_segment3
    IS
      SELECT mc.segment3
      FROM INV.MTL_ITEM_CATEGORIES mic,
        mtl_categories mc
      WHERE mic.inventory_item_id = p_inventory_item_id
      AND mic.organization_id     = g_master_org_id -- fnd_profile.value('AMS_ITEM_ORGANIZATION_ID')
      AND mic.category_set_id     = get_rep_cat_set_id
      AND mc.category_id          = MIC.CATEGORY_ID;
  BEGIN
    OPEN cur_get_segment3;
    FETCH cur_get_segment3 INTO lv_segment;
    CLOSE cur_get_segment3;
    RETURN lv_segment;
    /* select mic.segment3 --mic.segment2  --changed based on james/karen
    into lv_segment
    from apps.MTL_ITEM_CATEGORIES_V mic
    where 1=1
    and mic.organization_id   = fnd_profile.value('AMS_ITEM_ORGANIZATION_ID') --101--p_org_id
    and mic.inventory_item_id = p_inventory_item_id
    and mic.category_set_name = 'FTNT_REPORTING_CATEGORY';
    RETURN lv_segment;
    EXCEPTION
    WHEN OTHERS THEN
    RETURN  lv_segment;     */
  END get_product_class;
--Function to get Reporting Family
  FUNCTION get_report_family(
      p_inventory_item_id IN NUMBER,
      p_org_id            IN NUMBER)
    RETURN VARCHAR2 RESULT_CACHE RELIES_ON(
      MTL_ITEM_CATEGORIES)
  IS
    lv_segment VARCHAR2(100) := NULL;
    CURSOR cur_get_segment4
    IS
      SELECT mc.segment4
      FROM INV.MTL_ITEM_CATEGORIES mic,
        mtl_categories mc
      WHERE mic.inventory_item_id = p_inventory_item_id
      AND mic.organization_id     = g_master_org_id -- fnd_profile.value('AMS_ITEM_ORGANIZATION_ID')
      AND mic.category_set_id     = get_rep_cat_set_id
      AND mc.category_id          = MIC.CATEGORY_ID;
  BEGIN
    OPEN cur_get_segment4;
    FETCH cur_get_segment4 INTO lv_segment;
    CLOSE cur_get_segment4;
    RETURN lv_segment;
    /*  select mic.segment4 --mic.segment2  --changed based on james/karen
    into lv_segment
    from apps.MTL_ITEM_CATEGORIES_V mic
    where 1=1
    and mic.organization_id   = fnd_profile.value('AMS_ITEM_ORGANIZATION_ID') --101--p_org_id
    and mic.inventory_item_id = p_inventory_item_id
    and mic.category_set_name = 'FTNT_REPORTING_CATEGORY';
    RETURN lv_segment;
    EXCEPTION
    WHEN OTHERS THEN
    RETURN  lv_segment;     */
  END get_report_family;
  FUNCTION get_other_element_type(
      p_inventory_item_id IN NUMBER,
      p_org_id            IN NUMBER)
    RETURN VARCHAR2 RESULT_CACHE RELIES_ON(
      MTL_ITEM_CATEGORIES)
  IS
    lv_segment VARCHAR2(100) := NULL;
    CURSOR cur_get_segment3
    IS
      SELECT mc.segment3
      FROM INV.MTL_ITEM_CATEGORIES mic,
        mtl_categories mc
      WHERE mic.inventory_item_id = p_inventory_item_id
      AND mic.organization_id     = p_org_id
      AND mic.category_set_id     = get_rev_cat_set_id
      AND mc.category_id          = MIC.CATEGORY_ID;
  BEGIN
    OPEN cur_get_segment3;
    FETCH cur_get_segment3 INTO lv_segment;
    CLOSE cur_get_segment3;
    RETURN lv_segment;
    /*  select mic.segment3
    into lv_segment
    from apps.MTL_ITEM_CATEGORIES_V mic
    where 1=1
    and mic.organization_id   = p_org_id
    and mic.inventory_item_id = p_inventory_item_id
    and mic.category_set_name = 'FTNT_REVENUE_CATEGORY';
    RETURN lv_segment;
    EXCEPTION
    WHEN OTHERS THEN
    RETURN  lv_segment;    */
  END get_other_element_type;
  FUNCTION get_soho_type(
      p_inventory_item_id IN NUMBER,
      p_org_id            IN NUMBER)
    RETURN VARCHAR2 RESULT_CACHE RELIES_ON(
      MTL_ITEM_CATEGORIES)
  IS
    lv_segment VARCHAR2(100) := NULL;
    CURSOR cur_get_segment5
    IS
      SELECT mc.segment5
      FROM INV.MTL_ITEM_CATEGORIES mic,
        mtl_categories mc
      WHERE mic.inventory_item_id = p_inventory_item_id
      AND mic.organization_id     = p_org_id
      AND mic.category_set_id     = get_rev_cat_set_id
      AND mc.category_id          = MIC.CATEGORY_ID;
  BEGIN
    OPEN cur_get_segment5;
    FETCH cur_get_segment5 INTO lv_segment;
    CLOSE cur_get_segment5;
    RETURN lv_segment;
    /* select mic.segment5
    into lv_segment
    from apps.MTL_ITEM_CATEGORIES_V mic
    where 1=1
    and mic.organization_id   = p_org_id
    and mic.inventory_item_id = p_inventory_item_id
    and mic.category_set_name = 'FTNT_REVENUE_CATEGORY';
    RETURN lv_segment;
    EXCEPTION
    WHEN OTHERS THEN
    RETURN  lv_segment; */
  END get_soho_type;
  FUNCTION get_flag_97_2(
      p_inventory_item_id IN NUMBER,
      p_org_id            IN NUMBER)
    RETURN VARCHAR2 RESULT_CACHE RELIES_ON(
      MTL_ITEM_CATEGORIES)
  IS
    lv_segment VARCHAR2(100) := NULL;
    CURSOR cur_get_segment2
    IS
      SELECT mc.segment2
      FROM INV.MTL_ITEM_CATEGORIES mic,
        mtl_categories mc
      WHERE mic.inventory_item_id = p_inventory_item_id
      AND mic.organization_id     = p_org_id
      AND mic.category_set_id     = get_rev_cat_set_id
      AND mc.category_id          = MIC.CATEGORY_ID;
  BEGIN
    OPEN cur_get_segment2;
    FETCH cur_get_segment2 INTO lv_segment;
    CLOSE cur_get_segment2;
    RETURN lv_segment;
    /*   select mic.segment2
    into lv_segment
    from apps.MTL_ITEM_CATEGORIES_V mic
    where 1=1
    and mic.organization_id   = p_org_id
    and mic.inventory_item_id = p_inventory_item_id
    and mic.category_set_name = 'FTNT_REVENUE_CATEGORY';
    RETURN lv_segment;
    EXCEPTION
    WHEN OTHERS THEN
    RETURN  lv_segment;  */
  END get_flag_97_2;
  FUNCTION get_product_group(
      p_inventory_item_id IN NUMBER,
      p_org_id            IN NUMBER)
    RETURN VARCHAR2 RESULT_CACHE RELIES_ON(
      MTL_ITEM_CATEGORIES)
  IS
    lv_segment VARCHAR2(100) := NULL;
    CURSOR cur_get_segment5
    IS
      SELECT mc.segment5
      FROM INV.MTL_ITEM_CATEGORIES mic,
        mtl_categories mc
      WHERE mic.inventory_item_id = p_inventory_item_id
      AND mic.organization_id     = g_master_org_id -- fnd_profile.value('AMS_ITEM_ORGANIZATION_ID')
      AND mic.category_set_id     = get_rep_cat_set_id
      AND mc.category_id          = MIC.CATEGORY_ID;
  BEGIN
    OPEN cur_get_segment5;
    FETCH cur_get_segment5 INTO lv_segment;
    CLOSE cur_get_segment5;
    RETURN lv_segment;
    /* select mic.segment5
    into lv_segment
    from apps.MTL_ITEM_CATEGORIES_V mic
    where 1=1
    and mic.organization_id   = fnd_profile.value('AMS_ITEM_ORGANIZATION_ID') --101--p_org_id
    and mic.inventory_item_id = p_inventory_item_id
    and mic.category_set_name = 'FTNT_REPORTING_CATEGORY';
    RETURN lv_segment;
    EXCEPTION
    WHEN OTHERS THEN
    RETURN  lv_segment; */
  END get_product_group;
  FUNCTION get_embedded_service(
      p_ser_line_id      IN NUMBER,
      p_product_category IN VARCHAR2)
    RETURN VARCHAR2
  IS
    l_product_category VARCHAR2(100);
  BEGIN
    IF p_ser_line_id IS NULL THEN
      RETURN NULL;
    ELSE
     /* BEGIN
        SELECT product_category
        INTO l_product_category
        FROM XXFT_RPRO_OE_ORDER_DETAILS_V
        WHERE SALES_ORDER_LINE_ID = p_ser_line_id;
      EXCEPTION
      WHEN OTHERS THEN
        l_product_category := NULL;
      END; */
      IF NVL(l_product_category,'X') = 'HARDWARE' THEN
        RETURN 'E_'||p_product_category;
      ELSE
        RETURN 'S_'||p_product_category;
      END IF;
    END IF;
  END get_embedded_service;
--
-- Modified to get the embedded Service. Defect #3306.
--
  FUNCTION get_embedded_service(
      p_ser_ref_type_code IN VARCHAR2,
      p_line_id           IN NUMBER,
      p_ser_line_id       IN NUMBER,
      p_inventory_item_id IN NUMBER ,
      p_org_id            IN NUMBER)
    RETURN VARCHAR2 RESULT_CACHE RELIES_ON(
      MTL_ITEM_CATEGORIES)
  IS
    l_product_category VARCHAR2(100);
    lv_segment         mtl_categories.segment6%TYPE;
    l_co_term_cnt      NUMBER;
    ln_bundle_cnt      NUMBER;
    l_type             VARCHAR2(4);
    l_rule_start_dt    VARCHAR2(20);
    
    CURSOR cur_get_prod_cat
    IS
      SELECT mc.segment6
      FROM INV.MTL_ITEM_CATEGORIES mic,
        mtl_categories mc
      WHERE mic.inventory_item_id = p_inventory_item_id
      AND mic.organization_id     = p_org_id
      AND mic.category_set_id     = get_rev_cat_set_id
      AND mc.category_id          = MIC.CATEGORY_ID;
    /*select mic.segment6
    from apps.MTL_ITEM_CATEGORIES_V mic
    where 1=1
    and mic.organization_id   = p_org_id
    and mic.inventory_item_id = p_inventory_item_id
    and mic.category_set_name = 'FTNT_REVENUE_CATEGORY';*/
    CURSOR cur_get_top_line_cat (p_line_id NUMBER)
    IS
      SELECT mc.segment6
      FROM INV.MTL_ITEM_CATEGORIES mic,
        mtl_categories mc,
        oe_order_lines_all oola
      WHERE oola.line_id        = p_line_id
      AND mic.inventory_item_id = oola.inventory_item_id
      AND mic.organization_id   = p_org_id
      AND mic.category_set_id   = get_rev_cat_set_id
      AND mc.category_id        = MIC.CATEGORY_ID;
      
    CURSOR cur_get_ib_line_cat(p_instance_id IN NUMBER)
    IS
    SELECT mc.segment6
      FROM INV.MTL_ITEM_CATEGORIES mic,
            mtl_categories mc,
            csi_item_instances cii
     WHERE cii.instance_id       = p_instance_id
       AND mic.inventory_item_id = cii.inventory_item_id
       AND mic.organization_id   = p_org_id
       AND mic.category_set_id   = get_rev_cat_set_id 
       AND mc.category_id        = MIC.CATEGORY_ID;
    
    
    /*SELECT mic.segment6
    FROM oe_order_lines_all oola, MTL_ITEM_CATEGORIES_V mic
    WHERE line_id = p_line_id
    AND mic.organization_id   = p_org_id
    AND mic.inventory_item_id = oola.inventory_item_id
    AND mic.category_set_name = 'FTNT_REVENUE_CATEGORY'; */
    CURSOR cur_get_type(p_line_id IN NUMBER)
    IS
    SELECT
      CASE
        WHEN COUNT(1)> 0
        THEN 'E_'
        ELSE 'S_'
      END a
    FROM oe_order_lines_all
    WHERE header_id = ANY
      (SELECT header_id FROM oe_order_lines_all WHERE line_id = p_line_id
      )
    AND line_number =
      (SELECT line_number FROM oe_order_lines_all WHERE line_id = p_line_id
      )
    AND XXFT_RPRO_DATA_EXTRACT_PKG.get_product_category(inventory_item_id, 101)='HARDWARE'
    ;
    CURSOR cur_get_rule_start_date(p_line_id IN NUMBER)
    IS
    SELECT  CASE
    WHEN oel.item_type_code     ='INCLUDED'
    AND oel.ORDER_QUANTITY_UOM <> 'EA'
    THEN NVL(oel.service_start_date,NVL(oel.attribute2,XXFT_RPRO_DATA_EXTRACT_PKG.get_line_contract_date(oel.line_id,'START'))) --ITS#553115
    ELSE oel.service_start_date END start_date
    FROM oe_order_lines_all oel
    WHERE line_id =p_line_id;
    
  BEGIN
     -- Updated on 8-10-2016 for the ITS # 523105
     OPEN cur_get_type(p_line_id);
     FETCH cur_get_type INTO l_type;
     CLOSE cur_get_type;
     lv_segment := get_product_category(p_inventory_item_id,p_org_id);
     OPEN cur_get_rule_start_date(p_line_id);
     FETCH cur_get_rule_start_date INTO l_rule_start_dt;
     CLOSE cur_get_rule_start_date;
     IF l_rule_start_dt is not null
     THEN
        RETURN l_type||lv_segment;
     ELSE
        RETURN NULL;
     END IF;     
     
     ---Updated on 8-10-2016 for ITS #523105
     
     --
     -- Defect #3306. check if the quote is a Co-Term Quote.
     --
     SELECT count(1)
       INTO l_co_term_cnt
       FROM oe_order_lines_all oola,
            oe_order_sources os
      WHERE 1=1 
        AND oola.line_id         = p_line_id
        and os.order_source_id   = oola.order_source_id
        AND name                = any('Co-Term with Quote','Renew with Quote'); 

    IF l_co_term_cnt > 0 
    THEN
       lv_segment := get_product_category(p_inventory_item_id,p_org_id);
       RETURN 'S_'||lv_segment;
    END IF;
    -- Defect Embedded Support/Subscription changes.
    SELECT count(1)
      INTO ln_bundle_cnt
      FROM oe_order_lines_all 
     WHERE line_id = p_line_id
       and (top_model_line_id is not null OR link_to_line_id is not null);
       
    IF p_ser_line_id IS NULL THEN
      RETURN NULL;
    ELSE
      IF p_ser_ref_type_code ='CUSTOMER_PRODUCT'
      THEN
         /*OPEN cur_get_ib_line_cat(p_ser_line_id);
         FETCH cur_get_ib_line_cat INTO l_product_category;
         CLOSE cur_get_ib_line_cat; */
         lv_segment := get_product_category(p_inventory_item_id,p_org_id);
         RETURN 'S_'||lv_segment;
      ELSIF p_ser_ref_type_code ='ORDER' OR ln_bundle_cnt > 0
      THEN
         OPEN cur_get_top_line_cat(p_ser_line_id);
         FETCH cur_get_top_line_cat INTO l_product_category;
         CLOSE cur_get_top_line_cat;
      ELSE
         l_product_category := NULL;
      END IF;
      lv_segment :=get_product_category(p_inventory_item_id,p_org_id);
      /*OPEN cur_get_prod_cat;
      FETCH cur_get_prod_cat INTO lv_segment;
      CLOSE cur_get_prod_cat; */
      IF NVL(l_product_category,'X') = 'HARDWARE' THEN
        RETURN 'E_'||lv_segment;
      ELSE
        RETURN 'S_'||lv_segment;
      END IF;
    END IF;
  END get_embedded_service;
  
  FUNCTION get_implied_pcs(
      p_ser_line_id      IN NUMBER,
      p_product_category IN VARCHAR2)
    RETURN VARCHAR2
  IS
    l_product_category VARCHAR2(100);
  BEGIN
    IF p_ser_line_id IS NULL THEN
      RETURN NULL;
    ELSE
      /*BEGIN
        SELECT product_category
        INTO l_product_category
        FROM XXFT_RPRO_OE_ORDER_DETAILS_V
        WHERE SALES_ORDER_LINE_ID = p_ser_line_id;
      EXCEPTION
      WHEN OTHERS THEN
        l_product_category := NULL;
      END; */
      IF NVL(l_product_category,'X') = 'HARDWARE' THEN
        RETURN 'E_'||p_product_category;
      ELSE
        RETURN 'S_'||p_product_category;
      END IF;
    END IF;
  END get_implied_pcs;
  FUNCTION implied_pcs_days(
      p_cust_type    IN VARCHAR2,
      p_bill_country IN VARCHAR2,
      p_type         IN VARCHAR2)
    RETURN VARCHAR2
	RESULT_CACHE 
	RELIES_ON (fnd_lookup_values)
  IS
    lv_us_ca_implied_days         VARCHAR2(10) := 0;
    lv_rest_of_world_implied_days VARCHAR2(10) := 0;
    --lv_p_implied_days           VARCHAR2(10) := 0;
    lv_lag_days VARCHAR2(10) := 0;
  BEGIN
    SELECT attribute15,
      attribute14,
      attribute13
      --attribute12
    INTO lv_us_ca_implied_days,
      lv_rest_of_world_implied_days,
      --lv_p_implied_days,
      lv_lag_days
    FROM fnd_lookup_values
    WHERE lookup_type LIKE 'FTNT_REVPRO_IMPLIED_PCS'
    AND enabled_flag = 'Y'
    AND language     = 'US'
    AND TRUNC(sysdate) BETWEEN TRUNC(start_date_active) AND TRUNC(end_date_active);
    IF p_type = 'IMP' THEN
      IF p_bill_country IN ( 'US','CA') THEN
        RETURN lv_us_ca_implied_days;
      ELSE
        RETURN lv_rest_of_world_implied_days;
      END IF;
      --       IF p_cust_type ='P' THEN
      --          RETURN lv_p_implied_days;
      --       ELSIF p_cust_type = 'N' AND p_bill_country in ( 'US','CA') THEN
      --          RETURN lv_np_usa_implied_days;
      --       ELSIF p_cust_type = 'N' AND p_bill_country not in ( 'US','CA') THEN
      --          RETURN lv_np_non_usa_implied_days;
      --       END IF;
    ELSIF p_type     = 'LAG' THEN
      IF p_cust_type ='P' THEN
        RETURN lv_lag_days;
      ELSIF p_cust_type = 'N' THEN
        RETURN NULL;
      END IF;
    END IF;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN NULL;
  END;
  FUNCTION check_line_qty_change(
      p_line_id          NUMBER,
      p_ordered_quantity NUMBER)
    RETURN VARCHAR2
  IS
    lv_value       NUMBER := 0;
    lv_batch_value NUMBER := 0;
  BEGIN
    BEGIN
      SELECT COUNT(DISTINCT sales_order_line_batch_id)
      INTO lv_batch_value
      FROM xxft_rpro_order_details
      WHERE sales_order_line_id           = p_line_id
      AND NVL(processing_attribute5,'X') != 'BO';
    EXCEPTION
    WHEN OTHERS THEN
      NULL;
    END;
    IF lv_batch_value <> p_ordered_quantity THEN
      RETURN 'Y';
    ELSE
      RETURN 'N';
    END IF;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN 'N';
  END;
  FUNCTION get_def_account(
      p_rev_category IN VARCHAR2)
    RETURN VARCHAR2
  IS
    lv_tag VARCHAR2(100) := NULL;
  BEGIN
    SELECT flv.tag
    INTO lv_tag
    FROM apps.fnd_lookup_values flv
    WHERE 1                    =1
    AND upper(flv.lookup_code) = upper(p_rev_category)
    AND flv.lookup_type        = 'FTNT_REVPRO_DEF_ACCOUNTS';
    RETURN lv_tag;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN lv_tag;
  END get_def_account;
  FUNCTION get_unit_list_price(
      p_list_hdr_id IN NUMBER ,
      p_item_id     IN NUMBER)
    RETURN NUMBER
  IS
    l_price NUMBER;
    CURSOR cur_get_list_price
    IS
      SELECT --a.list_header_id,
        a.operand
      FROM QP_LIST_LINES_v a
      WHERE 1                         =1
      AND a.list_header_id            = p_list_hdr_id
      AND a.product_attribute_context = 'ITEM'
      AND a.product_attr_value        = TO_CHAR(p_item_id);
  BEGIN
    OPEN cur_get_list_price;
    FETCH cur_get_list_price INTO l_price;
    CLOSE cur_get_list_price;
    RETURN l_price;
  END;
  FUNCTION get_product_line(
      p_inventory_item_id IN NUMBER,
      p_org_id            IN NUMBER)
    RETURN VARCHAR2 RESULT_CACHE RELIES_ON(
      MTL_ITEM_CATEGORIES)
  IS
    lv_segment VARCHAR2(100) := NULL;
    CURSOR cur_get_segment1
    IS
      SELECT mc.segment1
      FROM INV.MTL_ITEM_CATEGORIES mic,
        mtl_categories mc
      WHERE mic.inventory_item_id = p_inventory_item_id
      AND mic.organization_id     = p_org_id
      AND mic.category_set_id     = get_rev_cat_set_id
      AND mc.category_id          = MIC.CATEGORY_ID;
  BEGIN
    OPEN cur_get_segment1;
    FETCH cur_get_segment1 INTO lv_segment;
    CLOSE cur_get_segment1;
    /* select mic.segment1
    into lv_segment
    from apps.MTL_ITEM_CATEGORIES_V mic
    where 1=1
    and mic.organization_id   = p_org_id
    and mic.inventory_item_id = p_inventory_item_id
    and mic.category_set_name = 'FTNT_REVENUE_CATEGORY'; */
    RETURN lv_segment;
  END get_product_line;
  FUNCTION get_partner_type(
      p_header_id IN NUMBER )
    RETURN NUMBER RESULT_CACHE RELIES_ON(
      hz_cust_accounts_all)
  IS
    l_channel_patner_type NUMBER:=0;
  BEGIN
    SELECT COUNT(1)
    INTO l_channel_patner_type
    FROM oe_order_headers_all ooha ,
      hz_cust_accounts_all hca
    WHERE ooha.header_id                        = p_header_id
    AND hca.cust_account_id                     = ooha.sold_to_org_id
    AND NVL(UPPER(hca.sales_channel_code),'@') <> 'CHANNEL_PARTNER';
    RETURN l_channel_patner_type;
  END;
  --
  -- #CR49.
  --
  FUNCTION check_non_serial_item(
      p_header_id IN NUMBER ,
      p_line_id   IN NUMBER )
    RETURN VARCHAR2 RESULT_CACHE RELIES_ON(
      fnd_lookup_values)
  IS
    l_non_serial_items    NUMBER :=0;
    l_channel_patner_type NUMBER :=0;
    CURSOR cur_non_serialized_items
    IS
      SELECT 1
      FROM dual
      WHERE EXISTS
        (SELECT 1
        FROM fnd_lookup_values flv,
          oe_order_lines_all oola
        WHERE 1          =1
        AND oola.line_id = p_line_id
        AND lookup_type LIKE 'FTNT_REVPRO_NON_SERIALIZED'
        AND lookup_code = get_product_line(oola.inventory_item_id, oola.ship_from_org_id)
        AND sysdate BETWEEN NVL(start_date_active,SYSDATE) AND NVL(end_date_active, sysdate+1)
        AND ENABLED_FLAG ='Y'
        );
  BEGIN
    OPEN cur_non_serialized_items;
    FETCH cur_non_serialized_items INTO l_non_serial_items;
    CLOSE cur_non_serialized_items;
    l_channel_patner_type:= get_partner_type(p_header_id);
    IF l_non_serial_items > 0 AND l_channel_patner_type > 0 THEN
      RETURN 'Y';
	  --RETURN 'N';
    ELSE
      RETURN 'N';
    END IF;
  END;
  --
  -- Get List Price for Credit memo transactions alone.
  --
FUNCTION get_list_price_for_cm(
    p_price_list_id     IN NUMBER,
    p_inventory_item_id IN NUMBER,
    p_price_date DATE)
  RETURN NUMBER RESULT_CACHE RELIES_ON(
    QP_LIST_LINES)
IS
  lv_list_price              VARCHAR2(100) := NULL;
  lv_price_list_id           NUMBER        := 0;
  lv_inventory_item_id       NUMBER        := 0;
  lv_secondary_price_list_id NUMBER        := 0;
  lv_count                   NUMBER        := 0;
  --
  -- Added on 6/3 to fix performance issue.
  --
  CURSOR cur_get_list_price
  IS
    SELECT QL.OPERAND
    FROM APPS.QP_LIST_LINES QL,
      APPS.QP_PRICING_ATTRIBUTES QP
    WHERE QL.LIST_HEADER_ID          = p_price_list_id
    AND QL.LIST_LINE_ID              = QP.LIST_LINE_ID
    AND QP.PRODUCT_ATTRIBUTE_CONTEXT = 'ITEM'
    AND QP.PRODUCT_ATTRIBUTE         = 'PRICING_ATTRIBUTE1'
    AND QP.PRODUCT_ATTR_VALUE        = TO_CHAR(p_inventory_item_id)
    AND TRUNC(p_price_date) BETWEEN NVL(TRUNC(QL.START_DATE_ACTIVE),TRUNC(p_price_date)) AND NVL(TRUNC(QL.END_DATE_ACTIVE),TRUNC(p_price_date));
  CURSOR cur_get_second_price
  IS
    SELECT QL.OPERAND
    FROM APPS.QP_LIST_LINES QL,
      APPS.QP_PRICING_ATTRIBUTES QP
    WHERE QL.LIST_HEADER_ID = ANY
      (SELECT list_header_id
      FROM apps.QP_SECONDARY_PRICE_LISTS_V
      WHERE parent_price_list_id = p_price_list_id
      )
  AND QL.LIST_LINE_ID              = QP.LIST_LINE_ID
  AND QP.PRODUCT_ATTRIBUTE_CONTEXT = 'ITEM'
  AND QP.PRODUCT_ATTRIBUTE         = 'PRICING_ATTRIBUTE1'
  AND QP.PRODUCT_ATTR_VALUE        = TO_CHAR(p_inventory_item_id)
  AND TRUNC(p_price_date) BETWEEN NVL(TRUNC(QL.START_DATE_ACTIVE),TRUNC(p_price_date)) AND NVL(TRUNC(QL.END_DATE_ACTIVE),TRUNC(p_price_date));
BEGIN
  -- Modified on 5/31 to alter the conditions.
  OPEN cur_get_list_price;
  FETCH cur_get_list_price INTO lv_list_price;
  CLOSE cur_get_list_price;
  IF lv_list_price IS NULL THEN
    OPEN cur_get_second_price;
    FETCH cur_get_second_price INTO lv_list_price;
    CLOSE cur_get_second_price;
  END IF;
RETURN lv_list_price;
EXCEPTION
WHEN OTHERS THEN
  RETURN lv_list_price;
END get_list_price_for_cm; 

  FUNCTION get_embedded_service_cm(
    p_line_id           IN NUMBER,
    p_inventory_item_id IN NUMBER,
    p_org_id            IN NUMBER)
  RETURN VARCHAR2
IS
  l_product_category VARCHAR2(100);
  lv_segment mtl_categories.segment6%TYPE;
  l_co_term_cnt   NUMBER;
  ln_bundle_cnt   NUMBER;
  l_type          VARCHAR2(4);
  l_rule_start_dt VARCHAR2(20);
BEGIN
  -- Updated on 8-10-2016 for the ITS # 523105
  lv_segment := get_product_category(p_inventory_item_id,p_org_id);
  IF lv_segment IN ('SUBSCRIPTION','PROFESSIONAL SERVICE','SUPPORT','SOFTWARE') THEN
    RETURN 'S_'||lv_segment;
  ELSE
    RETURN NULL;
  END IF;
END get_embedded_service_cm;

FUNCTION get_contract_start_date(p_contract_num    IN VARCHAR2)
RETURN DATE
IS
l_start_date DATE;
CURSOR cur_oracle_contract_details
IS
  SELECT DISTINCT
    b.start_date Start_Date--,--    B.end_date End_Date--,
   -- B.ATTRIBUTE3 Registration_Date,
  --  B.ATTRIBUTE4 Auto_Start_Date    
  FROM okc_k_lines_b a ,
    OKC_K_LINES_B b,
    okc_k_headers_all_b c
  WHERE a.chr_id        = c.id
  AND b.cle_id          = a.id
  --AND b.sts_code        ='ACTIVE'
  AND c.contract_number = p_contract_num;
BEGIN
  OPEN cur_oracle_contract_details;
  FETCH cur_oracle_contract_details INTO l_start_date;--,x_reg_date,x_auto_start_date;
  CLOSE cur_oracle_contract_details;
  RETURN l_start_date;
END;
FUNCTION get_contract_end_date(p_contract_num    IN VARCHAR2)
RETURN DATE
IS
l_end_date DATE;
CURSOR cur_oracle_contract_details
IS
  SELECT DISTINCT
    B.end_date End_Date  --,
   -- B.ATTRIBUTE3 Registration_Date,
  --  B.ATTRIBUTE4 Auto_Start_Date    
  FROM okc_k_lines_b a ,
    OKC_K_LINES_B b,
    okc_k_headers_all_b c
  WHERE a.chr_id        = c.id
  AND b.cle_id          = a.id
  --AND b.sts_code        ='ACTIVE'
  AND c.contract_number = p_contract_num;
BEGIN
  OPEN cur_oracle_contract_details;
  FETCH cur_oracle_contract_details INTO l_end_date;--,x_reg_date,x_auto_start_date;
  CLOSE cur_oracle_contract_details;
  RETURN l_end_date;
END;

procedure get_contract_dates(p_contract_num    IN VARCHAR2
                                                 ,x_start_date      OUT DATE
                                                 ,x_end_date        OUT DATE
                                                -- ,x_reg_date        OUT DATE
                                                -- ,x_auto_start_date OUT DATE
                                                 )
IS

CURSOR cur_oracle_contract_details
IS
  SELECT DISTINCT
    b.start_date Start_Date,
    B.end_date End_Date--,
   -- B.ATTRIBUTE3 Registration_Date,
  --  B.ATTRIBUTE4 Auto_Start_Date    
  FROM okc_k_lines_b a ,
    OKC_K_LINES_B b,
    okc_k_headers_all_b c
  WHERE a.chr_id        = c.id
  AND b.cle_id          = a.id
  --AND b.sts_code        ='ACTIVE'
  AND c.contract_number = p_contract_num;
BEGIN
  OPEN cur_oracle_contract_details;
  FETCH cur_oracle_contract_details INTO x_start_date,x_end_date;--,x_reg_date,x_auto_start_date;
  CLOSE cur_oracle_contract_details;
END;

function get_rma_cost(p_rma_line_id IN NUMBER)
RETURN NUMBER
IS
l_total_cost NUMBER;
l_ref_acct   NUMBER;

CURSOR cur_get_rma_cost
IS
SELECT A.BASE_TRANSACTION_VALUE,
  a.reference_account
FROM oe_order_headers_all ooha,
  oe_order_lines_all oola,
  mtl_material_transactions mmt,
  MTL_TRANSACTION_ACCOUNTS a,
  mfg_lookups b,
  gl_code_combinations gcc1
WHERE 1                          =1
AND oola.line_id                 = p_rma_line_id
AND mmt.trx_source_line_id (+)   = oola.line_id
AND a.transaction_id (+)         = mmt.transaction_id
AND b.lookup_type (+)            ='CST_ACCOUNTING_LINE_TYPE'
AND b.lookup_code (+)            = a.ACCOUNTING_LINE_TYPE
AND b.meaning (+)                ='Cost of Goods Sold'
AND gcc1.code_combination_id (+) = a.reference_account;

BEGIN
   OPEN cur_get_rma_cost;
   FETCH cur_get_rma_cost INTO l_total_cost,l_ref_acct;
   CLOSE cur_get_rma_cost;
   RETURN l_total_cost;
END;

FUNCTION get_line_contract_date(
    p_line_id IN NUMBER,
    P_type    IN VARCHAR2)
  RETURN VARCHAR2
IS
  l_start_date      VARCHAR2(40);
  l_end_date        VARCHAR2(40);
  l_auto_start_date VARCHAR2(40);
  CURSOR cur_get_line_start_date
  IS
    SELECT NVL(OKLB.ATTRIBUTE1,OKLB.START_DATE) Start_Date,
      NVL(OKLB.ATTRIBUTE2,OKLB.end_date) End_Date,
      DECODE(OOL.ATTRIBUTE13,'Y',OKLB.START_DATE,OKLB.ATTRIBUTE4) Auto_Start_Date
    FROM APPS.OE_ORDER_LINES_ALL OOL,
      APPS.OE_ORDER_HEADERS_ALL OOH,
      -- OKC_K_REL_OBJS REL,--Commented by obi on 9/8/2016
      (
      SELECT DISTINCT jtot_object1_code,
        OBJECT1_ID1,
        CHR_ID
      FROM OKC_K_REL_OBJS
      ) REL,--Added by Obi on 9/8/2016
    APPS.OKC_K_HEADERS_ALL_B OKH,
    APPS.OKC_K_LINES_B OKL,
    APPS.OKC_K_ITEMS OKI,
    APPS.OKC_K_LINES_B OKLB,
    APPS.OKC_K_ITEMS OKIB,
    APPS.CSI_ITEM_INSTANCES CII
  WHERE 1                   =1
  AND ool.line_id           =p_line_id
  AND OOH.HEADER_ID         = OOL.HEADER_ID
  AND OOH.ORG_ID            = OOL.ORG_ID
  AND REL.jtot_object1_code = 'OKX_ORDERLINE'
  AND REL.OBJECT1_ID1       = TO_CHAR(OOL.LINE_ID)
  AND REL.CHR_ID            = OKH.ID
  AND OKH.ID                = OKL.CHR_ID
  AND OKL.ID                = OKI.CLE_ID
  AND OKI.OBJECT1_ID1       = OOL.INVENTORY_ITEM_ID
  AND OKL.ID                = OKLB.CLE_ID
  AND OKLB.ID               = OKIB.CLE_ID
  AND OKIB.OBJECT1_ID1      = CII.INSTANCE_ID
  AND OKLB.sts_code         = ANY('ACTIVE','SIGNED') ;
  
  CURSOR cur_auto_start_date
  IS
SELECT NVL(FLV.ATTRIBUTE2,0) SER_DAYS
FROM apps.oe_order_headers_all ooh,
  apps.oe_transaction_types_tl ootl,
  APPS.FND_LOOKUP_VALUES flv,
  apps.oe_order_lines_all ool,
  hz_cust_site_uses_all hcsu,
  hz_cust_acct_sites_all hcs,
  hz_party_sites hps ,
  hz_locations hz,
  ra_territories RT
WHERE 1               =1
AND FLV.lookup_type   ='FTNT_S2S_ITEM_RELATIONSHIPS'
AND FLV.DESCRIPTION   =ootl.NAME
AND flv.attribute1    =rt.segment2
AND ooh.order_type_id =ootl.transaction_type_id
  --AND ooh.header_id          =:l_header_id
AND ooh.header_id         = ool.header_id
AND hcsu.site_use_id      =ooh.invoice_to_org_id
AND hcsu.cust_acct_site_id=hcs.cust_acct_site_id
AND hcs.party_site_id     =hps.party_site_id
AND hps.location_id       =hz.location_id
AND hcsu.site_use_code    ='BILL_TO'
AND HCSU.territory_id     =RT.territory_id
AND ool.line_id           =P_LINE_ID;

BEGIN
  OPEN cur_get_line_start_date;
  FETCH cur_get_line_start_date
  INTO l_start_date,
    l_end_date,
    l_auto_start_date;
  CLOSE cur_get_line_start_date;
  IF P_type ='START' THEN
    RETURN l_start_date;
  ELSIF P_type ='END' THEN
    RETURN l_end_date;
  ELSIF P_type ='AUTO' THEN
    RETURN l_auto_start_date;
  ELSE
    RETURN l_start_date;
  END IF;
END;

FUNCTION get_line_grace_period(
    P_LINE_ID IN NUMBER)
  RETURN NUMBER
IS
  l_grace_period NUMBER;
  CURSOR cur_get_grace_period
  IS
    SELECT NVL(FLV.ATTRIBUTE2,0) SER_DAYS
    FROM apps.oe_order_headers_all ooh,
      apps.oe_transaction_types_tl ootl,
      APPS.FND_LOOKUP_VALUES flv,
      apps.oe_order_lines_all ool,
      hz_cust_site_uses_all hcsu,
      hz_cust_acct_sites_all hcs,
      hz_party_sites hps ,
      hz_locations hz,
      ra_territories RT
    WHERE 1               =1
    AND FLV.lookup_type   ='FTNT_S2S_ITEM_RELATIONSHIPS'
    AND FLV.DESCRIPTION   =ootl.NAME
    AND flv.attribute1    =rt.segment2
    AND ooh.order_type_id =ootl.transaction_type_id
      --AND ooh.header_id          =:l_header_id
    AND ooh.header_id         = ool.header_id
    AND hcsu.site_use_id      =ooh.invoice_to_org_id
    AND hcsu.cust_acct_site_id=hcs.cust_acct_site_id
    AND hcs.party_site_id     =hps.party_site_id
    AND hps.location_id       =hz.location_id
    AND hcsu.site_use_code    ='BILL_TO'
    AND HCSU.territory_id     =RT.territory_id
    AND ool.line_id           =P_LINE_ID;
BEGIN
  OPEN cur_get_grace_period;
  FETCH cur_get_grace_period INTO l_grace_period;
  CLOSE cur_get_grace_period;
  RETURN l_grace_period;
END;

--ITS#575976 --
--
-- Get Order Classification of the order- Drop Ship or Coterm Or Online - for POS.
--
FUNCTION get_order_classification(p_line_id IN NUMBER)
RETURN VARCHAR2
IS

l_order_source_id NUMBER;
l_order_source_name oe_order_sources.name%TYPE;
l_auto_reg_flag VARCHAR2(1);
l_drop_ship_order VARCHAR2(1);
l_order_type      VARCHAR2(20);

CURSOR cur_get_order_source
IS
 SELECT oola.order_source_id,os.name,oola.ATTRIBUTE13, OOHA.ATTRIBUTE5
   FROM oe_order_lines_all oola, oe_order_sources os, oe_order_headers_all ooha
   where oola.line_id = p_line_id   
   and ooha.header_id = oola.header_id
   and os.order_source_id = ooha.order_source_id;

BEGIN
    OPEN  cur_get_order_source;
    FETCH cur_get_order_source INTO l_order_source_id,l_order_source_name,l_auto_reg_flag,l_drop_ship_order;
    CLOSE cur_get_order_source;
    IF NVL(l_drop_ship_order,'N') ='N'
    THEN
       l_order_type :=  'DROP SHIP';
    ELSIF l_order_source_name = 'Co-Term with Quote'
    THEN
       l_order_type :=  'COTERM';
    ELSIF l_order_source_name ='Forticare.OnlineRenewals'
    THEN
       l_order_type :=  'ONLINERENWAL';
    ELSIF l_order_source_name ='Renew with Quote' AND l_auto_reg_flag ='Y'
    THEN
       l_order_type :='AUTO REGISTERED';
    ELSE
        l_order_type := NULL;
    END IF;
    RETURN l_order_type;
EXCEPTION 
   WHEN OTHERS 
   THEN
      RETURN NULL;
END;
--ITS#575976 --
--
-- Get Order Classification of the order- Drop Ship or Coterm Or Online - for Grace Period.
--
FUNCTION get_line_source(p_line_id IN NUMBER)
RETURN VARCHAR2
IS

l_order_source_id NUMBER;
l_order_source_name oe_order_sources.name%TYPE;
l_auto_reg_flag VARCHAR2(1);
l_order_type      VARCHAR2(20);

CURSOR cur_get_order_source
IS
 SELECT oola.order_source_id,os.name,oola.ATTRIBUTE13
   FROM oe_order_lines_all oola, oe_order_sources os
   where oola.line_id = p_line_id      
   and os.order_source_id = oola.order_source_id;

BEGIN
    OPEN  cur_get_order_source;
    FETCH cur_get_order_source INTO l_order_source_id,l_order_source_name,l_auto_reg_flag;
    CLOSE cur_get_order_source;
    IF l_order_source_name = 'Co-Term with Quote'
    THEN
       l_order_type :=  'COTERM';
    ELSIF l_order_source_name ='Renew with Quote' AND l_auto_reg_flag ='Y'
    THEN
       l_order_type :='AUTO REGISTERED';
    ELSE
        l_order_type := NULL;
    END IF;
    RETURN l_order_type;
EXCEPTION 
   WHEN OTHERS 
   THEN
      RETURN NULL;
END;


END XXFT_RPRO_DATA_EXTRACT_PKG;

