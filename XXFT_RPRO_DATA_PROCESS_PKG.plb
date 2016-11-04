create or replace PACKAGE BODY XXFT_RPRO_DATA_PROCESS_PKG
AS
/* +=================================================================================================================+
-- |                         Fortinet, Inc.
-- |                         Sunnyvale, CA
-- +=================================================================================================================
-- |
-- |Object Name     : XXFT_RPRO_DATA_PROCESS_PKG.pkb
-- |
-- |Description      : Package to extract the sales orders, Invoices and credit
-- |                   Memo details to Revpro Interface Staging Tables.
-- |                   It is called by a program "FTNT Revpro Data Extract".even
__ |
-- |
-- |Change Record:
-- |===============
-- |Version   Date        Author             Remarks
-- |=======   =========== ==============     ============================
-- |1.0       12-FEB-2016 (NTT)              Initial code version
-- |1.1       31-MAR-2016 (NTT)              Modified to include the validations
-- |                                          for extraction and for closed so's
-- |1.2       27-APR-2016 (NTT)              Modified the validate_Intial_load to
-- |                                         update the status of Error records back to 'R'.
-- |                                         Credit Memo Fix to get by transaction type. (Defect#1723)
-- |1.3       17-MAY-2016 (NTT)              Added a condition in check_contracts_created to validate
-- |                                         serial quantity vs contract quantity. Also modified to add the
-- |                                         Parallel Hint for performance issue.
-- |1.4       19-MAY-2016 (NTT)              Added List Price validation pertaining to the defect#2626.
-- |1.5       20-MAY-2016 (NTT)              Adding additional Logic List price validation #2626.
-- |                                         modified to add REBATE and RETURN change per Request.
-- |1.6       21-MAY-2016 (NTT)              Added 1 day to support duration pertaining to the defect #2953.
-- | 1.7      02-Jun-2016 (NTT)              add get_list_price instead of get_unit_list_price.
-- | 1.8      06-Jun-2016 (NTT)              Consolidation of the Non Serialized sales order line transactions
-- |                                         into one batch instead of splitting them.
-- |1.9       08-JUN-2016 (NTT)              Modified to capture the Service Contracts Service start date and end date.#3239
-- |                                         Capture the service start and end dates per the request.
-- |1.10      11-JUN-2016 (NTT)              Actual Fulfillment Date in case of Non-Shippable Lines such as KIT items.#3289.
-- |1.11      12-JUN-2016 (NTT)              Modified to capture the Non-Serialized Item flag in the Attribute60.
-- |                                         attribute60 will be only updated at the initial populate.
-- |1.12      15-JUN-2016 (NTT)              Modified the logic for the CRA Credit Memo's to pick the top model line id
-- |                                         when a BOM is returned through serial number or contract number. #3307
-- |1.13      15-JUN-2016 (NTT)              Modified the registration events to pull the additional fields. #3314- CR#87.
-- |1.14      17-JUN-2016 (NTT)              Modified to add the condition check for Service type with Attribute44 in ERP reg Events.
-- |1.15      20-Jun-2016 (NTT)              Modified for the CR#85 to wait for the service letter date. All lines shall be sent over only
-- |                                         after the service letter date is populated.
-- |1.16      21-Jun-2016 (NTT)              Modified the history events to add the sts_code as Active to filter the terminated contract lines.
-- |1.17      21-Jun-2016 (NTT)              Modified for the CR85 to pull the service letter sent date.
-- |1.18      22-JUN-2016 (NTT)              Modified to ignore the Forticare.onlinerenewals orders to get service contract start and dates as rule start
-- |                                         and end dates.
-- |1.19      27-Jun-2016 (NTT)              Credit Memo's were inserted as 'P' initially and this is fixed to populate as 'N'.
-- |1.20      28-Jun-2016 (NTT)              Credit Memo for the non serialized and non contract based items the amount is not matching. Defect#3402.
-- |1.21      29-Jun-2016 (NTT)              Modified for the CR95 to propogate the subsequent Grace Period change in Forticare.
-- |
-- |1.22      04-Jul-2016 (NTT)              Modified the historical events to pull the signed and Active Contracts #3461.
-- |                                         Modified to add Return Flag as 'Y' for defect#3418.
-- |                                         Modified to include ext_sell_price as 0 for child items in a bundle. Defect#3425.
-- |                                         Modified to include Closed Status for CRA_Credit_memo Defect #3402.
-- |1.23       22-Jul-2016 (NTT)             Modified to add the condition to skip warranty items in the check contracts creation process #508695.
-- |1.24       25-JUl-2016 (NTT)             Add a validation to check for region of the order. If it is missing dont send to REvpro. #508676.
-- |1.25       29-Jul-2016 (NTT)             Added a validation to check if service info is populated for Included Items with Time as the UOM Class. #AA01234
-- |1.26       02-AUG-2016 (NTT)             Modifying to add logic to retrive the start and end dates for included and UOM class Time for which the contracts are missing.
-- |
-- |1.27       08-AUG-2016 (NTT)             Modified to add from and to dates for bulk process for pulling order by weekly. ITS Ticket #- 523105
-- |1.28       16-AUG-2016 (NTT)             populate the hardware ship date as the embedded support or subscription.ITS#528351.
-- |1.29       20-AUG-2016 (NTT)             Modified to extract manual credit memos for return sales orders. ITS#523169.
-- |1.30       30-AUG-2016 (NTT)             Modified to encode the character for fc_type as character as the historical events. ITS#544227 and ITS#544414 to get the pricelist from header.
-- |1.31       30-AUG-2016 (NTT)             Modified to remove the Actual Fullfillment date for non shippable items ITS#542467
-- |1.32       06-SEP-2016 (NTT)             Modified to exclude duplicate events ITS#540799 and Modified to include service reference type code for ITS#550041.
-- |1.33       08-SEP-2016 (NTT)             Modified to mark the records as processed for the grace period changes with start date as the auto start date.ITS#552383.
-- |1.34       09-SEP-2016 (NTT)             Added registration date parameter to process_events procedure ITS#544171
-- |1.35       09-SEP-2016 (NTT)             Modified for the NFR Orders Solution - ITS Ticket#540799.
-- |1.36       12-SEP-2016 (NTT)             Modified to get the actual cost of goods Sold account for the Credit memos that does not reference a sales order.ITS#524256
-- |1.37       12-SEP-2016 (NTT)             Modified to exclude the cancelled order lines from the list. ITS#553115, ITS#561763                                         
-- |1.38       21-SEP-2016 (NTT)             Modified for the Split scenario for the Non Serial and Non Channel Partner orders - inITS#566054
-- |1.39       22-SEP-2016 (NTT)             Modified to skip the validation for the service info missing orders based on a list.-ITS#562057
-- |1.40       26-SEP-2016 (NTT)             Modified to skip the sales order new line id as null for manual credit memos. -- ITS#569702.
-- |1.41       30-SEP-2016 (NTT)             Modified to send the dummy instance and active contract versus -- ITS#573774.  
-- |1.42       03-OCT-2016 (NTT)             Modified to check the XXFT_FO_WARRANTY for warranty related orders. - ITS#570061
-- |1.43       04-OCT-2016 (NTT)             Modified to send the return flag as 'N' for manual credit memos.ITS#575305
-- |1.44       07-OCT-2016 (NTT)             Modified for the rebates and Claims population ITS#576718
-- |1.45       26-OCT-2016 (NTT)             Modified to send Number3 as zero for Drop ship, Coterm with Quote, Renew with Quote auto registered.ITS#575976,ITS#599807
--
-- +================================================================================================================*/

  gn_user_id         NUMBER      := apps.FND_GLOBAL.USER_ID;
  gn_login_id        NUMBER      := APPS.FND_GLOBAL.LOGIN_ID;
  gn_request_id      NUMBER      := fnd_profile.VALUE('CONC_REQUEST_ID');
  lc_r_return_status VARCHAR2(1) := NULL;
  ln_sqlcode2        NUMBER;
  lc_sqlerrm2        VARCHAR2(2000);
  g_enable_debug     BOOLEAN:= TRUE;
  TYPE stg1_tbl_type
  IS
    TABLE OF XXFT_RPRO_OE_ORDER_DETAILS_V%ROWTYPE INDEX BY BINARY_INTEGER;
  TYPE rpro_order_details_tbl_type
  IS
    TABLE OF XXFT_RPRO_ORDER_DETAILS%ROWTYPE INDEX BY BINARY_INTEGER;
  TYPE serial_rec_type
  IS
    RECORD
    (
      line_id         NUMBER,
      serial_number   VARCHAR2(240),
      contract_number VARCHAR2(240),
      start_date      DATE, -- Defect #3239
      end_date        DATE, -- Defect #3239
      rank1           NUMBER );
  TYPE serial_tbl_type
  IS
    TABLE OF serial_rec_type INDEX BY BINARY_INTEGER;
    --global pl sql table to capture the serial and contract numbers at once.
    g_serial_tbl    serial_tbl_type;
  --
  -- Global collection to capture the Order Level Email Sent Date.
  --
  TYPE so_contract_hdr_rec
  IS
    RECORD
    (
       header_id         NUMBER,
       email_sent_flag   VARCHAR2(2),  -- In case of R - Populate the Email_Sent_Flag
       email_sent_date   DATE,         -- In case of R - Populate the Email Sent Date.
       processed_flag    VARCHAR2(2),  -- N-New R-Ready, NR-Not ready
       email_rqrd_flag   VARCHAR2(1)  -- Y- Yes Need it, N- No Not needed.
      );
  TYPE so_contract_hdr_tbl_type
  IS
    TABLE OF so_contract_hdr_rec INDEX BY BINARY_INTEGER;

  g_contract_hdr_tbl so_contract_hdr_tbl_type;

PROCEDURE error_log(
    P_error_message IN VARCHAR2)
IS
  lv_error_log VARCHAR2(2000):= NULL;
BEGIN
  dbms_output.put_line(p_error_message);
  fnd_file.put_line (fnd_file.LOG, p_error_message);
END;

PROCEDURE print_exception_log(
    P_error_message IN VARCHAR2)
IS
  lv_error_log VARCHAR2(2000):= NULL;
BEGIN
  dbms_output.put_line(p_error_message);
  fnd_file.put_line (fnd_file.LOG, p_error_message);
END;

PROCEDURE print_log(
    pc_message VARCHAR2)
IS
  ln_user_id NUMBER;
BEGIN
  -- SELECT fnd_global.user_id INTO ln_user_id FROM DUAL;
  IF g_enable_debug THEN
    IF (gn_user_id = -1) THEN
      DBMS_OUTPUT.PUT_LINE (pc_message);
    ELSE
      FND_FILE.PUT_LINE (FND_FILE.LOG, pc_message);
      --DBMS_OUTPUT.PUT_LINE (pc_message);
    END IF;
  END IF;
END print_log;

PROCEDURE print_output(
    pc_message VARCHAR2)
IS
  ln_user_id NUMBER;
BEGIN
  --SELECT fnd_global.user_id INTO ln_user_id FROM DUAL;
  -- SELECT fnd_global.user_id INTO ln_user_id FROM DUAL;
  IF g_enable_debug THEN
    IF (gn_user_id = -1) THEN
      DBMS_OUTPUT.PUT_LINE (pc_message);
    ELSE
      FND_FILE.PUT_LINE (FND_FILE.OUTPUT, pc_message);
    END IF;
  END IF;
END print_output;

PROCEDURE print_info_log(
    pc_message VARCHAR2)
IS
  ln_user_id NUMBER;
BEGIN
  --SELECT fnd_global.user_id INTO ln_user_id FROM DUAL;
  -- SELECT fnd_global.user_id INTO ln_user_id FROM DUAL;
  IF (gn_user_id = -1) THEN
    DBMS_OUTPUT.PUT_LINE (pc_message);
  ELSE
    FND_FILE.PUT_LINE (FND_FILE.LOG, pc_message);
  END IF;
END print_info_log;

FUNCTION get_actual_ship_date(
    p_line_id IN NUMBER)
  RETURN DATE
IS
  l_date DATE;
  --Defeect #3289. - Sending the Actual Fulfilment Date for non-shippable lines.
  CURSOR cur_get_ship_date
  IS
    SELECT actual_shipment_date --nvl(actual_shipment_date, ACTUAL_FULFILLMENT_DATE) ITS#542467 
    FROM oe_order_lines_all WHERE line_id = p_line_id;
    
BEGIN
  OPEN cur_get_ship_date;
  FETCH cur_get_ship_date INTO l_date;
  CLOSE cur_get_ship_date;
  RETURN l_date;
END;

FUNCTION CHECK_NFR_ITEM(
    p_item_id IN NUMBER)
  RETURN VARCHAR2
IS
  l_check_nfr_item VARCHAR2(2);
  CURSOR cur_nfr_item
  IS
    SELECT
      CASE
        WHEN COUNT(1) > 0
        THEN 'Y'
        ELSE 'N'
      END "Check_nfr_item"
    FROM mtl_system_items_b msi ,
      mtl_parameters mp
    WHERE msi.inventory_item_id   = p_item_id
    AND mp.master_organization_id = mp.organization_id
    AND msi.organization_id       = mp.organization_id
    AND msi.segment1 LIKE '%NFR%'
    AND msi.item_type ='PTO';
BEGIN
  OPEN cur_nfr_item;
  FETCH cur_nfr_item INTO l_check_nfr_item;
  CLOSE cur_nfr_item;
  RETURN l_check_nfr_item;
END;

--
-- Process to capture the Credit Memo Transactions in a Control table
--
PROCEDURE capture_cm_trxs
IS
  l_non_spr NUMBER;
  CURSOR cur_cm_trxs
  IS
    SELECT CM_HDR.CUSTOMER_TRX_ID ,
            CM_RTL.customer_trx_line_id ,
            CM_HDR.TRX_NUMBER ,
            CM_RTL.INTERFACE_LINE_ATTRIBUTE7 ,
            CM_RTL.INTERFACE_LINE_ATTRIBUTE2 ,
            CM_RTL.INTERFACE_LINE_ATTRIBUTE3
    FROM RA_CUSTOMER_TRX_ALL CM_HDR ,
         RA_CUST_TRX_TYPES_all CM_TYPE,
         RA_CUSTOMER_TRX_LINES_ALL CM_RTL,
         FND_LOOKUP_VALUES FLV,
         ozf_claim_lines_all cl
    WHERE FLV.LOOKUP_TYPE = 'XXFT_RPRO_CM_TRX_TYPES'
      AND FLV.ENABLED_FLAG  = 'Y'
      AND SYSDATE BETWEEN NVL(FLV.START_DATE_ACTIVE,SYSDATE-1) AND NVL(FLV.END_DATE_ACTIVE,SYSDATE+1)
      AND CM_TYPE.NAME = flv.meaning
      --AND CM_TYPE.TYPE            = 'CM'
      AND CM_HDR.CUST_TRX_TYPE_ID = CM_TYPE.CUST_TRX_TYPE_ID
      AND CM_RTL.customer_trx_id  = CM_HDR.customer_trx_id
      AND CM_RTL.LINE_TYPE        = 'LINE'
      AND NOT EXISTS
        (SELECT 1
           FROM XXFT_RPRO_ORDER_CONTROL xpoc
          WHERE XPOC.LINE_ID = CM_RTL.customer_trx_line_id
            AND trx_type ='CM'
      )
      AND CM_RTL.INTERFACE_LINE_ATTRIBUTE7 = 'FTNT SPR Claims'
      AND CM_RTL.INTERFACE_LINE_ATTRIBUTE2 = cl.claim_id
      AND CM_RTL.INTERFACE_LINE_ATTRIBUTE3 = cl.claim_line_id
      AND cl.attribute12 is not null
      AND cl.attribute11  NOT LIKE 'INV%';
BEGIN
  print_log('  + capture_cm_trxs start');
  --
  -- Loop through and
  --
  FOR rec_cm_trxs IN cur_cm_trxs
  LOOP
    print_log(' ----------------------------');
    print_log(' Trx Number :=>'||rec_cm_trxs.TRX_NUMBER);
    print_log(' Trx Number :=>'||rec_cm_trxs.TRX_NUMBER);
    print_log(' ----------------------------');
    --
    -- Capture the Triggering Transaction
    --
    IF rec_cm_trxs.INTERFACE_LINE_ATTRIBUTE7 ='FTNT SPR Claims' THEN
      SELECT COUNT(1)
      INTO l_non_spr
      FROM dual
      WHERE EXISTS
        (SELECT 1
           FROM ozf_claim_lines_all ocl
          WHERE TO_CHAR(ocl.claim_id)    = rec_cm_trxs.INTERFACE_LINE_ATTRIBUTE2
            AND TO_CHAR(ocl.claim_line_id) = rec_cm_trxs.INTERFACE_LINE_ATTRIBUTE3
            AND ocl.attribute12  IS NOT NULL
            AND ocl.attribute11  NOT LIKE 'INV%'
        );
      IF l_non_spr =0 THEN
        print_log(' Skipping the record');
        print_log(' ----------------------------');
        CONTINUE; -- skip to the next iteration.
      END IF;
    END IF;
    INSERT
    INTO XXFT.XXFT_RPRO_ORDER_CONTROL
      (
        HEADER_ID,
        ORDER_NUMBER,
        LINE_ID,
        STATUS,
        BOOKED_BY,
        TRX_TYPE,
        CREATED_BY,
        CREATION_DATE,
        LAST_UPDATED_BY,
        PROCESSED_FLAG,
        REQUEST_ID
      )
      VALUES
      (
        rec_cm_trxs.CUSTOMER_TRX_ID ,     --HEADER_ID,
        rec_cm_trxs.CUSTOMER_TRX_ID,      --ORDER_NUMBER,
        rec_cm_trxs.customer_trx_line_id, --LINE_ID,
        'CREATED',                        --STATUS,
        NULL,                             --BOOKED_BY,
        'CM',                             --TRX_TYPE,
        gn_user_id,
        SYSDATE,
        gn_user_id,
        'N',
        gn_request_id
      );
    print_log(' ----------------------------');
    COMMIT;
  END LOOP;
  print_log('  - capture_cm_trxs End');
EXCEPTION
WHEN OTHERS THEN
  print_exception_log(' Unexpected Exception capture_cm_trxs :=>'||SQLERRM);
  RAISE;
END;
  -- Concurrent Program
PROCEDURE extract_transactions(
    errbuff OUT VARCHAR2,
    retcode OUT NUMBER,
    p_from_date    IN VARCHAR2,
    p_order_number IN NUMBER,
    p_trx_type     IN VARCHAR2)
IS
  lc_return_status      VARCHAR2(1);
  parameter_exp         EXCEPTION;
  lv_message            VARCHAR2(2000):= NULL;
  lv_number_of_days     NUMBER;
  lv_cutoff_date        DATE;
  l_ready_cnt           NUMBER;
  l_processed_cnt       NUMBER;
  l_err_cnt             NUMBER;
  l_waiting_invoice_cnt NUMBER;
BEGIN
  print_info_log('Program start date and Time :=>'||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));
  SELECT NVL(TRUNC(APPS.FND_DATE.CANONICAL_TO_DATE(P_FROM_DATE)),SYSDATE-30)
  INTO lv_cutoff_date
  FROM dual;
  error_log('Inside extract_transactions');
  /* IF p_order_number IS NULL THEN
  RAISE parameter_exp;
  lv_message := 'Order Number is NULL';
  print_output('Order Number is Null :'||p_order_number);
  print_log('Order number is Null :'||p_order_number);
  END IF; */
  SELECT COUNT(1)
  INTO l_ready_cnt
  FROM xxft.xxft_rpro_order_control xpoc
  WHERE 1                 =1 --request_id = gn_request_id
  AND processed_flag      = ANY('R','E')
  AND status NOT         IN ('CANCELLED')
  AND NVL(TRX_TYPE,'ORD') ='ORD'
    --AND error_message is null
  AND EXISTS
    (SELECT 1
    FROM apps.FND_LOOKUP_VALUES flv,
      oe_order_headers_all ooha,
      OE_TRANSACTION_TYPES_TL ott
    WHERE ooha.header_id          = xpoc.header_id
    AND ott.transaction_type_id   = ooha.order_type_id
    AND flv.language              = 'US'
    AND NVL(flv.enabled_flag,'N') = 'Y'
    AND sysdate BETWEEN NVL(flv.start_date_active,sysdate) AND NVL(flv.end_date_active,sysdate + 1)
    AND flv.lookup_type        = 'REVPRO_ORDER_TYPE'
    AND UPPER(flv.lookup_code) = UPPER(ott.name)
    ) ;
  IF p_trx_type = 'SO' THEN
    print_output('Transaction type is SO.');
    print_log(' Transaction type is SO.');
    print_log(' Order Number :'||p_order_number);
    print_log(' From Date    :'||p_from_date);
    --------------error_log('Transactins of SO');
    get_order_details_multi(lv_cutoff_date,p_order_number);
    --capture_cm_trxs;
    --get_invoice_details (lv_cutoff_date,p_order_number);
  ELSIF p_trx_type = 'INV' THEN
    print_output('Transaction type is INV.');
    print_log('Transaction type is INV.');
    --------------error_log('Transactins of INV');
    capture_cm_trxs; --ITS#576718
    get_invoice_details (lv_cutoff_date,p_order_number);
  ELSIF p_trx_type = 'ALL' THEN
    --------------error_log('Transactins of ALL');
    print_output('Transaction type is SO and INV.');
    print_log('Transaction type is SO and INV.');
    capture_cm_trxs;
    get_order_details_multi(lv_cutoff_date,p_order_number);
    get_invoice_details (lv_cutoff_date,p_order_number);
  END IF;
  retcode :=0;
  errbuff := 'Transactions Exctracted successfully';
  SELECT COUNT(1)
  INTO l_processed_cnt
  FROM xxft.xxft_rpro_order_control
  WHERE request_id   = gn_request_id
  AND processed_flag ='P' ;
  SELECT COUNT(1)
  INTO l_err_cnt
  FROM xxft.xxft_rpro_order_control
  WHERE request_id   = gn_request_id
  AND processed_flag ='E';
  SELECT COUNT(1)
  INTO l_waiting_invoice_cnt
  FROM xxft.xxft_rpro_order_control
  WHERE request_id   = gn_request_id
  AND processed_flag ='NR' ;
  print_info_log(' Number of Sales Order Lines to be processed :=>'||l_ready_cnt);
  print_info_log(' Number of Sales Order Lines successfully  processed :=>'||l_processed_cnt);
  print_info_log(' Number of Sales Order Lines had valiation errors :=>'||l_err_cnt);
  print_info_log(' Number of Sales Order Lines are waiting for Invoice creation/Contract Creation :=>'||l_waiting_invoice_cnt);
  print_info_log('Program End date and Time :=>'||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));
EXCEPTION
WHEN parameter_exp THEN
  retcode := 2;
  errbuff := lv_message;
WHEN OTHERS THEN
  retcode :=2;
  errbuff := SUBSTR(sqlerrm,1,150);
END extract_transactions;

  -- Update Processing Attribute for missing info

  PROCEDURE update_revenue_cat_acct(p_order_line_id IN NUMBER)
  IS
  CURSOR cur_acct
  IS
   SELECT xxod.SALES_ORDER_LINE_BATCH_ID
   FROM XXFT_RPRO_ORDER_DETAILS XXOD
   WHERE (PRODUCT_FAMILY is null or PRODUCT_CATEGORY is null or PRODUCT_LINE is null or PRODUCT_CLASS is null or ATTRIBUTE15 is null
                                 or REV_ACCTG_SEG1 is null or REV_ACCTG_SEG2 is null or REV_ACCTG_SEG3 is null or REV_ACCTG_SEG4 is null
		                         or REV_ACCTG_SEG5 is null or REV_ACCTG_SEG6 is null or REV_ACCTG_SEG7 is null
			                     or COGS_R_SEG1 is null or COGS_R_SEG2 is null or COGS_R_SEG3 is null or COGS_R_SEG4 is null
								 or COGS_R_SEG5 is null or COGS_R_SEG6 is null or COGS_R_SEG7 is null
								 or DEF_ACCTG_SEG1 is null or DEF_ACCTG_SEG1 is null or DEF_ACCTG_SEG1 is null or DEF_ACCTG_SEG1 is null
								 or DEF_ACCTG_SEG1 is null or DEF_ACCTG_SEG1 is null or DEF_ACCTG_SEG1 is null
								 or COGS_D_SEG1 is null or COGS_D_SEG2 is null or COGS_D_SEG3 is null or COGS_D_SEG4 is null
								 or COGS_D_SEG5 is null or COGS_D_SEG6 is null or COGS_D_SEG7 is null)
		AND xxod.SALES_ORDER_LINE_ID = p_order_line_id ;

		l_cnt       NUMBER :=0;
    l_int       NUMBER :=0;

    BEGIN
	FOR lcu_cur_acct_rec in cur_acct
    LOOP

		Select count(*)
		into l_cnt
        from XXFT_RPRO_ORDER_DETAILS
        where SALES_ORDER_LINE_BATCH_ID = lcu_cur_acct_rec.SALES_ORDER_LINE_BATCH_ID;

		IF l_cnt > 1  THEN
			BEGIN
			UPDATE XXFT_RPRO_ORDER_DETAILS
			SET    PROCESSING_ATTRIBUTE1  = 'N'
			WHERE  SALES_ORDER_LINE_BATCH_ID = lcu_cur_acct_rec.SALES_ORDER_LINE_BATCH_ID;
			END;
		END IF;
			print_output('PROCESSING_ATTRIBUTE1 is Updated for '||lcu_cur_acct_rec.SALES_ORDER_LINE_BATCH_ID);
			print_log('PROCESSING_ATTRIBUTE1 is Updated for '||lcu_cur_acct_rec.SALES_ORDER_LINE_BATCH_ID);
    END LOOP;

	COMMIT;
    EXCEPTION
    WHEN OTHERS THEN
	FND_FILE.PUT_LINE(FND_FILE.LOG,'Error in the Update PROCESSING ATTRIBUTE1:' || SQLERRM);

  END update_revenue_cat_acct;

  --- Events procedures

  -- Cash Event
  PROCEDURE get_cash_event(p_order_number VARCHAR2)
  IS
    CURSOR cur_cash_event
    IS
      SELECT invoice_id,
             sales_order,
             sales_order_id,
             release_event_id,
             event_type
        FROM REVPRO.RPRO_FN_RELEASE_EVENTS_V
        WHERE event_type = 'CASH'
          AND sales_order = p_order_number;

    CURSOR cur_cash_amount(c_invoice_id NUMBER)
    IS
    SELECT a.amount_applied,
             a.applied_customer_trx_id customer_trx_id,
             b.trx_number,
             a.cash_receipt_id
        FROM ar_receivable_applications_all a,
             ra_customer_trx_all b
       WHERE a.applied_customer_trx_id = b.customer_trx_id
         and a.application_type          = 'CASH'
         AND a.status                    = 'APP'
         AND a.applied_customer_trx_id   = c_invoice_id
         AND NOT EXIsts (select 1 from revpro.rpro_fn_release_events c
         where c.cash_receipt_id = a.cash_receipt_id);
		 /* NOT EXISTS (select 1 from revpro.rpro_fn_release_events_hist c
         where c.cash_receipt_id = a.cash_receipt_id)*/

  BEGIN
    print_log('INSIDE CASH EVENT ');
    FOR i IN cur_cash_event
    LOOP
      print_log('Order that received Cash event : '||i.sales_order);
      FOR j IN cur_cash_amount(i.invoice_id)
      LOOP
      print_log('Received Cash amount : '||j.amount_applied);
        INSERT
        INTO REVPRO.RPRO_FN_RELEASE_EVENTS
          (
            STG_EVENT_ID,
            SALES_ORDER,
            SALES_ORDER_ID,
            RELEASE_AMOUNT,
            CASH_RECEIPT_ID,
            INVOICE_ID,
            EVENT_ID,
            EVENT_TYPE,
            PROCESSED_FLAG,
            CREATED_BY,
            CREATION_DATE,
            LAST_UPDATE_DATE,
            LAST_UPDATED_BY
          )
          VALUES
          (
            REVPRO.RPRO_FN_EVENTS_S.nextval,
            i.SALES_ORDER,
            i.sales_order_id,
            j.amount_applied,
            j.cash_receipt_id,
            i.invoice_id,
            i.release_event_id,
            i.event_type,
            'N',
            -1,
            SYSDATE,
            SYSDATE,
            -1
          );
      END LOOP;
      print_log('Cash event Processed successfully for order  : '||i.sales_order);
    END LOOP;
      print_log('Cash events successfully for all Orders');
    commit;
  EXCEPTION
  WHEN OTHERS THEN
    print_exception_log('Cash Event Exception :'||sqlerrm);
  END get_cash_event;

  -- Registration Event
  PROCEDURE get_reg_event(p_order_number NUMBER
                         , p_contract_number IN VARCHAR2
						 , p_registration_date IN VARCHAR2 --ITS#544171
						 )
  IS
  
     l_event_exists_count NUMBER;
  
    CURSOR cur_reg_event
    IS
      SELECT distinct
              contract_number
            , oracle_line_id
					  , event_type
					  , release_event_id
					  , orig_so_line_id
					  , sales_order_line_id
            , data_source
            , service_type  --added service type on 6/17 for Reg ERP events.
        FROM REVPRO.RPRO_FN_RELEASE_EVENTS_V a
       WHERE event_type  = 'REG'
         --AND module      = 'OKS'
         AND data_source ='ERP'
         AND sales_order = NVL(to_char(p_order_number),sales_order)
         AND contract_number = NVL(p_contract_number,contract_number)
         and exists (SELECT 1 FROM xxft_fo_contract 
		              where contract_number = a.contract_number and process_status='SUCCESS'
		                AND TRUNC(REGISTRATION_DATE) <= NVL(to_date(p_registration_date, 'DD-MON-YY'),TRUNC(REGISTRATION_DATE)) --ITS#544171
					)
        UNION
              SELECT distinct
              contract_number
            , oracle_line_id
					  , event_type
					  , release_event_id
					  , orig_so_line_id
					  , sales_order_line_id
            , data_source
            , service_type  --added service type on 6/17 for Reg ERP events.
        FROM REVPRO.RPRO_FN_RELEASE_EVENTS_V a
       WHERE event_type  = 'REG'
         --AND module      = 'OKS'
         AND data_source ='ERP'
         AND sales_order = NVL(to_char(p_order_number),sales_order)
         AND contract_number = NVL(p_contract_number,contract_number)
         and exists (SELECT 1 FROM XXFT_FO_WARRANTY  --ITS#570061
		              where TO_CHAR(WARRANTY_ID) = a.contract_number and process_status='SUCCESS'
		                AND TRUNC(REGISTRATION_DATE) <= NVL(to_date(p_registration_date, 'DD-MON-YY'),TRUNC(REGISTRATION_DATE)) --ITS#544171
					);

    CURSOR cur_history_reg_event
    IS
    SELECT DISTINCT contract_number ,
            event_type ,
            release_event_id,
            sales_order_line_id,
            data_source,
            fc_type
      FROM REVPRO.RPRO_FN_RELEASE_EVENTS_V a
     WHERE event_type = 'REG'
       AND data_source ='HISTORY'
       AND contract_number = NVL(p_contract_number,contract_number)
       AND exists (SELECT 1 FROM xxft_fo_contract where contract_number = a.contract_number and process_status='SUCCESS'
       AND TRUNC(REGISTRATION_DATE) <= NVL(to_date(p_registration_date, 'DD-MON-YY'),TRUNC(REGISTRATION_DATE)) --ITS#544171
       )
       UNION
           SELECT DISTINCT contract_number ,
            event_type ,
            release_event_id,
            sales_order_line_id,
            data_source,
            fc_type
      FROM REVPRO.RPRO_FN_RELEASE_EVENTS_V a
     WHERE event_type = 'REG'
       AND data_source ='HISTORY'
       AND contract_number = NVL(p_contract_number,contract_number)
       AND exists (SELECT 1 FROM XXFT_FO_WARRANTY XFW where TO_CHAR(WARRANTY_ID) = a.contract_number and process_status='SUCCESS' --ITS#570061
       AND TRUNC(REGISTRATION_DATE) <= NVL(to_date(p_registration_date, 'DD-MON-YY'),TRUNC(REGISTRATION_DATE)) --ITS#544171
       ); -- CR#87 - historical events Data.


    CURSOR cur_history_reg_event_details(c_contract_number VARCHAR2)
    IS  -- CR#87 - Include additional Fields for Historical Events.
    SELECT xfsp.support_type ,
            oal.id serv_line_id,
            NVL(oal.attribute1,msib.segment1) serv_item,
            OKLB.ATTRIBUTE1 Start_Date,
            OKLB.ATTRIBUTE2 End_Date,
            OKLB.ATTRIBUTE3 Registration_Date,
            OKLB.ATTRIBUTE4 Auto_Start_Date,
            CII.SERIAL_NUMBER SERIAL_NUMBER,
            oah.CONTRACT_NUMBER contract_number
      FROM apps.okc_k_items serv_oki ,
            apps.okc_k_lines_b oal ,
            apps.okc_k_headers_all_b oah ,
            apps.mtl_system_items_b msib ,
            xxft.xxft_fo_sup_pkg xfsp,
            APPS.OKC_K_LINES_B OKLB,
            APPS.OKC_K_ITEMS OKIB,
            APPS.CSI_ITEM_INSTANCES CII
     WHERE 1                        =1
       AND serv_oki.jtot_object1_code = 'OKX_SERVICE'
       AND serv_oki.cle_id            = oal.id
       AND oah.id                     = oal.chr_id
       AND serv_oki.object1_id1       = msib.inventory_item_id
       AND msib.organization_id       =(SELECT organization_id
                                          FROM mtl_parameters
                                         WHERE organization_id = master_organization_id
                                        )
       AND oah.contract_number                                    = c_contract_number
       AND SUBSTR(msib.segment1,INSTR(msib.segment1,'-',1,3)+1,3) = TO_CHAR( xfsp.support_package)
       AND oal.ID                                                 = OKLB.CLE_ID
       AND OKLB.ID                                                = OKIB.CLE_ID
       AND OKIB.OBJECT1_ID1                                       = CII.INSTANCE_ID
       AND OKLB.sts_code= any('ACTIVE','SIGNED','EXPIRED') --Including Expired Status
       AND NOT EXISTS  /* ITS#540799*/
           (SELECT cis.instance_status_id
              FROM CSI_INSTANCE_STATUSES cis
             WHERE cis.name             ='EXPIRED'
               AND cis.instance_status_id = cii.instance_status_id
           ); --#3461




    CURSOR cur_reg_event_details(c_contract_number VARCHAR2)
    IS
    SELECT  OOH.ORDER_NUMBER       order_number,
             OKH.CONTRACT_NUMBER contract_number,
             OKLB.ATTRIBUTE1     Start_Date,
             OKLB.ATTRIBUTE2     End_Date,
             OKLB.ATTRIBUTE3     Registration_Date,
             OKLB.ATTRIBUTE4     Auto_Start_Date,
             CII.SERIAL_NUMBER   serial_numnber,
             xrid.attribute44,
             count(1)           no_of_childs --, CR#87 - historical events Data
--             xrid.sales_order_line_id order_line_id
             FROM APPS.OE_ORDER_LINES_ALL OOL,
             APPS.OE_ORDER_HEADERS_ALL OOH,
             -- OKC_K_REL_OBJS REL,--Commented by obi on 9/8/2016
			(SELECT DISTINCT jtot_object1_code,OBJECT1_ID1,CHR_ID FROM OKC_K_REL_OBJS) REL,--Added by Obi on 9/8/2016
             APPS.OKC_K_HEADERS_ALL_B OKH,
             APPS.OKC_K_LINES_B OKL,
             APPS.OKC_K_ITEMS OKI,
             APPS.OKC_K_LINES_B OKLB,
             APPS.OKC_K_ITEMS OKIB,
             APPS.CSI_ITEM_INSTANCES CII,
             xxft.XXFT_RPRO_order_DETAILS xrid
      WHERE 1=1
        AND OKH.CONTRACT_NUMBER   = c_contract_number
        AND OOH.HEADER_ID         = OOL.HEADER_ID
        and xrid.attribute45      =  to_char(OKH.CONTRACT_NUMBER)
        AND OOH.ORG_ID            = OOL.ORG_ID
        and ool.line_id           = xrid.sales_order_line_id
        AND REL.jtot_object1_code = 'OKX_ORDERLINE'
        AND REL.OBJECT1_ID1       = to_char(OOL.LINE_ID)
        AND REL.CHR_ID            = OKH.ID		
        AND OKH.ID                = OKL.CHR_ID
        AND OKL.ID                = OKI.CLE_ID
        AND OKI.OBJECT1_ID1       = OOL.INVENTORY_ITEM_ID
        AND OKL.ID                = OKLB.CLE_ID
        AND OKLB.ID               = OKIB.CLE_ID
        AND OKIB.OBJECT1_ID1      = CII.INSTANCE_ID
		AND OKLB.sts_code= any('ACTIVE','SIGNED','EXPIRED') --Added by Obi on 9/8/2016
        AND NOT EXISTS       /* ITS#540799*/
           (SELECT cis.instance_status_id
              FROM CSI_INSTANCE_STATUSES cis
             WHERE cis.name             ='EXPIRED'
               AND cis.instance_status_id = cii.instance_status_id
           )
        group by OOH.ORDER_NUMBER     ,
             OKH.CONTRACT_NUMBER ,
             OKLB.ATTRIBUTE1     ,
             OKLB.ATTRIBUTE2     ,
             OKLB.ATTRIBUTE3     ,
             OKLB.ATTRIBUTE4     ,
             CII.SERIAL_NUMBER   ,
             xrid.attribute44;

     CURSOR cur_new_reg_events
     IS
     SELECT STG_EVENT_ID,
            SO_LINE_ID,
            REGISTRATION_DATE,
            RULE_START_DATE,
            RULE_END_DATE,
            EVENT_ID,
            EVENT_TYPE,
            PROCESSED_FLAG,
            SALES_ORDER,
            CREATED_BY,
            CREATION_DATE,
            LAST_UPDATE_DATE,
            LAST_UPDATED_BY,
     	  		service_type,
		   	    contract_number,
			      serial_number,
            no_of_childs,
            data_source,
            fc_type
       FROM XXFT.XXFT_RPRO_FN_RELEASE_EVENTS
      WHERE PROCESSED_FLAG = 'N'
        AND event_type     = 'REG';


  BEGIN
    print_info_log('INSIDE REGISTATION EVENTS ');

    FOR i IN cur_reg_event
     LOOP
      print_info_log('Event REG Contract Number : '||i.contract_number);
      FOR j IN cur_reg_event_details(i.contract_number)
      LOOP
         print_info_log('INSIDE REG EVENT DETAILS SO Line ID   :'||i.orig_so_line_id);
         print_info_log('INSIDE REG EVENT DETAILS - Start Date :'||j.Start_Date);
         print_info_log('INSIDE REG EVENT DETAILS - End Date   :'||j.end_Date);
         print_info_log('INSIDE REG EVENT DETAILS - Reg Date   :'||j.Registration_Date);
         print_info_log('INSIDE REG EVENT DETAILS - ServiceType:'||i.service_type);


      IF  j.registration_date IS NOT NULL THEN
        IF NVL(i.service_type,j.attribute44) = j.attribute44
        THEN
           --
           -- Check if the Event is already extracted.
           --
--           SELECT COUNT(1)
--             INTO l_event_exists_count
--             FROM XXFT_RPRO_FN_RELEASE_EVENTS
--            WHERE SO_LINE_ID = i.sales_order_line_id
--            and SALES_ORDER =  j.order_number
--            and EVENT_ID   =  i.release_event_id
--            and EVENT_TYPE   =  i.event_type;
--           
--          IF l_event_exists_count =0 
--          THEN
           INSERT
           INTO XXFT_RPRO_FN_RELEASE_EVENTS --REVPRO.RPRO_FN_RELEASE_EVENTS
          (
            STG_EVENT_ID,
            SO_LINE_ID,
            REGISTRATION_DATE,
            RULE_START_DATE,
            RULE_END_DATE,
            EVENT_ID,
            EVENT_TYPE,
            PROCESSED_FLAG,
            SALES_ORDER,
            CREATED_BY,
            CREATION_DATE,
            LAST_UPDATE_DATE,
            LAST_UPDATED_BY,
     	  		service_type,
		   	    contract_number,
			      serial_number,
            no_of_childs,
            data_source
          )
          VALUES
          (
            REVPRO.RPRO_FN_EVENTS_S.nextval,
            i.sales_order_line_id,
            to_date(j.registration_date,'DD-MON-YY'),
            to_date(j.start_date,'DD-MON-YY'),
            to_date(j.end_date,'DD-MON-YY'),
            i.release_event_id,
            i.event_type,
            'N',
            j.order_number,
            -1,
            SYSDATE,
            SYSDATE,
            -1,
			      j.attribute44,
			      j.contract_number,
			      j.serial_numnber,
            j.no_of_childs,
            i.data_source
          );
--        END IF;
        --print_log('REGISTRATION Events - Order Line Id    : '||j.order_line_id);
        print_info_log('REGISTRATION Events - registration_date: '||j.registration_date);
        END IF;
       END IF;
      END LOOP;
    END LOOP;
   commit;
    IF p_order_number is null
    THEN
       FOR i IN cur_history_reg_event
       LOOP
         print_info_log('History Event REG Contract Number : '||i.contract_number);
         FOR j IN cur_history_reg_event_details(i.contract_number)
         LOOP
            -- print_log('INSIDE REG History EVENT DETAILS SO Line ID   :'||j.orig_so_line_id);
            print_info_log('INSIDE REG History EVENT DETAILS Service LineID :'||j.serv_line_id);
            print_info_log('INSIDE REG History EVENT DETAILS - Start Date :'||j.Start_Date);
            print_info_log('INSIDE REG History EVENT DETAILS - End Date   :'||j.end_Date);
            print_info_log('INSIDE REG History EVENT DETAILS - Reg Date   :'||j.Registration_Date);

      IF  j.registration_date IS NOT NULL
        AND to_char(i.fc_type) = to_char(j.support_type) -- ITS#544227
      THEN
--            SELECT COUNT(1)
--             INTO l_event_exists_count
--             FROM XXFT_RPRO_FN_RELEASE_EVENTS
--            WHERE SO_LINE_ID = i.sales_order_line_id
--            and EVENT_ID   =  i.release_event_id
--            and EVENT_TYPE   =  i.event_type;
--        IF   l_event_exists_count = 0 
--        THEN 
            
        INSERT
        INTO XXFT_RPRO_FN_RELEASE_EVENTS --REVPRO.RPRO_FN_RELEASE_EVENTS
          (
            STG_EVENT_ID,
            SO_LINE_ID,
            REGISTRATION_DATE,
            RULE_START_DATE,
            RULE_END_DATE,
            EVENT_ID,
            EVENT_TYPE,
            PROCESSED_FLAG,
            SALES_ORDER,
            CREATED_BY,
            CREATION_DATE,
            LAST_UPDATE_DATE,
            LAST_UPDATED_BY,
            fc_type,
		   	    contract_number,
			      serial_number,
            data_source,
            no_of_childs
          )
          VALUES
          (
            REVPRO.RPRO_FN_EVENTS_S.nextval,
            i.sales_order_line_id,
            to_date(j.registration_date,'DD-MON-YY'),
            to_date(j.start_date,'DD-MON-YY'),
            to_date(j.end_date,'DD-MON-YY'),
            i.release_event_id,
            i.event_type,
            'N',
            NULL,
            -1,
            SYSDATE,
            SYSDATE,
            -1,
            j.support_type,
            j.contract_number,
            j.serial_number,
            i.data_source,
            1
          );
--          END IF;
             -- print_log('History REGISTRATION Events - Order Line Id    : '||j.order_line_id);
           print_info_log('History REGISTRATION Events - registration_date: '||j.registration_date);
           END IF;
         END LOOP;
       END LOOP;
    END IF;

   --
   -- Loop Through the Events and Populate the RevproEvents table.
   --
   FOR rec_events IN cur_new_reg_events
   LOOP
   BEGIN
     INSERT
        INTO REVPRO.RPRO_FN_RELEASE_EVENTS
          (
            STG_EVENT_ID,
            SO_LINE_ID,
            REGISTRATION_DATE,
            RULE_START_DATE,
            RULE_END_DATE,
            EVENT_ID,
            EVENT_TYPE,
            PROCESSED_FLAG,
            SALES_ORDER,
            CREATED_BY,
            CREATION_DATE,
            LAST_UPDATE_DATE,
            LAST_UPDATED_BY,
            fc_type,
		   	    contract_number,
			      serial_number,
            data_source,
            no_of_childs
          )
        VALUES(
            rec_events.STG_EVENT_ID,
            rec_events.SO_LINE_ID,
            rec_events.REGISTRATION_DATE,
            rec_events.RULE_START_DATE,
            rec_events.RULE_END_DATE,
            rec_events.EVENT_ID,
            rec_events.EVENT_TYPE,
            rec_events.PROCESSED_FLAG,
            rec_events.SALES_ORDER,
            rec_events.CREATED_BY,
            rec_events.CREATION_DATE,
            rec_events.LAST_UPDATE_DATE,
            rec_events.LAST_UPDATED_BY,
            rec_events.fc_type,
		   	    rec_events.contract_number,
			      rec_events.serial_number,
            rec_events.data_source,
            rec_events.no_of_childs
        );

      UPDATE XXFT.XXFT_RPRO_FN_RELEASE_EVENTS
          SET processed_flag='P',
              last_update_date = SYSDATE,
              last_updated_by = gn_user_id,
              request_id = gn_request_id
        WHERE STG_EVENT_ID = rec_events.STG_EVENT_ID;
        COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         print_exception_log(' Unexpected exception while inserting the Events:=>'||SQLERRM);
         RAISE;
   END;
   END LOOP;

   print_info_log('REGISTRATION Events Processed Successfully');
  EXCEPTION
  WHEN OTHERS THEN
    print_exception_log('Exception in REGISTRATION Events '||sqlerrm);
  END get_reg_event;

  PROCEDURE get_pcs_event
  IS
    CURSOR cur_reg_event
    IS
      SELECT *
        FROM REVPRO.RPRO_FN_RELEASE_EVENTS_V
       WHERE event_type  = 'REG';

    CURSOR cur_reg_event_details(c_order_line_id NUMBER, c_serial_number VARCHAR2)
    IS
      SELECT distinct  OOH.ORDER_NUMBER ,
             OOL.ORDERED_ITEM ,
             ool.line_id,
             OKH.CONTRACT_NUMBER CONTRACT_NUMBER,
             OKH.ID,
             OKLB.ATTRIBUTE1 Start_Date,
             OKLB.ATTRIBUTE2 End_Date,
             OKLB.ATTRIBUTE3 Registration_Date,
             OKLB.ATTRIBUTE4 Auto_Start_Date,
             CII.SERIAL_NUMBER,
             xrod.event_type event_type,
             xrod.sales_order_line_id order_line_id
        FROM APPS.OE_ORDER_LINES_ALL OOL,
             APPS.OE_ORDER_HEADERS_ALL OOH,
             OKC_K_REL_OBJS REL,
             APPS.OKC_K_HEADERS_ALL_B OKH,
             APPS.OKC_K_LINES_B OKL,
             APPS.OKC_K_ITEMS OKI,
             APPS.OKC_K_LINES_B OKLB,
             APPS.OKC_K_ITEMS OKIB,
             APPS.CSI_ITEM_INSTANCES CII,
             revpro.rpro_fn_release_events_v xrod
      WHERE 1=1
      AND OOL.LINE_ID           = c_order_line_id
      and ool.line_id           = xrod.oracle_line_id
      AND OOH.HEADER_ID         = OOL.HEADER_ID
      AND OOH.ORG_ID            = OOL.ORG_ID
      and CII.SERIAL_NUMBER     = xrod.serial_number
      and CII.SERIAL_NUMBER     = c_serial_number
      AND REL.jtot_object1_code = 'OKX_ORDERLINE'
      AND REL.OBJECT1_ID1       = to_char(OOL.LINE_ID)
      AND REL.CHR_ID            = OKH.ID
      AND OKH.ID                = OKL.CHR_ID
      AND OKL.ID                = OKI.CLE_ID
      AND OKI.OBJECT1_ID1       = OOL.INVENTORY_ITEM_ID
      AND OKL.ID                = OKLB.CLE_ID
      AND OKLB.ID               = OKIB.CLE_ID
      AND OKIB.OBJECT1_ID1      = CII.INSTANCE_ID;

  BEGIN
    print_log('INSIDE PCS EVENTS ');

    FOR i IN cur_reg_event
     LOOP
       print_log('INSIDE REG EVENT '||i.orig_so_line_id);
       print_log('INSIDE REG EVENT '||i.serial_number);
      FOR j IN cur_reg_event_details(i.orig_so_line_id,i.serial_number)
      LOOP
         print_log('INSIDE REG EVENT DETAILS '||j.Start_Date);
         print_log('INSIDE REG EVENT DETAILS'||j.end_Date);
         print_log('INSIDE REG EVENT DETAILS'||j.Registration_Date);

      IF  j.registration_date IS NOT NULL THEN
        INSERT
        INTO REVPRO.RPRO_FN_RELEASE_EVENTS
          (
            STG_EVENT_ID,
            SO_LINE_ID,
            REGISTRATION_DATE,
            RULE_START_DATE,
            RULE_END_DATE,
            EVENT_ID,
            EVENT_TYPE,
            PROCESSED_FLAG,
            CREATED_BY,
            CREATION_DATE,
            LAST_UPDATE_DATE,
            LAST_UPDATED_BY
          )
          VALUES
          (
            REVPRO.RPRO_FN_EVENTS_S.nextval,
            j.order_line_id,
            j.registration_date,
            j.start_date,
            j.end_date,
            i.release_event_id,
            i.event_type,
            'N',
            -1,
            SYSDATE,
            SYSDATE,
            -1
          );
       END IF;
      END LOOP;
    END LOOP;
  EXCEPTION
  WHEN OTHERS THEN
    print_exception_log('Exception in PCS Events '||sqlerrm);
  END get_pcs_event;

  -- SAAS Event
  PROCEDURE get_saas_event
  IS
    CURSOR cur_saas_event
    IS
      SELECT *
        FROM REVPRO.RPRO_FN_RELEASE_EVENTS_V
       WHERE event_type = 'REG';

    CURSOR cur_saas_event_details(c_order_line_id NUMBER)
    IS
      SELECT DISTINCT oela.line_id line_id ,
             csi.attribute3 license_number ,
             TRUNC(csi.creation_date) creation_date,
             msib.segment1 ,
             oela.line_number ,
             csi.quantity ,
             oela.unit_selling_price amount ,
             csi.attribute3 ecard_number ,
             csi.attribute4 ecard_activation_code ,
             NULL ecard_expiration_date ,
             csi.attribute9 eval_period ,
             csi.attribute1 registration_date
        FROM csi_item_instances csi ,
             oe_order_lines_all oela ,
             mtl_system_items_b msib
       WHERE csi.last_oe_order_line_id    = oela.line_id
         AND oela.line_id                 = c_order_line_id
         AND msib.inventory_item_id       = csi.inventory_item_id
         AND csi.last_vld_organization_id = msib.organization_id
         AND csi.attribute3  IS NOT NULL;
  BEGIN
    FOR i IN cur_saas_event
    LOOP
      FOR j IN cur_saas_event_details(i.sales_order_line_id)
      LOOP
        INSERT
        INTO REVPRO.RPRO_FN_RELEASE_EVENTS
          (
            STG_EVENT_ID,
            SO_LINE_ID,
            REGISTRATION_DATE,
            RULE_START_DATE,
            RULE_END_DATE,
            EVENT_ID,
            EVENT_TYPE,
            PROCESSED_FLAG,
            CREATED_BY,
            CREATION_DATE,
            LAST_UPDATE_DATE,
            LAST_UPDATED_BY
          )
          VALUES
          (
            REVPRO.RPRO_FN_EVENTS_S.nextval,
            j.line_id,
            to_date(j.registration_date,'DD-MON-YY'),
            j.ecard_activation_code,
            j.ecard_expiration_date,
            i.release_event_id,
            i.event_type,
            'N',
            -1,
            SYSDATE,
            SYSDATE,
            -1
          );
      END LOOP;
    END LOOP;
  EXCEPTION
  WHEN OTHERS THEN
    NULL;
  END get_saas_event;

    -- SAAS Event
  PROCEDURE get_saas_event(p_order_number NUMBER )
  IS
    CURSOR cur_saas_event
    IS
      SELECT *
        FROM REVPRO.RPRO_FN_RELEASE_EVENTS_V
       WHERE event_type  = 'REG'
         AND module      = 'IB'
         AND sales_order = NVL(p_order_number,sales_order);

    CURSOR cur_saas_event_details(cp_order_number NUMBER)
    IS
      SELECT oela.line_id line_id,
             oeh.order_number,
             csi.attribute3 license_number ,
             TRUNC(csi.creation_date) creation_date ,
             msib.segment1 ,
             oela.line_number ,
             csi.quantity ,
             oela.unit_selling_price amount ,
             csi.attribute3 ecard_number ,
             csi.attribute4 ecard_activation_code ,
             NULL ecard_expiration_date ,
             csi.attribute9 eval_period ,
             csi.attribute1 registration_date
        FROM oe_order_headers_all oeh,
             csi_item_instances csi,
             oe_order_lines_all oela ,
             mtl_system_items_b msib
       WHERE 1=1
          AND oeh.order_number            = cp_order_number
         AND  oeh.header_id               = oela.header_id
         AND csi.last_oe_order_line_id    = oela.line_id
         AND msib.inventory_item_id       = csi.inventory_item_id
         AND csi.last_vld_organization_id = msib.organization_id
         AND csi.attribute1  IS NOT NULL;

  BEGIN
    print_log('INSIDE SAAS REGISTATION EVENTS');
    FOR i IN cur_saas_event
    LOOP
      FOR j IN cur_saas_event_details(i.sales_order)
      LOOP
        print_log('Saas Events - Line Id : '||j.line_id);
        print_log('Saas Events - Registration : '||j.registration_date);
        print_log('Saas Events - Registration : '||j.ecard_activation_code);
        INSERT
        INTO REVPRO.RPRO_FN_RELEASE_EVENTS
          (
            STG_EVENT_ID,
            SO_LINE_ID,
            REGISTRATION_DATE,
            RULE_START_DATE,
            RULE_END_DATE,
            EVENT_ID,
            EVENT_TYPE,
            PROCESSED_FLAG,
            SALES_ORDER,
            CREATED_BY,
            CREATION_DATE,
            LAST_UPDATE_DATE,
            LAST_UPDATED_BY
          )
          VALUES
          (
            REVPRO.RPRO_FN_EVENTS_S.nextval,
            j.line_id,
            to_date(j.registration_date,'DD-MON-YY'),
            j.ecard_activation_code,
            j.ecard_expiration_date,
            i.release_event_id,
            i.event_type,
            'N',
            j.order_number,
            -1,
            SYSDATE,
            SYSDATE,
            -1
          );
      END LOOP;
    END LOOP;
  EXCEPTION
  WHEN OTHERS THEN
    print_exception_log('Exception in PCS Events '||sqlerrm);
  END get_saas_event;

  -- Delivery Event
  PROCEDURE get_delivery_event
  IS
    CURSOR cur_del_event
    IS
      SELECT *
        FROM REVPRO.RPRO_FN_RELEASE_EVENTS_V
       WHERE event_type = 'DEL';

    CURSOR cur_del_event_details(c_order_line_id NUMBER)
    IS
      SELECT oel.line_id,
             oel.flow_status_code,
             oeh.order_number
        FROM oe_order_lines_all    oel,
             oe_order_headers_all  oeh
       WHERE 1 = 1
         AND oel.header_id        =  oeh.header_id
         AND oel.flow_status_code = 'CLOSED'
         AND oel.line_id          =  c_order_line_id;
  BEGIN
    print_log('INSIDE DELIVERY EVENTS ');
    FOR i IN cur_del_event
    LOOP
      FOR j IN cur_del_event_details(i.sales_order_line_id)
      LOOP
        print_log('DELIVERY EVENTS - Order Number  :'||j.order_number);
        print_log('DELIVERY EVENTS - Order Line Id :'||j.line_id);
        print_log('DELIVERY EVENTS - Order Line Id :'||j.line_id);

        INSERT
        INTO REVPRO.RPRO_FN_RELEASE_EVENTS
          (
            STG_EVENT_ID,
            SO_LINE_ID,
            RELEASE_PERCENT,
            EVENT_ID,
            EVENT_TYPE,
            PROCESSED_FLAG,
            SALES_ORDER,
            CREATED_BY,
            CREATION_DATE,
            LAST_UPDATE_DATE,
            LAST_UPDATED_BY
          )
          VALUES
          (
            REVPRO.RPRO_FN_EVENTS_S.nextval,
            j.line_id,
            '100',
            i.release_event_id,
            i.event_type,
            'N',
            j.order_number,
            -1,
            SYSDATE,
            SYSDATE,
            -1
          );
      END LOOP;
    END LOOP;
  EXCEPTION
  WHEN OTHERS THEN
    NULL;
  END get_delivery_event;

  PROCEDURE process_events
    (
      errbuff OUT VARCHAR2 ,
      retcode OUT NUMBER,
      p_order_number  IN NUMBER,
      p_contract_number IN VARCHAR2,
	  p_registration_date IN VARCHAR2 --ITS#544171
    )
  IS
  BEGIN
     print_log('INSIDE PROCESS EVENTS ');
     -- Calling SAAS event

     --print_log('Calling SAAS event ');
     --get_saas_event;
     --print_log('Completed SAAS event ');

    -- Calling PCS event
     print_log('Calling PCS event ');
     get_reg_event(p_order_number,p_contract_number,p_registration_date); --ITS#544171
     print_log('Completed PCS event ');

    -- Cash Event
     print_log('Calling CASH event ');
     get_cash_event(p_order_number);
     print_log('Completed CASH event ');

    -- Delivery Event
     --print_log('Calling Delivery event ');
     --get_delivery_event;
     --print_log('Completed Delivery event ');
    --
    -- Get Grace Period Event CR#95
    --
    print_log('Calling Grace Period Event Change');
    --get_grace_period_event;
    print_log('Completing Grace Period Event Change');

     retcode :=0;
     errbuff := 'Events Exctracted successfully';
  EXCEPTION
     WHEN OTHERS THEN
        retcode :=2;
        errbuff := SUBSTR(sqlerrm,1,150);
  END process_events;

  PROCEDURE get_invoice_details(p_from_date IN VARCHAR2, p_order_number IN NUMBER)
	IS
    ln_qty_cnt         NUMBER      := 1;
    lv_batch_id        NUMBER      := 0;
    l_cnt              NUMBER      := 0;
    l_cnt_cm           NUMBER      := 0;
    l_batch_id         NUMBER      := 0;
    lv_cra_cnt         NUMBER      := 0;
    l_non_serialized   VARCHAR2(2) := 'Y';
    lv_non_serial_item VARCHAR2(2);
    l_cm_quantity      NUMBER;
    ln_serial_exists   NUMBER;
    l_insert_cnt       NUMBER;
    l_cr_only_line     NUMBER;

    TYPE spr_cm_inv_rec IS RECORD(
		INVOICE_NUMBER            VARCHAR2(20) ,
		invoice_id                NUMBER ,
		invoice_type              VARCHAR2(30) ,
		invoice_line              NUMBER ,
		invoice_line_id           NUMBER ,
		quantity_credited         NUMBER ,
		extended_amount           NUMBER ,
		invoice_date              DATE ,
		due_date                  DATE ,
		orig_inv_line_id          NUMBER ,
		sales_order               VARCHAR2(50) ,
		SALES_ORDER_LINE          VARCHAR2(30) ,
		interface_line_attribute6 VARCHAR2(150) ,
		sales_order_new_line_id   NUMBER ,
		sales_order_line_batch_id NUMBER ,
		INTERFACE_LINE_ATTRIBUTE7 VARCHAR2(150) ,
		Serial_Number             VARCHAR2(30) ,
		process_flag              VARCHAR2(1) ,
		Error_message             VARCHAR2(4000)
    );
    TYPE cra_cm_inv_rec IS RECORD(
		ORDER_NUMBER         NUMBER ,
		serial_number        VARCHAR2(30) ,
		contract_number      VARCHAR2(240) ,
		orig_line_id         NUMBER ,
		invoice_type         VARCHAR2(30) ,
		orig_invoice_id      NUMBER ,
		orig_invoice_number  VARCHAR2(20) ,
		orig_due_date        DATE ,
		orig_invoice_date    DATE ,
		orig_invoice_line_id NUMBER ,
		orig_invoice_line    NUMBER ,
		cm_invoice_id        NUMBER ,
		cm_extended_amount   NUMBER ,
		cm_invoice_number    NUMBER ,
		cm_invoice_line_id   NUMBER ,
		cm_line_number       NUMBER ,
		cm_line_desc         VARCHAR2(240) ,
		cm_line_tl_desc      VARCHAR2(1000) ,
		cm_qty               NUMBER ,
		cm_sales_order       VARCHAR2(50) ,
		cm_ext_amount        NUMBER ,
		return_line_id       NUMBER ,
		process_flag         VARCHAR2(1) ,
		Error_message        VARCHAR2(4000)
    );

    TYPE spr_cm_tbl_type IS TABLE OF spr_cm_inv_rec
    INDEX BY BINARY_INTEGER;
    TYPE cra_cm_tbl_type IS TABLE OF cra_cm_inv_rec
    INDEX BY BINARY_INTEGER;

    lt_spr_cm_tbl      spr_cm_tbl_type;
    lt_cra_cm_tbl      cra_cm_tbl_type;
	  l_ch_batch_ln_tbl  rpro_order_details_tbl_type;

   CURSOR cur_invoice
    IS
      SELECT /*+ PARALLEL(trxlcm,DEFAULT) */
		    trx.trx_number             invoice_number,
            trx.customer_trx_id             invoice_id,
            'CM'                            invoice_type,
            trxlcm.line_number              invoice_line,
            trxlcm.customer_trx_line_id     invoice_line_id,
            cl.quantity                     quantity_credited,
            trxlcm.extended_amount          extended_amount,
            trx.trx_date                    invoice_date,
            trx.term_due_date               due_date,
            (SELECT xrin.INVOICE_id
            FROM XXFT.XXFT_RPRO_INVOICE_DETAILS xrin
            WHERE xrin.SALES_ORDER_NEW_LINE_ID = xrod.SALES_ORDER_NEW_LINE_ID
            and xrin.tran_type = 'INV'
            AND rownum                      < 2
            )                               orig_inv_line_id,
            xrod.sales_order                sales_order,                      -- trxlinv.sales_order,
            to_char(xrod.sales_order_line)           SALES_ORDER_LINE,                 -- trxlinv.SALES_ORDER_LINE,
            to_char(xrod.SALES_ORDER_LINE_ID)        interface_line_attribute6,        --trxlinv.interface_line_attribute6,
            xrod.sales_order_new_line_id    sales_order_new_line_id,
            xrod.sales_order_line_batch_id  sales_order_line_batch_id,
            TRXLCM.INTERFACE_LINE_ATTRIBUTE7,
            NULL Serial_Number
          FROM apps.ra_customer_trx_all trx,
               apps.ra_customer_trx_lines_all trxlcm,
          ---  apps.ra_cust_trx_types_all trxt,
               XXFT_RPRO_ORDER_DETAILS xrod,
               ozf_claim_lines_all cl,
			   xxft.xxft_rpro_order_control xroc
          WHERE 1                              = 1
            and xroc.trx_type                      = 'CM'
            and xroc.processed_flag                = 'N'
		        and trxlcm.customer_trx_line_id        = xroc.line_id
        --  AND xrod.sales_order                 = c_order_number
            AND trx.customer_trx_id              = trxlcm.customer_trx_id
            AND TRXLCM.INTERFACE_LINE_ATTRIBUTE7 = 'FTNT SPR Claims'
            AND TRXLCM.INTERFACE_LINE_ATTRIBUTE2 = cl.claim_id
            AND TRXLCM.INTERFACE_LINE_ATTRIBUTE3 = cl.claim_line_id
            AND xrod.sales_order_line_batch_id   = cl.attribute12
            AND cl.attribute11  NOT LIKE 'INV%'
       ;



      -- CRA Contratual and Non Contrataul Credit Memo
      CURSOR cra_credit_memo
       IS
    		SELECT ORIG_OH.ORDER_NUMBER                    orig_order,
			  RMA_SER.from_serial_number                   serial_number,
			  RMA_OL.attribute12                           contract_number,
			  orig_ol.line_id                              orig_line_id,
			  'CM'                                         invoice_type,
			  orig_rt.customer_trx_id                      orig_invoice_id,
			  Orig_Rt.Trx_Number                           orig_invoice_number,
			  orig_rt.term_due_date                        orig_due_date,
			  orig_rt.trx_date                             orig_invoice_date,
			  ORIG_RTL.customer_trx_line_id                orig_invoice_line_id,
			  ORIG_RTL.LINE_NUMBER                         orig_invoice_line,
			  CM_RTL.customer_trx_id                       cm_invoice_id,
			  CM_RTL.EXTENDED_AMOUNT/ABS(CM_RTL.QUANTITY_credited) cm_extended_amount, /* Defect #3402 */
			  CM_Rt.Trx_Number                             cm_invoice_number,
			  CM_RTL.customer_trx_line_id                  cm_invoice_line_id,
			  CM_RTL.LINE_NUMBER                           cm_line_number,
			  CM_RTL.DESCRIPTION                           cm_line_desc,
			  Cm_Rtl.Translated_Description                cm_line_tl_desc,
			  CM_RTL.QUANTITY_credited                     cm_qty,
			  CM_RTL.SALES_ORDER                           cm_sales_order,
			  CM_RTL.EXTENDED_AMOUNT                       cm_ext_amount,
			  RMA_OL.LINE_ID                               return_line_id,
        CM_Rt.Trx_date                               cm_invoice_date,
        CM_Rt.term_due_date                          cm_due_date
		FROM XXFT.xxft_rpro_order_control xpoc ,
		  oe_order_lines_all RMA_OL ,
		  RA_CUSTOMER_TRX_ALL CM_RT ,
		  RA_CUSTOMER_TRX_LINES_ALL CM_RTL ,
		  OE_ORDER_HEADERS_ALL ORIG_OH ,
		  OE_ORDER_LINES_ALL ORIG_OL ,
		  RA_CUSTOMER_TRX_ALL ORIG_RT ,
		  RA_CUSTOMER_TRX_LINES_ALL ORIG_RTL,
		  OE_LOT_SERIAL_NUMBERS RMA_SER
		WHERE NVL(xpoc.trx_type,'ORD')         ='ORD'
		AND xpoc.processed_flag                = any('R')
    AND NVL(xpoc.inv_processed_flag,'R')   ='R'
    and XPOC.ORDER_NUMBER = NVL(p_order_number,XPOC.ORDER_NUMBER) --
    AND xpoc.status                        = 'CLOSED' --Defect #3402
		AND RMA_OL.line_id                     = xpoc.line_id
		AND RMA_OL.line_category_code          ='RETURN'
		AND CM_RTL.interface_line_attribute6   = TO_CHAR(RMA_OL.LINE_ID)
		AND CM_RTL.LINE_TYPE                   = 'LINE'
		AND CM_RT.CUSTOMER_TRX_ID              = CM_RTL.CUSTOMER_TRX_ID
		AND ORIG_OL.line_id                    = NVL(rma_ol.reference_line_id,RMA_OL.ATTRIBUTE14)
		AND ORIG_OH.header_id                  = ORIG_OL.header_id
		AND ORIG_RTL.interface_line_attribute6 =  NVL(to_char(ORIG_OL.top_model_line_id),TO_CHAR(ORIG_OL.LINE_ID)) --Defect #3307
		AND ORIG_RTL.LINE_TYPE                 = 'LINE'
		AND ORIG_RT.CUSTOMER_TRX_ID            = ORIG_RTL.CUSTOMER_TRX_ID
		AND rma_ser.line_id(+)                 = rma_ol.line_id ;

      CURSOR cur_revpro(c_new_line_id NUMBER)
       IS
	     SELECT /*+ PARALLEL(rod,DEFAULT) */
		        rod.*
          FROM XXFT_RPRO_ORDER_DETAILS rod
	       where sales_order_line_batch_id = c_new_line_id
         order by NUMBER2;
       --
	   --
	   --
       CURSOR cur_get_batch_serial(p_line_id IN NUMBER
	                              ,p_serial  IN VARCHAR2)
       IS
       SELECT a.sales_order_line_batch_id
         FROM xxft_rpro_order_details a
        WHERE 1=1
          AND a.sales_order_line_id = p_line_id
          AND a.attribute33         = p_serial
		  AND NOT EXISTS (
			 SELECT 1
			   FROM xxft_rpro_invoice_details b
			  WHERE b.sales_order_line_batch_id = a.sales_order_line_batch_id
			    AND b.tran_type                   = 'CM'
		  )
		;

		CURSOR cur_get_batch_non_serial(p_line_id IN NUMBER)
		IS
		SELECT MIN(a.sales_order_line_batch_id)
		--into l_batch_id
		FROM xxft_rpro_order_details a
		WHERE 1                            = 1
		 AND a.sales_order_line_id          = p_line_id
		 AND NOT EXISTS (
			 SELECT 1
			   FROM xxft_rpro_invoice_details b
			  WHERE b.sales_order_line_batch_id = a.sales_order_line_batch_id
				AND b.tran_type                   = 'CM'
				and NVL(b.attribute47,'N') <> 'REBATE'
		 );

     CURSOR cur_get_non_serialized_flag(p_batch_id IN NUMBER)
     IS
     SELECT distinct attribute60
       FROM xxft.xxft_rpro_order_details
      WHERE sales_order_line_batch_id = p_batch_id
      ;

  	BEGIN
        print_info_log('  + Inside Procedure get_invoice_details:=>'||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));
        --ITS#523169
        validate_cm_trx(p_order_number);
        extract_cm_trx(p_order_number);
		--	print_log('ORDER NUMBER : '||p_order_number);
	    -- p_order_number:= order_control_rec.order_number;
         -- Regular Invoice
 	    FOR invoice_rec IN cur_invoice
		  LOOP
         l_insert_cnt := 0;
		     print_log('Sales Order New Line ID : '||invoice_rec.sales_order_new_line_id);

           IF invoice_rec.invoice_type = 'CM'
            and invoice_rec.INTERFACE_LINE_ATTRIBUTE7 <> 'FTNT SPR Claims' then
              l_batch_id  := Null;
              IF invoice_rec.serial_number is not null
              THEN
                 BEGIN
                    SELECT sales_order_line_batch_id
                      INTO l_batch_id
                      FROM xxft_rpro_order_details
                     WHERE 1=1
                       AND sales_order_line_id = invoice_rec.interface_line_attribute6
                       AND attribute33         = invoice_rec.serial_number;
                 EXCEPTION
                    WHEN OTHERS THEN
                    NULL;
                 END;
             ELSE
               Begin
                  SELECT MIN(a.sales_order_line_batch_id)
                    into l_batch_id
                    FROM xxft_rpro_order_details a
                   WHERE 1                            = 1
                     AND a.sales_order_line_id          = invoice_rec.interface_line_attribute6
                     AND NOT EXISTS (
					     SELECT 1
                           FROM xxft_rpro_invoice_details b
                          WHERE b.sales_order_line_batch_id = a.sales_order_line_batch_id
                            AND b.tran_type                   = 'CM'
                            and NVL(b.attribute47,'N') <> 'REBATE'
					 );
               Exception
			      when others then
                  null;
               End;
            END IF;
            invoice_rec.sales_order_line_batch_id:=l_batch_id;
         END IF;
        OPEN cur_get_non_serialized_flag(invoice_rec.sales_order_line_batch_id);
        FETCH cur_get_non_serialized_flag INTO l_non_serialized;
        CLOSE cur_get_non_serialized_flag;

        FOR lcu_rpro IN cur_revpro(invoice_rec.sales_order_line_batch_id)
        LOOP
        BEGIN
          IF invoice_rec.invoice_type = 'INV' THEN
            SELECT COUNT(*)
            INTO l_cnt
            FROM XXFT_RPRO_INVOICE_DETAILS
            WHERE sales_order_new_line_id = lcu_rpro.sales_order_new_line_id
            AND tran_type                 = 'INV';
          END IF;
          IF invoice_rec.invoice_type = 'CM' AND invoice_rec.INTERFACE_LINE_ATTRIBUTE7 <> 'FTNT SPR Claims' THEN
            SELECT COUNT(*)
            INTO l_cnt_cm
            FROM XXFT_RPRO_INVOICE_DETAILS
            WHERE sales_order_new_line_id = lcu_rpro.sales_order_new_line_id
            AND tran_type                 = 'CM'
            AND NVL(attribute47,'N')     <> 'REBATE';
          ELSE
            SELECT COUNT(*)
            INTO l_cnt_cm
            FROM XXFT_RPRO_INVOICE_DETAILS
            WHERE sales_order_new_line_id = lcu_rpro.sales_order_new_line_id
            AND tran_type                 = 'CM'
            AND NVL(attribute47,'N')      = 'REBATE';
          END IF;
        END;
        print_output('Checking if Line Details are already there or not ');
        print_log('Checking if Line Details are already there or not ');
        IF l_cnt_cm =0 AND invoice_rec.invoice_type = 'CM'
        THEN
           print_output('Inserting Line: '||lcu_rpro.sales_order_new_line_id);
           print_log('Inserting Line: '||lcu_rpro.sales_order_new_line_id);

            INSERT INTO XXFT_RPRO_INVOICE_DETAILS
                  (
                   TRAN_TYPE
                  ,ITEM_ID
                  ,ITEM_NUMBER
                  ,ITEM_DESC
                  ,PRODUCT_FAMILY
                  ,PRODUCT_CATEGORY
                  ,PRODUCT_LINE
                  ,PRODUCT_CLASS
                  ,PRICE_LIST_NAME
                  ,UNIT_LIST_PRICE
                  ,UNIT_SELL_PRICE
                  ,EXT_SELL_PRICE
                  ,EXT_LIST_PRICE
                  ,REC_AMT
                  ,DEF_AMT
                  ,COST_AMOUNT
                  ,COST_REC_AMT
                  ,COST_DEF_AMT
                  ,TRANS_CURR_CODE
                  ,EX_RATE
                  ,BASE_CURR_CODE
                  ,COST_CURR_CODE
                  ,COST_EX_RATE
                  ,RCURR_EX_RATE
                  ,ACCOUNTING_PERIOD
                  ,ACCOUNTING_RULE
                  ,RULE_START_DATE
                  ,RULE_END_DATE
                  ,VAR_RULE_ID
                  ,PO_NUM
                  ,QUOTE_NUM
                  ,SALES_ORDER
                  ,SALES_ORDER_LINE
                  ,SALES_ORDER_ID
                  ,SALES_ORDER_LINE_ID
                  ,SALES_ORDER_NEW_LINE_ID
                  ,SALES_ORDER_LINE_BATCH_ID
                  ,SHIP_DATE
                  ,SO_BOOK_DATE
                  ,TRANS_DATE
                  ,SCHEDULE_SHIP_DATE
                  ,QUANTITY_SHIPPED
                  ,QUANTITY_ORDERED
                  ,QUANTITY_CANCELED
                  ,SALESREP_NAME
                  ,SALES_REP_ID
                  ,ORDER_TYPE
                  ,ORDER_LINE_TYPE
                  ,SERVICE_REFERENCE_LINE_ID
                  ,INVOICE_NUMBER
                  ,INVOICE_TYPE
                  ,INVOICE_LINE
                  ,INVOICE_ID
                  ,INVOICE_LINE_ID
                  ,QUANTITY_INVOICED
                  ,INVOICE_DATE
                  ,DUE_DATE
                  ,ORIG_INV_LINE_ID
                  ,CUSTOMER_ID
                  ,CUSTOMER_NAME
                  ,CUSTOMER_CLASS
                  ,BILL_TO_ID
                  ,BILL_TO_CUSTOMER_NAME
                  ,BILL_TO_CUSTOMER_NUMBER
                  ,BILL_TO_COUNTRY
                  ,SHIP_TO_ID
                  ,SHIP_TO_CUSTOMER_NAME
                  ,SHIP_TO_CUSTOMER_NUMBER
                  ,SHIP_TO_COUNTRY
                  ,BUSINESS_UNIT
                  ,ORG_ID
                  ,SOB_ID
                  ,SEC_ATTR_VALUE
                  ,RETURN_FLAG
                  ,CANCELLED_FLAG
                  ,FLAG_97_2
                  ,PCS_FLAG
                  ,UNDELIVERED_FLAG
                  ,STATED_FLAG
                  ,ELIGIBLE_FOR_CV
                  ,ELIGIBLE_FOR_FV
                  ,DEFERRED_REVENUE_FLAG
                  ,NON_CONTINGENT_FLAG
                  ,UNBILLED_ACCOUNTING_FLAG
                  ,DEAL_ID
                  ,LAG_DAYS
                  ,ATTRIBUTE1
                  ,ATTRIBUTE2
                  ,ATTRIBUTE3
                  ,ATTRIBUTE4
                  ,ATTRIBUTE5
                  ,ATTRIBUTE6
                  ,ATTRIBUTE7
                  ,ATTRIBUTE8
                  ,ATTRIBUTE9
                  ,ATTRIBUTE10
                  ,ATTRIBUTE11
                  ,ATTRIBUTE12
                  ,ATTRIBUTE13
                  ,ATTRIBUTE14
                  ,ATTRIBUTE15
                  ,ATTRIBUTE16
                  ,ATTRIBUTE17
                  ,ATTRIBUTE18
                  ,ATTRIBUTE19
                  ,ATTRIBUTE20
                  ,ATTRIBUTE21
                  ,ATTRIBUTE22
                  ,ATTRIBUTE23
                  ,ATTRIBUTE24
                  ,ATTRIBUTE25
                  ,ATTRIBUTE26
                  ,ATTRIBUTE27
                  ,ATTRIBUTE28
                  ,ATTRIBUTE29
                  ,ATTRIBUTE30
                  ,ATTRIBUTE31
                  ,ATTRIBUTE32
                  ,ATTRIBUTE33
                  ,ATTRIBUTE34
                  ,ATTRIBUTE35
                  ,ATTRIBUTE36
                  ,ATTRIBUTE37
                  ,ATTRIBUTE38
                  ,ATTRIBUTE39
                  ,ATTRIBUTE40
                  ,ATTRIBUTE41
                  ,ATTRIBUTE42
                  ,ATTRIBUTE43
                  ,ATTRIBUTE44
                  ,ATTRIBUTE45
                  ,ATTRIBUTE46
                  ,ATTRIBUTE47
                  ,ATTRIBUTE48
                  ,ATTRIBUTE49
                  ,ATTRIBUTE50
                  ,ATTRIBUTE51
                  ,ATTRIBUTE52
                  ,ATTRIBUTE53
                  ,ATTRIBUTE54
                  ,ATTRIBUTE55
                  ,ATTRIBUTE56
                  ,ATTRIBUTE57
                  ,ATTRIBUTE58
                  ,ATTRIBUTE59
                  ,ATTRIBUTE60
                  ,DATE1
                  ,DATE2
                  ,DATE3
                  ,DATE4
                  ,DATE5
                  ,NUMBER1
                  ,NUMBER2
                  ,NUMBER3
                  ,NUMBER4
                  ,NUMBER5
                  ,NUMBER6
                  ,NUMBER7
                  ,NUMBER8
                  ,NUMBER9
                  ,NUMBER10
                  ,NUMBER11
                  ,NUMBER12
                  ,NUMBER13
                  ,NUMBER14
                  ,NUMBER15
                  ,REV_ACCTG_SEG1
                  ,REV_ACCTG_SEG2
                  ,REV_ACCTG_SEG3
                  ,REV_ACCTG_SEG4
                  ,REV_ACCTG_SEG5
                  ,REV_ACCTG_SEG6
                  ,REV_ACCTG_SEG7
                  ,REV_ACCTG_SEG8
                  ,REV_ACCTG_SEG9
                  ,REV_ACCTG_SEG10
                  ,DEF_ACCTG_SEG1
                  ,DEF_ACCTG_SEG2
                  ,DEF_ACCTG_SEG3
                  ,DEF_ACCTG_SEG4
                  ,DEF_ACCTG_SEG5
                  ,DEF_ACCTG_SEG6
                  ,DEF_ACCTG_SEG7
                  ,DEF_ACCTG_SEG8
                  ,DEF_ACCTG_SEG9
                  ,DEF_ACCTG_SEG10
                  ,COGS_R_SEG1
                  ,COGS_R_SEG2
                  ,COGS_R_SEG3
                  ,COGS_R_SEG4
                  ,COGS_R_SEG5
                  ,COGS_R_SEG6
                  ,COGS_R_SEG7
                  ,COGS_R_SEG8
                  ,COGS_R_SEG9
                  ,COGS_R_SEG10
                  ,COGS_D_SEG1
                  ,COGS_D_SEG2
                  ,COGS_D_SEG3
                  ,COGS_D_SEG4
                  ,COGS_D_SEG5
                  ,COGS_D_SEG6
                  ,COGS_D_SEG7
                  ,COGS_D_SEG8
                  ,COGS_D_SEG9
                  ,COGS_D_SEG10
                  ,LT_DEFERRED_ACCOUNT
                  ,LT_DCOGS_ACCOUNT
                  ,REV_DIST_ID
                  ,COST_DIST_ID
                  ,BOOK_ID
                  ,BNDL_CONFIG_ID
                  ,so_last_update_date
                  ,so_line_creation_date
                  ,PROCESSED_FLAG
                  ,ERROR_MESSAGE
                  ,CREATION_DATE
                  ,LAST_UPDATE_DATE
                 )
              VALUES
                  (
                   'CM' --lcu_rpro.TRAN_TYPE
                  ,lcu_rpro.ITEM_ID
                  ,lcu_rpro.ITEM_NUMBER
                  ,lcu_rpro.ITEM_DESC
                  ,lcu_rpro.PRODUCT_FAMILY
                  ,lcu_rpro.PRODUCT_CATEGORY
                  ,lcu_rpro.PRODUCT_LINE
                  ,lcu_rpro.PRODUCT_CLASS
                  ,lcu_rpro.PRICE_LIST_NAME
                  ,lcu_rpro.UNIT_LIST_PRICE
                  ,lcu_rpro.UNIT_SELL_PRICE
                  ,DECODE(lcu_rpro.EXT_SELL_PRICE,0,lcu_rpro.EXT_SELL_PRICE,invoice_rec.extended_amount)
                  --DECODE(invoice_rec.invoice_type,'CM',invoice_rec.extended_amount,lcu_rpro.EXT_SELL_PRICE) -- invoice_rec.extended_amount
                  ,lcu_rpro.EXT_LIST_PRICE
                  ,lcu_rpro.REC_AMT
                  ,lcu_rpro.DEF_AMT
                  ,decode(invoice_rec.INTERFACE_LINE_ATTRIBUTE7,'FTNT SPR Claims',0,lcu_rpro.COST_AMOUNT)
                  ,lcu_rpro.COST_REC_AMT
                  ,lcu_rpro.COST_DEF_AMT
                  ,lcu_rpro.TRANS_CURR_CODE
                  ,lcu_rpro.EX_RATE
                  ,lcu_rpro.BASE_CURR_CODE
                  ,lcu_rpro.COST_CURR_CODE
                  ,lcu_rpro.COST_EX_RATE
                  ,lcu_rpro.RCURR_EX_RATE
                  ,lcu_rpro.ACCOUNTING_PERIOD
                  ,lcu_rpro.ACCOUNTING_RULE
                  ,lcu_rpro.RULE_START_DATE
                  ,lcu_rpro.RULE_END_DATE
                  ,lcu_rpro.VAR_RULE_ID
                  ,lcu_rpro.PO_NUM
                  ,lcu_rpro.QUOTE_NUM
                  ,lcu_rpro.SALES_ORDER
                  ,lcu_rpro.SALES_ORDER_LINE
                  ,lcu_rpro.SALES_ORDER_ID
                  ,lcu_rpro.SALES_ORDER_LINE_ID
                  ,lcu_rpro.SALES_ORDER_NEW_LINE_ID
                  ,lcu_rpro.SALES_ORDER_LINE_BATCH_ID
                  ,lcu_rpro.SHIP_DATE
                  ,lcu_rpro.SO_BOOK_DATE
                  ,lcu_rpro.TRANS_DATE
                  ,lcu_rpro.SCHEDULE_SHIP_DATE
                  ,lcu_rpro.QUANTITY_SHIPPED
                  ,lcu_rpro.QUANTITY_ORDERED
                  ,lcu_rpro.QUANTITY_CANCELED
                  ,lcu_rpro.SALESREP_NAME
                  ,lcu_rpro.SALES_REP_ID
                  ,lcu_rpro.ORDER_TYPE
                  ,lcu_rpro.ORDER_LINE_TYPE
                  ,lcu_rpro.SERVICE_REFERENCE_LINE_ID
                  ,invoice_rec.INVOICE_NUMBER
                  ,invoice_rec.INVOICE_TYPE
                  ,invoice_rec.INVOICE_LINE
                  ,invoice_rec.INVOICE_ID
                  ,invoice_rec.INVOICE_ID||'-'||lcu_rpro.SALES_ORDER_NEW_LINE_ID --invoice_rec.INVOICE_LINE_ID
                  ,DECODE(l_non_serialized,'Y',invoice_rec.QUANTITY_CREDITED,1)--1 --invoice_rec.QUANTITY_INVOICED
                  ,invoice_rec.INVOICE_DATE
                  ,invoice_rec.DUE_DATE
                  ,DECODE(invoice_rec.ORIG_INV_LINE_ID,
                  NULL,
                  NULL,
                  invoice_rec.ORIG_INV_LINE_ID||'-'||lcu_rpro.SALES_ORDER_NEW_LINE_ID)
                  ,lcu_rpro.CUSTOMER_ID
                  ,lcu_rpro.CUSTOMER_NAME
                  ,lcu_rpro.CUSTOMER_CLASS
                  ,lcu_rpro.BILL_TO_ID
                  ,lcu_rpro.BILL_TO_CUSTOMER_NAME
                  ,lcu_rpro.BILL_TO_CUSTOMER_NUMBER
                  ,lcu_rpro.BILL_TO_COUNTRY
                  ,lcu_rpro.SHIP_TO_ID
                  ,lcu_rpro.SHIP_TO_CUSTOMER_NAME
                  ,lcu_rpro.SHIP_TO_CUSTOMER_NUMBER
                  ,lcu_rpro.SHIP_TO_COUNTRY
                  ,lcu_rpro.BUSINESS_UNIT
                  ,lcu_rpro.ORG_ID
                  ,lcu_rpro.SOB_ID
                  ,lcu_rpro.SEC_ATTR_VALUE
                  ,DECODE(invoice_rec.invoice_type,'CM','Y',NULL) --lcu_rpro.RETURN_FLAG
                  ,lcu_rpro.CANCELLED_FLAG
                  ,lcu_rpro.FLAG_97_2
                  ,lcu_rpro.PCS_FLAG
                  ,lcu_rpro.UNDELIVERED_FLAG
                  ,lcu_rpro.STATED_FLAG
                  ,lcu_rpro.ELIGIBLE_FOR_CV
                  ,lcu_rpro.ELIGIBLE_FOR_FV
                  ,lcu_rpro.DEFERRED_REVENUE_FLAG
                  ,lcu_rpro.NON_CONTINGENT_FLAG
                  ,lcu_rpro.UNBILLED_ACCOUNTING_FLAG
                  ,lcu_rpro.DEAL_ID
                  ,lcu_rpro.LAG_DAYS
                  ,lcu_rpro.ATTRIBUTE1
                  ,lcu_rpro.ATTRIBUTE2
                  ,lcu_rpro.ATTRIBUTE3
                  ,lcu_rpro.ATTRIBUTE4
                  ,lcu_rpro.ATTRIBUTE5
                  ,lcu_rpro.ATTRIBUTE6
                  ,lcu_rpro.ATTRIBUTE7
                  ,lcu_rpro.ATTRIBUTE8
                  ,lcu_rpro.ATTRIBUTE9
                  ,lcu_rpro.ATTRIBUTE10
                  ,lcu_rpro.ATTRIBUTE11
                  ,lcu_rpro.ATTRIBUTE12
                  ,lcu_rpro.ATTRIBUTE13
                  ,lcu_rpro.ATTRIBUTE14
                  ,lcu_rpro.ATTRIBUTE15
                  ,lcu_rpro.ATTRIBUTE16
                  ,lcu_rpro.ATTRIBUTE17
                  ,lcu_rpro.ATTRIBUTE18
                  ,lcu_rpro.ATTRIBUTE19
                  ,lcu_rpro.ATTRIBUTE20
                  ,lcu_rpro.ATTRIBUTE21
                  ,lcu_rpro.ATTRIBUTE22
                  ,lcu_rpro.ATTRIBUTE23
                  ,lcu_rpro.ATTRIBUTE24
                  ,lcu_rpro.ATTRIBUTE25
                  ,lcu_rpro.ATTRIBUTE26
                  ,lcu_rpro.ATTRIBUTE27
                  ,lcu_rpro.ATTRIBUTE28
                  ,lcu_rpro.ATTRIBUTE29
                  ,lcu_rpro.ATTRIBUTE30
                  ,lcu_rpro.ATTRIBUTE31
                  ,lcu_rpro.ATTRIBUTE32
                  ,lcu_rpro.ATTRIBUTE33
                  ,lcu_rpro.ATTRIBUTE34
                  ,lcu_rpro.ATTRIBUTE35
                  ,lcu_rpro.ATTRIBUTE36
                  ,lcu_rpro.ATTRIBUTE37
                  ,lcu_rpro.ATTRIBUTE38
                  ,lcu_rpro.ATTRIBUTE39
                  ,lcu_rpro.ATTRIBUTE40
                  ,lcu_rpro.ATTRIBUTE41
                  ,lcu_rpro.ATTRIBUTE42
                  ,lcu_rpro.ATTRIBUTE43
                  ,lcu_rpro.ATTRIBUTE44
                  ,lcu_rpro.ATTRIBUTE45
                  ,lcu_rpro.ATTRIBUTE46
                  ,decode(invoice_rec.INTERFACE_LINE_ATTRIBUTE7,'FTNT SPR Claims','REBATE','RETURN')--lcu_rpro.ATTRIBUTE47
                  ,lcu_rpro.ATTRIBUTE48
                  ,lcu_rpro.ATTRIBUTE49
                  ,lcu_rpro.ATTRIBUTE50
                  ,lcu_rpro.ATTRIBUTE51
                  ,lcu_rpro.ATTRIBUTE52
                  ,lcu_rpro.ATTRIBUTE53
                  ,lcu_rpro.ATTRIBUTE54
                  ,lcu_rpro.ATTRIBUTE55
                  ,lcu_rpro.ATTRIBUTE56
                  ,lcu_rpro.ATTRIBUTE57
                  ,lcu_rpro.ATTRIBUTE58
                  ,lcu_rpro.ATTRIBUTE59
                  ,lcu_rpro.ATTRIBUTE60
                  ,lcu_rpro.DATE1
                  ,lcu_rpro.DATE2
                  ,lcu_rpro.DATE3
                  ,lcu_rpro.DATE4
                  ,lcu_rpro.DATE5
                  ,lcu_rpro.NUMBER1
                  ,invoice_rec.INVOICE_LINE_ID --lcu_rpro.NUMBER2
                  ,lcu_rpro.NUMBER3
                  ,lcu_rpro.NUMBER4
                  ,lcu_rpro.NUMBER5
                  ,lcu_rpro.NUMBER6
                  ,lcu_rpro.NUMBER7
                  ,lcu_rpro.NUMBER8
                  ,lcu_rpro.NUMBER9
                  ,lcu_rpro.NUMBER10
                  ,lcu_rpro.NUMBER11
                  ,lcu_rpro.NUMBER12
                  ,lcu_rpro.NUMBER13
                  ,lcu_rpro.NUMBER14
                  ,NULL -- lcu_rpro.NUMBER15      --Request Id.
                  ,lcu_rpro.REV_ACCTG_SEG1
                  ,lcu_rpro.REV_ACCTG_SEG2
                  ,lcu_rpro.REV_ACCTG_SEG3
                  ,lcu_rpro.REV_ACCTG_SEG4
                  ,lcu_rpro.REV_ACCTG_SEG5
                  ,lcu_rpro.REV_ACCTG_SEG6
                  ,lcu_rpro.REV_ACCTG_SEG7
                  ,lcu_rpro.REV_ACCTG_SEG8
                  ,lcu_rpro.REV_ACCTG_SEG9
                  ,lcu_rpro.REV_ACCTG_SEG10
                  ,lcu_rpro.DEF_ACCTG_SEG1
                  ,lcu_rpro.DEF_ACCTG_SEG2
                  ,lcu_rpro.DEF_ACCTG_SEG3
                  ,lcu_rpro.DEF_ACCTG_SEG4
                  ,lcu_rpro.DEF_ACCTG_SEG5
                  ,lcu_rpro.DEF_ACCTG_SEG6
                  ,lcu_rpro.DEF_ACCTG_SEG7
                  ,lcu_rpro.DEF_ACCTG_SEG8
                  ,lcu_rpro.DEF_ACCTG_SEG9
                  ,lcu_rpro.DEF_ACCTG_SEG10
                  ,lcu_rpro.COGS_R_SEG1
                  ,lcu_rpro.COGS_R_SEG2
                  ,lcu_rpro.COGS_R_SEG3
                  ,lcu_rpro.COGS_R_SEG4
                  ,lcu_rpro.COGS_R_SEG5
                  ,lcu_rpro.COGS_R_SEG6
                  ,lcu_rpro.COGS_R_SEG7
                  ,lcu_rpro.COGS_R_SEG8
                  ,lcu_rpro.COGS_R_SEG9
                  ,lcu_rpro.COGS_R_SEG10
                  ,lcu_rpro.COGS_D_SEG1
                  ,lcu_rpro.COGS_D_SEG2
                  ,lcu_rpro.COGS_D_SEG3
                  ,lcu_rpro.COGS_D_SEG4
                  ,lcu_rpro.COGS_D_SEG5
                  ,lcu_rpro.COGS_D_SEG6
                  ,lcu_rpro.COGS_D_SEG7
                  ,lcu_rpro.COGS_D_SEG8
                  ,lcu_rpro.COGS_D_SEG9
                  ,lcu_rpro.COGS_D_SEG10
                  ,lcu_rpro.LT_DEFERRED_ACCOUNT
                  ,lcu_rpro.LT_DCOGS_ACCOUNT
                  ,lcu_rpro.REV_DIST_ID
                  ,lcu_rpro.COST_DIST_ID
                  ,lcu_rpro.BOOK_ID
                  ,lcu_rpro.BNDL_CONFIG_ID
                  ,SYSDATE --lcu_rpro.so_last_update_date
                  ,SYSDATE --lcu_rpro.so_line_creation_date
                  ,'N' --lcu_rpro.PROCESSED_FLAG
                  ,lcu_rpro.ERROR_MESSAGE
                  ,sysdate--lcu_rpro.CREATION_DATE
                  ,sysdate--lcu_rpro.LAST_UPDATE_DATE
                  );
                  l_insert_cnt := l_insert_cnt+1;
				ELSE
					print_output('Can not Insert as Line Details are already exist: '||lcu_rpro.sales_order_new_line_id);
					print_log('Can not Insert as Line Details are already exist: '||lcu_rpro.sales_order_new_line_id);
				END IF;
 		    END LOOP;
		--
		-- Update the status of the record.
		--
    IF l_insert_cnt > 0 OR l_cnt_cm >0
    THEN
		 UPDATE xxft_rpro_order_control
			set processed_flag ='P'
         , inv_processed_flag='P'
			   ,error_message = NULL
			   ,last_update_date = SYSDATE
			   ,last_updated_by = fnd_global.user_id
			   ,request_id       = gn_request_id
		   WHERE line_id = invoice_rec.invoice_line_id
		     AND processed_flag ='N'
         AND trx_type='CM';
		 COMMIT;
     END IF;
     END LOOP;
	 COMMIT;
     print_info_log('  + Inside Procedure before CRA Credit Memo:=>'||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));
	  -- All credit memos should be addressed in the above scenario. we need to check the cra and confirm it.
     -- CRA Credit Memo
     FOR cra_cm_rec IN cra_credit_memo 
     LOOP
     
         print_log('CRA CM : '||cra_cm_rec.orig_order);
         lv_batch_id      := 0;
         lv_non_serial_item := NULL;
         l_insert_cnt := 0;
         IF cra_cm_rec.contract_number is null
           and cra_cm_rec.serial_number IS NOT NULL THEN
          -- Check for receipt only return with receipt order line type.US Credit Only - H/W,US Credit Only,SG Credit Only,SG Credit Only - H/W
          
          SELECT COUNT(1)
          INTO l_cr_only_line
          FROM oe_order_lines_all oola ,
            oe_transaction_types_tl ott
          WHERE ott.transaction_type_id = oola.line_type_id
          AND EXISTS
            (SELECT 1
            FROM fnd_lookup_values flv
            WHERE flv.meaning   = ott.name
            AND flv.enabled_flag='Y'
            AND sysdate BETWEEN NVL(start_date_active, sysdate-1) AND NVL(end_date_active, sysdate+1)
            );
           
           --
           -- Checking if the serial Number is received or not.
           --
          IF l_cr_only_line = 0 
          THEN
            SELECT COUNT(1)
              INTO ln_serial_exists
            FROM rcv_transactions rt,
              rcv_serial_transactions rst
            WHERE rt.oe_order_line_id =cra_cm_rec.return_line_id
            AND rt.transaction_type   ='DELIVER'
            AND rst.shipment_line_id  = rt.shipment_line_id
            AND rst.serial_num        = cra_cm_rec.serial_number;
            IF ln_serial_exists = 0
            THEN
               print_info_log('  + Serial Number is not actually received. hence skipping the loop.:=>'||cra_cm_rec.serial_number);
               CONTINUE;-- Continue to the next loop and search for the serail.
            END IF;
          END IF;

           BEGIN
              SELECT sales_order_line_batch_id
               INTO lv_batch_id
               FROM xxft_rpro_order_details
              WHERE sales_order         = cra_cm_rec.orig_order
                AND sales_order_line_id = cra_cm_rec.orig_line_id
                AND attribute33         = cra_cm_rec.serial_number;
           EXCEPTION
             WHEN OTHERS THEN
             NULL;
           END;
         ELSIF cra_cm_rec.contract_number IS NOT NULL
             and cra_cm_rec.serial_number is null THEN
           BEGIN
              SELECT sales_order_line_batch_id
               INTO lv_batch_id
               FROM xxft_rpro_order_details
              WHERE sales_order         = cra_cm_rec.orig_order
                AND sales_order_line_id = cra_cm_rec.orig_line_id
                AND attribute45         = cra_cm_rec.contract_number;
           EXCEPTION
             WHEN OTHERS THEN
             NULL;
           END;
         ELSE
            OPEN cur_get_batch_non_serial(cra_cm_rec.orig_line_id);
            FETCH cur_get_batch_non_serial INTO lv_batch_id;
            CLOSE cur_get_batch_non_serial;
            lv_non_serial_item := 'Y';
         END IF;


        OPEN cur_get_non_serialized_flag(lv_batch_id);
        FETCH cur_get_non_serialized_flag INTO l_non_serialized;
        CLOSE cur_get_non_serialized_flag;
        /* Defect #3402 */
        IF l_non_serialized ='Y'
        THEN
           --
           -- In case of NonSerialized Order, pass only one batch with different quantity.
           --
            l_cm_quantity :=1;
        ELSE
           --
           -- In case of NonSerialized item, pass the cm quantity for the batch with quantity invoiced as 1.
           --
          IF lv_non_serial_item ='Y'
          THEN
             l_cm_quantity := abs(cra_cm_rec.cm_qty);
          ELSE
             l_cm_quantity := 1;
          END IF;
        END IF;
        print_info_log('  + Inside Procedure. l_cm_quantity:=>'||l_cm_quantity||'-'||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));
        print_info_log('  + Inside Procedure. lv_non_serial_item:=>'||lv_non_serial_item||'-'||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));
        print_info_log('  + Inside Procedure. l_non_serialized:=>'||l_non_serialized||'-'||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));
        print_info_log('  + Inside Procedure. lv_batch_id:=>'||lv_batch_id||'-'||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));

        /* Defect #3402 */
        FOR i IN 1..l_cm_quantity
        LOOP
           --
           -- In case of Non Serial and non Contract based Item, then pick the min batch.
           --
           IF lv_non_serial_item = 'Y'
           THEN
              OPEN cur_get_batch_non_serial(cra_cm_rec.orig_line_id);
              FETCH cur_get_batch_non_serial INTO lv_batch_id;
              CLOSE cur_get_batch_non_serial;
           END IF;

           --print_info_log('  + Inside Procedure. l_non_serialized:=>'||l_non_serialized||'-'||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));
           print_info_log('  + Inside Procedure. lv_batch_id:=>'||lv_batch_id||'-'||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));

           FOR lcu_rpro IN cur_revpro(lv_batch_id)
           LOOP
              BEGIN
                SELECT COUNT(*)
                INTO lv_cra_cnt
                FROM XXFT_RPRO_INVOICE_DETAILS
                WHERE sales_order_new_line_id = lcu_rpro.sales_order_new_line_id
                AND invoice_type              = 'CM';
              END;

           IF lv_cra_cnt = 0  THEN
					    -- print_output('Inserting Line: '||lcu_rpro.sales_order_new_line_id);
					    -- print_log('Inserting Line: '||lcu_rpro.sales_order_new_line_id);
              INSERT INTO XXFT_RPRO_INVOICE_DETAILS
                        (
                         TRAN_TYPE
                        ,ITEM_ID
                        ,ITEM_NUMBER
                        ,ITEM_DESC
                        ,PRODUCT_FAMILY
                        ,PRODUCT_CATEGORY
                        ,PRODUCT_LINE
                        ,PRODUCT_CLASS
                        ,PRICE_LIST_NAME
                        ,UNIT_LIST_PRICE
                        ,UNIT_SELL_PRICE
                        ,EXT_SELL_PRICE
                        ,EXT_LIST_PRICE
                        ,REC_AMT
                        ,DEF_AMT
                        ,COST_AMOUNT
                        ,COST_REC_AMT
                        ,COST_DEF_AMT
                        ,TRANS_CURR_CODE
                        ,EX_RATE
                        ,BASE_CURR_CODE
                        ,COST_CURR_CODE
                        ,COST_EX_RATE
                        ,RCURR_EX_RATE
                        ,ACCOUNTING_PERIOD
                        ,ACCOUNTING_RULE
                        ,RULE_START_DATE
                        ,RULE_END_DATE
                        ,VAR_RULE_ID
                        ,PO_NUM
                        ,QUOTE_NUM
                        ,SALES_ORDER
                        ,SALES_ORDER_LINE
                        ,SALES_ORDER_ID
                        ,SALES_ORDER_LINE_ID
                        ,SALES_ORDER_NEW_LINE_ID
                        ,SALES_ORDER_LINE_BATCH_ID
                        ,SHIP_DATE
                        ,SO_BOOK_DATE
                        ,TRANS_DATE
                        ,SCHEDULE_SHIP_DATE
                        ,QUANTITY_SHIPPED
                        ,QUANTITY_ORDERED
                        ,QUANTITY_CANCELED
                        ,SALESREP_NAME
                        ,SALES_REP_ID
                        ,ORDER_TYPE
                        ,ORDER_LINE_TYPE
                        ,SERVICE_REFERENCE_LINE_ID
                        ,INVOICE_NUMBER
                        ,INVOICE_TYPE
                        ,INVOICE_LINE
                        ,INVOICE_ID
                        ,INVOICE_LINE_ID
                        ,QUANTITY_INVOICED
                        ,INVOICE_DATE
                        ,DUE_DATE
                        ,ORIG_INV_LINE_ID
                        ,CUSTOMER_ID
                        ,CUSTOMER_NAME
                        ,CUSTOMER_CLASS
                        ,BILL_TO_ID
                        ,BILL_TO_CUSTOMER_NAME
                        ,BILL_TO_CUSTOMER_NUMBER
                        ,BILL_TO_COUNTRY
                        ,SHIP_TO_ID
                        ,SHIP_TO_CUSTOMER_NAME
                        ,SHIP_TO_CUSTOMER_NUMBER
                        ,SHIP_TO_COUNTRY
                        ,BUSINESS_UNIT
                        ,ORG_ID
                        ,SOB_ID
                        ,SEC_ATTR_VALUE
                        ,RETURN_FLAG
                        ,CANCELLED_FLAG
                        ,FLAG_97_2
                        ,PCS_FLAG
                        ,UNDELIVERED_FLAG
                        ,STATED_FLAG
                        ,ELIGIBLE_FOR_CV
                        ,ELIGIBLE_FOR_FV
                        ,DEFERRED_REVENUE_FLAG
                        ,NON_CONTINGENT_FLAG
                        ,UNBILLED_ACCOUNTING_FLAG
                        ,DEAL_ID
                        ,LAG_DAYS
                        ,ATTRIBUTE1
                        ,ATTRIBUTE2
                        ,ATTRIBUTE3
                        ,ATTRIBUTE4
                        ,ATTRIBUTE5
                        ,ATTRIBUTE6
                        ,ATTRIBUTE7
                        ,ATTRIBUTE8
                        ,ATTRIBUTE9
                        ,ATTRIBUTE10
                        ,ATTRIBUTE11
                        ,ATTRIBUTE12
                        ,ATTRIBUTE13
                        ,ATTRIBUTE14
                        ,ATTRIBUTE15
                        ,ATTRIBUTE16
                        ,ATTRIBUTE17
                        ,ATTRIBUTE18
                        ,ATTRIBUTE19
                        ,ATTRIBUTE20
                        ,ATTRIBUTE21
                        ,ATTRIBUTE22
                        ,ATTRIBUTE23
                        ,ATTRIBUTE24
                        ,ATTRIBUTE25
                        ,ATTRIBUTE26
                        ,ATTRIBUTE27
                        ,ATTRIBUTE28
                        ,ATTRIBUTE29
                        ,ATTRIBUTE30
                        ,ATTRIBUTE31
                        ,ATTRIBUTE32
                        ,ATTRIBUTE33
                        ,ATTRIBUTE34
                        ,ATTRIBUTE35
                        ,ATTRIBUTE36
                        ,ATTRIBUTE37
                        ,ATTRIBUTE38
                        ,ATTRIBUTE39
                        ,ATTRIBUTE40
                        ,ATTRIBUTE41
                        ,ATTRIBUTE42
                        ,ATTRIBUTE43
                        ,ATTRIBUTE44
                        ,ATTRIBUTE45
                        ,ATTRIBUTE46
                        ,ATTRIBUTE47
                        ,ATTRIBUTE48
                        ,ATTRIBUTE49
                        ,ATTRIBUTE50
                        ,ATTRIBUTE51
                        ,ATTRIBUTE52
                        ,ATTRIBUTE53
                        ,ATTRIBUTE54
                        ,ATTRIBUTE55
                        ,ATTRIBUTE56
                        ,ATTRIBUTE57
                        ,ATTRIBUTE58
                        ,ATTRIBUTE59
                        ,ATTRIBUTE60
                        ,DATE1
                        ,DATE2
                        ,DATE3
                        ,DATE4
                        ,DATE5
                        ,NUMBER1
                        ,NUMBER2
                        ,NUMBER3
                        ,NUMBER4
                        ,NUMBER5
                        ,NUMBER6
                        ,NUMBER7
                        ,NUMBER8
                        ,NUMBER9
                        ,NUMBER10
                        ,NUMBER11
                        ,NUMBER12
                        ,NUMBER13
                        ,NUMBER14
                        ,NUMBER15
                        ,REV_ACCTG_SEG1
                        ,REV_ACCTG_SEG2
                        ,REV_ACCTG_SEG3
                        ,REV_ACCTG_SEG4
                        ,REV_ACCTG_SEG5
                        ,REV_ACCTG_SEG6
                        ,REV_ACCTG_SEG7
                        ,REV_ACCTG_SEG8
                        ,REV_ACCTG_SEG9
                        ,REV_ACCTG_SEG10
                        ,DEF_ACCTG_SEG1
                        ,DEF_ACCTG_SEG2
                        ,DEF_ACCTG_SEG3
                        ,DEF_ACCTG_SEG4
                        ,DEF_ACCTG_SEG5
                        ,DEF_ACCTG_SEG6
                        ,DEF_ACCTG_SEG7
                        ,DEF_ACCTG_SEG8
                        ,DEF_ACCTG_SEG9
                        ,DEF_ACCTG_SEG10
                        ,COGS_R_SEG1
                        ,COGS_R_SEG2
                        ,COGS_R_SEG3
                        ,COGS_R_SEG4
                        ,COGS_R_SEG5
                        ,COGS_R_SEG6
                        ,COGS_R_SEG7
                        ,COGS_R_SEG8
                        ,COGS_R_SEG9
                        ,COGS_R_SEG10
                        ,COGS_D_SEG1
                        ,COGS_D_SEG2
                        ,COGS_D_SEG3
                        ,COGS_D_SEG4
                        ,COGS_D_SEG5
                        ,COGS_D_SEG6
                        ,COGS_D_SEG7
                        ,COGS_D_SEG8
                        ,COGS_D_SEG9
                        ,COGS_D_SEG10
                        ,LT_DEFERRED_ACCOUNT
                        ,LT_DCOGS_ACCOUNT
                        ,REV_DIST_ID
                        ,COST_DIST_ID
                        ,BOOK_ID
                        ,BNDL_CONFIG_ID
                        ,so_last_update_date
                        ,so_line_creation_date
                        ,PROCESSED_FLAG
                        ,ERROR_MESSAGE
                        ,CREATION_DATE
                        ,LAST_UPDATE_DATE
                       )
                    VALUES
                        (
                         'CM' --lcu_rpro.TRAN_TYPE
                        ,lcu_rpro.ITEM_ID
                        ,lcu_rpro.ITEM_NUMBER
                        ,lcu_rpro.ITEM_DESC
                        ,lcu_rpro.PRODUCT_FAMILY
                        ,lcu_rpro.PRODUCT_CATEGORY
                        ,lcu_rpro.PRODUCT_LINE
                        ,lcu_rpro.PRODUCT_CLASS
                        ,lcu_rpro.PRICE_LIST_NAME
                        ,lcu_rpro.UNIT_LIST_PRICE
                        ,lcu_rpro.UNIT_SELL_PRICE
                        ,DECODE(lcu_rpro.EXT_SELL_PRICE,0,lcu_rpro.EXT_SELL_PRICE,DECODE(l_non_serialized,'Y',cra_cm_rec.cm_ext_amount,cra_cm_rec.cm_extended_amount)) -- Defect#
                        --, DECODE(l_non_serialized,'Y',cra_cm_rec.cm_ext_amount,cra_cm_rec.cm_extended_amount) -- Added on 6/29
                        --DECODE(invoice_rec.invoice_type,'CM',invoice_rec.extended_amount,lcu_rpro.EXT_SELL_PRICE) -- invoice_rec.extended_amount
                        ,lcu_rpro.EXT_LIST_PRICE
                        ,lcu_rpro.REC_AMT
                        ,lcu_rpro.DEF_AMT
                        ,lcu_rpro.COST_AMOUNT
                        ,lcu_rpro.COST_REC_AMT
                        ,lcu_rpro.COST_DEF_AMT
                        ,lcu_rpro.TRANS_CURR_CODE
                        ,lcu_rpro.EX_RATE
                        ,lcu_rpro.BASE_CURR_CODE
                        ,lcu_rpro.COST_CURR_CODE
                        ,lcu_rpro.COST_EX_RATE
                        ,lcu_rpro.RCURR_EX_RATE
                        ,lcu_rpro.ACCOUNTING_PERIOD
                        ,lcu_rpro.ACCOUNTING_RULE
                        ,lcu_rpro.RULE_START_DATE
                        ,lcu_rpro.RULE_END_DATE
                        ,lcu_rpro.VAR_RULE_ID
                        ,lcu_rpro.PO_NUM
                        ,lcu_rpro.QUOTE_NUM
                        ,lcu_rpro.SALES_ORDER
                        ,lcu_rpro.SALES_ORDER_LINE
                        ,lcu_rpro.SALES_ORDER_ID
                        ,lcu_rpro.SALES_ORDER_LINE_ID
                        ,lcu_rpro.SALES_ORDER_NEW_LINE_ID
                        ,lcu_rpro.SALES_ORDER_LINE_BATCH_ID
                        ,lcu_rpro.SHIP_DATE
                        ,lcu_rpro.SO_BOOK_DATE
                        ,lcu_rpro.TRANS_DATE
                        ,lcu_rpro.SCHEDULE_SHIP_DATE
                        ,lcu_rpro.QUANTITY_SHIPPED
                        ,lcu_rpro.QUANTITY_ORDERED
                        ,lcu_rpro.QUANTITY_CANCELED
                        ,lcu_rpro.SALESREP_NAME
                        ,lcu_rpro.SALES_REP_ID
                        ,lcu_rpro.ORDER_TYPE
                        ,lcu_rpro.ORDER_LINE_TYPE
                        ,lcu_rpro.SERVICE_REFERENCE_LINE_ID
                        ,cra_cm_rec.CM_INVOICE_NUMBER
                        ,cra_cm_rec.INVOICE_TYPE
                        ,cra_cm_rec.CM_LINE_NUMBER
                        ,cra_cm_rec.CM_INVOICE_ID
                        ,cra_cm_rec.CM_INVOICE_ID||'-'||lcu_rpro.SALES_ORDER_NEW_LINE_ID --invoice_rec.INVOICE_LINE_ID
                        ,DECODE(l_non_serialized,'Y',cra_cm_rec.cm_qty,1) --invoice_rec.QUANTITY_INVOICED
                        ,cra_cm_rec.cm_invoice_date -- ITS#585378
                        ,cra_cm_rec.cm_due_date --ITS#585378
                        ,DECODE(cra_cm_rec.ORIG_INVOICE_LINE_ID,
                        NULL,
                        NULL,
                        cra_cm_rec.ORIG_INVOICE_ID||'-'||lcu_rpro.SALES_ORDER_NEW_LINE_ID)
                        ,lcu_rpro.CUSTOMER_ID
                        ,lcu_rpro.CUSTOMER_NAME
                        ,lcu_rpro.CUSTOMER_CLASS
                        ,lcu_rpro.BILL_TO_ID
                        ,lcu_rpro.BILL_TO_CUSTOMER_NAME
                        ,lcu_rpro.BILL_TO_CUSTOMER_NUMBER
                        ,lcu_rpro.BILL_TO_COUNTRY
                        ,lcu_rpro.SHIP_TO_ID
                        ,lcu_rpro.SHIP_TO_CUSTOMER_NAME
                        ,lcu_rpro.SHIP_TO_CUSTOMER_NUMBER
                        ,lcu_rpro.SHIP_TO_COUNTRY
                        ,lcu_rpro.BUSINESS_UNIT
                        ,lcu_rpro.ORG_ID
                        ,lcu_rpro.SOB_ID
                        ,lcu_rpro.SEC_ATTR_VALUE
                        ,'Y'--lcu_rpro.RETURN_FLAG
                        ,lcu_rpro.CANCELLED_FLAG
                        ,lcu_rpro.FLAG_97_2
                        ,lcu_rpro.PCS_FLAG
                        ,lcu_rpro.UNDELIVERED_FLAG
                        ,lcu_rpro.STATED_FLAG
                        ,lcu_rpro.ELIGIBLE_FOR_CV
                        ,lcu_rpro.ELIGIBLE_FOR_FV
                        ,lcu_rpro.DEFERRED_REVENUE_FLAG
                        ,lcu_rpro.NON_CONTINGENT_FLAG
                        ,lcu_rpro.UNBILLED_ACCOUNTING_FLAG
                        ,lcu_rpro.DEAL_ID
                        ,lcu_rpro.LAG_DAYS
                        ,lcu_rpro.ATTRIBUTE1
                        ,lcu_rpro.ATTRIBUTE2
                        ,lcu_rpro.ATTRIBUTE3
                        ,lcu_rpro.ATTRIBUTE4
                        ,lcu_rpro.ATTRIBUTE5
                        ,lcu_rpro.ATTRIBUTE6
                        ,lcu_rpro.ATTRIBUTE7
                        ,lcu_rpro.ATTRIBUTE8
                        ,lcu_rpro.ATTRIBUTE9
                        ,lcu_rpro.ATTRIBUTE10
                        ,lcu_rpro.ATTRIBUTE11
                        ,lcu_rpro.ATTRIBUTE12
                        ,lcu_rpro.ATTRIBUTE13
                        ,lcu_rpro.ATTRIBUTE14
                        ,lcu_rpro.ATTRIBUTE15
                        ,lcu_rpro.ATTRIBUTE16
                        ,lcu_rpro.ATTRIBUTE17
                        ,lcu_rpro.ATTRIBUTE18
                        ,lcu_rpro.ATTRIBUTE19
                        ,lcu_rpro.ATTRIBUTE20
                        ,lcu_rpro.ATTRIBUTE21
                        ,lcu_rpro.ATTRIBUTE22
                        ,lcu_rpro.ATTRIBUTE23
                        ,lcu_rpro.ATTRIBUTE24
                        ,lcu_rpro.ATTRIBUTE25
                        ,lcu_rpro.ATTRIBUTE26
                        ,lcu_rpro.ATTRIBUTE27
                        ,lcu_rpro.ATTRIBUTE28
                        ,lcu_rpro.ATTRIBUTE29
                        ,lcu_rpro.ATTRIBUTE30
                        ,lcu_rpro.ATTRIBUTE31
                        ,lcu_rpro.ATTRIBUTE32
                        ,lcu_rpro.ATTRIBUTE33
                        ,lcu_rpro.ATTRIBUTE34
                        ,lcu_rpro.ATTRIBUTE35
                        ,lcu_rpro.ATTRIBUTE36
                        ,lcu_rpro.ATTRIBUTE37
                        ,lcu_rpro.ATTRIBUTE38
                        ,lcu_rpro.ATTRIBUTE39
                        ,lcu_rpro.ATTRIBUTE40
                        ,lcu_rpro.ATTRIBUTE41
                        ,lcu_rpro.ATTRIBUTE42
                        ,lcu_rpro.ATTRIBUTE43
                        ,lcu_rpro.ATTRIBUTE44
                        ,lcu_rpro.ATTRIBUTE45
                        ,lcu_rpro.ATTRIBUTE46
                        ,'RETURN' --lcu_rpro.ATTRIBUTE47		 --Defect #3418.
                        ,lcu_rpro.ATTRIBUTE48
                        ,lcu_rpro.ATTRIBUTE49
                        ,lcu_rpro.ATTRIBUTE50
                        ,lcu_rpro.ATTRIBUTE51
                        ,lcu_rpro.ATTRIBUTE52
                        ,lcu_rpro.ATTRIBUTE53
                        ,lcu_rpro.ATTRIBUTE54
                        ,lcu_rpro.ATTRIBUTE55
                        ,lcu_rpro.ATTRIBUTE56
                        ,lcu_rpro.ATTRIBUTE57
                        ,lcu_rpro.ATTRIBUTE58
                        ,lcu_rpro.ATTRIBUTE59
                        ,lcu_rpro.ATTRIBUTE60
                        ,lcu_rpro.DATE1
                        ,lcu_rpro.DATE2
                        ,lcu_rpro.DATE3
                        ,lcu_rpro.DATE4
                        ,lcu_rpro.DATE5
                        ,lcu_rpro.NUMBER1
                        ,cra_cm_rec.CM_INVOICE_LINE_ID --lcu_rpro.NUMBER2
                        ,lcu_rpro.NUMBER3
                        ,lcu_rpro.NUMBER4
                        ,lcu_rpro.NUMBER5
                        ,lcu_rpro.NUMBER6
                        ,lcu_rpro.NUMBER7
                        ,lcu_rpro.NUMBER8
                        ,lcu_rpro.NUMBER9
                        ,lcu_rpro.NUMBER10
                        ,lcu_rpro.NUMBER11
                        ,lcu_rpro.NUMBER12
                        ,lcu_rpro.NUMBER13
                        ,lcu_rpro.NUMBER14
                        ,NULL -- lcu_rpro.NUMBER15      Request ID - needs to be null.
                        ,lcu_rpro.REV_ACCTG_SEG1
                        ,lcu_rpro.REV_ACCTG_SEG2
                        ,lcu_rpro.REV_ACCTG_SEG3
                        ,lcu_rpro.REV_ACCTG_SEG4
                        ,lcu_rpro.REV_ACCTG_SEG5
                        ,lcu_rpro.REV_ACCTG_SEG6
                        ,lcu_rpro.REV_ACCTG_SEG7
                        ,lcu_rpro.REV_ACCTG_SEG8
                        ,lcu_rpro.REV_ACCTG_SEG9
                        ,lcu_rpro.REV_ACCTG_SEG10
                        ,lcu_rpro.DEF_ACCTG_SEG1
                        ,lcu_rpro.DEF_ACCTG_SEG2
                        ,lcu_rpro.DEF_ACCTG_SEG3
                        ,lcu_rpro.DEF_ACCTG_SEG4
                        ,lcu_rpro.DEF_ACCTG_SEG5
                        ,lcu_rpro.DEF_ACCTG_SEG6
                        ,lcu_rpro.DEF_ACCTG_SEG7
                        ,lcu_rpro.DEF_ACCTG_SEG8
                        ,lcu_rpro.DEF_ACCTG_SEG9
                        ,lcu_rpro.DEF_ACCTG_SEG10
                        ,lcu_rpro.COGS_R_SEG1
                        ,lcu_rpro.COGS_R_SEG2
                        ,lcu_rpro.COGS_R_SEG3
                        ,lcu_rpro.COGS_R_SEG4
                        ,lcu_rpro.COGS_R_SEG5
                        ,lcu_rpro.COGS_R_SEG6
                        ,lcu_rpro.COGS_R_SEG7
                        ,lcu_rpro.COGS_R_SEG8
                        ,lcu_rpro.COGS_R_SEG9
                        ,lcu_rpro.COGS_R_SEG10
                        ,lcu_rpro.COGS_D_SEG1
                        ,lcu_rpro.COGS_D_SEG2
                        ,lcu_rpro.COGS_D_SEG3
                        ,lcu_rpro.COGS_D_SEG4
                        ,lcu_rpro.COGS_D_SEG5
                        ,lcu_rpro.COGS_D_SEG6
                        ,lcu_rpro.COGS_D_SEG7
                        ,lcu_rpro.COGS_D_SEG8
                        ,lcu_rpro.COGS_D_SEG9
                        ,lcu_rpro.COGS_D_SEG10
                        ,lcu_rpro.LT_DEFERRED_ACCOUNT
                        ,lcu_rpro.LT_DCOGS_ACCOUNT
                        ,lcu_rpro.REV_DIST_ID
                        ,lcu_rpro.COST_DIST_ID
                        ,lcu_rpro.BOOK_ID
                        ,lcu_rpro.BNDL_CONFIG_ID
                        ,sysdate --lcu_rpro.so_last_update_date
                        ,sysdate --lcu_rpro.so_line_creation_date
                        ,'N'--lcu_rpro.PROCESSED_FLAG
                        ,lcu_rpro.ERROR_MESSAGE
                        ,sysdate--lcu_rpro.CREATION_DATE
                        ,sysdate--lcu_rpro.LAST_UPDATE_DATE
                        );
                l_insert_cnt := l_insert_cnt+1;
              ELSE
                print_output('Can not Insert as Line Details are already exist: '||lcu_rpro.sales_order_new_line_id);
                print_log('Can not Insert as Line Details are already exist: '||lcu_rpro.sales_order_new_line_id);
              END IF;
         END LOOP;
       END LOOP; -- Loop of CM-Quantity.
		--
		-- Update the status of the record.
		--
    IF l_insert_cnt > 0 
    THEN
		   UPDATE xxft_rpro_order_control
		     	set processed_flag ='P'
             ,inv_processed_flag='P'
			       ,error_message = NULL
			      ,last_update_date = SYSDATE
			     ,last_updated_by = fnd_global.user_id
			     ,request_id       = gn_request_id
		   WHERE line_id = cra_cm_rec.return_line_id
		     AND processed_flag = any('R','P')
         AND NVL(inv_processed_flag,'R')='R'
         AND trx_type ='ORD'; -- added on the 6/27 for updating the return order line status correctly.
       COMMIT;
     END IF;
      END LOOP;
      commit;
 		 lc_r_return_status:= 'S';
     print_info_log('  + Inside Procedure. get_invoice_details:=>'||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));
	EXCEPTION
	   WHEN OTHERS THEN
          print_exception_log('Others '||sqlerrm);
		  lc_r_return_status:= 'E';
	END get_invoice_details;


--Seperate Credit Memo details extraction.
-- loop through the line id based on the cra/non cra order types
  -- Run through them
  -- Check If we have to get a credit memo for SPR alone.
  --
  --
PROCEDURE get_invoice_details_new(--p_from_date IN VARCHAR2
                                  --, p_line_id   IN NUMBER
								p_order_number  IN NUMBER)
	IS
	ln_qty_cnt          NUMBER := 1;
	lv_batch_id         NUMBER := 0;
    l_cnt               NUMBER := 0;
    l_cnt_cm            NUMBER := 0;
    l_batch_id          NUMBER := 0;
    lv_cra_cnt          NUMBER := 0;
	l_non_serialized    VARCHAR2(2);

	CURSOR cur_order_control
	IS
	SELECT *
	  FROM xxft.xxft_rpro_order_control xpoc
	 WHERE xpoc.status ='CLOSED'
	   AND NOT EXISTS
	      (SELECT 1
	       FROM XXFT_RPRO_INVOICE_DETAILS
	       WHERE SALES_ORDER_LINE_id = xpoc.line_id
	      )
	   AND EXISTS
	     (SELECT 1
	        FROM oe_transaction_types_all ott,
		            oe_order_lines_all oola
	       WHERE oola.line_id          = xpoc.line_id
	         AND ott.TRANSACTION_TYPE_ID = oola.line_type_id
	         AND ott.order_category_code ='RETURN'
	  );


   CURSOR cur_invoice(c_order_number IN VARCHAR2)
    IS
    SELECT
        /*+ PARALLEL(trxl,DEFAULT) */
        trx.trx_number invoice_number,
        trx.customer_trx_id invoice_id,
        trxt.type invoice_type,
        trxl.line_number invoice_line,
        trxl.customer_trx_line_id invoice_line_id,
        trxl.quantity_invoiced quantity_invoiced,
        trxl.extended_amount extended_amount,
        trx.trx_date invoice_date,
        trx.term_due_date due_date,
        TO_CHAR(trxl.previous_customer_trx_line_id) orig_inv_line_id,
        trxl.sales_order,
        trxl.SALES_ORDER_LINE,
        trxl.interface_line_attribute6,
        xrod.sales_order_new_line_id sales_order_new_line_id,
        xrod.sales_order_line_id so_line_id,
        xrod.sales_order_id so_header_id,
        xrod.sales_order_line_batch_id sales_order_line_batch_id,
        TRXL.INTERFACE_LINE_ATTRIBUTE7,
        NULL Serial_Number
      FROM apps.ra_customer_trx_all trx,
        apps.ra_customer_trx_lines_all trxl,
        apps.ra_cust_trx_types_all trxt,
        XXFT_RPRO_ORDER_DETAILS xrod,
        oe_order_lines_all ool,
        xxft.xxft_rpro_order_control xpoc
      WHERE 1                             =1
      AND xpoc.order_number               = p_order_number
      AND xpoc.processed_flag             ='P'
      AND xpoc.status                     ='CLOSED'
      AND xpoc.inv_processed_flag         ='R'
      AND xrod.sales_order_line_id        = xpoc.line_id
      AND trx.customer_trx_id             = trxl.customer_trx_id
      AND trxl.interface_line_context     = 'ORDER ENTRY'
      AND trxl.line_type                  = 'LINE'
      AND trxt.type                       = 'INV'
      AND trx.cust_trx_type_id            = trxt.cust_trx_type_id
      AND trx.org_id                      = trxt.org_id
      AND trxl.interface_line_attribute6  = to_char(xrod.sales_order_line_id)
      and trxl.inventory_item_id          = ool.inventory_item_id
      AND ool.line_id                     = xrod.sales_order_line_id
      AND ool.Item_Type_Code NOT         IN ('CLASS','OPTION','CONFIG','INCLUDED')
      AND ((ool.top_model_line_id        IS NOT NULL
      AND ool.top_model_line_id           = ool.line_id)
      OR (ool.top_model_line_id          IS NULL))
      AND NVL(processing_attribute5,'X') != 'BO'
      --    AND xrod.flow_status_code ='CLOSED'
        UNION ALL
      SELECT
        /*+ PARALLEL(trxlcm,DEFAULT) */
        trx.trx_number invoice_number,
        trx.customer_trx_id invoice_id,
        trxt.type invoice_type,
        trxlcm.line_number invoice_line,
        trxlcm.customer_trx_line_id invoice_line_id,
        cl.quantity quantity_invoiced,
        trxlcm.extended_amount extended_amount,
        trx.trx_date invoice_date,
        trx.term_due_date due_date,
        (SELECT xrin.INVOICE_id
        FROM XXFT.XXFT_RPRO_INVOICE_DETAILS xrin
        WHERE xrin.SALES_ORDER_NEW_LINE_ID = xrod.SALES_ORDER_NEW_LINE_ID
        AND xrin.tran_type                 = 'INV'
        AND rownum                         < 2
        ) orig_inv_line_id,
        xrod.sales_order sales_order,                                -- trxlinv.sales_order,
        TO_CHAR(xrod.sales_order_line) SALES_ORDER_LINE,             -- trxlinv.SALES_ORDER_LINE,
        TO_CHAR(xrod.SALES_ORDER_LINE_ID) interface_line_attribute6, --trxlinv.interface_line_attribute6,
        xrod.sales_order_new_line_id sales_order_new_line_id,
        xrod.sales_order_line_id so_line_id,
        xrod.sales_order_id so_header_id,
        xrod.sales_order_line_batch_id sales_order_line_batch_id,
        TRXLCM.INTERFACE_LINE_ATTRIBUTE7,
        NULL Serial_Number
      FROM apps.ra_customer_trx_all trx,
        apps.ra_customer_trx_lines_all trxlcm,
        apps.ra_cust_trx_types_all trxt,
        XXFT_RPRO_ORDER_DETAILS xrod,
        ozf_claim_lines_all cl,
        xxft.xxft_rpro_order_control xpoc
      WHERE 1                              = 1
      AND xpoc.order_number                = p_order_number
      AND xpoc.processed_flag              ='P'
      AND xpoc.status                      ='CLOSED'
      AND xpoc.inv_processed_flag          ='R'
      AND xrod.sales_order_line_id         = xpoc.line_id
      AND trx.customer_trx_id              = trxlcm.customer_trx_id
      AND TRXLCM.INTERFACE_LINE_ATTRIBUTE7 = 'FTNT SPR Claims'
      AND TRXLCM.INTERFACE_LINE_ATTRIBUTE2 = cl.claim_id
      AND TRXLCM.INTERFACE_LINE_ATTRIBUTE3 = cl.claim_line_id
      AND xrod.sales_order_line_batch_id   = cl.attribute12
      AND cl.attribute11 NOT LIKE 'INV%'
      AND trxlcm.line_type     = 'LINE'
      AND trxt.type            = 'CM'
      AND trx.cust_trx_type_id = trxt.cust_trx_type_id
      AND trx.org_id           = trxt.org_id
        --   AND xrod.flow_status_code ='CLOSED'
        ;
      CURSOR cur_revpro(c_new_line_id NUMBER)
       IS
	   SELECT /*+ PARALLEL(rod,DEFAULT) */
			     rod.*
         FROM XXFT_RPRO_ORDER_DETAILS rod
	    where sales_order_line_batch_id = c_new_line_id
        order by NUMBER2;

     CURSOR cur_get_non_serialized_flag(p_batch_id IN NUMBER)
     IS
     SELECT distinct attribute60
       FROM xxft.xxft_rpro_order_details
      WHERE sales_order_line_batch_id = p_batch_id
      ;

  	BEGIN
		--	print_log('ORDER NUMBER : '||p_order_number);
	    -- p_order_number:= order_control_rec.order_number;
         -- Regular Invoice
 	    FOR invoice_rec IN cur_invoice(NULL)
      LOOP
        print_log('Sales Order New Line ID : '||invoice_rec.sales_order_new_line_id);
        IF invoice_rec.invoice_type     = 'CM' AND invoice_rec.INTERFACE_LINE_ATTRIBUTE7 <> 'FTNT SPR Claims' THEN
          l_batch_id                   := NULL;
          IF invoice_rec.serial_number IS NOT NULL THEN
            BEGIN
              SELECT sales_order_line_batch_id
              INTO l_batch_id
              FROM xxft_rpro_order_details
              WHERE 1                 =1
              AND sales_order_line_id = invoice_rec.interface_line_attribute6
              AND attribute33         = invoice_rec.serial_number;
            EXCEPTION
            WHEN OTHERS THEN
              NULL;
            END;
          ELSE
            BEGIN
              SELECT MIN(a.sales_order_line_batch_id)
              INTO l_batch_id
              FROM xxft_rpro_order_details a
              WHERE 1                              = 1
              AND a.sales_order_line_id            = invoice_rec.interface_line_attribute6
              AND a.sales_order_line_batch_id NOT IN
                (SELECT DISTINCT b.sales_order_line_batch_id
                FROM xxft_rpro_invoice_details b
                WHERE b.sales_order_line_batch_id = a.sales_order_line_batch_id
                AND b.tran_type                   = 'CM'
                AND NVL(b.attribute47,'N')       <> 'REBATE'
                );
            EXCEPTION
            WHEN OTHERS THEN
              NULL;
            END;
          END IF;
          invoice_rec.sales_order_line_batch_id:=l_batch_id;
        END IF;
         OPEN cur_get_non_serialized_flag(invoice_rec.sales_order_line_batch_id);
        FETCH cur_get_non_serialized_flag INTO l_non_serialized;
        CLOSE cur_get_non_serialized_flag;

        FOR lcu_rpro IN cur_revpro(invoice_rec.sales_order_line_batch_id)
        LOOP
          BEGIN
            IF invoice_rec.invoice_type = 'INV' THEN
              SELECT COUNT(*)
              INTO l_cnt
              FROM XXFT_RPRO_INVOICE_DETAILS
              WHERE sales_order_new_line_id = lcu_rpro.sales_order_new_line_id
              AND tran_type                 = 'INV';
            END IF;
            IF invoice_rec.invoice_type = 'CM' AND invoice_rec.INTERFACE_LINE_ATTRIBUTE7 <> 'FTNT SPR Claims'
            THEN
              SELECT COUNT(*)
              INTO l_cnt_cm
              FROM XXFT_RPRO_INVOICE_DETAILS
              WHERE sales_order_new_line_id = lcu_rpro.sales_order_new_line_id
              AND tran_type                 = 'CM'
              AND NVL(attribute47,'N')     <> 'REBATE';
            ELSE
              SELECT COUNT(*)
              INTO l_cnt_cm
              FROM XXFT_RPRO_INVOICE_DETAILS
              WHERE sales_order_new_line_id = lcu_rpro.sales_order_new_line_id
              AND tran_type                 = 'CM'
              AND NVL(attribute47,'N')      = 'REBATE';
            END IF;
          END;
          print_output('Checking if Line Details are already there or not ');
          print_log('Checking if Line Details are already there or not ');
          IF l_cnt =0 AND invoice_rec.invoice_type = 'INV'
          THEN
             print_output('Inserting Line: '||lcu_rpro.sales_order_new_line_id);
             print_log('Inserting Line: '||lcu_rpro.sales_order_new_line_id);

            INSERT INTO XXFT_RPRO_INVOICE_DETAILS
                  (
                   TRAN_TYPE
                  ,ITEM_ID
                  ,ITEM_NUMBER
                  ,ITEM_DESC
                  ,PRODUCT_FAMILY
                  ,PRODUCT_CATEGORY
                  ,PRODUCT_LINE
                  ,PRODUCT_CLASS
                  ,PRICE_LIST_NAME
                  ,UNIT_LIST_PRICE
                  ,UNIT_SELL_PRICE
                  ,EXT_SELL_PRICE
                  ,EXT_LIST_PRICE
                  ,REC_AMT
                  ,DEF_AMT
                  ,COST_AMOUNT
                  ,COST_REC_AMT
                  ,COST_DEF_AMT
                  ,TRANS_CURR_CODE
                  ,EX_RATE
                  ,BASE_CURR_CODE
                  ,COST_CURR_CODE
                  ,COST_EX_RATE
                  ,RCURR_EX_RATE
                  ,ACCOUNTING_PERIOD
                  ,ACCOUNTING_RULE
                  ,RULE_START_DATE
                  ,RULE_END_DATE
                  ,VAR_RULE_ID
                  ,PO_NUM
                  ,QUOTE_NUM
                  ,SALES_ORDER
                  ,SALES_ORDER_LINE
                  ,SALES_ORDER_ID
                  ,SALES_ORDER_LINE_ID
                  ,SALES_ORDER_NEW_LINE_ID
                  ,SALES_ORDER_LINE_BATCH_ID
                  ,SHIP_DATE
                  ,SO_BOOK_DATE
                  ,TRANS_DATE
                  ,SCHEDULE_SHIP_DATE
                  ,QUANTITY_SHIPPED
                  ,QUANTITY_ORDERED
                  ,QUANTITY_CANCELED
                  ,SALESREP_NAME
                  ,SALES_REP_ID
                  ,ORDER_TYPE
                  ,ORDER_LINE_TYPE
                  ,SERVICE_REFERENCE_LINE_ID
                  ,INVOICE_NUMBER
                  ,INVOICE_TYPE
                  ,INVOICE_LINE
                  ,INVOICE_ID
                  ,INVOICE_LINE_ID
                  ,QUANTITY_INVOICED
                  ,INVOICE_DATE
                  ,DUE_DATE
                  ,ORIG_INV_LINE_ID
                  ,CUSTOMER_ID
                  ,CUSTOMER_NAME
                  ,CUSTOMER_CLASS
                  ,BILL_TO_ID
                  ,BILL_TO_CUSTOMER_NAME
                  ,BILL_TO_CUSTOMER_NUMBER
                  ,BILL_TO_COUNTRY
                  ,SHIP_TO_ID
                  ,SHIP_TO_CUSTOMER_NAME
                  ,SHIP_TO_CUSTOMER_NUMBER
                  ,SHIP_TO_COUNTRY
                  ,BUSINESS_UNIT
                  ,ORG_ID
                  ,SOB_ID
                  ,SEC_ATTR_VALUE
                  ,RETURN_FLAG
                  ,CANCELLED_FLAG
                  ,FLAG_97_2
                  ,PCS_FLAG
                  ,UNDELIVERED_FLAG
                  ,STATED_FLAG
                  ,ELIGIBLE_FOR_CV
                  ,ELIGIBLE_FOR_FV
                  ,DEFERRED_REVENUE_FLAG
                  ,NON_CONTINGENT_FLAG
                  ,UNBILLED_ACCOUNTING_FLAG
                  ,DEAL_ID
                  ,LAG_DAYS
                  ,ATTRIBUTE1
                  ,ATTRIBUTE2
                  ,ATTRIBUTE3
                  ,ATTRIBUTE4
                  ,ATTRIBUTE5
                  ,ATTRIBUTE6
                  ,ATTRIBUTE7
                  ,ATTRIBUTE8
                  ,ATTRIBUTE9
                  ,ATTRIBUTE10
                  ,ATTRIBUTE11
                  ,ATTRIBUTE12
                  ,ATTRIBUTE13
                  ,ATTRIBUTE14
                  ,ATTRIBUTE15
                  ,ATTRIBUTE16
                  ,ATTRIBUTE17
                  ,ATTRIBUTE18
                  ,ATTRIBUTE19
                  ,ATTRIBUTE20
                  ,ATTRIBUTE21
                  ,ATTRIBUTE22
                  ,ATTRIBUTE23
                  ,ATTRIBUTE24
                  ,ATTRIBUTE25
                  ,ATTRIBUTE26
                  ,ATTRIBUTE27
                  ,ATTRIBUTE28
                  ,ATTRIBUTE29
                  ,ATTRIBUTE30
                  ,ATTRIBUTE31
                  ,ATTRIBUTE32
                  ,ATTRIBUTE33
                  ,ATTRIBUTE34
                  ,ATTRIBUTE35
                  ,ATTRIBUTE36
                  ,ATTRIBUTE37
                  ,ATTRIBUTE38
                  ,ATTRIBUTE39
                  ,ATTRIBUTE40
                  ,ATTRIBUTE41
                  ,ATTRIBUTE42
                  ,ATTRIBUTE43
                  ,ATTRIBUTE44
                  ,ATTRIBUTE45
                  ,ATTRIBUTE46
                  ,ATTRIBUTE47
                  ,ATTRIBUTE48
                  ,ATTRIBUTE49
                  ,ATTRIBUTE50
                  ,ATTRIBUTE51
                  ,ATTRIBUTE52
                  ,ATTRIBUTE53
                  ,ATTRIBUTE54
                  ,ATTRIBUTE55
                  ,ATTRIBUTE56
                  ,ATTRIBUTE57
                  ,ATTRIBUTE58
                  ,ATTRIBUTE59
                  ,ATTRIBUTE60
                  ,DATE1
                  ,DATE2
                  ,DATE3
                  ,DATE4
                  ,DATE5
                  ,NUMBER1
                  ,NUMBER2
                  ,NUMBER3
                  ,NUMBER4
                  ,NUMBER5
                  ,NUMBER6
                  ,NUMBER7
                  ,NUMBER8
                  ,NUMBER9
                  ,NUMBER10
                  ,NUMBER11
                  ,NUMBER12
                  ,NUMBER13
                  ,NUMBER14
                  ,NUMBER15
                  ,REV_ACCTG_SEG1
                  ,REV_ACCTG_SEG2
                  ,REV_ACCTG_SEG3
                  ,REV_ACCTG_SEG4
                  ,REV_ACCTG_SEG5
                  ,REV_ACCTG_SEG6
                  ,REV_ACCTG_SEG7
                  ,REV_ACCTG_SEG8
                  ,REV_ACCTG_SEG9
                  ,REV_ACCTG_SEG10
                  ,DEF_ACCTG_SEG1
                  ,DEF_ACCTG_SEG2
                  ,DEF_ACCTG_SEG3
                  ,DEF_ACCTG_SEG4
                  ,DEF_ACCTG_SEG5
                  ,DEF_ACCTG_SEG6
                  ,DEF_ACCTG_SEG7
                  ,DEF_ACCTG_SEG8
                  ,DEF_ACCTG_SEG9
                  ,DEF_ACCTG_SEG10
                  ,COGS_R_SEG1
                  ,COGS_R_SEG2
                  ,COGS_R_SEG3
                  ,COGS_R_SEG4
                  ,COGS_R_SEG5
                  ,COGS_R_SEG6
                  ,COGS_R_SEG7
                  ,COGS_R_SEG8
                  ,COGS_R_SEG9
                  ,COGS_R_SEG10
                  ,COGS_D_SEG1
                  ,COGS_D_SEG2
                  ,COGS_D_SEG3
                  ,COGS_D_SEG4
                  ,COGS_D_SEG5
                  ,COGS_D_SEG6
                  ,COGS_D_SEG7
                  ,COGS_D_SEG8
                  ,COGS_D_SEG9
                  ,COGS_D_SEG10
                  ,LT_DEFERRED_ACCOUNT
                  ,LT_DCOGS_ACCOUNT
                  ,REV_DIST_ID
                  ,COST_DIST_ID
                  ,BOOK_ID
                  ,BNDL_CONFIG_ID
                  ,so_last_update_date
                  ,so_line_creation_date
                  ,PROCESSED_FLAG
                  ,ERROR_MESSAGE
                  ,CREATION_DATE
                  ,LAST_UPDATE_DATE
                 )
              VALUES
                  (
                   'INV' --lcu_rpro.TRAN_TYPE
                  ,lcu_rpro.ITEM_ID
                  ,lcu_rpro.ITEM_NUMBER
                  ,lcu_rpro.ITEM_DESC
                  ,lcu_rpro.PRODUCT_FAMILY
                  ,lcu_rpro.PRODUCT_CATEGORY
                  ,lcu_rpro.PRODUCT_LINE
                  ,lcu_rpro.PRODUCT_CLASS
                  ,lcu_rpro.PRICE_LIST_NAME
                  ,lcu_rpro.UNIT_LIST_PRICE
                  ,lcu_rpro.UNIT_SELL_PRICE
                  ,DECODE(invoice_rec.invoice_type,'CM',invoice_rec.extended_amount,lcu_rpro.EXT_SELL_PRICE) -- invoice_rec.extended_amount
                  ,lcu_rpro.EXT_LIST_PRICE
                  ,lcu_rpro.REC_AMT
                  ,lcu_rpro.DEF_AMT
                  ,lcu_rpro.COST_AMOUNT
                  ,lcu_rpro.COST_REC_AMT
                  ,lcu_rpro.COST_DEF_AMT
                  ,lcu_rpro.TRANS_CURR_CODE
                  ,lcu_rpro.EX_RATE
                  ,lcu_rpro.BASE_CURR_CODE
                  ,lcu_rpro.COST_CURR_CODE
                  ,lcu_rpro.COST_EX_RATE
                  ,lcu_rpro.RCURR_EX_RATE
                  ,lcu_rpro.ACCOUNTING_PERIOD
                  ,lcu_rpro.ACCOUNTING_RULE
                  ,lcu_rpro.RULE_START_DATE
                  ,lcu_rpro.RULE_END_DATE
                  ,lcu_rpro.VAR_RULE_ID
                  ,lcu_rpro.PO_NUM
                  ,lcu_rpro.QUOTE_NUM
                  ,lcu_rpro.SALES_ORDER
                  ,lcu_rpro.SALES_ORDER_LINE
                  ,lcu_rpro.SALES_ORDER_ID
                  ,lcu_rpro.SALES_ORDER_LINE_ID
                  ,lcu_rpro.SALES_ORDER_NEW_LINE_ID
                  ,lcu_rpro.SALES_ORDER_LINE_BATCH_ID
                  ,lcu_rpro.SHIP_DATE
                  ,lcu_rpro.SO_BOOK_DATE
                  ,lcu_rpro.TRANS_DATE
                  ,lcu_rpro.SCHEDULE_SHIP_DATE
                  ,lcu_rpro.QUANTITY_SHIPPED
                  ,lcu_rpro.QUANTITY_ORDERED
                  ,lcu_rpro.QUANTITY_CANCELED
                  ,lcu_rpro.SALESREP_NAME
                  ,lcu_rpro.SALES_REP_ID
                  ,lcu_rpro.ORDER_TYPE
                  ,lcu_rpro.ORDER_LINE_TYPE
                  ,lcu_rpro.SERVICE_REFERENCE_LINE_ID
                  ,invoice_rec.INVOICE_NUMBER
                  ,invoice_rec.INVOICE_TYPE
                  ,invoice_rec.INVOICE_LINE
                  ,invoice_rec.INVOICE_ID
                  ,invoice_rec.INVOICE_ID||'-'||lcu_rpro.SALES_ORDER_NEW_LINE_ID --invoice_rec.INVOICE_LINE_ID
                  ,decode(l_non_serialized,'Y',invoice_rec.QUANTITY_INVOICED,1) --invoice_rec.QUANTITY_INVOICED
                  ,invoice_rec.INVOICE_DATE
                  ,invoice_rec.DUE_DATE
                  ,DECODE(invoice_rec.ORIG_INV_LINE_ID
                  ,NULL
                  ,NULL
                  ,invoice_rec.ORIG_INV_LINE_ID||'-'||lcu_rpro.SALES_ORDER_NEW_LINE_ID)
                  ,lcu_rpro.CUSTOMER_ID
                  ,lcu_rpro.CUSTOMER_NAME
                  ,lcu_rpro.CUSTOMER_CLASS
                  ,lcu_rpro.BILL_TO_ID
                  ,lcu_rpro.BILL_TO_CUSTOMER_NAME
                  ,lcu_rpro.BILL_TO_CUSTOMER_NUMBER
                  ,lcu_rpro.BILL_TO_COUNTRY
                  ,lcu_rpro.SHIP_TO_ID
                  ,lcu_rpro.SHIP_TO_CUSTOMER_NAME
                  ,lcu_rpro.SHIP_TO_CUSTOMER_NUMBER
                  ,lcu_rpro.SHIP_TO_COUNTRY
                  ,lcu_rpro.BUSINESS_UNIT
                  ,lcu_rpro.ORG_ID
                  ,lcu_rpro.SOB_ID
                  ,lcu_rpro.SEC_ATTR_VALUE
                  ,DECODE(invoice_rec.invoice_type,'CM','Y',NULL) --lcu_rpro.RETURN_FLAG
                  ,lcu_rpro.CANCELLED_FLAG
                  ,lcu_rpro.FLAG_97_2
                  ,lcu_rpro.PCS_FLAG
                  ,lcu_rpro.UNDELIVERED_FLAG
                  ,lcu_rpro.STATED_FLAG
                  ,lcu_rpro.ELIGIBLE_FOR_CV
                  ,lcu_rpro.ELIGIBLE_FOR_FV
                  ,lcu_rpro.DEFERRED_REVENUE_FLAG
                  ,lcu_rpro.NON_CONTINGENT_FLAG
                  ,lcu_rpro.UNBILLED_ACCOUNTING_FLAG
                  ,lcu_rpro.DEAL_ID
                  ,lcu_rpro.LAG_DAYS
                  ,lcu_rpro.ATTRIBUTE1
                  ,lcu_rpro.ATTRIBUTE2
                  ,lcu_rpro.ATTRIBUTE3
                  ,lcu_rpro.ATTRIBUTE4
                  ,lcu_rpro.ATTRIBUTE5
                  ,lcu_rpro.ATTRIBUTE6
                  ,lcu_rpro.ATTRIBUTE7
                  ,lcu_rpro.ATTRIBUTE8
                  ,lcu_rpro.ATTRIBUTE9
                  ,lcu_rpro.ATTRIBUTE10
                  ,lcu_rpro.ATTRIBUTE11
                  ,lcu_rpro.ATTRIBUTE12
                  ,lcu_rpro.ATTRIBUTE13
                  ,lcu_rpro.ATTRIBUTE14
                  ,lcu_rpro.ATTRIBUTE15
                  ,lcu_rpro.ATTRIBUTE16
                  ,lcu_rpro.ATTRIBUTE17
                  ,lcu_rpro.ATTRIBUTE18
                  ,lcu_rpro.ATTRIBUTE19
                  ,lcu_rpro.ATTRIBUTE20
                  ,lcu_rpro.ATTRIBUTE21
                  ,lcu_rpro.ATTRIBUTE22
                  ,lcu_rpro.ATTRIBUTE23
                  ,lcu_rpro.ATTRIBUTE24
                  ,lcu_rpro.ATTRIBUTE25
                  ,lcu_rpro.ATTRIBUTE26
                  ,lcu_rpro.ATTRIBUTE27
                  ,lcu_rpro.ATTRIBUTE28
                  ,lcu_rpro.ATTRIBUTE29
                  ,lcu_rpro.ATTRIBUTE30
                  ,lcu_rpro.ATTRIBUTE31
                  ,lcu_rpro.ATTRIBUTE32
                  ,lcu_rpro.ATTRIBUTE33
                  ,lcu_rpro.ATTRIBUTE34
                  ,lcu_rpro.ATTRIBUTE35
                  ,lcu_rpro.ATTRIBUTE36
                  ,lcu_rpro.ATTRIBUTE37
                  ,lcu_rpro.ATTRIBUTE38
                  ,lcu_rpro.ATTRIBUTE39
                  ,lcu_rpro.ATTRIBUTE40
                  ,lcu_rpro.ATTRIBUTE41
                  ,lcu_rpro.ATTRIBUTE42
                  ,lcu_rpro.ATTRIBUTE43
                  ,lcu_rpro.ATTRIBUTE44
                  ,lcu_rpro.ATTRIBUTE45
                  ,lcu_rpro.ATTRIBUTE46
                  ,lcu_rpro.ATTRIBUTE47
                  ,lcu_rpro.ATTRIBUTE48
                  ,lcu_rpro.ATTRIBUTE49
                  ,lcu_rpro.ATTRIBUTE50
                  ,lcu_rpro.ATTRIBUTE51
                  ,lcu_rpro.ATTRIBUTE52
                  ,lcu_rpro.ATTRIBUTE53
                  ,lcu_rpro.ATTRIBUTE54
                  ,lcu_rpro.ATTRIBUTE55
                  ,lcu_rpro.ATTRIBUTE56
                  ,lcu_rpro.ATTRIBUTE57
                  ,lcu_rpro.ATTRIBUTE58
                  ,lcu_rpro.ATTRIBUTE59
                  ,lcu_rpro.ATTRIBUTE60
                  ,lcu_rpro.DATE1
                  ,lcu_rpro.DATE2
                  ,lcu_rpro.DATE3
                  ,lcu_rpro.DATE4
                  ,lcu_rpro.DATE5
                  ,lcu_rpro.NUMBER1
                  ,invoice_rec.INVOICE_LINE_ID --lcu_rpro.NUMBER2
                  ,lcu_rpro.NUMBER3
                  ,lcu_rpro.NUMBER4
                  ,lcu_rpro.NUMBER5
                  ,lcu_rpro.NUMBER6
                  ,lcu_rpro.NUMBER7
                  ,lcu_rpro.NUMBER8
                  ,lcu_rpro.NUMBER9
                  ,lcu_rpro.NUMBER10
                  ,lcu_rpro.NUMBER11
                  ,lcu_rpro.NUMBER12
                  ,lcu_rpro.NUMBER13
                  ,lcu_rpro.NUMBER14
                  ,NULL--lcu_rpro.NUMBER15      Request ID.
                  ,lcu_rpro.REV_ACCTG_SEG1
                  ,lcu_rpro.REV_ACCTG_SEG2
                  ,lcu_rpro.REV_ACCTG_SEG3
                  ,lcu_rpro.REV_ACCTG_SEG4
                  ,lcu_rpro.REV_ACCTG_SEG5
                  ,lcu_rpro.REV_ACCTG_SEG6
                  ,lcu_rpro.REV_ACCTG_SEG7
                  ,lcu_rpro.REV_ACCTG_SEG8
                  ,lcu_rpro.REV_ACCTG_SEG9
                  ,lcu_rpro.REV_ACCTG_SEG10
                  ,lcu_rpro.DEF_ACCTG_SEG1
                  ,lcu_rpro.DEF_ACCTG_SEG2
                  ,lcu_rpro.DEF_ACCTG_SEG3
                  ,lcu_rpro.DEF_ACCTG_SEG4
                  ,lcu_rpro.DEF_ACCTG_SEG5
                  ,lcu_rpro.DEF_ACCTG_SEG6
                  ,lcu_rpro.DEF_ACCTG_SEG7
                  ,lcu_rpro.DEF_ACCTG_SEG8
                  ,lcu_rpro.DEF_ACCTG_SEG9
                  ,lcu_rpro.DEF_ACCTG_SEG10
                  ,lcu_rpro.COGS_R_SEG1
                  ,lcu_rpro.COGS_R_SEG2
                  ,lcu_rpro.COGS_R_SEG3
                  ,lcu_rpro.COGS_R_SEG4
                  ,lcu_rpro.COGS_R_SEG5
                  ,lcu_rpro.COGS_R_SEG6
                  ,lcu_rpro.COGS_R_SEG7
                  ,lcu_rpro.COGS_R_SEG8
                  ,lcu_rpro.COGS_R_SEG9
                  ,lcu_rpro.COGS_R_SEG10
                  ,lcu_rpro.COGS_D_SEG1
                  ,lcu_rpro.COGS_D_SEG2
                  ,lcu_rpro.COGS_D_SEG3
                  ,lcu_rpro.COGS_D_SEG4
                  ,lcu_rpro.COGS_D_SEG5
                  ,lcu_rpro.COGS_D_SEG6
                  ,lcu_rpro.COGS_D_SEG7
                  ,lcu_rpro.COGS_D_SEG8
                  ,lcu_rpro.COGS_D_SEG9
                  ,lcu_rpro.COGS_D_SEG10
                  ,lcu_rpro.LT_DEFERRED_ACCOUNT
                  ,lcu_rpro.LT_DCOGS_ACCOUNT
                  ,lcu_rpro.REV_DIST_ID
                  ,lcu_rpro.COST_DIST_ID
                  ,lcu_rpro.BOOK_ID
                  ,lcu_rpro.BNDL_CONFIG_ID
                  ,SYSDATE --lcu_rpro.so_last_update_date
                  ,SYSDATE --lcu_rpro.so_line_creation_date
                  ,'N' --lcu_rpro.PROCESSED_FLAG
                  ,lcu_rpro.ERROR_MESSAGE
                  ,sysdate --lcu_rpro.CREATION_DATE
                  ,sysdate --lcu_rpro.LAST_UPDATE_DATE
                  );

				ELSIF l_cnt >0 AND  invoice_rec.invoice_type = 'INV'
        THEN
           --
           -- Update the Invoice details with Contract Number, Serial Number, start date, end date,ext sell price, unit sell price -ITS#573774
           --
           Update XXFT_RPRO_INVOICE_DETAILS
              set processed_flag='N'
                  ,ext_sell_price = lcu_rpro.ext_sell_price
                  ,QUANTITY_ORDERED = lcu_rpro.QUANTITY_ORDERED
                  ,attribute45     = lcu_rpro.attribute45
                  ,attribute33     = lcu_rpro.attribute33
                  ,rule_start_date = lcu_rpro.rule_start_date
                  ,rule_end_date   = lcu_rpro.rule_end_date
                  ,last_update_date  = SYSDATE
            WHERE sales_order_new_line_id = lcu_rpro.sales_order_new_line_id
              AND tran_type ='INV';
         
        
        ELSIF l_cnt_cm =0 and invoice_rec.invoice_type  = 'CM' then

					  print_output('Inserting Line: '||lcu_rpro.sales_order_new_line_id);
					  print_log('Inserting Line: '||lcu_rpro.sales_order_new_line_id);

            INSERT INTO XXFT_RPRO_INVOICE_DETAILS
                  (
                   TRAN_TYPE
                  ,ITEM_ID
                  ,ITEM_NUMBER
                  ,ITEM_DESC
                  ,PRODUCT_FAMILY
                  ,PRODUCT_CATEGORY
                  ,PRODUCT_LINE
                  ,PRODUCT_CLASS
                  ,PRICE_LIST_NAME
                  ,UNIT_LIST_PRICE
                  ,UNIT_SELL_PRICE
                  ,EXT_SELL_PRICE
                  ,EXT_LIST_PRICE
                  ,REC_AMT
                  ,DEF_AMT
                  ,COST_AMOUNT
                  ,COST_REC_AMT
                  ,COST_DEF_AMT
                  ,TRANS_CURR_CODE
                  ,EX_RATE
                  ,BASE_CURR_CODE
                  ,COST_CURR_CODE
                  ,COST_EX_RATE
                  ,RCURR_EX_RATE
                  ,ACCOUNTING_PERIOD
                  ,ACCOUNTING_RULE
                  ,RULE_START_DATE
                  ,RULE_END_DATE
                  ,VAR_RULE_ID
                  ,PO_NUM
                  ,QUOTE_NUM
                  ,SALES_ORDER
                  ,SALES_ORDER_LINE
                  ,SALES_ORDER_ID
                  ,SALES_ORDER_LINE_ID
                  ,SALES_ORDER_NEW_LINE_ID
                  ,SALES_ORDER_LINE_BATCH_ID
                  ,SHIP_DATE
                  ,SO_BOOK_DATE
                  ,TRANS_DATE
                  ,SCHEDULE_SHIP_DATE
                  ,QUANTITY_SHIPPED
                  ,QUANTITY_ORDERED
                  ,QUANTITY_CANCELED
                  ,SALESREP_NAME
                  ,SALES_REP_ID
                  ,ORDER_TYPE
                  ,ORDER_LINE_TYPE
                  ,SERVICE_REFERENCE_LINE_ID
                  ,INVOICE_NUMBER
                  ,INVOICE_TYPE
                  ,INVOICE_LINE
                  ,INVOICE_ID
                  ,INVOICE_LINE_ID
                  ,QUANTITY_INVOICED
                  ,INVOICE_DATE
                  ,DUE_DATE
                  ,ORIG_INV_LINE_ID
                  ,CUSTOMER_ID
                  ,CUSTOMER_NAME
                  ,CUSTOMER_CLASS
                  ,BILL_TO_ID
                  ,BILL_TO_CUSTOMER_NAME
                  ,BILL_TO_CUSTOMER_NUMBER
                  ,BILL_TO_COUNTRY
                  ,SHIP_TO_ID
                  ,SHIP_TO_CUSTOMER_NAME
                  ,SHIP_TO_CUSTOMER_NUMBER
                  ,SHIP_TO_COUNTRY
                  ,BUSINESS_UNIT
                  ,ORG_ID
                  ,SOB_ID
                  ,SEC_ATTR_VALUE
                  ,RETURN_FLAG
                  ,CANCELLED_FLAG
                  ,FLAG_97_2
                  ,PCS_FLAG
                  ,UNDELIVERED_FLAG
                  ,STATED_FLAG
                  ,ELIGIBLE_FOR_CV
                  ,ELIGIBLE_FOR_FV
                  ,DEFERRED_REVENUE_FLAG
                  ,NON_CONTINGENT_FLAG
                  ,UNBILLED_ACCOUNTING_FLAG
                  ,DEAL_ID
                  ,LAG_DAYS
                  ,ATTRIBUTE1
                  ,ATTRIBUTE2
                  ,ATTRIBUTE3
                  ,ATTRIBUTE4
                  ,ATTRIBUTE5
                  ,ATTRIBUTE6
                  ,ATTRIBUTE7
                  ,ATTRIBUTE8
                  ,ATTRIBUTE9
                  ,ATTRIBUTE10
                  ,ATTRIBUTE11
                  ,ATTRIBUTE12
                  ,ATTRIBUTE13
                  ,ATTRIBUTE14
                  ,ATTRIBUTE15
                  ,ATTRIBUTE16
                  ,ATTRIBUTE17
                  ,ATTRIBUTE18
                  ,ATTRIBUTE19
                  ,ATTRIBUTE20
                  ,ATTRIBUTE21
                  ,ATTRIBUTE22
                  ,ATTRIBUTE23
                  ,ATTRIBUTE24
                  ,ATTRIBUTE25
                  ,ATTRIBUTE26
                  ,ATTRIBUTE27
                  ,ATTRIBUTE28
                  ,ATTRIBUTE29
                  ,ATTRIBUTE30
                  ,ATTRIBUTE31
                  ,ATTRIBUTE32
                  ,ATTRIBUTE33
                  ,ATTRIBUTE34
                  ,ATTRIBUTE35
                  ,ATTRIBUTE36
                  ,ATTRIBUTE37
                  ,ATTRIBUTE38
                  ,ATTRIBUTE39
                  ,ATTRIBUTE40
                  ,ATTRIBUTE41
                  ,ATTRIBUTE42
                  ,ATTRIBUTE43
                  ,ATTRIBUTE44
                  ,ATTRIBUTE45
                  ,ATTRIBUTE46
                  ,ATTRIBUTE47
                  ,ATTRIBUTE48
                  ,ATTRIBUTE49
                  ,ATTRIBUTE50
                  ,ATTRIBUTE51
                  ,ATTRIBUTE52
                  ,ATTRIBUTE53
                  ,ATTRIBUTE54
                  ,ATTRIBUTE55
                  ,ATTRIBUTE56
                  ,ATTRIBUTE57
                  ,ATTRIBUTE58
                  ,ATTRIBUTE59
                  ,ATTRIBUTE60
                  ,DATE1
                  ,DATE2
                  ,DATE3
                  ,DATE4
                  ,DATE5
                  ,NUMBER1
                  ,NUMBER2
                  ,NUMBER3
                  ,NUMBER4
                  ,NUMBER5
                  ,NUMBER6
                  ,NUMBER7
                  ,NUMBER8
                  ,NUMBER9
                  ,NUMBER10
                  ,NUMBER11
                  ,NUMBER12
                  ,NUMBER13
                  ,NUMBER14
                  ,NUMBER15
                  ,REV_ACCTG_SEG1
                  ,REV_ACCTG_SEG2
                  ,REV_ACCTG_SEG3
                  ,REV_ACCTG_SEG4
                  ,REV_ACCTG_SEG5
                  ,REV_ACCTG_SEG6
                  ,REV_ACCTG_SEG7
                  ,REV_ACCTG_SEG8
                  ,REV_ACCTG_SEG9
                  ,REV_ACCTG_SEG10
                  ,DEF_ACCTG_SEG1
                  ,DEF_ACCTG_SEG2
                  ,DEF_ACCTG_SEG3
                  ,DEF_ACCTG_SEG4
                  ,DEF_ACCTG_SEG5
                  ,DEF_ACCTG_SEG6
                  ,DEF_ACCTG_SEG7
                  ,DEF_ACCTG_SEG8
                  ,DEF_ACCTG_SEG9
                  ,DEF_ACCTG_SEG10
                  ,COGS_R_SEG1
                  ,COGS_R_SEG2
                  ,COGS_R_SEG3
                  ,COGS_R_SEG4
                  ,COGS_R_SEG5
                  ,COGS_R_SEG6
                  ,COGS_R_SEG7
                  ,COGS_R_SEG8
                  ,COGS_R_SEG9
                  ,COGS_R_SEG10
                  ,COGS_D_SEG1
                  ,COGS_D_SEG2
                  ,COGS_D_SEG3
                  ,COGS_D_SEG4
                  ,COGS_D_SEG5
                  ,COGS_D_SEG6
                  ,COGS_D_SEG7
                  ,COGS_D_SEG8
                  ,COGS_D_SEG9
                  ,COGS_D_SEG10
                  ,LT_DEFERRED_ACCOUNT
                  ,LT_DCOGS_ACCOUNT
                  ,REV_DIST_ID
                  ,COST_DIST_ID
                  ,BOOK_ID
                  ,BNDL_CONFIG_ID
                  ,so_last_update_date
                  ,so_line_creation_date
                  ,PROCESSED_FLAG
                  ,ERROR_MESSAGE
                  ,CREATION_DATE
                  ,LAST_UPDATE_DATE
                 )
              VALUES
                  (
                   'CM' --lcu_rpro.TRAN_TYPE
                  ,lcu_rpro.ITEM_ID
                  ,lcu_rpro.ITEM_NUMBER
                  ,lcu_rpro.ITEM_DESC
                  ,lcu_rpro.PRODUCT_FAMILY
                  ,lcu_rpro.PRODUCT_CATEGORY
                  ,lcu_rpro.PRODUCT_LINE
                  ,lcu_rpro.PRODUCT_CLASS
                  ,lcu_rpro.PRICE_LIST_NAME
                  ,lcu_rpro.UNIT_LIST_PRICE
                  ,lcu_rpro.UNIT_SELL_PRICE
                  ,DECODE(lcu_rpro.EXT_SELL_PRICE,0,lcu_rpro.EXT_SELL_PRICE,invoice_rec.extended_amount)
                  --DECODE(invoice_rec.invoice_type,'CM',invoice_rec.extended_amount,lcu_rpro.EXT_SELL_PRICE) -- invoice_rec.extended_amount
                  ,lcu_rpro.EXT_LIST_PRICE
                  ,lcu_rpro.REC_AMT
                  ,lcu_rpro.DEF_AMT
                  ,decode(invoice_rec.INTERFACE_LINE_ATTRIBUTE7,'FTNT SPR Claims',0,lcu_rpro.COST_AMOUNT)
                  ,lcu_rpro.COST_REC_AMT
                  ,lcu_rpro.COST_DEF_AMT
                  ,lcu_rpro.TRANS_CURR_CODE
                  ,lcu_rpro.EX_RATE
                  ,lcu_rpro.BASE_CURR_CODE
                  ,lcu_rpro.COST_CURR_CODE
                  ,lcu_rpro.COST_EX_RATE
                  ,lcu_rpro.RCURR_EX_RATE
                  ,lcu_rpro.ACCOUNTING_PERIOD
                  ,lcu_rpro.ACCOUNTING_RULE
                  ,lcu_rpro.RULE_START_DATE
                  ,lcu_rpro.RULE_END_DATE
                  ,lcu_rpro.VAR_RULE_ID
                  ,lcu_rpro.PO_NUM
                  ,lcu_rpro.QUOTE_NUM
                  ,lcu_rpro.SALES_ORDER
                  ,lcu_rpro.SALES_ORDER_LINE
                  ,lcu_rpro.SALES_ORDER_ID
                  ,lcu_rpro.SALES_ORDER_LINE_ID
                  ,lcu_rpro.SALES_ORDER_NEW_LINE_ID
                  ,lcu_rpro.SALES_ORDER_LINE_BATCH_ID
                  ,lcu_rpro.SHIP_DATE
                  ,lcu_rpro.SO_BOOK_DATE
                  ,lcu_rpro.TRANS_DATE
                  ,lcu_rpro.SCHEDULE_SHIP_DATE
                  ,lcu_rpro.QUANTITY_SHIPPED
                  ,lcu_rpro.QUANTITY_ORDERED
                  ,lcu_rpro.QUANTITY_CANCELED
                  ,lcu_rpro.SALESREP_NAME
                  ,lcu_rpro.SALES_REP_ID
                  ,lcu_rpro.ORDER_TYPE
                  ,lcu_rpro.ORDER_LINE_TYPE
                  ,lcu_rpro.SERVICE_REFERENCE_LINE_ID
                  ,invoice_rec.INVOICE_NUMBER
                  ,invoice_rec.INVOICE_TYPE
                  ,invoice_rec.INVOICE_LINE
                  ,invoice_rec.INVOICE_ID
                  ,invoice_rec.INVOICE_ID||'-'||lcu_rpro.SALES_ORDER_NEW_LINE_ID --invoice_rec.INVOICE_LINE_ID
                  ,DECODE(l_non_serialized,'Y',invoice_rec.QUANTITY_INVOICED,1) --invoice_rec.QUANTITY_INVOICED
                  ,invoice_rec.INVOICE_DATE
                  ,invoice_rec.DUE_DATE
                  ,DECODE(invoice_rec.ORIG_INV_LINE_ID,
                  NULL,
                  NULL,
                  invoice_rec.ORIG_INV_LINE_ID||'-'||lcu_rpro.SALES_ORDER_NEW_LINE_ID)
                  ,lcu_rpro.CUSTOMER_ID
                  ,lcu_rpro.CUSTOMER_NAME
                  ,lcu_rpro.CUSTOMER_CLASS
                  ,lcu_rpro.BILL_TO_ID
                  ,lcu_rpro.BILL_TO_CUSTOMER_NAME
                  ,lcu_rpro.BILL_TO_CUSTOMER_NUMBER
                  ,lcu_rpro.BILL_TO_COUNTRY
                  ,lcu_rpro.SHIP_TO_ID
                  ,lcu_rpro.SHIP_TO_CUSTOMER_NAME
                  ,lcu_rpro.SHIP_TO_CUSTOMER_NUMBER
                  ,lcu_rpro.SHIP_TO_COUNTRY
                  ,lcu_rpro.BUSINESS_UNIT
                  ,lcu_rpro.ORG_ID
                  ,lcu_rpro.SOB_ID
                  ,lcu_rpro.SEC_ATTR_VALUE
                  ,DECODE(invoice_rec.invoice_type,'CM','Y',NULL) --lcu_rpro.RETURN_FLAG
                  ,lcu_rpro.CANCELLED_FLAG
                  ,lcu_rpro.FLAG_97_2
                  ,lcu_rpro.PCS_FLAG
                  ,lcu_rpro.UNDELIVERED_FLAG
                  ,lcu_rpro.STATED_FLAG
                  ,lcu_rpro.ELIGIBLE_FOR_CV
                  ,lcu_rpro.ELIGIBLE_FOR_FV
                  ,lcu_rpro.DEFERRED_REVENUE_FLAG
                  ,lcu_rpro.NON_CONTINGENT_FLAG
                  ,lcu_rpro.UNBILLED_ACCOUNTING_FLAG
                  ,lcu_rpro.DEAL_ID
                  ,lcu_rpro.LAG_DAYS
                  ,lcu_rpro.ATTRIBUTE1
                  ,lcu_rpro.ATTRIBUTE2
                  ,lcu_rpro.ATTRIBUTE3
                  ,lcu_rpro.ATTRIBUTE4
                  ,lcu_rpro.ATTRIBUTE5
                  ,lcu_rpro.ATTRIBUTE6
                  ,lcu_rpro.ATTRIBUTE7
                  ,lcu_rpro.ATTRIBUTE8
                  ,lcu_rpro.ATTRIBUTE9
                  ,lcu_rpro.ATTRIBUTE10
                  ,lcu_rpro.ATTRIBUTE11
                  ,lcu_rpro.ATTRIBUTE12
                  ,lcu_rpro.ATTRIBUTE13
                  ,lcu_rpro.ATTRIBUTE14
                  ,lcu_rpro.ATTRIBUTE15
                  ,lcu_rpro.ATTRIBUTE16
                  ,lcu_rpro.ATTRIBUTE17
                  ,lcu_rpro.ATTRIBUTE18
                  ,lcu_rpro.ATTRIBUTE19
                  ,lcu_rpro.ATTRIBUTE20
                  ,lcu_rpro.ATTRIBUTE21
                  ,lcu_rpro.ATTRIBUTE22
                  ,lcu_rpro.ATTRIBUTE23
                  ,lcu_rpro.ATTRIBUTE24
                  ,lcu_rpro.ATTRIBUTE25
                  ,lcu_rpro.ATTRIBUTE26
                  ,lcu_rpro.ATTRIBUTE27
                  ,lcu_rpro.ATTRIBUTE28
                  ,lcu_rpro.ATTRIBUTE29
                  ,lcu_rpro.ATTRIBUTE30
                  ,lcu_rpro.ATTRIBUTE31
                  ,lcu_rpro.ATTRIBUTE32
                  ,lcu_rpro.ATTRIBUTE33
                  ,lcu_rpro.ATTRIBUTE34
                  ,lcu_rpro.ATTRIBUTE35
                  ,lcu_rpro.ATTRIBUTE36
                  ,lcu_rpro.ATTRIBUTE37
                  ,lcu_rpro.ATTRIBUTE38
                  ,lcu_rpro.ATTRIBUTE39
                  ,lcu_rpro.ATTRIBUTE40
                  ,lcu_rpro.ATTRIBUTE41
                  ,lcu_rpro.ATTRIBUTE42
                  ,lcu_rpro.ATTRIBUTE43
                  ,lcu_rpro.ATTRIBUTE44
                  ,lcu_rpro.ATTRIBUTE45
                  ,lcu_rpro.ATTRIBUTE46
                  ,decode(invoice_rec.INTERFACE_LINE_ATTRIBUTE7,'FTNT SPR Claims','REBATE','RETURN')--lcu_rpro.ATTRIBUTE47
                  ,lcu_rpro.ATTRIBUTE48
                  ,lcu_rpro.ATTRIBUTE49
                  ,lcu_rpro.ATTRIBUTE50
                  ,lcu_rpro.ATTRIBUTE51
                  ,lcu_rpro.ATTRIBUTE52
                  ,lcu_rpro.ATTRIBUTE53
                  ,lcu_rpro.ATTRIBUTE54
                  ,lcu_rpro.ATTRIBUTE55
                  ,lcu_rpro.ATTRIBUTE56
                  ,lcu_rpro.ATTRIBUTE57
                  ,lcu_rpro.ATTRIBUTE58
                  ,lcu_rpro.ATTRIBUTE59
                  ,lcu_rpro.ATTRIBUTE60
                  ,lcu_rpro.DATE1
                  ,lcu_rpro.DATE2
                  ,lcu_rpro.DATE3
                  ,lcu_rpro.DATE4
                  ,lcu_rpro.DATE5
                  ,lcu_rpro.NUMBER1
                  ,invoice_rec.INVOICE_LINE_ID --lcu_rpro.NUMBER2
                  ,lcu_rpro.NUMBER3
                  ,lcu_rpro.NUMBER4
                  ,lcu_rpro.NUMBER5
                  ,lcu_rpro.NUMBER6
                  ,lcu_rpro.NUMBER7
                  ,lcu_rpro.NUMBER8
                  ,lcu_rpro.NUMBER9
                  ,lcu_rpro.NUMBER10
                  ,lcu_rpro.NUMBER11
                  ,lcu_rpro.NUMBER12
                  ,lcu_rpro.NUMBER13
                  ,lcu_rpro.NUMBER14
                  ,NULL --lcu_rpro.NUMBER15      --Request Id.
                  ,lcu_rpro.REV_ACCTG_SEG1
                  ,lcu_rpro.REV_ACCTG_SEG2
                  ,lcu_rpro.REV_ACCTG_SEG3
                  ,lcu_rpro.REV_ACCTG_SEG4
                  ,lcu_rpro.REV_ACCTG_SEG5
                  ,lcu_rpro.REV_ACCTG_SEG6
                  ,lcu_rpro.REV_ACCTG_SEG7
                  ,lcu_rpro.REV_ACCTG_SEG8
                  ,lcu_rpro.REV_ACCTG_SEG9
                  ,lcu_rpro.REV_ACCTG_SEG10
                  ,lcu_rpro.DEF_ACCTG_SEG1
                  ,lcu_rpro.DEF_ACCTG_SEG2
                  ,lcu_rpro.DEF_ACCTG_SEG3
                  ,lcu_rpro.DEF_ACCTG_SEG4
                  ,lcu_rpro.DEF_ACCTG_SEG5
                  ,lcu_rpro.DEF_ACCTG_SEG6
                  ,lcu_rpro.DEF_ACCTG_SEG7
                  ,lcu_rpro.DEF_ACCTG_SEG8
                  ,lcu_rpro.DEF_ACCTG_SEG9
                  ,lcu_rpro.DEF_ACCTG_SEG10
                  ,lcu_rpro.COGS_R_SEG1
                  ,lcu_rpro.COGS_R_SEG2
                  ,lcu_rpro.COGS_R_SEG3
                  ,lcu_rpro.COGS_R_SEG4
                  ,lcu_rpro.COGS_R_SEG5
                  ,lcu_rpro.COGS_R_SEG6
                  ,lcu_rpro.COGS_R_SEG7
                  ,lcu_rpro.COGS_R_SEG8
                  ,lcu_rpro.COGS_R_SEG9
                  ,lcu_rpro.COGS_R_SEG10
                  ,lcu_rpro.COGS_D_SEG1
                  ,lcu_rpro.COGS_D_SEG2
                  ,lcu_rpro.COGS_D_SEG3
                  ,lcu_rpro.COGS_D_SEG4
                  ,lcu_rpro.COGS_D_SEG5
                  ,lcu_rpro.COGS_D_SEG6
                  ,lcu_rpro.COGS_D_SEG7
                  ,lcu_rpro.COGS_D_SEG8
                  ,lcu_rpro.COGS_D_SEG9
                  ,lcu_rpro.COGS_D_SEG10
                  ,lcu_rpro.LT_DEFERRED_ACCOUNT
                  ,lcu_rpro.LT_DCOGS_ACCOUNT
                  ,lcu_rpro.REV_DIST_ID
                  ,lcu_rpro.COST_DIST_ID
                  ,lcu_rpro.BOOK_ID
                  ,lcu_rpro.BNDL_CONFIG_ID
                  ,SYSDATE --lcu_rpro.so_last_update_date
                  ,SYSDATE --lcu_rpro.so_line_creation_date
                  ,'N' --lcu_rpro.PROCESSED_FLAG
                  ,lcu_rpro.ERROR_MESSAGE
                  ,sysdate--lcu_rpro.CREATION_DATE
                  ,sysdate--lcu_rpro.LAST_UPDATE_DATE
                  );
				ELSE
					print_output('Can not Insert as Line Details are already exist: '||lcu_rpro.sales_order_new_line_id);
					print_log('Can not Insert as Line Details are already exist: '||lcu_rpro.sales_order_new_line_id);
				END IF;
 		    END LOOP;
        --
        -- Update the inv processed flag status.
        --
		  /*  UPDATE xxft_rpro_order_control
			      set ,inv_processed_flag='P'
                 ,error_message = NULL
			           ,last_update_date = SYSDATE
			          ,last_updated_by = fnd_global.user_id
			          ,request_id       = gn_request_id
		   WHERE line_id = cra_cm_rec.return_line_id
		     AND inv_processed_flag='R'; */
     END LOOP;
		 COMMIT;
   	 lc_r_return_status:= 'S';
	EXCEPTION
		 WHEN OTHERS THEN
     print_exception_log('Others '||sqlerrm);
		 lc_r_return_status:= 'E';

	END get_invoice_details_new;


FUNCTION GET_LINE_COST(P_LINE_ID IN NUMBER)
RETURN NUMBER
IS
    l_cost_amount number := 0;
	CURSOR cur_get_cost
	IS
	/*SELECT rpo.cost_amount--, rpo.sales_order_line_id
      FROM XXFT_RPRO_OE_ORDER_DETAILS_V rpo
     WHERE sales_order_line_id =P_LINE_ID; */
SELECT ROUND ((XXFT_RPRO_DATA_EXTRACT_PKG.GET_COST_AMOUNT(oel.inventory_item_id,oel.line_id,oel.ship_from_org_id,'Y',oel.ato_line_id)/ordered_quantity),2) cost_amount
FROM OE_ORDER_LINES_ALL OEL
WHERE LINE_ID = P_LINE_ID;

     
BEGIN
   OPEN cur_get_cost;
   FETCH cur_get_cost INTO l_cost_amount;
   CLOSE cur_get_cost;
   return l_cost_amount;
END;
--
--ITS#562057
--
FUNCTION check_svc_exception_list(P_HEADER_ID IN NUMBER)
RETURN VARCHAR2
IS

l_excp_list Varchar2(2);

   CURSOR cur_exception_list
   IS
   SELECT CASE WHEN COUNT(1) > 0 THEN 'Y' ELSE 'N' end
     FROM oe_order_headers_all
    WHERE header_id = p_header_id
      and exists( SELECT 1 
                     FROM fnd_lookup_values
                    WHERE lookup_type='FTNT_REVPRO_SKIP_ORDER_VAL'
                       and enabled_flag ='Y'
                       and trunc(SYSDATE) between NVL(start_date_active,sysdate-1) and NVL(end_date_active, sysdate+1)
                       and lookup_code= to_char(order_number)
      );

BEGIN
   OPEN cur_exception_list;
   FETCH cur_exception_list INTO l_excp_list;
   CLOSE cur_exception_list;
   RETURN l_excp_list;
END;


PROCEDURE update_cost_amount(p_parent_line_id IN NUMBER
                              ,p_header_id      IN NUMBER)
  IS
	CURSOR cur_get_cost
	IS
	SELECT rpo.cost_amount, rpo.sales_order_line_id
      FROM XXFT_RPRO_OE_ORDER_DETAILS_V rpo
     WHERE sales_order_line_id = any (
          SELECT oola.line_id
            FROM oe_order_lines_all oola
           WHERE oola.TOP_MODEL_LINE_ID = p_parent_line_id
             AND ordered_item NOT like '%WARRANTY%'
             AND header_id = p_header_id
		--	 AND oola.TOP_MODEL_LINE_ID <> oola.line_id
		   UNION
		  SELECT oola.line_id
			FROM oe_order_lines_all oola
		   WHERE oola.link_to_line_id = p_parent_line_id
			 AND ordered_item NOT like '%WARRANTY%'
			 AND header_id = p_header_id
           UNION
          SELECT oola.line_id
            FROM oe_order_lines_all oola
           WHERE oola.service_reference_line_id = p_parent_line_id
             AND OOLA.SERVICE_REFERENCE_TYPE_CODE ='ORDER' /* ITS 550041 */
             AND ordered_item NOT like '%WARRANTY%'
             AND header_id = p_header_id
           UNION
          SELECT oola.line_id
            FROM oe_order_lines_all oola
           WHERE 1=1
           and OOLA.SERVICE_REFERENCE_TYPE_CODE ='ORDER' /* ITS 550041 */
           AND oola.service_reference_line_id IN (SELECT line_id
            FROM oe_order_lines_all oola
           WHERE oola.top_model_line_id= p_parent_line_id
              AND header_id = p_header_id
		   UNION
          SELECT oola.line_id
            FROM oe_order_lines_all oola
           WHERE oola.link_to_line_id = p_parent_line_id
             AND ordered_item NOT like '%WARRANTY%'
             AND header_id = p_header_id
           )
           AND ordered_item NOT like '%WARRANTY%'
           AND header_id = p_header_id
     );

  BEGIN
     --
	 -- Get the cost of the line and
	 --
	 FOR get_cost_rec IN cur_get_cost
	 LOOP
	    UPDATE xxft_rpro_order_details
		   SET cost_amount = get_cost_rec.cost_amount
		 WHERE sales_order_line_id = get_cost_rec.sales_order_line_id;
	 END LOOP;
	 COMMIT;
  EXCEPTION
     WHEN OTHERS
	 THEN
	    print_log(' Unexpected Exception in the update_cost_amount :=>'||SQLERRM);
		RAISE;
  END;


   PROCEDURE validate_intial_load(p_order_number IN NUMBER)
   IS
     l_error_msg     VARCHAR2(4000);
	 l_ch_error_msg  VARCHAR2(4000);
     ex_dml_errors   EXCEPTION;
     PRAGMA          EXCEPTION_INIT(ex_dml_errors, -24381);

    type order_control_rec IS RECORD(
	    line_id             NUMBER
	  , header_id           NUMBER
	  , order_number        NUMBER
	  , ordered_item        VARCHAR2(240)
	  , price_list_id       NUMBER
	  , list_price          NUMBER
	  , revenue_ccid        NUMBER
	  , unearned_ccid       NUMBER
	  , deferred_cogs       NUMBER
      , product_family    VARCHAR2(240)
      , product_category  VARCHAR2(240)
      , product_line      VARCHAR2(240)
      , product_class     VARCHAR2(240)
      , attribute15       VARCHAR2(240)
      , region            VARCHAR2(240)
      , item_type_code    VARCHAR2(240)
      , uom_class         VARCHAR2(240)
      , service_start_dt  VARCHAR2(40)
      ,service_end_dt     VARCHAR2(40)
	  , process_flag        VARCHAR2(1)
	  , error_message       VARCHAR2(4000)
	);

	TYPE order_control_tbl_type IS TABLE OF order_control_rec
	INDEX BY BINARY_INTEGER;
	l_ord_ctl_tbl1 order_control_tbl_type;
	l_ord_ctl_tbl2 order_control_tbl_type;
	--
	-- Get parent Line to validate for missing fields.
	--
	CURSOR cur_pa_line_details
	IS
	SELECT  xpoc.line_id
	      , xpoc.header_id
		  , xpoc.order_number
		  , oola.ordered_item
		  , NVL(oola.price_list_id,ooha.price_list_id) -- ITS#544414
          , XXFT_RPRO_DATA_EXTRACT_PKG.get_list_price(oola.line_id,NVL(oola.price_list_id,ooha.price_list_id), oola.inventory_item_id, oola.pricing_date) price
		  , RCT.GL_ID_REV
		  , RCT.GL_ID_UNEARNED
		  , mp.deferred_cogs_account
          , XXFT_RPRO_DATA_EXTRACT_PKG.get_product_family(oola.inventory_item_id,oola.ship_from_org_id) product_family
          , XXFT_RPRO_DATA_EXTRACT_PKG.get_product_category(oola.inventory_item_id,oola.ship_from_org_id) product_category
          , XXFT_RPRO_DATA_EXTRACT_PKG.get_product_line(oola.inventory_item_id,oola.ship_from_org_id) product_line
          , XXFT_RPRO_DATA_EXTRACT_PKG.get_product_class(oola.inventory_item_id,oola.ship_from_org_id) product_class
          , XXFT_RPRO_DATA_EXTRACT_PKG.get_product_group(oola.inventory_item_id,oola.ship_from_org_id) attribute15
          , XXFT_RPRO_DATA_EXTRACT_PKG.get_region(oola.line_id) region -- added for ticket # 508676
          , oola.ITEM_TYPE_CODE
          , UOM.UOM_CLASS
            ,CASE
                WHEN oola.item_type_code     ='INCLUDED'
                 AND UPPER(UOM.UOM_CLASS)  = 'TIME'
                THEN NVL(oola.service_start_date,NVL(oola.attribute2,XXFT_RPRO_DATA_EXTRACT_PKG.get_line_contract_date(oola.line_id,'START'))) --ITS#553115
                ELSE oola.service_start_date
                END rule_start_date,
              CASE
                WHEN oola.item_type_code     ='INCLUDED'
                AND upper(UOM.UOM_CLASS) = 'TIME' --oola1.ORDER_QUANTITY_UOM <> 'EA'
                THEN NVL(oola.service_end_date,NVL(oola.attribute19,XXFT_RPRO_DATA_EXTRACT_PKG.get_line_contract_date(oola.line_id,'END'))) --ITS#553115
                ELSE oola.service_end_date
              END rule_end_date
		      , xpoc.processed_flag
		      , xpoc.error_message
	  FROM XXFT_RPRO_ORDER_CONTROL xpoc
	      ,oe_order_lines_all oola
		  ,oe_order_headers_all ooha
		  ,OE_TRANSACTION_TYPES_ALL OTT
      ,apps.oe_transaction_types_tl ott1
		  ,RA_CUST_TRX_TYPES_ALL RCT
		  ,mtl_parameters mp
      ,MTL_UNITS_OF_MEASURE_VL uom
	 WHERE xpoc.processed_flag   = ANY('R','E')
     AND xpoc.Status             IN ('BOOKED','CLOSED','AWAITING_FULFILLMENT')
	 AND xpoc.order_number       = NVL(p_order_number,xpoc.order_number)
	 AND oola.line_id            = xpoc.line_id
	 AND ooha.header_id          = xpoc.header_id
	 AND OTT.TRANSACTION_TYPE_ID = ooha.order_type_id
     AND ott1.transaction_type_id = ott.transaction_type_id
	 AND RCT.CUST_TRX_TYPE_ID    = ott.cust_trx_type_id
	 AND mp.organization_id(+) = oola.ship_from_org_id
   and oola.line_category_code='ORDER'
	 AND EXISTS
         (SELECT 1
            FROM apps.FND_LOOKUP_VALUES flv
           WHERE flv.language            = 'US'
             AND NVL(flv.enabled_flag,'N') = 'Y'
             AND sysdate BETWEEN NVL(flv.start_date_active,sysdate) AND NVL(flv.end_date_active,sysdate + 1)
             AND flv.lookup_type        = 'REVPRO_ORDER_TYPE'
             AND UPPER(flv.lookup_code) = UPPER(ott1.name)
         )
	 AND NOT EXISTS ( SELECT 1
	                    FROM xxft_rpro_order_details
					   WHERE sales_order_line_id = xpoc.line_id
     )
   AND uom.uom_code = oola.ORDER_QUANTITY_UOM;
	--
	-- Get child lines for validation.
	--
	CURSOR cur_ch_line_details(c_parent_line_id NUMBER
	                          ,c_header_id      NUMBER)
	IS
	SELECT    oola1.line_id
            , oola1.header_id
		    , ooha.order_number
		    , oola1.ordered_item
		    , NVL(oola1.price_list_id,ooha.price_list_id) --ITS#544414
			, XXFT_RPRO_DATA_EXTRACT_PKG.get_list_price(oola1.line_id,NVL(oola1.price_list_id,ooha.price_list_id), oola1.inventory_item_id, oola1.pricing_date) price
		    , RCT.GL_ID_REV
		    , RCT.GL_ID_UNEARNED
		    , mp.deferred_cogs_account
            , XXFT_RPRO_DATA_EXTRACT_PKG.get_product_family(oola1.inventory_item_id,oola1.ship_from_org_id) product_family
            , XXFT_RPRO_DATA_EXTRACT_PKG.get_product_category(oola1.inventory_item_id,oola1.ship_from_org_id) product_category
            , XXFT_RPRO_DATA_EXTRACT_PKG.get_product_line(oola1.inventory_item_id,oola1.ship_from_org_id) product_line
            , XXFT_RPRO_DATA_EXTRACT_PKG.get_product_class(oola1.inventory_item_id,oola1.ship_from_org_id) product_class
            , XXFT_RPRO_DATA_EXTRACT_PKG.get_product_group(oola1.inventory_item_id,oola1.ship_from_org_id) attribute15
            ,XXFT_RPRO_DATA_EXTRACT_PKG.get_region(oola1.line_id) region -- added for the ticket #508676
            ,OOLA1.ITEM_TYPE_CODE
            ,UOM.UOM_CLASS
            ,CASE
                WHEN oola1.item_type_code     ='INCLUDED'
                 AND UPPER(UOM.UOM_CLASS)  = 'TIME'
                THEN NVL(oola1.service_start_date,NVL(oola1.attribute2,XXFT_RPRO_DATA_EXTRACT_PKG.get_line_contract_date(oola1.line_id,'START')))
                ELSE oola1.service_start_date
                END rule_start_date,
              CASE
                WHEN oola1.item_type_code     ='INCLUDED'
                AND UPPER(UOM.UOM_CLASS) = 'TIME' --oola1.ORDER_QUANTITY_UOM <> 'EA'
                THEN NVL(oola1.service_end_date,NVL(oola1.attribute19,XXFT_RPRO_DATA_EXTRACT_PKG.get_line_contract_date(oola1.line_id,'END')))
                ELSE oola1.service_end_date
              END rule_end_date
        , NULL process_flag
		    , NULL error_message
	  FROM oe_order_lines_all oola1
		  ,oe_order_headers_all ooha
		  ,OE_TRANSACTION_TYPES_ALL OTT
      ,apps.oe_transaction_types_tl ott1
      ,RA_CUST_TRX_TYPES_ALL RCT
		  ,mtl_parameters mp
      ,MTL_UNITS_OF_MEASURE_VL uom
	 WHERE oola1.line_id = any (
		  SELECT oola.line_id
			FROM oe_order_lines_all oola
		   WHERE oola.TOP_MODEL_LINE_ID = c_parent_line_id
			 AND ordered_item NOT like '%WARRANTY%'
			 AND header_id = c_header_id
			 AND oola.TOP_MODEL_LINE_ID <> oola.line_id
		   UNION
		  SELECT oola.line_id
			FROM oe_order_lines_all oola
		   WHERE oola.service_reference_line_id = c_parent_line_id
			 AND ordered_item NOT like '%WARRANTY%'
			 AND header_id = c_header_id
		   UNION
		  SELECT oola.line_id
			FROM oe_order_lines_all oola
		   WHERE 1=1
       AND OOLA.SERVICE_REFERENCE_TYPE_CODE ='ORDER' /* ITS 550041 */
       AND oola.service_reference_line_id IN (SELECT line_id
			FROM oe_order_lines_all oola
		   WHERE oola.top_model_line_id= c_parent_line_id
			  AND header_id = c_header_id
		   UNION
		  SELECT oola.line_id
			FROM oe_order_lines_all oola
		   WHERE oola.link_to_line_id = c_parent_line_id
			 AND ordered_item NOT like '%WARRANTY%'
			 AND header_id = c_header_id
		   )
		   AND ordered_item NOT like '%WARRANTY%'
		   AND header_id = c_header_id
	 )
	 AND ooha.header_id          = oola1.header_id
	 AND OTT.TRANSACTION_TYPE_ID = ooha.order_type_id
	 AND OTT1.transaction_type_id = ott.transaction_type_id
     AND RCT.CUST_TRX_TYPE_ID    = ott.cust_trx_type_id
	 AND mp.organization_id(+) = oola1.ship_from_org_id
   AND uom.uom_code = oola1.ORDER_QUANTITY_UOM
    ;
BEGIN
   print_log('   + Inside Procedure validate_intial_load');
   print_info_log('  + Inside Procedure validate_intial_load:=>'||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));
   -- Check if the lines to be extracted are ready to be extracted
	   -- Check if the line has Cost of Goods Sold, Revenue COGS and Pricelist are there
	     -- If exists then continue checking the lines.
	     -- Get the child lines from the parent line
		   -- check Check if the line has Cost of Goods Sold, Revenue COGS and Pricelist are there
		     -- If any of the line does not have then process flag as 'E' and error message as
			      -- One of the child line does not have required fields.
	         -- If all of them are there then leave it as it is.

   OPEN cur_pa_line_details;
   FETCH cur_pa_line_details BULK COLLECT INTO l_ord_ctl_tbl1;
   CLOSE cur_pa_line_details;
   print_log('   +  l_ord_ctl_tbl1.COUNT :->'||l_ord_ctl_tbl1.count);
   FOR p1_cnt IN 1..l_ord_ctl_tbl1.COUNT
   LOOP
	  l_error_msg := NULL;
	  IF l_ord_ctl_tbl2.COUNT > 0
	  THEN
	     l_ord_ctl_tbl2.DELETE;
	  END IF;
	  IF l_ord_ctl_tbl1(p1_cnt).product_family IS NULL
	  THEN
	     l_error_msg :=l_error_msg||':=:'||' Product Family';
	  END IF;
	  IF l_ord_ctl_tbl1(p1_cnt).product_category IS NULL
	  THEN
	     l_error_msg := l_error_msg||':=:'||'Product Category';
	  END IF;
	  IF l_ord_ctl_tbl1(p1_cnt).product_line IS NULL
	  THEN
	     l_error_msg := l_error_msg||':=:'||'Product Line';
	  END IF;
	  IF l_ord_ctl_tbl1(p1_cnt).product_class IS NULL
	  THEN
	     l_error_msg := l_error_msg||':=:'||'Product Class ';
	  END IF;
	  IF l_ord_ctl_tbl1(p1_cnt).attribute15 IS NULL
	  THEN
	     l_error_msg := l_error_msg||':=:'||' Product Group';
	  END IF;
	  IF l_ord_ctl_tbl1(p1_cnt).price_list_id IS NULL
	  THEN
	     l_error_msg := l_error_msg||':=:'||' price List';
	  END IF;
      IF l_ord_ctl_tbl1(p1_cnt).revenue_ccid IS NULL
	  THEN
	     l_error_msg := l_error_msg||':=:'||' revenue COGS ';
	  END IF;
      IF l_ord_ctl_tbl1(p1_cnt).unearned_ccid IS NULL
	  THEN
	     l_error_msg := l_error_msg||':=:'||' Unearned COGS';
	  END IF;
	  IF l_ord_ctl_tbl1(p1_cnt).deferred_cogs IS NULL
	  THEN
	     l_error_msg := l_error_msg||':=:'||' Deferred COGS';
	  END IF;
	  IF l_ord_ctl_tbl1(p1_cnt).list_price IS NULL or l_ord_ctl_tbl1(p1_cnt).list_price = 0
	  THEN
	     l_error_msg := l_error_msg||':=:'||' Unit List Price';
	  END IF;
    IF l_ord_ctl_tbl1(p1_cnt).region IS NULL
	  THEN
	     l_error_msg := l_error_msg||':=:'||' Region'; -- added for the ticket #508676
	  END IF;
    IF (l_ord_ctl_tbl1(p1_cnt).item_type_code IN ('INCLUDED','SERVICE') AND  UPPER(l_ord_ctl_tbl1(p1_cnt).uom_class) ='TIME')
    THEN
       IF l_ord_ctl_tbl1(p1_cnt).service_start_dt is null or l_ord_ctl_tbl1(p1_cnt).service_end_dt is null
       THEN
          IF check_svc_exception_list(l_ord_ctl_tbl1(p1_cnt).header_id) = 'Y'  -- ITS#
          THEN
             NULL; --ignore this order.
          ELSE
             l_error_msg := l_error_msg||':=:'||' Service Information ';
          END IF;          
       END IF;
    END IF;

	  IF l_error_msg IS NOT NULL
	  THEN
	      l_error_msg := l_error_msg||' missing for :=>'||l_ord_ctl_tbl1(p1_cnt).ordered_item||CHR(13);
	  END IF;
	  IF l_error_msg IS NULL
	  THEN
	     OPEN cur_ch_line_details(c_parent_line_id => l_ord_ctl_tbl1(p1_cnt).line_id
	                              ,c_header_id      => l_ord_ctl_tbl1(p1_cnt).header_id);
		 FETCH cur_ch_line_details BULK COLLECT INTO l_ord_ctl_tbl2;
		 CLOSE cur_ch_line_details;
         print_log('   +  l_ord_ctl_tbl2.COUNT :->'||l_ord_ctl_tbl2.count);
		 FOR ln1_cnt IN 1..l_ord_ctl_tbl2.COUNT
		 LOOP
		    l_ch_error_msg := NULL;
			IF l_ord_ctl_tbl2(ln1_cnt).product_family IS NULL
			THEN
			  l_ch_error_msg :=l_ch_error_msg||':=:'||' Product Family';
			END IF;
			IF l_ord_ctl_tbl2(ln1_cnt).product_category IS NULL
			THEN
			  l_ch_error_msg := l_ch_error_msg||':=:'||'Product Category';
			END IF;
			IF l_ord_ctl_tbl2(ln1_cnt).product_line IS NULL
			THEN
			  l_ch_error_msg := l_ch_error_msg||':=:'||'Product Line';
			END IF;
			IF l_ord_ctl_tbl2(ln1_cnt).product_class IS NULL
			THEN
			  l_ch_error_msg := l_ch_error_msg||':=:'||'Product Class ';
			END IF;
			IF l_ord_ctl_tbl2(ln1_cnt).attribute15 IS NULL
			THEN
			  l_ch_error_msg := l_ch_error_msg||':=:'||' Product Group';
			END IF;
			IF l_ord_ctl_tbl2(ln1_cnt).price_list_id IS NULL
			THEN
			  l_ch_error_msg := l_ch_error_msg||':=:'||' price List';
			END IF;
			IF l_ord_ctl_tbl2(ln1_cnt).revenue_ccid IS NULL
			THEN
			  l_ch_error_msg := l_ch_error_msg||':=:'||' revenue COGS ';
			END IF;
			IF l_ord_ctl_tbl2(ln1_cnt).unearned_ccid IS NULL
			THEN
			  l_ch_error_msg := l_ch_error_msg||':=:'||' Unearned COGS';
			END IF;
			IF l_ord_ctl_tbl2(ln1_cnt).deferred_cogs IS NULL
			THEN
			  l_ch_error_msg := l_ch_error_msg||':=:'||' Deferred COGS';
			END IF;
            IF l_ord_ctl_tbl2(ln1_cnt).list_price IS NULL or l_ord_ctl_tbl2(ln1_cnt).list_price =0
			THEN
			  l_ch_error_msg := l_ch_error_msg||':=:'||' Unit List Price';
			END IF;
      IF l_ord_ctl_tbl2(ln1_cnt).region IS NULL
			THEN
			  l_ch_error_msg := l_ch_error_msg||':=:'||' Region'; -- added for the ticket #508676
			END IF;
			IF (l_ord_ctl_tbl2(ln1_cnt).item_type_code IN ('INCLUDED','SERVICE') AND UPPER(l_ord_ctl_tbl2(ln1_cnt).uom_class) ='TIME')
      THEN
         IF l_ord_ctl_tbl2(ln1_cnt).service_start_dt is null or l_ord_ctl_tbl2(ln1_cnt).service_end_dt is null
         THEN
            IF check_svc_exception_list(l_ord_ctl_tbl2(ln1_cnt).header_id) = 'Y'
            THEN
               NULL; -- Skipping this order.
            ELSE
               l_ch_error_msg := l_ch_error_msg||':=:'||' Service Information';
            END IF;
            
         END IF;
      END IF;

			IF l_ch_error_msg IS NOT NULL
			THEN
			  l_error_msg := l_error_msg||l_ch_error_msg||' missing for :=>'||l_ord_ctl_tbl2(ln1_cnt).ordered_item||CHR(13);
			END IF;
		  END LOOP;
	   END IF;
	   print_log('    l_error_msg :=>'||l_error_msg);
	   IF l_error_msg is not null
	   THEN
	      l_ord_ctl_tbl1(p1_cnt).process_flag :='E';
		  l_ord_ctl_tbl1(p1_cnt).error_message :=l_error_msg;
	   ELSE
	      IF l_ord_ctl_tbl1(p1_cnt).process_flag ='E'
		  THEN
	         l_ord_ctl_tbl1(p1_cnt).process_flag :='R';
		  END IF;
		  l_ord_ctl_tbl1(p1_cnt).error_message :=NULL;
	   END IF;
	END LOOP;

    l_ord_ctl_tbl2 := l_ord_ctl_tbl1;
	IF l_ord_ctl_tbl1.COUNT > 0
	THEN
	   print_log('Before Updating  the records for  '||l_ord_ctl_tbl1.COUNT);
	   BEGIN
		  FORALL X in l_ord_ctl_tbl1.FIRST..l_ord_ctl_tbl1.LAST SAVE EXCEPTIONS
			UPDATE XXFT.XXFT_RPRO_ORDER_CONTROL
			   SET processed_flag   = l_ord_ctl_tbl1(X).process_flag
				  ,error_message    = l_ord_ctl_tbl1(X).error_message
				  ,last_update_date = SYSDATE
				  ,last_updated_by  = gn_user_id
				  ,request_id       = gn_request_id
			 WHERE line_id          = l_ord_ctl_tbl2(X).line_id
         AND trx_type ='ORD';
		   COMMIT;
	   EXCEPTION
		  WHEN ex_dml_errors THEN
			--    l_error_count := SQL%BULK_EXCEPTIONS.count;
			 print_log('Unexpected Exception when Updating validation status: ' || SQL%BULK_EXCEPTIONS.count);
			 FOR p IN 1 .. SQL%BULK_EXCEPTIONS.count LOOP
			    print_log('Error: ' || p ||
						   ' Array Index: ' || SQL%BULK_EXCEPTIONS(p).error_index ||
							 ' Message: ' || SQLERRM(-SQL%BULK_EXCEPTIONS(p).ERROR_CODE));
			 END LOOP;
			 ROLLBACK;
		  WHEN OTHERS
		  THEN
		     print_log('  Unexpected Exception when Updating records #2:=>'||SQLERRM);
	         RAISE;
	   END;
    END IF;
    --
    -- Update the whole order not to extract to revpro.
    --
    /*UPDATE XXFT.XXFT_RPRO_ORDER_CONTROL
       SET processed_flag='NE'
           ,error_message = error_message||':=:Not to extract Whole Order as the lines have issue.'
      WHERE order_number = any (SELECT Distinct order_number
                                   FROM xxft.xxft_rpro_order_control
                                   WHERE processed_flag ='E'
                                     AND error_message like '%Service Information%');
      COMMIT; */

	 print_info_log('  + Inside Procedure validate_intial_load exit:=>'||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));
  EXCEPTION
     WHEN OTHERS
	   THEN
	    print_exception_log(' Unexpected exception in validate_intial_load :=>'||SQLERRM);
		RAISE;
  END validate_intial_load;

  PROCEDURE Initial_Populate(p_order_number IN NUMBER
  )
  IS
  -- Intial
    lt_ch_ln_tbl1       stg1_tbl_type;
    lt_tp_ln_dtl        stg1_tbl_type;
    lt_ord_dtls         rpro_order_details_tbl_type;
  -----declaring variables----------
	lv_batch_id                NUMBER := 0;
	ln_count                   NUMBER := 0;
	lv_parent_line_id          NUMBER := 0;
	lv_parent_last_update_date DATE;
	lv_last_update_date        DATE;
	lv_serial_number           VARCHAR2(100);
	lv_quantity                NUMBER := 0;
	lv_line_id                 NUMBER;
	lv_serial_number_child     VARCHAR2(100);
	lv_spr_number              VARCHAR2(100);
	lv_commit_count            NUMBER := 0;       ---- Added By Mohit on 25/02/2016  For Performance Tunning.
	L_CNT                      NUMBER;
    ln_qty_cnt                 NUMBER;
	l_non_serialized           VARCHAR2(2);
  l_check_nfr_order          VARCHAR2(1); -- ITS Ticket#540799

    ex_dml_errors EXCEPTION;
    PRAGMA EXCEPTION_INIT(ex_dml_errors, -24381);

  CURSOR cur_parent_lines
  IS
  SELECT oeh.order_number order_number,
		  oeh.header_id,
		  oel.line_id,
		  oel.ordered_quantity,
		  oel.last_update_date,
		  oel.cancelled_flag,
		  oel.flow_status_code,
		  oel.attribute12,
		  oel.attribute13,
		  msi.item_type,
		  oel.ordered_item
    FROM XXFT.XXFT_RPRO_ORDER_CONTROL xpoc,
         apps.oe_order_headers_all oeh,
		 apps.oe_transaction_types_tl ott,
		 apps.oe_order_lines_all oel,
		 apps.mtl_system_items_b msi,
		 apps.mtl_parameters mp
   WHERE xpoc.processed_flag ='R'
     AND xpoc.trx_type ='ORD'
     AND xpoc.Status IN('BOOKED','CLOSED','AWAITING_FULFILLMENT')
	 AND xpoc.line_id = oel.line_id
     AND oeh.order_number     = NVL(p_order_number,oeh.order_number)
	 AND oeh.header_id        = oel.header_id
	 AND oeh.order_type_id    = ott.transaction_type_id
	 AND oel.link_to_line_id IS NULL
	 AND oel.ordered_item NOT LIKE '%WARRANTY%'
     AND EXISTS
         (SELECT 1
            FROM apps.FND_LOOKUP_VALUES flv
           WHERE flv.language            = 'US'
             AND NVL(flv.enabled_flag,'N') = 'Y'
             AND sysdate BETWEEN NVL(flv.start_date_active,sysdate) AND NVL(flv.end_date_active,sysdate + 1)
             AND flv.lookup_type        = 'REVPRO_ORDER_TYPE'
             AND UPPER(flv.lookup_code) = UPPER(ott.name)
         )
	 AND msi.inventory_item_id = oel.inventory_item_id
	 AND mp.master_organization_id = mp.organization_id
	 AND msi.organization_id = mp.organization_id
	 AND NOT EXISTS ( SELECT 1
	                    FROM xxft_rpro_order_details
					   WHERE sales_order_line_id = xpoc.line_id
     );
    -- Get parent line details seperately.
	CURSOR cur_parent_line_details(p_line_id IN NUMBER)
	IS
	SELECT rpo.*
      FROM XXFT_RPRO_OE_ORDER_DETAILS_V rpo
     WHERE sales_order_line_id = p_line_id;
	 -- Get Child Lines for the
	CURSOR cur_revpro(c_parent_line_id IN NUMBER
				   ,c_header_id      IN NUMBER)
	IS
	SELECT rpo.*
	  FROM XXFT_RPRO_OE_ORDER_DETAILS_V rpo
	 WHERE sales_order_line_id = any (
		  SELECT oola.line_id
			FROM oe_order_lines_all oola
		   WHERE oola.TOP_MODEL_LINE_ID = c_parent_line_id
			 AND ordered_item NOT like '%WARRANTY%'
			 AND header_id = c_header_id
			 AND oola.TOP_MODEL_LINE_ID <> oola.line_id
		   UNION
		  SELECT oola.line_id
			FROM oe_order_lines_all oola
		   WHERE OOLA.SERVICE_REFERENCE_TYPE_CODE ='ORDER' /* ITS 550041 */
       and oola.service_reference_line_id = c_parent_line_id
			 AND ordered_item NOT like '%WARRANTY%'
			 AND header_id = c_header_id
		   UNION
		  SELECT oola.line_id
			FROM oe_order_lines_all oola
		   WHERE oola.link_to_line_id = c_parent_line_id
			 AND oola.ordered_item NOT like '%WARRANTY%'
			 AND oola.header_id = c_header_id
		    UNION
		  SELECT oola.line_id
			FROM oe_order_lines_all oola
		   WHERE OOLA.SERVICE_REFERENCE_TYPE_CODE ='ORDER' /* ITS 550041 */
       and oola.service_reference_line_id IN (SELECT line_id
			FROM oe_order_lines_all oola
		   WHERE oola.top_model_line_id= c_parent_line_id
			  AND header_id = c_header_id
		   UNION
		  SELECT oola.line_id
			FROM oe_order_lines_all oola
		   WHERE oola.link_to_line_id = c_parent_line_id
			 AND ordered_item NOT like '%WARRANTY%'
			 AND header_id = c_header_id
		   )
		   AND ordered_item NOT like '%WARRANTY%'
		   AND header_id = c_header_id
	 );

 BEGIN
    print_log('   + Inside Procedure Initial_Populate');
	print_info_log('  + Inside Procedure Initial_Populate:=>'||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));
	FOR parent_lines_rec IN cur_parent_lines
	  LOOP ----- Parent Lines loop
		IF lt_ord_dtls.COUNT > 0
		THEN
		   lt_ord_dtls.DELETE;
		END IF;
		print_log(' Checking Line Details Already Exits or Not');
		print_log(' parent line Id : '||parent_lines_rec.line_id);
		l_non_serialized := xxft_rpro_data_extract_pkg.check_non_serial_item(parent_lines_rec.header_id, parent_lines_rec.line_id);
		IF l_non_serialized ='Y'
		THEN
		   ln_qty_cnt :=1; -- Only One Batch will be sent to Revpro.
		ELSE
		   ln_qty_cnt := parent_lines_rec.ordered_quantity;
		END IF;
		print_output('Inserting Parent Line Details For Quantity : '||parent_lines_rec.ordered_quantity);
		print_log('Inserting Parent Line Details For Quantity : '||parent_lines_rec.ordered_quantity);
		IF lt_ch_ln_tbl1.COUNT > 0
		THEN
		   lt_ch_ln_tbl1.DELETE;
		END IF;
		IF lt_tp_ln_dtl.COUNT>0
		THEN
		   lt_tp_ln_dtl.DELETE;
		END IF;
		--
		-- Get the Parent Line details first.
		--
		OPEN cur_parent_line_details(parent_lines_rec.line_id);
		FETCH cur_parent_line_details BULK COLLECT INTO lt_tp_ln_dtl;
		CLOSE cur_parent_line_details;
		print_log('Number of records before collect lt_ch_ln_tbl1: '||lt_ch_ln_tbl1.COUNT);
		-- Fetch all the line details once.
		OPEN cur_revpro(parent_lines_rec.line_id,parent_lines_rec.header_id);
		FETCH cur_revpro BULK COLLECT INTO lt_ch_ln_tbl1;
		CLOSE cur_revpro;
		print_log('Number of records captured after bulk collect lt_ch_ln_tbl1: '||lt_ch_ln_tbl1.COUNT||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));
		lv_commit_count := 0;
		FOR j IN 1.. ln_qty_cnt
		LOOP --For quantity
		 lv_batch_id := XXFT_RPRO_LINE_BATCH_ID_S.NEXTVAL;
		 /*SELECT XXFT_RPRO_LINE_BATCH_ID_S.NEXTVAL
		   INTO lv_batch_id FROM dual; */
		 lv_commit_count :=lv_commit_count+1;
		 --
		 -- Insert the Parent level record.
		 --
		 FOR h_indx IN 1..lt_tp_ln_dtl.COUNT
		 LOOP
			l_cnt := lt_ord_dtls.COUNT+1;
        --ITS# - NFR Orders Check
      l_check_nfr_order := CHECK_NFR_ITEM(lt_tp_ln_dtl(h_indx).ITEM_ID);-- ITS Ticket#540799 
      
			lt_ord_dtls(l_cnt).TRAN_TYPE          := lt_tp_ln_dtl(h_indx).TRAN_TYPE;
			lt_ord_dtls(l_cnt).ITEM_ID            := lt_tp_ln_dtl(h_indx).ITEM_ID;
			lt_ord_dtls(l_cnt).ITEM_NUMBER        := lt_tp_ln_dtl(h_indx).ITEM_NUMBER;
			lt_ord_dtls(l_cnt).ITEM_DESC          := lt_tp_ln_dtl(h_indx).ITEM_DESC;
			lt_ord_dtls(l_cnt).PRODUCT_FAMILY     := lt_tp_ln_dtl(h_indx).PRODUCT_FAMILY;
			lt_ord_dtls(l_cnt).PRODUCT_CATEGORY   := lt_tp_ln_dtl(h_indx).PRODUCT_CATEGORY;
			lt_ord_dtls(l_cnt).PRODUCT_LINE       := lt_tp_ln_dtl(h_indx).PRODUCT_LINE;
			lt_ord_dtls(l_cnt).PRODUCT_CLASS      := lt_tp_ln_dtl(h_indx).PRODUCT_CLASS;
			lt_ord_dtls(l_cnt).PRICE_LIST_NAME    := lt_tp_ln_dtl(h_indx).PRICE_LIST_NAME;
			lt_ord_dtls(l_cnt).UNIT_LIST_PRICE    := lt_tp_ln_dtl(h_indx).UNIT_LIST_PRICE;
			lt_ord_dtls(l_cnt).UNIT_SELL_PRICE    := lt_tp_ln_dtl(h_indx).UNIT_SELL_PRICE;
			IF l_non_serialized = 'Y'
			THEN
         lt_ord_dtls(l_cnt).EXT_SELL_PRICE     := round(lt_tp_ln_dtl(h_indx).EXT_SELL_PRICE*parent_lines_rec.ordered_quantity,2);
         IF l_CHECK_NFR_ORDER ='Y' --ITS#540799
         THEN
         lt_ord_dtls(l_cnt).EXT_LIST_PRICE     := NULL;
         ELSE
         lt_ord_dtls(l_cnt).EXT_LIST_PRICE     := round(lt_tp_ln_dtl(h_indx).UNIT_LIST_PRICE*parent_lines_rec.ordered_quantity,2);
         END IF;			   
			   lt_ord_dtls(l_cnt).QUANTITY_ORDERED   := parent_lines_rec.ordered_quantity;
			ELSE
			   lt_ord_dtls(l_cnt).EXT_SELL_PRICE     := lt_tp_ln_dtl(h_indx).EXT_SELL_PRICE;
			   IF l_CHECK_NFR_ORDER = 'Y' --ITS#540799
         THEN
           lt_ord_dtls(l_cnt).EXT_LIST_PRICE     := NULL;
         ELSE
            lt_ord_dtls(l_cnt).EXT_LIST_PRICE     := lt_tp_ln_dtl(h_indx).UNIT_LIST_PRICE;
         END IF;
         
--         lt_ord_dtls(l_cnt).EXT_LIST_PRICE     := lt_tp_ln_dtl(h_indx).UNIT_LIST_PRICE;
         
         lt_ord_dtls(l_cnt).QUANTITY_ORDERED             :=1;
			END IF;

		    lt_ord_dtls(l_cnt).REC_AMT            := lt_tp_ln_dtl(h_indx).REC_AMT;
			lt_ord_dtls(l_cnt).DEF_AMT            := lt_tp_ln_dtl(h_indx).DEF_AMT;
		    lt_ord_dtls(l_cnt).COST_AMOUNT         := lt_tp_ln_dtl(h_indx).COST_AMOUNT;
		    lt_ord_dtls(l_cnt).COST_REC_AMT        := lt_tp_ln_dtl(h_indx).COST_REC_AMT;
		    lt_ord_dtls(l_cnt).COST_DEF_AMT        := lt_tp_ln_dtl(h_indx).COST_DEF_AMT;
		   lt_ord_dtls(l_cnt).TRANS_CURR_CODE     := lt_tp_ln_dtl(h_indx).TRANS_CURR_CODE;
		   lt_ord_dtls(l_cnt).EX_RATE             := lt_tp_ln_dtl(h_indx).EX_RATE;
		   lt_ord_dtls(l_cnt).BASE_CURR_CODE      := lt_tp_ln_dtl(h_indx).BASE_CURR_CODE;
		   lt_ord_dtls(l_cnt).COST_CURR_CODE      := lt_tp_ln_dtl(h_indx).COST_CURR_CODE;
		   lt_ord_dtls(l_cnt).COST_EX_RATE        := lt_tp_ln_dtl(h_indx).COST_EX_RATE;
		   lt_ord_dtls(l_cnt).RCURR_EX_RATE       := lt_tp_ln_dtl(h_indx).RCURR_EX_RATE;
		   lt_ord_dtls(l_cnt).ACCOUNTING_PERIOD   := lt_tp_ln_dtl(h_indx).ACCOUNTING_PERIOD;
		   lt_ord_dtls(l_cnt).ACCOUNTING_RULE     := lt_tp_ln_dtl(h_indx).ACCOUNTING_RULE;
		   lt_ord_dtls(l_cnt).RULE_START_DATE     := lt_tp_ln_dtl(h_indx).RULE_START_DATE;
		   lt_ord_dtls(l_cnt).RULE_END_DATE       := lt_tp_ln_dtl(h_indx).RULE_END_DATE;
		   lt_ord_dtls(l_cnt).VAR_RULE_ID         := lt_tp_ln_dtl(h_indx).VAR_RULE_ID;
		   lt_ord_dtls(l_cnt).PO_NUM              := lt_tp_ln_dtl(h_indx).PO_NUM;
		   lt_ord_dtls(l_cnt).QUOTE_NUM           := lt_tp_ln_dtl(h_indx).QUOTE_NUM;
		   lt_ord_dtls(l_cnt).SALES_ORDER         := lt_tp_ln_dtl(h_indx).SALES_ORDER;
		   lt_ord_dtls(l_cnt).SALES_ORDER_LINE    := lt_tp_ln_dtl(h_indx).SALES_ORDER_LINE;
		   lt_ord_dtls(l_cnt).SALES_ORDER_ID      := lt_tp_ln_dtl(h_indx).SALES_ORDER_ID;
		   lt_ord_dtls(l_cnt).SALES_ORDER_LINE_ID := lt_tp_ln_dtl(h_indx).SALES_ORDER_LINE_ID;
		 --  lt_ord_dtls(l_cnt).SALES_ORDER_NEW_LINE_ID      :=XXFT_RPRO_NEW_LINE_ID_S.NEXTVAL ;
		   SELECT XXFT_RPRO_NEW_LINE_ID_S.NEXTVAL
		   INTO lt_ord_dtls(l_cnt).SALES_ORDER_NEW_LINE_ID
		   FROM DUAL;
		   lt_ord_dtls(l_cnt).SALES_ORDER_LINE_BATCH_ID    :=lv_batch_id;
		   lt_ord_dtls(l_cnt).SHIP_DATE                    :=lt_tp_ln_dtl(h_indx).SHIP_DATE;
		   lt_ord_dtls(l_cnt).SO_BOOK_DATE                 :=lt_tp_ln_dtl(h_indx).SO_BOOK_DATE;
		   lt_ord_dtls(l_cnt).TRANS_DATE                   :=lt_tp_ln_dtl(h_indx).TRANS_DATE;
		   lt_ord_dtls(l_cnt).SCHEDULE_SHIP_DATE           :=lt_tp_ln_dtl(h_indx).SCHEDULE_SHIP_DATE;
		   --  lt_ord_dtls(l_cnt).--QUANTITY_SHIPPED :=QUANTITY_SHIPPED;

		   lt_ord_dtls(l_cnt).QUANTITY_CANCELED            :=lt_tp_ln_dtl(h_indx).QUANTITY_CANCELED;
		   lt_ord_dtls(l_cnt).SALESREP_NAME                :=lt_tp_ln_dtl(h_indx).SALESREP_NAME;
		   lt_ord_dtls(l_cnt).SALES_REP_ID                 :=lt_tp_ln_dtl(h_indx).SALES_REP_ID;
		   lt_ord_dtls(l_cnt).ORDER_TYPE                   :=lt_tp_ln_dtl(h_indx).ORDER_TYPE;
		   lt_ord_dtls(l_cnt).ORDER_LINE_TYPE              :=lt_tp_ln_dtl(h_indx).ORDER_LINE_TYPE;
		   lt_ord_dtls(l_cnt).SERVICE_REFERENCE_LINE_ID    :=lt_tp_ln_dtl(h_indx).SERVICE_REFERENCE_LINE_ID;
		   lt_ord_dtls(l_cnt).CUSTOMER_ID                  :=lt_tp_ln_dtl(h_indx).CUSTOMER_ID;
		   lt_ord_dtls(l_cnt).CUSTOMER_NAME                :=lt_tp_ln_dtl(h_indx).CUSTOMER_NAME;
		   lt_ord_dtls(l_cnt).CUSTOMER_CLASS               :=lt_tp_ln_dtl(h_indx).CUSTOMER_CLASS;
		   lt_ord_dtls(l_cnt).BILL_TO_ID                   :=lt_tp_ln_dtl(h_indx).BILL_TO_ID;
		   lt_ord_dtls(l_cnt).BILL_TO_CUSTOMER_NAME        :=lt_tp_ln_dtl(h_indx).BILL_TO_CUSTOMER_NAME;
		   lt_ord_dtls(l_cnt).BILL_TO_CUSTOMER_NUMBER      :=lt_tp_ln_dtl(h_indx).BILL_TO_CUSTOMER_NUMBER;
		   lt_ord_dtls(l_cnt).BILL_TO_COUNTRY              :=lt_tp_ln_dtl(h_indx).BILL_TO_COUNTRY;
		   lt_ord_dtls(l_cnt).SHIP_TO_ID                   :=lt_tp_ln_dtl(h_indx).SHIP_TO_ID;
		   lt_ord_dtls(l_cnt).SHIP_TO_CUSTOMER_NAME        :=lt_tp_ln_dtl(h_indx).SHIP_TO_CUSTOMER_NAME;
		   lt_ord_dtls(l_cnt).SHIP_TO_CUSTOMER_NUMBER      :=lt_tp_ln_dtl(h_indx).SHIP_TO_CUSTOMER_NUMBER;
		   lt_ord_dtls(l_cnt).SHIP_TO_COUNTRY              :=lt_tp_ln_dtl(h_indx).SHIP_TO_COUNTRY;
		   lt_ord_dtls(l_cnt).BUSINESS_UNIT                :=lt_tp_ln_dtl(h_indx).BUSINESS_UNIT;
		   lt_ord_dtls(l_cnt).ORG_ID                       :=lt_tp_ln_dtl(h_indx).ORG_ID;
		   lt_ord_dtls(l_cnt).SOB_ID                       :=lt_tp_ln_dtl(h_indx).SOB_ID;
		   lt_ord_dtls(l_cnt).SEC_ATTR_VALUE               :=lt_tp_ln_dtl(h_indx).SEC_ATTR_VALUE;
		   lt_ord_dtls(l_cnt).RETURN_FLAG                  :=lt_tp_ln_dtl(h_indx).RETURN_FLAG;
		   lt_ord_dtls(l_cnt).CANCELLED_FLAG               :=lt_tp_ln_dtl(h_indx).CANCELLED_FLAG;
		   --lt_ord_dtls(l_cnt).FLAG_97_2 := DECODE(lt_tp_ln_dtl(h_indx).FLAG_97_2,'BESP','N','Y');
		   IF lt_tp_ln_dtl(h_indx).FLAG_97_2 ='BESP'
		   THEN
			  lt_ord_dtls(l_cnt).FLAG_97_2 := 'N';
		   ELSE
			  lt_ord_dtls(l_cnt).FLAG_97_2 := 'Y';
		   END IF;
		   lt_ord_dtls(l_cnt).PCS_FLAG                    :=lt_tp_ln_dtl(h_indx).PCS_FLAG;
		   lt_ord_dtls(l_cnt).UNDELIVERED_FLAG            :=lt_tp_ln_dtl(h_indx).UNDELIVERED_FLAG;
		   lt_ord_dtls(l_cnt).STATED_FLAG                 :=lt_tp_ln_dtl(h_indx).STATED_FLAG;
		   --lt_ord_dtls(l_cnt).ELIGIBLE_FOR_CV :=DECODE(lt_tp_ln_dtl(h_indx).PRODUCT_LINE,'NON-ALLOCATED ITEMS','N',lt_tp_ln_dtl(h_indx).ELIGIBLE_FOR_CV);
		   IF lt_tp_ln_dtl(h_indx).PRODUCT_LINE = 'NON-ALLOCATED ITEMS'
		   THEN
			  lt_ord_dtls(l_cnt).ELIGIBLE_FOR_CV := 'N';
		   ELSE
			 lt_ord_dtls(l_cnt).ELIGIBLE_FOR_CV := lt_tp_ln_dtl(h_indx).ELIGIBLE_FOR_CV;
		   END IF;
		   lt_ord_dtls(l_cnt).ELIGIBLE_FOR_FV           :=lt_tp_ln_dtl(h_indx).ELIGIBLE_FOR_FV;
		   lt_ord_dtls(l_cnt).DEFERRED_REVENUE_FLAG     :=lt_tp_ln_dtl(h_indx).DEFERRED_REVENUE_FLAG;
		   lt_ord_dtls(l_cnt).NON_CONTINGENT_FLAG       :=lt_tp_ln_dtl(h_indx).NON_CONTINGENT_FLAG;
		   lt_ord_dtls(l_cnt).UNBILLED_ACCOUNTING_FLAG  :=lt_tp_ln_dtl(h_indx).UNBILLED_ACCOUNTING_FLAG;
		   lt_ord_dtls(l_cnt).DEAL_ID                   :=lt_tp_ln_dtl(h_indx).DEAL_ID;
		   lt_ord_dtls(l_cnt).LAG_DAYS                  :=lt_tp_ln_dtl(h_indx).LAG_DAYS;
		   lt_ord_dtls(l_cnt).ATTRIBUTE1                :=lt_tp_ln_dtl(h_indx).ATTRIBUTE1;
		   lt_ord_dtls(l_cnt).ATTRIBUTE2                :=lt_tp_ln_dtl(h_indx).ATTRIBUTE2;
		   lt_ord_dtls(l_cnt).ATTRIBUTE3                :=lt_tp_ln_dtl(h_indx).ATTRIBUTE3;
		   lt_ord_dtls(l_cnt).ATTRIBUTE4                :=lt_tp_ln_dtl(h_indx).ATTRIBUTE4;
		   lt_ord_dtls(l_cnt).ATTRIBUTE5                :=lt_tp_ln_dtl(h_indx).ATTRIBUTE5;
		   lt_ord_dtls(l_cnt).ATTRIBUTE6                :=lt_tp_ln_dtl(h_indx).ATTRIBUTE6;
		   lt_ord_dtls(l_cnt).ATTRIBUTE7                :=lt_tp_ln_dtl(h_indx).ATTRIBUTE7;
		   lt_ord_dtls(l_cnt).ATTRIBUTE8                :=lt_tp_ln_dtl(h_indx).ATTRIBUTE8;
		   lt_ord_dtls(l_cnt).ATTRIBUTE9                :=lt_tp_ln_dtl(h_indx).ATTRIBUTE9;
		   lt_ord_dtls(l_cnt).ATTRIBUTE10               :=lt_tp_ln_dtl(h_indx).ATTRIBUTE10;
		   lt_ord_dtls(l_cnt).ATTRIBUTE11               :=lt_tp_ln_dtl(h_indx).ATTRIBUTE11;

		   lt_ord_dtls(l_cnt).ATTRIBUTE12               :=lt_tp_ln_dtl(h_indx).ATTRIBUTE12;
		   lt_ord_dtls(l_cnt).ATTRIBUTE13               :=lt_tp_ln_dtl(h_indx).ATTRIBUTE13;
		   lt_ord_dtls(l_cnt).ATTRIBUTE14               :=lt_tp_ln_dtl(h_indx).ATTRIBUTE14;
		   lt_ord_dtls(l_cnt).ATTRIBUTE15               :=lt_tp_ln_dtl(h_indx).ATTRIBUTE15;
		   lt_ord_dtls(l_cnt).ATTRIBUTE16               :=lt_tp_ln_dtl(h_indx).ATTRIBUTE16;
		   lt_ord_dtls(l_cnt).ATTRIBUTE17               :=lt_tp_ln_dtl(h_indx).ATTRIBUTE17;
		   lt_ord_dtls(l_cnt).ATTRIBUTE18               :=lt_tp_ln_dtl(h_indx).ATTRIBUTE18;
		   lt_ord_dtls(l_cnt).ATTRIBUTE19               :=lt_tp_ln_dtl(h_indx).ATTRIBUTE19;
		   lt_ord_dtls(l_cnt).ATTRIBUTE20               :=lt_tp_ln_dtl(h_indx).ATTRIBUTE20;
		   lt_ord_dtls(l_cnt).ATTRIBUTE21               := lt_tp_ln_dtl(h_indx).ATTRIBUTE21 ;
		   lt_ord_dtls(l_cnt).ATTRIBUTE22               :=lt_tp_ln_dtl(h_indx).ATTRIBUTE22;
		   lt_ord_dtls(l_cnt).ATTRIBUTE23               :=lt_tp_ln_dtl(h_indx).ATTRIBUTE23;
		   --l_CHECK_NFR_ORDER  ITS #540799 
       IF l_CHECK_NFR_ORDER ='Y'
       THEN
       lt_ord_dtls(l_cnt).ATTRIBUTE24               :=NULL;
       ELSE
       lt_ord_dtls(l_cnt).ATTRIBUTE24               :=lt_tp_ln_dtl(h_indx).ATTRIBUTE24;
       END IF;
--       lt_ord_dtls(l_cnt).ATTRIBUTE24               :=lt_tp_ln_dtl(h_indx).ATTRIBUTE24;
       
		   lt_ord_dtls(l_cnt).ATTRIBUTE25               :=lt_tp_ln_dtl(h_indx).ATTRIBUTE25;
		   lt_ord_dtls(l_cnt).ATTRIBUTE26               :=lt_tp_ln_dtl(h_indx).ATTRIBUTE26;
		   lt_ord_dtls(l_cnt).ATTRIBUTE27               :=lt_tp_ln_dtl(h_indx).ATTRIBUTE27;
		   lt_ord_dtls(l_cnt).ATTRIBUTE28               :=lt_tp_ln_dtl(h_indx).ATTRIBUTE28;
		   lt_ord_dtls(l_cnt).ATTRIBUTE29               :=lt_tp_ln_dtl(h_indx).ATTRIBUTE29;
		   lt_ord_dtls(l_cnt).ATTRIBUTE30               :=lt_tp_ln_dtl(h_indx).ATTRIBUTE30;
		   lt_ord_dtls(l_cnt).ATTRIBUTE31               :=lt_tp_ln_dtl(h_indx).ATTRIBUTE31;
		   lt_ord_dtls(l_cnt).ATTRIBUTE32               :=lt_tp_ln_dtl(h_indx).ATTRIBUTE32;
		   lt_ord_dtls(l_cnt).ATTRIBUTE33               :=lt_tp_ln_dtl(h_indx).ATTRIBUTE33;
		   lt_ord_dtls(l_cnt).ATTRIBUTE34               :=lt_tp_ln_dtl(h_indx).ATTRIBUTE34;
		   lt_ord_dtls(l_cnt).ATTRIBUTE35               :=lt_tp_ln_dtl(h_indx).ATTRIBUTE35;
		  -- lt_ord_dtls(l_cnt).ATTRIBUTE36 := DECODE(lt_tp_ln_dtl(h_indx).ATTRIBUTE44,'E_SUPPORT',XXFT_RPRO_DATA_EXTRACT_PKG.implied_pcs_days(lt_tp_ln_dtl(h_indx).ATTRIBUTE37,lt_tp_ln_dtl(h_indx).bill_to_country,'LAG'),null);
		   IF lt_tp_ln_dtl(h_indx).ATTRIBUTE44 ='E_SUPPORT'
		   THEN
			  lt_ord_dtls(l_cnt).ATTRIBUTE36 := XXFT_RPRO_DATA_EXTRACT_PKG.implied_pcs_days(
														lt_tp_ln_dtl(h_indx).ATTRIBUTE37
														,lt_tp_ln_dtl(h_indx).bill_to_country
														,'LAG');
		   ELSE
			  lt_ord_dtls(l_cnt).ATTRIBUTE36  := NULL;
		   END IF;
       --ITS#575976 --
       IF lt_tp_ln_dtl(h_indx).ATTRIBUTE47 IN ('DROP SHIP','COTERM','AUTO REGISTERED')
       THEN       
          lt_ord_dtls(l_cnt).ATTRIBUTE37 :='N'; -- Ensure the Revpro excludes POS for drop ship, coterm and Renew with Quote Auto Registered.
       ELSE 
          lt_ord_dtls(l_cnt).ATTRIBUTE37 :=lt_tp_ln_dtl(h_indx).ATTRIBUTE37;
       END IF;
		   
		   lt_ord_dtls(l_cnt).ATTRIBUTE38 :='ERP';
		   lt_ord_dtls(l_cnt).ATTRIBUTE39 :=lt_tp_ln_dtl(h_indx).ATTRIBUTE39;
		   lt_ord_dtls(l_cnt).ATTRIBUTE40 :=lt_tp_ln_dtl(h_indx).ATTRIBUTE40;
		   lt_ord_dtls(l_cnt).ATTRIBUTE41 :=lt_tp_ln_dtl(h_indx).ATTRIBUTE41;
		   lt_ord_dtls(l_cnt).ATTRIBUTE42 :=NULL;
		   --lt_ord_dtls(l_cnt).ATTRIBUTE43 :=DECODE(upper(lt_tp_ln_dtl(h_indx).FLAG_97_2),'BESP OR VSOE','Y','N') --lcu_rpro.ATTRIBUTE43;
		   IF upper(lt_tp_ln_dtl(h_indx).FLAG_97_2) = 'BESP OR VSOE'
		   THEN
			  lt_ord_dtls(l_cnt).ATTRIBUTE43  := 'Y';
		   ELSE
			  lt_ord_dtls(l_cnt).ATTRIBUTE43  := 'N';
		   END IF;
		   lt_ord_dtls(l_cnt).ATTRIBUTE44 :=lt_tp_ln_dtl(h_indx).ATTRIBUTE44;
		   lt_ord_dtls(l_cnt).ATTRIBUTE45 :=lt_tp_ln_dtl(h_indx).ATTRIBUTE45;
		   lt_ord_dtls(l_cnt).ATTRIBUTE46 :=lt_tp_ln_dtl(h_indx).PRODUCT_FAMILY;  -- Attribute46 is updated as Product Family.
		   lt_ord_dtls(l_cnt).ATTRIBUTE47 :=lt_tp_ln_dtl(h_indx).ATTRIBUTE47;
		   lt_ord_dtls(l_cnt).ATTRIBUTE48 :=lt_tp_ln_dtl(h_indx).ATTRIBUTE48;
		   lt_ord_dtls(l_cnt).ATTRIBUTE49 :=lt_tp_ln_dtl(h_indx).ATTRIBUTE49;
		   lt_ord_dtls(l_cnt).ATTRIBUTE50 :=lt_tp_ln_dtl(h_indx).ATTRIBUTE50;
		   lt_ord_dtls(l_cnt).ATTRIBUTE51 :=lt_tp_ln_dtl(h_indx).ATTRIBUTE51;
		   lt_ord_dtls(l_cnt).ATTRIBUTE52 :=lt_tp_ln_dtl(h_indx).ATTRIBUTE52;
		   lt_ord_dtls(l_cnt).ATTRIBUTE53 :=lt_tp_ln_dtl(h_indx).ATTRIBUTE53;
		   lt_ord_dtls(l_cnt).ATTRIBUTE54 :=lt_tp_ln_dtl(h_indx).ATTRIBUTE54;
		   lt_ord_dtls(l_cnt).ATTRIBUTE55 :=lt_tp_ln_dtl(h_indx).ATTRIBUTE55;
		   lt_ord_dtls(l_cnt).ATTRIBUTE56 :=lt_tp_ln_dtl(h_indx).ATTRIBUTE56;
		   lt_ord_dtls(l_cnt).ATTRIBUTE57 :=lt_tp_ln_dtl(h_indx).ATTRIBUTE57;
		   lt_ord_dtls(l_cnt).ATTRIBUTE58 :=lt_tp_ln_dtl(h_indx).ATTRIBUTE58;
		   lt_ord_dtls(l_cnt).ATTRIBUTE59 :=lt_tp_ln_dtl(h_indx).ATTRIBUTE59;
		   lt_ord_dtls(l_cnt).ATTRIBUTE60 :=l_non_serialized;--lt_tp_ln_dtl(h_indx).ATTRIBUTE60;
		   lt_ord_dtls(l_cnt).DATE1 :=lt_tp_ln_dtl(h_indx).DATE1;
		   lt_ord_dtls(l_cnt).DATE2 :=lt_tp_ln_dtl(h_indx).DATE2;
		   lt_ord_dtls(l_cnt).DATE3 :=lt_tp_ln_dtl(h_indx).DATE3;
		   lt_ord_dtls(l_cnt).DATE4 :=lt_tp_ln_dtl(h_indx).DATE4;
		   lt_ord_dtls(l_cnt).DATE5 :=lt_tp_ln_dtl(h_indx).DATE5;
		   lt_ord_dtls(l_cnt).NUMBER1 :=lt_tp_ln_dtl(h_indx).NUMBER1;
		   lt_ord_dtls(l_cnt).NUMBER2 :=lt_tp_ln_dtl(h_indx).NUMBER2;
       --ITS#599807 ---
       IF lt_tp_ln_dtl(h_indx).ATTRIBUTE52 IN ('AUTO REGISTERED') 
       THEN
          lt_ord_dtls(l_cnt).NUMBER3 := 0;       
       ELSIF lt_tp_ln_dtl(h_indx).RULE_START_DATE is not null
       THEN
		   lt_ord_dtls(l_cnt).NUMBER3 := XXFT_RPRO_DATA_EXTRACT_PKG.get_grace_period(
													   lt_tp_ln_dtl(h_indx).ATTRIBUTE44
													  ,lt_tp_ln_dtl(h_indx).product_line
													  ,lt_tp_ln_dtl(h_indx).SALES_ORDER_LINE_ID);
       ELSE
          lt_ord_dtls(l_cnt).NUMBER3 := 0;
       END IF;

		   lt_ord_dtls(l_cnt).NUMBER4 :=lt_tp_ln_dtl(h_indx).NUMBER4;
		   IF lt_tp_ln_dtl(h_indx).ATTRIBUTE44 = 'E_SUPPORT'
		   THEN
			  lt_ord_dtls(l_cnt).NUMBER5 := XXFT_RPRO_DATA_EXTRACT_PKG.implied_pcs_days(
														 lt_tp_ln_dtl(h_indx).ATTRIBUTE37
														,lt_tp_ln_dtl(h_indx).bill_to_country
														,'IMP');
		   ELSE
			  lt_ord_dtls(l_cnt).NUMBER5 := NULL;
		   END IF;
		   lt_ord_dtls(l_cnt).NUMBER6 :=(lt_tp_ln_dtl(h_indx).RULE_END_DATE - lt_tp_ln_dtl(h_indx).RULE_START_DATE) +1 ; -- Added for defect#2953.
		   lt_ord_dtls(l_cnt).NUMBER7 :=lv_batch_id;
		   lt_ord_dtls(l_cnt).NUMBER8 :=lt_tp_ln_dtl(h_indx).NUMBER8;
		   lt_ord_dtls(l_cnt).NUMBER9 :=lt_tp_ln_dtl(h_indx).NUMBER9;
		   lt_ord_dtls(l_cnt).NUMBER10 :=lt_tp_ln_dtl(h_indx).NUMBER10;
		   lt_ord_dtls(l_cnt).NUMBER11 :=lt_tp_ln_dtl(h_indx).NUMBER11;
		   lt_ord_dtls(l_cnt).NUMBER12 :=lt_tp_ln_dtl(h_indx).NUMBER12;
		   lt_ord_dtls(l_cnt).NUMBER13 :=lt_tp_ln_dtl(h_indx).NUMBER13;
		   lt_ord_dtls(l_cnt).NUMBER14 :=lt_tp_ln_dtl(h_indx).NUMBER14;
		   lt_ord_dtls(l_cnt).NUMBER15 :=lt_tp_ln_dtl(h_indx).NUMBER15;
		   lt_ord_dtls(l_cnt).REV_ACCTG_SEG1 :=lt_tp_ln_dtl(h_indx).REV_ACCTG_SEG1;
		   lt_ord_dtls(l_cnt).REV_ACCTG_SEG2 :=lt_tp_ln_dtl(h_indx).REV_ACCTG_SEG2;
		   lt_ord_dtls(l_cnt).REV_ACCTG_SEG3 :=lt_tp_ln_dtl(h_indx).REV_ACCTG_SEG3;
		   lt_ord_dtls(l_cnt).REV_ACCTG_SEG4 :=lt_tp_ln_dtl(h_indx).REV_ACCTG_SEG4;
		   lt_ord_dtls(l_cnt).REV_ACCTG_SEG5 :=lt_tp_ln_dtl(h_indx).REV_ACCTG_SEG5;
		   lt_ord_dtls(l_cnt).REV_ACCTG_SEG6 :=lt_tp_ln_dtl(h_indx).REV_ACCTG_SEG6;
		   lt_ord_dtls(l_cnt).REV_ACCTG_SEG7 :=lt_tp_ln_dtl(h_indx).REV_ACCTG_SEG7;
		   lt_ord_dtls(l_cnt).REV_ACCTG_SEG8 :=lt_tp_ln_dtl(h_indx).REV_ACCTG_SEG8;
		   lt_ord_dtls(l_cnt).REV_ACCTG_SEG9 :=lt_tp_ln_dtl(h_indx).REV_ACCTG_SEG9;
		   lt_ord_dtls(l_cnt).REV_ACCTG_SEG10 :=lt_tp_ln_dtl(h_indx).REV_ACCTG_SEG10;
		   lt_ord_dtls(l_cnt).DEF_ACCTG_SEG1 :=lt_tp_ln_dtl(h_indx).DEF_ACCTG_SEG1;
		   lt_ord_dtls(l_cnt).DEF_ACCTG_SEG2 :=lt_tp_ln_dtl(h_indx).DEF_ACCTG_SEG2;
		   lt_ord_dtls(l_cnt).DEF_ACCTG_SEG3 :=lt_tp_ln_dtl(h_indx).DEF_ACCTG_SEG3;
		   lt_ord_dtls(l_cnt).DEF_ACCTG_SEG4 :=lt_tp_ln_dtl(h_indx).DEF_ACCTG_SEG4;
		   lt_ord_dtls(l_cnt).DEF_ACCTG_SEG5 :=lt_tp_ln_dtl(h_indx).DEF_ACCTG_SEG5;
		   lt_ord_dtls(l_cnt).DEF_ACCTG_SEG6 :=lt_tp_ln_dtl(h_indx).DEF_ACCTG_SEG6;
		   lt_ord_dtls(l_cnt).DEF_ACCTG_SEG7 :=lt_tp_ln_dtl(h_indx).DEF_ACCTG_SEG7;
		   lt_ord_dtls(l_cnt).DEF_ACCTG_SEG8 :=lt_tp_ln_dtl(h_indx).DEF_ACCTG_SEG8;
		   lt_ord_dtls(l_cnt).DEF_ACCTG_SEG9 :=lt_tp_ln_dtl(h_indx).DEF_ACCTG_SEG9;
		   lt_ord_dtls(l_cnt).DEF_ACCTG_SEG10 :=lt_tp_ln_dtl(h_indx).DEF_ACCTG_SEG10;
		   lt_ord_dtls(l_cnt).COGS_R_SEG1 :=lt_tp_ln_dtl(h_indx).COGS_R_SEG1;
		   lt_ord_dtls(l_cnt).COGS_R_SEG2 :=lt_tp_ln_dtl(h_indx).COGS_R_SEG2;
		   lt_ord_dtls(l_cnt).COGS_R_SEG3 :=lt_tp_ln_dtl(h_indx).COGS_R_SEG3;
		   lt_ord_dtls(l_cnt).COGS_R_SEG4 :=lt_tp_ln_dtl(h_indx).COGS_R_SEG4;
		   lt_ord_dtls(l_cnt).COGS_R_SEG5 :=lt_tp_ln_dtl(h_indx).COGS_R_SEG5;
		   lt_ord_dtls(l_cnt).COGS_R_SEG6 :=lt_tp_ln_dtl(h_indx).COGS_R_SEG6;
		   lt_ord_dtls(l_cnt).COGS_R_SEG7 :=lt_tp_ln_dtl(h_indx).COGS_R_SEG7;
		   lt_ord_dtls(l_cnt).COGS_R_SEG8 :=lt_tp_ln_dtl(h_indx).COGS_R_SEG8;
		   lt_ord_dtls(l_cnt).COGS_R_SEG9 :=lt_tp_ln_dtl(h_indx).COGS_R_SEG9;
		   lt_ord_dtls(l_cnt).COGS_R_SEG10 :=lt_tp_ln_dtl(h_indx).COGS_R_SEG10;
		   lt_ord_dtls(l_cnt).COGS_D_SEG1 :=lt_tp_ln_dtl(h_indx).COGS_D_SEG1;
		   lt_ord_dtls(l_cnt).COGS_D_SEG2 :=lt_tp_ln_dtl(h_indx).COGS_D_SEG2;
		   lt_ord_dtls(l_cnt).COGS_D_SEG3 :=lt_tp_ln_dtl(h_indx).COGS_D_SEG3;
		   lt_ord_dtls(l_cnt).COGS_D_SEG4 :=lt_tp_ln_dtl(h_indx).COGS_D_SEG4;
		   lt_ord_dtls(l_cnt).COGS_D_SEG5 :=lt_tp_ln_dtl(h_indx).COGS_D_SEG5;
		   lt_ord_dtls(l_cnt).COGS_D_SEG6 :=lt_tp_ln_dtl(h_indx).COGS_D_SEG6;
		   lt_ord_dtls(l_cnt).COGS_D_SEG7 :=lt_tp_ln_dtl(h_indx).COGS_D_SEG7;
		   lt_ord_dtls(l_cnt).COGS_D_SEG8 :=lt_tp_ln_dtl(h_indx).COGS_D_SEG8;
		   lt_ord_dtls(l_cnt).COGS_D_SEG9 :=lt_tp_ln_dtl(h_indx).COGS_D_SEG9;
		   lt_ord_dtls(l_cnt).COGS_D_SEG10 :=lt_tp_ln_dtl(h_indx).COGS_D_SEG10;
		   lt_ord_dtls(l_cnt).LT_DEFERRED_ACCOUNT :=lt_tp_ln_dtl(h_indx).LT_DEFERRED_ACCOUNT;
		   lt_ord_dtls(l_cnt).LT_DCOGS_ACCOUNT :=lt_tp_ln_dtl(h_indx).LT_DCOGS_ACCOUNT;
		   lt_ord_dtls(l_cnt).REV_DIST_ID :=NULL;
		   lt_ord_dtls(l_cnt).COST_DIST_ID :=NULL;
		   lt_ord_dtls(l_cnt).BOOK_ID :=lt_tp_ln_dtl(h_indx).BOOK_ID;
		   lt_ord_dtls(l_cnt).BNDL_CONFIG_ID :=lt_tp_ln_dtl(h_indx).BNDL_CONFIG_ID;
		   lt_ord_dtls(l_cnt).so_last_update_date :=lt_tp_ln_dtl(h_indx).so_last_update_date;
		   lt_ord_dtls(l_cnt).so_line_creation_date :=lt_tp_ln_dtl(h_indx).so_line_creation_date;
		   lt_ord_dtls(l_cnt).PROCESSED_FLAG :='N';
		   lt_ord_dtls(l_cnt).ERROR_MESSAGE:= NULL;
	--          lt_ord_dtls(l_cnt).--,CREATED_B:=;
		   lt_ord_dtls(l_cnt).CREATION_DATE :=SYSDATE;
		   lt_ord_dtls(l_cnt).LAST_UPDATE_DATE :=SYSDATE;
		   lt_ord_dtls(l_cnt).PARENT_LINE :='Y';
		   lt_ord_dtls(l_cnt).order_line_status :=parent_lines_rec.flow_status_code;
		   lt_ord_dtls(l_cnt).conc_request_id:=gn_request_id;
	--		   lt_ord_dtls(l_cnt).processing_phase := 'BOOKED';
		   -- capture the SPR Number for Child Lines.
		   lv_spr_number := lt_tp_ln_dtl(h_indx).ATTRIBUTE34;
		 END LOOP;
		 IF parent_lines_rec.ordered_item LIKE '%FTK%' AND  parent_lines_rec.item_type IN ('PTO')
		 THEN
			-- Do Not Extract Lines for the Forti Token Bundle.
			print_log('   --Do not Insert lines for Fortitoken.');
		 ELSE
			-- Insert into the table.
			FOR i IN 1..lt_ch_ln_tbl1.COUNT
			LOOP
        l_cnt                               := lt_ord_dtls.COUNT+1;
        lt_ord_dtls(l_cnt).TRAN_TYPE        := lt_ch_ln_tbl1(i).TRAN_TYPE;
        lt_ord_dtls(l_cnt).ITEM_ID          := lt_ch_ln_tbl1(i).ITEM_ID;
        lt_ord_dtls(l_cnt).ITEM_NUMBER      :=lt_ch_ln_tbl1(i).ITEM_NUMBER;
        lt_ord_dtls(l_cnt).ITEM_DESC        :=lt_ch_ln_tbl1(i).ITEM_DESC;
        lt_ord_dtls(l_cnt).PRODUCT_FAMILY   :=lt_ch_ln_tbl1(i).PRODUCT_FAMILY;
        lt_ord_dtls(l_cnt).PRODUCT_CATEGORY :=lt_ch_ln_tbl1(i).PRODUCT_CATEGORY;
        lt_ord_dtls(l_cnt).PRODUCT_LINE     :=lt_ch_ln_tbl1(i).PRODUCT_LINE;
        lt_ord_dtls(l_cnt).PRODUCT_CLASS    :=lt_ch_ln_tbl1(i).PRODUCT_CLASS;
        lt_ord_dtls(l_cnt).PRICE_LIST_NAME  :=lt_ch_ln_tbl1(i).PRICE_LIST_NAME;
        lt_ord_dtls(l_cnt).UNIT_LIST_PRICE  :=lt_ch_ln_tbl1(i).UNIT_LIST_PRICE;
        lt_ord_dtls(l_cnt).UNIT_SELL_PRICE  :=lt_ch_ln_tbl1(i).UNIT_SELL_PRICE;
			   IF l_non_serialized = 'Y'
			   THEN
			      lt_ord_dtls(l_cnt).EXT_SELL_PRICE     := round(lt_ch_ln_tbl1(i).EXT_SELL_PRICE*parent_lines_rec.ordered_quantity,2);
			      lt_ord_dtls(l_cnt).EXT_LIST_PRICE     := round(lt_ch_ln_tbl1(i).UNIT_LIST_PRICE*parent_lines_rec.ordered_quantity,2);
			      lt_ord_dtls(l_cnt).QUANTITY_ORDERED   := parent_lines_rec.ordered_quantity;
			   ELSE
          
			      lt_ord_dtls(l_cnt).EXT_SELL_PRICE     := lt_ch_ln_tbl1(i).EXT_SELL_PRICE;
			      lt_ord_dtls(l_cnt).EXT_LIST_PRICE     := lt_ch_ln_tbl1(i).UNIT_LIST_PRICE;
		          lt_ord_dtls(l_cnt).QUANTITY_ORDERED             :=1;
			   END IF;
			 --  lt_ord_dtls(l_cnt).EXT_SELL_PRICE     :=lt_ch_ln_tbl1(i).EXT_SELL_PRICE;
			   --lt_ord_dtls(l_cnt).EXT_LIST_PRICE     :=lt_ch_ln_tbl1(i).UNIT_LIST_PRICE;
			   lt_ord_dtls(l_cnt).REC_AMT            :=lt_ch_ln_tbl1(i).REC_AMT;
			   lt_ord_dtls(l_cnt).DEF_AMT            := lt_ch_ln_tbl1(i).DEF_AMT;
			   lt_ord_dtls(l_cnt).COST_AMOUNT         := lt_ch_ln_tbl1(i).COST_AMOUNT;
			   lt_ord_dtls(l_cnt).COST_REC_AMT        := lt_ch_ln_tbl1(i).COST_REC_AMT;
				   lt_ord_dtls(l_cnt).COST_DEF_AMT        := lt_ch_ln_tbl1(i).COST_DEF_AMT;
				   lt_ord_dtls(l_cnt).TRANS_CURR_CODE     :=lt_ch_ln_tbl1(i).TRANS_CURR_CODE;
				   lt_ord_dtls(l_cnt).EX_RATE             :=lt_ch_ln_tbl1(i).EX_RATE;
				   lt_ord_dtls(l_cnt).BASE_CURR_CODE      :=lt_ch_ln_tbl1(i).BASE_CURR_CODE;
				   lt_ord_dtls(l_cnt).COST_CURR_CODE      :=lt_ch_ln_tbl1(i).COST_CURR_CODE;
				   lt_ord_dtls(l_cnt).COST_EX_RATE        :=lt_ch_ln_tbl1(i).COST_EX_RATE;
				   lt_ord_dtls(l_cnt).RCURR_EX_RATE       :=lt_ch_ln_tbl1(i).RCURR_EX_RATE;
				   lt_ord_dtls(l_cnt).ACCOUNTING_PERIOD   :=lt_ch_ln_tbl1(i).ACCOUNTING_PERIOD;
				   lt_ord_dtls(l_cnt).ACCOUNTING_RULE     :=lt_ch_ln_tbl1(i).ACCOUNTING_RULE;
				   lt_ord_dtls(l_cnt).RULE_START_DATE     :=lt_ch_ln_tbl1(i).RULE_START_DATE;
				   lt_ord_dtls(l_cnt).RULE_END_DATE       :=lt_ch_ln_tbl1(i).RULE_END_DATE;
				   lt_ord_dtls(l_cnt).VAR_RULE_ID         :=lt_ch_ln_tbl1(i).VAR_RULE_ID;
				   lt_ord_dtls(l_cnt).PO_NUM              :=lt_ch_ln_tbl1(i).PO_NUM;
				   lt_ord_dtls(l_cnt).QUOTE_NUM           :=lt_ch_ln_tbl1(i).QUOTE_NUM;
				   lt_ord_dtls(l_cnt).SALES_ORDER         :=lt_ch_ln_tbl1(i).SALES_ORDER;
				   lt_ord_dtls(l_cnt).SALES_ORDER_LINE    :=lt_ch_ln_tbl1(i).SALES_ORDER_LINE;
				   lt_ord_dtls(l_cnt).SALES_ORDER_ID      :=lt_ch_ln_tbl1(i).SALES_ORDER_ID;
				   lt_ord_dtls(l_cnt).SALES_ORDER_LINE_ID :=lt_ch_ln_tbl1(i).SALES_ORDER_LINE_ID;
				 --  lt_ord_dtls(l_cnt).SALES_ORDER_NEW_LINE_ID      :=XXFT_RPRO_NEW_LINE_ID_S.NEXTVAL ;
				   SELECT XXFT_RPRO_NEW_LINE_ID_S.NEXTVAL
				   INTO lt_ord_dtls(l_cnt).SALES_ORDER_NEW_LINE_ID
				   FROM DUAL;
				   lt_ord_dtls(l_cnt).SALES_ORDER_LINE_BATCH_ID    :=lv_batch_id;
				   lt_ord_dtls(l_cnt).SHIP_DATE                    :=lt_ch_ln_tbl1(i).SHIP_DATE;
				   lt_ord_dtls(l_cnt).SO_BOOK_DATE                 :=lt_ch_ln_tbl1(i).SO_BOOK_DATE;
				   lt_ord_dtls(l_cnt).TRANS_DATE                   :=lt_ch_ln_tbl1(i).TRANS_DATE;
				   lt_ord_dtls(l_cnt).SCHEDULE_SHIP_DATE           :=lt_ch_ln_tbl1(i).SCHEDULE_SHIP_DATE;
				   --  lt_ord_dtls(l_cnt).--QUANTITY_SHIPPED :=QUANTITY_SHIPPED;
				   --lt_ord_dtls(l_cnt).QUANTITY_ORDERED             :=1;
				   lt_ord_dtls(l_cnt).QUANTITY_CANCELED            :=lt_ch_ln_tbl1(i).QUANTITY_CANCELED;
				   lt_ord_dtls(l_cnt).SALESREP_NAME                :=lt_ch_ln_tbl1(i).SALESREP_NAME;
				   lt_ord_dtls(l_cnt).SALES_REP_ID                 :=lt_ch_ln_tbl1(i).SALES_REP_ID;
				   lt_ord_dtls(l_cnt).ORDER_TYPE                   :=lt_ch_ln_tbl1(i).ORDER_TYPE;
				   lt_ord_dtls(l_cnt).ORDER_LINE_TYPE              :=lt_ch_ln_tbl1(i).ORDER_LINE_TYPE;
				   lt_ord_dtls(l_cnt).SERVICE_REFERENCE_LINE_ID    :=lt_ch_ln_tbl1(i).SERVICE_REFERENCE_LINE_ID;
				   lt_ord_dtls(l_cnt).CUSTOMER_ID                  :=lt_ch_ln_tbl1(i).CUSTOMER_ID;
				   lt_ord_dtls(l_cnt).CUSTOMER_NAME                :=lt_ch_ln_tbl1(i).CUSTOMER_NAME;
				   lt_ord_dtls(l_cnt).CUSTOMER_CLASS               :=lt_ch_ln_tbl1(i).CUSTOMER_CLASS;
				   lt_ord_dtls(l_cnt).BILL_TO_ID                   :=lt_ch_ln_tbl1(i).BILL_TO_ID;
				   lt_ord_dtls(l_cnt).BILL_TO_CUSTOMER_NAME        :=lt_ch_ln_tbl1(i).BILL_TO_CUSTOMER_NAME;
				   lt_ord_dtls(l_cnt).BILL_TO_CUSTOMER_NUMBER      :=lt_ch_ln_tbl1(i).BILL_TO_CUSTOMER_NUMBER;
				   lt_ord_dtls(l_cnt).BILL_TO_COUNTRY              :=lt_ch_ln_tbl1(i).BILL_TO_COUNTRY;
				   lt_ord_dtls(l_cnt).SHIP_TO_ID                   :=lt_ch_ln_tbl1(i).SHIP_TO_ID;
				   lt_ord_dtls(l_cnt).SHIP_TO_CUSTOMER_NAME        :=lt_ch_ln_tbl1(i).SHIP_TO_CUSTOMER_NAME;
				   lt_ord_dtls(l_cnt).SHIP_TO_CUSTOMER_NUMBER      :=lt_ch_ln_tbl1(i).SHIP_TO_CUSTOMER_NUMBER;
				   lt_ord_dtls(l_cnt).SHIP_TO_COUNTRY              :=lt_ch_ln_tbl1(i).SHIP_TO_COUNTRY;
				   lt_ord_dtls(l_cnt).BUSINESS_UNIT                :=lt_ch_ln_tbl1(i).BUSINESS_UNIT;
				   lt_ord_dtls(l_cnt).ORG_ID                       :=lt_ch_ln_tbl1(i).ORG_ID;
				   lt_ord_dtls(l_cnt).SOB_ID                       :=lt_ch_ln_tbl1(i).SOB_ID;
				   lt_ord_dtls(l_cnt).SEC_ATTR_VALUE               :=lt_ch_ln_tbl1(i).SEC_ATTR_VALUE;
				   lt_ord_dtls(l_cnt).RETURN_FLAG                  :=lt_ch_ln_tbl1(i).RETURN_FLAG;
				   lt_ord_dtls(l_cnt).CANCELLED_FLAG               :=lt_ch_ln_tbl1(i).CANCELLED_FLAG;
				   --lt_ord_dtls(l_cnt).FLAG_97_2 := DECODE(lt_ch_ln_tbl1(i).FLAG_97_2,'BESP','N','Y');
				   IF lt_ch_ln_tbl1(i).FLAG_97_2 ='BESP'
				   THEN
					  lt_ord_dtls(l_cnt).FLAG_97_2 := 'N';
				   ELSE
					  lt_ord_dtls(l_cnt).FLAG_97_2 := 'Y';
				   END IF;
				   lt_ord_dtls(l_cnt).PCS_FLAG                    :=lt_ch_ln_tbl1(i).PCS_FLAG;
				   lt_ord_dtls(l_cnt).UNDELIVERED_FLAG            :=lt_ch_ln_tbl1(i).UNDELIVERED_FLAG;
				   lt_ord_dtls(l_cnt).STATED_FLAG                 :=lt_ch_ln_tbl1(i).STATED_FLAG;
				   --lt_ord_dtls(l_cnt).ELIGIBLE_FOR_CV :=DECODE(lt_ch_ln_tbl1(i).PRODUCT_LINE,'NON-ALLOCATED ITEMS','N',lt_ch_ln_tbl1(i).ELIGIBLE_FOR_CV);
				   IF lt_ch_ln_tbl1(i).PRODUCT_LINE = 'NON-ALLOCATED ITEMS'
				   THEN
					  lt_ord_dtls(l_cnt).ELIGIBLE_FOR_CV := 'N';
				   ELSE
					 lt_ord_dtls(l_cnt).ELIGIBLE_FOR_CV := lt_ch_ln_tbl1(i).ELIGIBLE_FOR_CV;
				   END IF;
				   lt_ord_dtls(l_cnt).ELIGIBLE_FOR_FV           :=lt_ch_ln_tbl1(i).ELIGIBLE_FOR_FV;
				   lt_ord_dtls(l_cnt).DEFERRED_REVENUE_FLAG     :=lt_ch_ln_tbl1(i).DEFERRED_REVENUE_FLAG;
				   lt_ord_dtls(l_cnt).NON_CONTINGENT_FLAG       :=lt_ch_ln_tbl1(i).NON_CONTINGENT_FLAG;
				   lt_ord_dtls(l_cnt).UNBILLED_ACCOUNTING_FLAG  :=lt_ch_ln_tbl1(i).UNBILLED_ACCOUNTING_FLAG;
				   lt_ord_dtls(l_cnt).DEAL_ID                   :=lt_ch_ln_tbl1(i).DEAL_ID;
				   lt_ord_dtls(l_cnt).LAG_DAYS                  :=lt_ch_ln_tbl1(i).LAG_DAYS;
				   lt_ord_dtls(l_cnt).ATTRIBUTE1                :=lt_ch_ln_tbl1(i).ATTRIBUTE1;
				   lt_ord_dtls(l_cnt).ATTRIBUTE2                :=lt_ch_ln_tbl1(i).ATTRIBUTE2;
				   lt_ord_dtls(l_cnt).ATTRIBUTE3                :=lt_ch_ln_tbl1(i).ATTRIBUTE3;
				   lt_ord_dtls(l_cnt).ATTRIBUTE4                :=lt_ch_ln_tbl1(i).ATTRIBUTE4;
				   lt_ord_dtls(l_cnt).ATTRIBUTE5                :=lt_ch_ln_tbl1(i).ATTRIBUTE5;
				   lt_ord_dtls(l_cnt).ATTRIBUTE6                :=lt_ch_ln_tbl1(i).ATTRIBUTE6;
				   lt_ord_dtls(l_cnt).ATTRIBUTE7                :=lt_ch_ln_tbl1(i).ATTRIBUTE7;
				   lt_ord_dtls(l_cnt).ATTRIBUTE8                :=lt_ch_ln_tbl1(i).ATTRIBUTE8;
				   lt_ord_dtls(l_cnt).ATTRIBUTE9                :=lt_ch_ln_tbl1(i).ATTRIBUTE9;
				   lt_ord_dtls(l_cnt).ATTRIBUTE10               :=lt_ch_ln_tbl1(i).ATTRIBUTE10;
				   lt_ord_dtls(l_cnt).ATTRIBUTE11               :=lt_ch_ln_tbl1(i).ATTRIBUTE11;
           ---ITS#599807 ---
           /*IF lt_ch_ln_tbl1(i).ATTRIBUTE52 IN ('AUTO REGISTERED')
           THEN
              lt_ord_dtls(l_cnt).NUMBER3                ='COTERM';
           ELSE 
           lt_ord_dtls(l_cnt).ATTRIBUTE12               :=lt_ch_ln_tbl1(i).ATTRIBUTE12;
           END IF; */
				   lt_ord_dtls(l_cnt).ATTRIBUTE12               :=lt_ch_ln_tbl1(i).ATTRIBUTE12;
				   lt_ord_dtls(l_cnt).ATTRIBUTE13 :=lt_ch_ln_tbl1(i).ATTRIBUTE13;
				   lt_ord_dtls(l_cnt).ATTRIBUTE14 :=lt_ch_ln_tbl1(i).ATTRIBUTE14;
				   lt_ord_dtls(l_cnt).ATTRIBUTE15 :=lt_ch_ln_tbl1(i).ATTRIBUTE15;
				   lt_ord_dtls(l_cnt).ATTRIBUTE16 :=lt_ch_ln_tbl1(i).ATTRIBUTE16;
				   lt_ord_dtls(l_cnt).ATTRIBUTE17 :=lt_ch_ln_tbl1(i).ATTRIBUTE17;
				   lt_ord_dtls(l_cnt).ATTRIBUTE18 :=lt_ch_ln_tbl1(i).ATTRIBUTE18;
				   lt_ord_dtls(l_cnt).ATTRIBUTE19 :=lt_ch_ln_tbl1(i).ATTRIBUTE19;
				   lt_ord_dtls(l_cnt).ATTRIBUTE20 :=lt_ch_ln_tbl1(i).ATTRIBUTE20;
				   lt_ord_dtls(l_cnt).ATTRIBUTE21  := lt_ch_ln_tbl1(i).ATTRIBUTE21 ;
				   lt_ord_dtls(l_cnt).ATTRIBUTE22 :=lt_ch_ln_tbl1(i).ATTRIBUTE22;
				   lt_ord_dtls(l_cnt).ATTRIBUTE23 :=lt_ch_ln_tbl1(i).ATTRIBUTE23;
           IF l_CHECK_NFR_ORDER ='Y' -- ITS#540799
           THEN
           lt_ord_dtls(l_cnt).ATTRIBUTE24 :=NULL;
           ELSE 
           lt_ord_dtls(l_cnt).ATTRIBUTE24 :=lt_ch_ln_tbl1(i).ATTRIBUTE24;
           END IF;
--				   lt_ord_dtls(l_cnt).ATTRIBUTE24 :=lt_ch_ln_tbl1(i).ATTRIBUTE24;
				   lt_ord_dtls(l_cnt).ATTRIBUTE25 :=lt_ch_ln_tbl1(i).ATTRIBUTE25;
				   lt_ord_dtls(l_cnt).ATTRIBUTE26 :=lt_ch_ln_tbl1(i).ATTRIBUTE26;
				   lt_ord_dtls(l_cnt).ATTRIBUTE27 :=lt_ch_ln_tbl1(i).ATTRIBUTE27;
				   lt_ord_dtls(l_cnt).ATTRIBUTE28 :=lt_ch_ln_tbl1(i).ATTRIBUTE28;
				   lt_ord_dtls(l_cnt).ATTRIBUTE29 :=lt_ch_ln_tbl1(i).ATTRIBUTE29;
				   lt_ord_dtls(l_cnt).ATTRIBUTE30 :=lt_ch_ln_tbl1(i).ATTRIBUTE30;
				   lt_ord_dtls(l_cnt).ATTRIBUTE31 :=lt_ch_ln_tbl1(i).ATTRIBUTE31;
				   lt_ord_dtls(l_cnt).ATTRIBUTE32 :=lt_ch_ln_tbl1(i).ATTRIBUTE32;
				   lt_ord_dtls(l_cnt).ATTRIBUTE33 :=lt_ch_ln_tbl1(i).ATTRIBUTE33;
				   lt_ord_dtls(l_cnt).ATTRIBUTE34 := lv_spr_number;
				   lt_ord_dtls(l_cnt).ATTRIBUTE35 :=lt_ch_ln_tbl1(i).ATTRIBUTE35;
				  -- lt_ord_dtls(l_cnt).ATTRIBUTE36 := DECODE(lt_ch_ln_tbl1(i).ATTRIBUTE44,'E_SUPPORT',XXFT_RPRO_DATA_EXTRACT_PKG.implied_pcs_days(lt_ch_ln_tbl1(i).ATTRIBUTE37,lt_ch_ln_tbl1(i).bill_to_country,'LAG'),null);
				   IF lt_ch_ln_tbl1(i).ATTRIBUTE44 ='E_SUPPORT'
				   THEN
					  lt_ord_dtls(l_cnt).ATTRIBUTE36 := XXFT_RPRO_DATA_EXTRACT_PKG.implied_pcs_days(
																lt_ch_ln_tbl1(i).ATTRIBUTE37
																,lt_ch_ln_tbl1(i).bill_to_country
																,'LAG');
				   ELSE
					  lt_ord_dtls(l_cnt).ATTRIBUTE36  := NULL;
				   END IF;
           --ITS#575976 --
           IF lt_ch_ln_tbl1(i).ATTRIBUTE47 IN ('DROP SHIP','COTERM','AUTO REGISTERED')
           THEN       
              lt_ord_dtls(l_cnt).ATTRIBUTE37 :='N';
           ELSE
              lt_ord_dtls(l_cnt).ATTRIBUTE37 :=lt_ch_ln_tbl1(i).ATTRIBUTE37;
           END IF;
				   
				   lt_ord_dtls(l_cnt).ATTRIBUTE38 :='ERP';
				   lt_ord_dtls(l_cnt).ATTRIBUTE39 :=lt_ch_ln_tbl1(i).ATTRIBUTE39;
				   lt_ord_dtls(l_cnt).ATTRIBUTE40 :=lt_ch_ln_tbl1(i).ATTRIBUTE40;
				   lt_ord_dtls(l_cnt).ATTRIBUTE41 :=lt_ch_ln_tbl1(i).ATTRIBUTE41;
				   lt_ord_dtls(l_cnt).ATTRIBUTE42 :=NULL;
				   --lt_ord_dtls(l_cnt).ATTRIBUTE43 :=DECODE(upper(lt_ch_ln_tbl1(i).FLAG_97_2),'BESP OR VSOE','Y','N') --lcu_rpro.ATTRIBUTE43;
				   IF upper(lt_ch_ln_tbl1(i).FLAG_97_2) = 'BESP OR VSOE'
				   THEN
					  lt_ord_dtls(l_cnt).ATTRIBUTE43  := 'Y';
				   ELSE
					  lt_ord_dtls(l_cnt).ATTRIBUTE43  := 'N';
				   END IF;
				   lt_ord_dtls(l_cnt).ATTRIBUTE44 :=lt_ch_ln_tbl1(i).ATTRIBUTE44;
				   lt_ord_dtls(l_cnt).ATTRIBUTE45 :=lt_ch_ln_tbl1(i).ATTRIBUTE45;
				   lt_ord_dtls(l_cnt).ATTRIBUTE46 :=lt_ch_ln_tbl1(i).PRODUCT_FAMILY;
				   lt_ord_dtls(l_cnt).ATTRIBUTE47 :=lt_ch_ln_tbl1(i).ATTRIBUTE47;
				   lt_ord_dtls(l_cnt).ATTRIBUTE48 :=lt_ch_ln_tbl1(i).ATTRIBUTE48;
				   lt_ord_dtls(l_cnt).ATTRIBUTE49 :=lt_ch_ln_tbl1(i).ATTRIBUTE49;
				   lt_ord_dtls(l_cnt).ATTRIBUTE50 :=lt_ch_ln_tbl1(i).ATTRIBUTE50;
				   lt_ord_dtls(l_cnt).ATTRIBUTE51 :=lt_ch_ln_tbl1(i).ATTRIBUTE51;
				   lt_ord_dtls(l_cnt).ATTRIBUTE52 :=lt_ch_ln_tbl1(i).ATTRIBUTE52;
				   lt_ord_dtls(l_cnt).ATTRIBUTE53 :=lt_ch_ln_tbl1(i).ATTRIBUTE53;
				   lt_ord_dtls(l_cnt).ATTRIBUTE54 :=lt_ch_ln_tbl1(i).ATTRIBUTE54;
				   lt_ord_dtls(l_cnt).ATTRIBUTE55 :=lt_ch_ln_tbl1(i).ATTRIBUTE55;
				   lt_ord_dtls(l_cnt).ATTRIBUTE56 :=lt_ch_ln_tbl1(i).ATTRIBUTE56;
				   lt_ord_dtls(l_cnt).ATTRIBUTE57 :=lt_ch_ln_tbl1(i).ATTRIBUTE57;
				   lt_ord_dtls(l_cnt).ATTRIBUTE58 :=lt_ch_ln_tbl1(i).ATTRIBUTE58;
				   lt_ord_dtls(l_cnt).ATTRIBUTE59 :=lt_ch_ln_tbl1(i).ATTRIBUTE59;
				   lt_ord_dtls(l_cnt).ATTRIBUTE60 :=l_non_serialized; --lt_ch_ln_tbl1(i).ATTRIBUTE60;
				   lt_ord_dtls(l_cnt).DATE1       :=lt_ch_ln_tbl1(i).DATE1;
				   lt_ord_dtls(l_cnt).DATE2       :=lt_ch_ln_tbl1(i).DATE2;
				   lt_ord_dtls(l_cnt).DATE3       :=lt_ch_ln_tbl1(i).DATE3;
				   lt_ord_dtls(l_cnt).DATE4       :=lt_ch_ln_tbl1(i).DATE4;
				   lt_ord_dtls(l_cnt).DATE5       :=lt_ch_ln_tbl1(i).DATE5;
				   lt_ord_dtls(l_cnt).NUMBER1     :=lt_ch_ln_tbl1(i).NUMBER1;
				   lt_ord_dtls(l_cnt).NUMBER2     :=lt_ch_ln_tbl1(i).NUMBER2;
           --ITS#599807 ---
           IF lt_ch_ln_tbl1(i).ATTRIBUTE52 IN ('AUTO REGISTERED') 
           THEN
              lt_ord_dtls(l_cnt).NUMBER3 := 0;       
          ELSIF lt_ch_ln_tbl1(i).RULE_START_DATE is not null
           THEN
              lt_ord_dtls(l_cnt).NUMBER3     := XXFT_RPRO_DATA_EXTRACT_PKG.get_grace_period(
															                 lt_ch_ln_tbl1(i).ATTRIBUTE44
															                ,lt_ch_ln_tbl1(i).product_line
															                ,lt_ch_ln_tbl1(i).SALES_ORDER_LINE_ID);
           ELSE
              lt_ord_dtls(l_cnt).NUMBER3     :=0;
           END IF;
				   lt_ord_dtls(l_cnt).NUMBER4     :=lt_ch_ln_tbl1(i).NUMBER4;
				   IF lt_ch_ln_tbl1(i).ATTRIBUTE44 = 'E_SUPPORT'
				   THEN
					  lt_ord_dtls(l_cnt).NUMBER5    := XXFT_RPRO_DATA_EXTRACT_PKG.implied_pcs_days(
																               lt_ch_ln_tbl1(i).ATTRIBUTE37
																              ,lt_ch_ln_tbl1(i).bill_to_country
																              ,'IMP');
				   ELSE
					  lt_ord_dtls(l_cnt).NUMBER5 := NULL;
				   END IF;
				   lt_ord_dtls(l_cnt).NUMBER6 :=(lt_ch_ln_tbl1(i).RULE_END_DATE - lt_ch_ln_tbl1(i).RULE_START_DATE)+1 ; -- Defect 2953 -Added 1 day bump for Support duration.
				   lt_ord_dtls(l_cnt).NUMBER7 :=lv_batch_id;
          lt_ord_dtls(l_cnt).NUMBER8  :=lt_ch_ln_tbl1(i).NUMBER8;
          lt_ord_dtls(l_cnt).NUMBER9  :=lt_ch_ln_tbl1(i).NUMBER9;
          lt_ord_dtls(l_cnt).NUMBER10 :=lt_ch_ln_tbl1(i).NUMBER10;
          lt_ord_dtls(l_cnt).NUMBER11 :=lt_ch_ln_tbl1(i).NUMBER11;
          lt_ord_dtls(l_cnt).NUMBER12 :=lt_ch_ln_tbl1(i).NUMBER12;
          lt_ord_dtls(l_cnt).NUMBER13 :=lt_ch_ln_tbl1(i).NUMBER13;
          lt_ord_dtls(l_cnt).NUMBER14 :=lt_ch_ln_tbl1(i).NUMBER14;
          lt_ord_dtls(l_cnt).NUMBER15 :=lt_ch_ln_tbl1(i).NUMBER15;
				   lt_ord_dtls(l_cnt).REV_ACCTG_SEG1 :=lt_ch_ln_tbl1(i).REV_ACCTG_SEG1;
				   lt_ord_dtls(l_cnt).REV_ACCTG_SEG2 :=lt_ch_ln_tbl1(i).REV_ACCTG_SEG2;
				   lt_ord_dtls(l_cnt).REV_ACCTG_SEG3 :=lt_ch_ln_tbl1(i).REV_ACCTG_SEG3;
				   lt_ord_dtls(l_cnt).REV_ACCTG_SEG4 :=lt_ch_ln_tbl1(i).REV_ACCTG_SEG4;
				   lt_ord_dtls(l_cnt).REV_ACCTG_SEG5 :=lt_ch_ln_tbl1(i).REV_ACCTG_SEG5;
				   lt_ord_dtls(l_cnt).REV_ACCTG_SEG6 :=lt_ch_ln_tbl1(i).REV_ACCTG_SEG6;
				   lt_ord_dtls(l_cnt).REV_ACCTG_SEG7 :=lt_ch_ln_tbl1(i).REV_ACCTG_SEG7;
				   lt_ord_dtls(l_cnt).REV_ACCTG_SEG8 :=lt_ch_ln_tbl1(i).REV_ACCTG_SEG8;
				   lt_ord_dtls(l_cnt).REV_ACCTG_SEG9 :=lt_ch_ln_tbl1(i).REV_ACCTG_SEG9;
				   lt_ord_dtls(l_cnt).REV_ACCTG_SEG10 :=lt_ch_ln_tbl1(i).REV_ACCTG_SEG10;
				   lt_ord_dtls(l_cnt).DEF_ACCTG_SEG1 :=lt_ch_ln_tbl1(i).DEF_ACCTG_SEG1;
				   lt_ord_dtls(l_cnt).DEF_ACCTG_SEG2 :=lt_ch_ln_tbl1(i).DEF_ACCTG_SEG2;
				   lt_ord_dtls(l_cnt).DEF_ACCTG_SEG3 :=lt_ch_ln_tbl1(i).DEF_ACCTG_SEG3;
				   lt_ord_dtls(l_cnt).DEF_ACCTG_SEG4 :=lt_ch_ln_tbl1(i).DEF_ACCTG_SEG4;
				   lt_ord_dtls(l_cnt).DEF_ACCTG_SEG5 :=lt_ch_ln_tbl1(i).DEF_ACCTG_SEG5;
				   lt_ord_dtls(l_cnt).DEF_ACCTG_SEG6 :=lt_ch_ln_tbl1(i).DEF_ACCTG_SEG6;
				   lt_ord_dtls(l_cnt).DEF_ACCTG_SEG7 :=lt_ch_ln_tbl1(i).DEF_ACCTG_SEG7;
				   lt_ord_dtls(l_cnt).DEF_ACCTG_SEG8 :=lt_ch_ln_tbl1(i).DEF_ACCTG_SEG8;
				   lt_ord_dtls(l_cnt).DEF_ACCTG_SEG9 :=lt_ch_ln_tbl1(i).DEF_ACCTG_SEG9;
				   lt_ord_dtls(l_cnt).DEF_ACCTG_SEG10 :=lt_ch_ln_tbl1(i).DEF_ACCTG_SEG10;
				   lt_ord_dtls(l_cnt).COGS_R_SEG1 :=lt_ch_ln_tbl1(i).COGS_R_SEG1;
				   lt_ord_dtls(l_cnt).COGS_R_SEG2 :=lt_ch_ln_tbl1(i).COGS_R_SEG2;
				   lt_ord_dtls(l_cnt).COGS_R_SEG3 :=lt_ch_ln_tbl1(i).COGS_R_SEG3;
				   lt_ord_dtls(l_cnt).COGS_R_SEG4 :=lt_ch_ln_tbl1(i).COGS_R_SEG4;
				   lt_ord_dtls(l_cnt).COGS_R_SEG5 :=lt_ch_ln_tbl1(i).COGS_R_SEG5;
				   lt_ord_dtls(l_cnt).COGS_R_SEG6 :=lt_ch_ln_tbl1(i).COGS_R_SEG6;
				   lt_ord_dtls(l_cnt).COGS_R_SEG7 :=lt_ch_ln_tbl1(i).COGS_R_SEG7;
				   lt_ord_dtls(l_cnt).COGS_R_SEG8 :=lt_ch_ln_tbl1(i).COGS_R_SEG8;
				   lt_ord_dtls(l_cnt).COGS_R_SEG9 :=lt_ch_ln_tbl1(i).COGS_R_SEG9;
				   lt_ord_dtls(l_cnt).COGS_R_SEG10 :=lt_ch_ln_tbl1(i).COGS_R_SEG10;
				   lt_ord_dtls(l_cnt).COGS_D_SEG1 :=lt_ch_ln_tbl1(i).COGS_D_SEG1;
				   lt_ord_dtls(l_cnt).COGS_D_SEG2 :=lt_ch_ln_tbl1(i).COGS_D_SEG2;
				   lt_ord_dtls(l_cnt).COGS_D_SEG3 :=lt_ch_ln_tbl1(i).COGS_D_SEG3;
				   lt_ord_dtls(l_cnt).COGS_D_SEG4 :=lt_ch_ln_tbl1(i).COGS_D_SEG4;
				   lt_ord_dtls(l_cnt).COGS_D_SEG5 :=lt_ch_ln_tbl1(i).COGS_D_SEG5;
				   lt_ord_dtls(l_cnt).COGS_D_SEG6 :=lt_ch_ln_tbl1(i).COGS_D_SEG6;
				   lt_ord_dtls(l_cnt).COGS_D_SEG7 :=lt_ch_ln_tbl1(i).COGS_D_SEG7;
				   lt_ord_dtls(l_cnt).COGS_D_SEG8 :=lt_ch_ln_tbl1(i).COGS_D_SEG8;
				   lt_ord_dtls(l_cnt).COGS_D_SEG9 :=lt_ch_ln_tbl1(i).COGS_D_SEG9;
				   lt_ord_dtls(l_cnt).COGS_D_SEG10 :=lt_ch_ln_tbl1(i).COGS_D_SEG10;
				   lt_ord_dtls(l_cnt).LT_DEFERRED_ACCOUNT :=lt_ch_ln_tbl1(i).LT_DEFERRED_ACCOUNT;
				   lt_ord_dtls(l_cnt).LT_DCOGS_ACCOUNT :=lt_ch_ln_tbl1(i).LT_DCOGS_ACCOUNT;
				   lt_ord_dtls(l_cnt).REV_DIST_ID :=NULL;
				   lt_ord_dtls(l_cnt).COST_DIST_ID :=NULL;
				   lt_ord_dtls(l_cnt).BOOK_ID :=lt_ch_ln_tbl1(i).BOOK_ID;
				   lt_ord_dtls(l_cnt).BNDL_CONFIG_ID :=lt_ch_ln_tbl1(i).BNDL_CONFIG_ID;
				   lt_ord_dtls(l_cnt).so_last_update_date :=lt_ch_ln_tbl1(i).so_last_update_date;
				   lt_ord_dtls(l_cnt).so_line_creation_date :=lt_ch_ln_tbl1(i).so_line_creation_date;
				   lt_ord_dtls(l_cnt).PROCESSED_FLAG :='N';
				   lt_ord_dtls(l_cnt).ERROR_MESSAGE:= NULL;
		--          lt_ord_dtls(l_cnt).--,CREATED_B:=;
				   lt_ord_dtls(l_cnt).CREATION_DATE :=SYSDATE;
				   lt_ord_dtls(l_cnt).LAST_UPDATE_DATE :=SYSDATE;
				   lt_ord_dtls(l_cnt).PARENT_LINE :='N';
				   lt_ord_dtls(l_cnt).order_line_status := parent_lines_rec.flow_status_code;
				   lt_ord_dtls(l_cnt).conc_request_id:= gn_request_id;
	--               lt_ord_dtls(l_cnt).processing_phase := 'BOOKED';
			    END LOOP; -- Loop through the Line Recs PL/SQL -Table.
		   END IF; -- FTK and PTO Check End If.
			 print_log('Batch ID '||lv_batch_id);
        IF SIGN(lv_commit_count-500) = 1 THEN
          BEGIN
            FORALL k IN 1..lt_ord_dtls.COUNT
            INSERT INTO XXFT_RPRO_ORDER_DETAILS VALUES lt_ord_dtls
              (k
              );
            COMMIT;
            lt_ord_dtls.DELETE;
            FND_FILE.PUT_LINE(FND_FILE.LOG,' Inserting the Batch:=>'||J);
            lv_commit_count :=0;
          EXCEPTION
          WHEN OTHERS THEN
            FND_FILE.PUT_LINE(FND_FILE.LOG,'  Unexpected Exception when inserting records #1:=>'||SQLERRM);
          END;
        END IF;
		  END LOOP;       --- Quantity
		  fnd_file.put_line(fnd_file.LOG,' lt_ord_dtls.count :=> '||lt_ord_dtls.COUNT);
		  FOR i IN 1..lt_ord_dtls.COUNT
		  LOOP
			 print_log(' ==============================================');
			 print_log(' sales_order :=>'||lt_ord_dtls(i).sales_order);
			 print_log(' sales_order_line_id :=>'||lt_ord_dtls(i).sales_order_line_id);
			 print_log(' item_number :=>'||lt_ord_dtls(i).item_number);
			 print_log(' order_line_status :=>'||lt_ord_dtls(i).order_line_status);
			 print_log(' QUANTITY_ORDERED :=>'||lt_ord_dtls(i).QUANTITY_ORDERED);
			 print_log(' sales_order_new_line_id :=>'||lt_ord_dtls(i).sales_order_new_line_id);
		  END LOOP;
		  IF lt_ord_dtls.COUNT > 0
		  THEN
			 print_log('Before inserting the records for  '||lt_ord_dtls.COUNT);
			 print_log('Before inserting the records for  '||lt_ord_dtls.COUNT);
			 BEGIN
				FORALL Q in lt_ord_dtls.FIRST..lt_ord_dtls.LAST SAVE EXCEPTIONS
				   INSERT INTO XXFT.XXFT_RPRO_ORDER_DETAILS values lt_ord_dtls(Q);
				COMMIT;
			 EXCEPTION
				  WHEN ex_dml_errors THEN
				  --    l_error_count := SQL%BULK_EXCEPTIONS.count;
					  fnd_file.put_line(fnd_file.LOG,'Unexpected Exception when inserting records #2: ' || SQL%BULK_EXCEPTIONS.count);
					  FOR p IN 1 .. SQL%BULK_EXCEPTIONS.count LOOP
						fnd_file.put_line(fnd_file.LOG,'Error: ' || p ||
						   ' Array Index: ' || SQL%BULK_EXCEPTIONS(p).error_index ||
							 ' Message: ' || SQLERRM(-SQL%BULK_EXCEPTIONS(p).ERROR_CODE)||
							 ' Sales Order New Line Id :=>'||lt_ord_dtls(p).sales_order_new_line_id);
					  END LOOP;
					  ROLLBACK;
				WHEN OTHERS
				THEN
				  FND_FILE.PUT_LINE(FND_FILE.LOG,'  Unexpected Exception when inserting records #2:=>'||SQLERRM);
				  RAISE;
			 END;
		  END IF;

		  fnd_file.put_line(fnd_file.LOG,' Number of Lines in the Table:=>'||lt_ord_dtls.COUNT);
	/*	UPDATE xxft_rpro_order_details
		   set conc_request_id = gn_request_id
		 WHERE sales_order_id =  parent_lines_rec.header_id
		 and (sales_order_line_id = parent_lines_rec.line_id or attribute41 = to_char(parent_lines_rec.line_id));
		 COMMIT; */
		 UPDATE xxft_rpro_order_control
			set processed_flag ='P'
			   ,error_message = NULL
			   ,last_update_date = SYSDATE
			   ,last_updated_by = fnd_global.user_id
			   ,request_id       = gn_request_id
		   WHERE line_id = parent_lines_rec.line_id
		     AND status NOT IN ('CLOSED')
         AND trx_type='ORD';
		 COMMIT;
	  END LOOP; --- Parent line loop
	  print_info_log('  + Inside Procedure Initial_Populate exit:=>'||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));
  EXCEPTION
     WHEN OTHERS
	 THEN
	    print_log(' Unexpected Exception in Initial Populate Process :=>'||SQLERRM);
	    RAISE;
  END;

--
-- Function to check if the Order needs SVC letter to be sent.
--
FUNCTION check_order_needs_svc(p_header_id IN NUMBER)
RETURN VARCHAR2
IS
l_email_rqrd_flag VARCHAR2(1);
j                 NUMBER;
ln_online_Renewals NUMBER;

BEGIN
   print_info_log('  + Inside check_order_needs_svc :=>'||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));
   dbms_output.put_line('  + Inside check_order_needs_svc :=>'||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));
   --
   -- Checking if global temp collection has the order details.
   --
   IF g_contract_hdr_tbl.COUNT > 0
   THEN
     FOR i IN 1..g_contract_hdr_tbl.COUNT
     LOOP
        IF g_contract_hdr_tbl(i).header_id = p_header_id
        THEN
           l_email_rqrd_flag:=g_contract_hdr_tbl(i).email_rqrd_flag;
           EXIT;
        END IF;
     END LOOP;
   END IF;
   --
   -- return the Flag if found in the global temp.
   --
   IF l_email_rqrd_flag is not NULL
   THEN
      RETURN l_email_rqrd_flag;
   END IF;
   SELECT count(1)
     INTO ln_online_Renewals
     FROM oe_order_headers_all ooha
    WHERE ooha.header_id = p_header_id
      AND EXISTS
       (SELECT 1
          FROM oe_order_sources os
         WHERE 1=1
           and os.order_source_id   = ooha.order_source_id
           AND name                = 'Forticare.OnlineRenewals'
    );
   IF ln_online_Renewals > 0
   THEN
      l_email_rqrd_flag := 'N';
      j:= g_contract_hdr_tbl.COUNT+1;
      g_contract_hdr_tbl(j).header_id       := p_header_id;
      g_contract_hdr_tbl(j).email_rqrd_flag := l_email_rqrd_flag;
      print_info_log('  + Inside check_order_needs_svc Online Renewal l_email_rqrd_flag:=>'||l_email_rqrd_flag||'-'||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));
      dbms_output.put_line('  + Inside check_order_needs_svc Online Renewal l_email_rqrd_flag:=>'||l_email_rqrd_flag||'-'||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));
      RETURN l_email_rqrd_flag;
   END IF;
   SELECT CASE WHEN COUNT(1) > 0 THEN
                 'Y'
                ELSE
                 'N'
                END "Email_Rqrd_Flag"
     INTO  l_email_rqrd_flag
  FROM oe_order_lines_all oola,
    mtl_system_items_b msi,
    mtl_parameters mp
  WHERE oola.header_id         = p_header_id
  AND oola.top_model_line_id = oola.line_id
  AND msi.inventory_item_id  = oola.inventory_item_id
  AND mp.organization_id     = mp.master_organization_id
  AND msi.organization_id    = mp.organization_id
  AND oola.flow_status_code  NOT IN ('CANCELLED') --ITS#553115
  AND EXISTS
    (SELECT 1
    FROM fnd_lookup_values
    WHERE lookup_type ='ITEM_TYPE'
    AND lookup_code   =msi.item_type
    AND meaning       = 'FTNT Professional Services'
    AND enabled_flag  ='Y'
    AND attribute1    ='Y'
    AND sysdate BETWEEN NVL(start_date_active,SYSDATE) AND NVL(end_date_active,sysdate+1)
    )
    AND msi.contract_item_type_code ='SERVICE';
   --
   -- If we dont find the order in the temp table. check with actual query.
   --
   SELECT CASE WHEN COUNT(1) > 0 THEN
                 'Y'
                ELSE
                 'N'
                END "Email_Rqrd_Flag"
     INTO  l_email_rqrd_flag
     FROM oe_order_lines_all oola,
           mtl_system_items_b msi,
           mtl_parameters mp
    WHERE oola.header_id        = p_header_id
      AND oola.top_model_line_id = oola.line_id
      AND msi.inventory_item_id = oola.inventory_item_id
      AND mp.organization_id    = mp.master_organization_id
      AND msi.organization_id   = mp.organization_id
      AND oola.flow_status_code  NOT IN ('CANCELLED') --ITS#553115
      AND EXISTS
        (SELECT 1
           FROM fnd_lookup_values
          WHERE lookup_type ='ITEM_TYPE'
            AND lookup_code   =msi.item_type
            AND enabled_flag  ='Y'
            AND attribute1    ='Y'
            AND meaning       <> 'FTNT Professional Services'
            AND sysdate BETWEEN NVL(start_date_active,SYSDATE) AND NVL(end_date_active,sysdate+1)
        )
       AND NOT EXISTS(SELECT 1
                          FROM fnd_lookup_values
                         WHERE lookup_type = 'FTNT_S2S_UNSUPPORTED_SKUS'
                            AND enabled_flag  ='Y'
                            AND lookup_code   = oola.ordered_item
                            AND sysdate BETWEEN NVL(start_date_active, sysdate) AND NVL(end_date_active, sysdate+1)
            );
    IF l_email_rqrd_flag = 'N'
    THEN
       SELECT CASE WHEN COUNT(1) > 0 THEN
                       'Y'
                      ELSE
                       'N'
                      END "Email_Rqrd_Flag"
           INTO  l_email_rqrd_flag
           FROM oe_order_lines_all oola,
                 mtl_system_items_b msi,
                 mtl_parameters mp
          WHERE oola.header_id        = p_header_id
            AND oola.top_model_line_id is null
            and oola.link_to_line_id is null
            AND msi.inventory_item_id = oola.inventory_item_id
            AND mp.organization_id    = mp.master_organization_id
            AND msi.organization_id   = mp.organization_id
            AND oola.flow_status_code  NOT IN ('CANCELLED') --ITS#553115
            AND EXISTS
              (SELECT 1
                 FROM fnd_lookup_values
                WHERE lookup_type ='ITEM_TYPE'
                  AND lookup_code   =msi.item_type
                  AND enabled_flag  ='Y'
                  AND attribute1    ='Y'
                  AND meaning       <> 'FTNT Professional Services'
                  AND sysdate BETWEEN NVL(start_date_active,SYSDATE) AND NVL(end_date_active,sysdate+1)
              )
            AND NOT EXISTS(SELECT 1
                                FROM fnd_lookup_values
                               WHERE lookup_type = 'FTNT_S2S_UNSUPPORTED_SKUS'
                                 AND enabled_flag  ='Y'
                                 AND lookup_code   = oola.ordered_item
                                AND sysdate BETWEEN NVL(start_date_active, sysdate) AND NVL(end_date_active, sysdate+1)
            );
    END IF;
    j:= g_contract_hdr_tbl.COUNT+1;
    g_contract_hdr_tbl(j).header_id       := p_header_id;
    g_contract_hdr_tbl(j).email_rqrd_flag := l_email_rqrd_flag;
    print_info_log('  + Inside check_order_needs_svc l_email_rqrd_flag:=>'||l_email_rqrd_flag||'-'||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));
    dbms_output.put_line('  + Inside check_order_needs_svc l_email_rqrd_flag:=>'||l_email_rqrd_flag||'-'||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));
    RETURN l_email_rqrd_flag;
EXCEPTION
   WHEN OTHERS THEN
      RAISE_APPLICATION_ERROR(-20002,'Unexpected Exception in check_order_needs_svc:=>'||SQLERRM);
END;
--
-- Function to get the email sent date for the order.
--
FUNCTION get_email_sent_date(p_header_id IN NUMBER)
RETURN DATE
IS
 l_processed_flag  VARCHAR2(2);
 l_email_sent_flag VARCHAR2(1);
 l_email_sent_date DATE;
 l_email_rqrd_flag VARCHAR2(1);
 j                 NUMBER;

  CURSOR cur_get_email_date
  IS
  SELECT MAX(email_date)
    FROM XXFT.XXFT_OF_ORDER XO ,
         oe_order_headers_all oola
   WHERE oola.header_id   = p_header_id
     AND xo.order_number    =oola.order_number
     AND xo.email_sent_flag ='Y';
BEGIN
   print_info_log('  + Inside get_email_sent_date :=>'||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));
   --
   IF g_contract_hdr_tbl.COUNT > 0
   THEN
     FOR i IN 1..g_contract_hdr_tbl.COUNT
     LOOP
        IF g_contract_hdr_tbl(i).header_id = p_header_id AND g_contract_hdr_tbl(i).processed_flag = 'R'
        THEN
           l_processed_flag  := g_contract_hdr_tbl(i).processed_flag;
           l_email_sent_flag := g_contract_hdr_tbl(i).email_sent_flag;
           l_email_sent_date := g_contract_hdr_tbl(i).email_sent_date;
           l_email_rqrd_flag := g_contract_hdr_tbl(i).email_rqrd_flag;
           EXIT;
        END IF;
     END LOOP;
   END IF;
   IF l_email_rqrd_flag ='N'
   THEN
      RETURN NULL;
   END IF;
   IF l_processed_flag ='R'
   THEN
      RETURN l_email_sent_date;
   ELSE
      OPEN cur_get_email_date;
      FETCH cur_get_email_date INTO l_email_sent_date;
      CLOSE cur_get_email_date;
      --
      -- If email is sent then capture the Order in the Global Temp table.
      --
      IF l_email_sent_date is not null
      THEN
         l_email_sent_flag := 'Y';
         l_processed_flag  := 'R';
      ELSE
         l_email_sent_flag := 'N';
         l_processed_flag  := 'NR';
      END IF;
      IF g_contract_hdr_tbl.COUNT > 0
      THEN
        FOR i IN 1..g_contract_hdr_tbl.COUNT
        LOOP
           IF g_contract_hdr_tbl(i).header_id = p_header_id
           THEN
              g_contract_hdr_tbl(i).processed_flag:= l_processed_flag;
              g_contract_hdr_tbl(i).email_sent_flag := l_email_sent_flag;
              g_contract_hdr_tbl(i).email_sent_date:= l_email_sent_date;
              EXIT;
           END IF;
        END LOOP;
     END IF;
     print_info_log('  + Exiting get_email_sent_date :=>'||TO_CHAR(l_email_sent_date, 'DD-MON-YYYY'));
     RETURN l_email_sent_date;
   END IF;
EXCEPTION
   WHEN OTHERS
   THEN
      RAISE_APPLICATION_ERROR(-20002,'Unexpected Exception in get_email_sent_date:=>'||SQLERRM);
END;

--
-- Check to see if the order line is embedded support or subscription- ITS#528351
--
FUNCTION check_line_is_embedded(p_line_id IN NUMBER)
RETURN VARCHAR2
IS
   l_embedded_sup_flag VARCHAR2(2);
   
   CURSOR cur_line_embedded
   IS
   SELECT SUBSTR(attribute44,1,1)
     FROM xxft_rpro_oe_order_details_v
    WHERE sales_order_line_id =p_line_id;
BEGIN
   OPEN  cur_line_embedded;
   FETCH cur_line_embedded INTO l_embedded_sup_flag;
   CLOSE cur_line_embedded;
   IF l_embedded_sup_flag ='E'
   THEN
      RETURN 'Y';
   ELSE
      RETURN 'N';
   END IF;
END;

--
-- Check to see if the order line is Coterm with Quote/Forticare Online Renewals, Renew with Quote with Auto RegFlag as Y. #508695
--
FUNCTION check_line_is_coterm(p_line_id IN NUMBER)
RETURN VARCHAR2
IS
   l_order_source_id   NUMBER;
   l_order_source_name VARCHAR2(240);
   l_auto_reg_flag     VARCHAR2(240);
 CURSOR cur_line_source
 IS
 SELECT oola.order_source_id,os.name,oola.ATTRIBUTE13
   FROM oe_order_lines_all oola, oe_order_sources os
   where oola.line_id = p_line_id
   and os.order_source_id = oola.order_source_id;
BEGIN
    OPEN  cur_line_source;
    FETCH cur_line_source INTO l_order_source_id,l_order_source_name,l_auto_reg_flag;
    CLOSE cur_line_source;
    IF l_order_source_name ='Co-Term with Quote'
    THEN
       RETURN 'Y';
    ELSIF l_order_source_name ='Forticare.OnlineRenewals'
    THEN
       RETURN 'Y';
    ELSIF l_order_source_name ='Renew with Quote' AND l_auto_reg_flag ='Y'
    THEN
       RETURN 'Y';
    ELSE
       RETURN 'N';
    END IF;
END;

 --
 -- Check Contracts Created Process to validate If contracts were created and invoicing
 -- Completed.
 FUNCTION check_contracts_created(p_line_id IN NUMBER,
                                  p_err_msg  IN OUT VARCHAR2)
 RETURN VARCHAR2
 IS
	 l_service_line_id    NUMBER;
	 l_ordered_item       oe_order_lines_all.ordered_item%TYPE;
	 l_ordered_qty        oe_order_lines_all.ordered_quantity%TYPE;
	 l_serial_qty         NUMBER := 0;
	 l_contract_qty       NUMBER := 0;
	 l_return_status      VARCHAR2(3);
	 l_customer_trx_id    NUMBER;
	 l_serial_tbl1        serial_tbl_type;
   l_cnt                NUMBER;
	 l_serialized_line_id NUMBER;
   l_header_id          NUMBER;
   l_email_required     VARCHAR2(1);
   l_email_sent_date    DATE;
   l_coterm_line        VARCHAR2(1);
   l_embedded_sup_sub_line VARCHAR2(1);
   l_svc_exp_flag       VARCHAR2(1);
     --
	 -- Services
	 --
	 CURSOR cur_services_exists_check(p_header_id IN NUMBER)
	 IS
	  SELECT oola.line_id, oola.ordered_quantity
		FROM mtl_system_items_b msi
			,oe_order_lines_all oola
	   WHERE 1=1
		 AND (oola.top_model_line_id = p_line_id OR oola.line_id = p_line_id)
		 and msi.inventory_item_id = oola.inventory_item_id
		 and msi.organization_id = oola.ship_from_org_id
		 and msi.contract_item_type_code ='SERVICE'
     and oola.header_id =p_header_id
     and oola.ordered_item not like '%WARRANTY%' --Ticket #508695
	   UNION
	  SELECT oola.line_id, oola.ordered_quantity
		FROM mtl_system_items_b msi
			   ,oe_order_lines_all oola
	   WHERE oola.SERVICE_REFERENCE_LINE_ID = p_line_id
     and OOLA.SERVICE_REFERENCE_TYPE_CODE ='ORDER' /* ITS 550041 */
		 and msi.inventory_item_id = oola.inventory_item_id
		 and msi.organization_id = oola.ship_from_org_id
		 and msi.contract_item_type_code ='SERVICE'
     and oola.header_id =p_header_id
     and oola.ordered_item not like '%WARRANTY%'; -- Ticket #508695

    CURSOR cur_check_contracts_created(p_service_line_id NUMBER)
	IS
	SELECT count(KRO.CHR_ID)
        FROM OKC_K_REL_OBJS kro
         , OKC_K_HEADERS_ALL_B okh
	 WHERE 1                   = 1
	   AND KRO.JTOT_OBJECT1_CODE ='OKX_ORDERLINE'
	   AND kro.object1_id1       = p_service_line_id
	   AND kro.object1_id2       ='#'
     and okh.id = kro.chr_id
     and okh.sts_code=any('ACTIVE','SIGNED','EXPIRED'); -- Modified to get the active contracts count.
--      FROM OKC_K_REL_OBJS kro
--	 WHERE 1                   = 1
--	   AND KRO.JTOT_OBJECT1_CODE ='OKX_ORDERLINE'
--	   AND kro.object1_id1       = p_service_line_id
--	   AND kro.object1_id2       ='#';
--     
     
	--
	-- Check for invoice is created or not.
	--
	CURSOR cur_check_invoice_created
	IS
	SELECT customer_trx_id
	  FROM ra_customer_trx_lines_all
	 where interface_line_attribute6 =  to_char(p_line_id);
    --
	-- get all Contracts.
	--
	CURSOR cur_get_all_contracts(p_service_line_id IN NUMBER)
	IS
 SELECT p_line_id,
	       csi.serial_number,
		   okh.contract_number,
           oklb1.start_date,
           oklb1.end_date,
		   row_number() over (order by kro.object1_id1) Rank1
	  FROM --oe_order_lines_all oola,
		   OKC_K_REL_OBJS kro,
		   okc_k_items okib,
		   CSI.CSI_ITEM_INSTANCES csi,
		   okc_k_headers_all_b okh,
       apps.okc_k_lines_b oklb,
       apps.okc_k_lines_b oklb1,
       apps.okc_k_lines_b okl
	 WHERE 1                   =1 --line_id =3103
	    --AND oola.header_id        =3276
		--and oola.line_id = p_service_line_id
		AND KRO.JTOT_OBJECT1_CODE ='OKX_ORDERLINE'
		AND kro.object1_id1       = p_service_line_id
		AND kro.object1_id2       ='#'
		--and ok.id= kro.cle_id
		AND okib.jtot_object1_code ='OKX_CUSTPROD'
		AND csi.instance_id        = okib.object1_id1
		AND okh.id                 = KRO.CHR_ID
    and oklb1.sts_code= any('ACTIVE','SIGNED','EXPIRED')
    AND okh.sts_code = any('ACTIVE','SIGNED','EXPIRED') --ITS#573774
    AND kro.cle_id = okl.id
    AND okl.cle_id = oklb1.cle_id
    and oklb.id = oklb1.cle_id
    and oklb1.id = okib.cle_id
    and okh.id = oklb1.dnz_chr_id    ;-- ITS#573774


	--
	-- Defect #3239.
	--
	CURSOR cur_get_all_serials(p_fg_line_id IN NUMBER)
	IS
    select p_line_id ,
           msn.serial_number,
           NULL,
		   NULL,
		   NULL,
		   row_number() over (order by oola.line_id) Rank1
      from wsh_serial_numbers wsn
          ,wsh_delivery_details wdd
          ,oe_order_lines_all oola, mtl_serial_numbers msn
     where oola.line_id = p_fg_line_id
       and wdd.source_line_id = oola.line_id
       and wdd.source_header_id = oola.header_id
       and wsn.delivery_detail_id = wdd.delivery_detail_id
       and msn.inventory_item_id = oola.inventory_item_id
       and msn.serial_number between wsn.fm_serial_number and wsn.to_serial_number;


	CURSOR cur_get_fg_line
	IS
    SELECT oola.line_id
      FROM oe_order_lines_all oola
     WHERE 1                     =1
       AND (oola.top_model_line_id = p_line_id
           OR oola.line_id         = p_line_id)
       AND EXISTS
        (SELECT 1
           FROM mtl_system_items_b msi
          WHERE 1 =1
            AND NVL(msi.serial_number_control_code,1) <>1
            AND msi.inventory_item_id                  =oola.inventory_item_id
            AND msi.organization_id                    =oola.ship_from_org_id
      );

    CURSOR cur_get_header_id
    IS
    SELECT header_id
      FROM oe_order_lines_all
     WHERE line_id = p_line_id;


 BEGIN
    print_log('  + check_contracts_created begin');
    print_info_log('  + Inside Procedure check_contracts_created :=>'||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));
    p_err_msg :=  NULL;
    OPEN cur_get_header_id;
    FETCH cur_get_header_id INTO l_header_id;
    CLOSE cur_get_header_id;
    --
    -- Check if the order is in the exception list to be extracted even if the service contract is missing. As we have a credit memo for this order.
    --
    l_svc_exp_flag := check_svc_exception_list(l_header_id); --ITS#562057    
    IF l_svc_exp_flag='Y'
    THEN
       RETURN 'R';
    END IF;   
    
    --
    -- Check if the order shall need to send the svc letter in email.
    --
    l_email_required:= check_order_needs_svc(l_header_id);
    --
    -- Coterm order/Forticare.Online Renewal Order/Renew with Quote Order.
    --
    l_coterm_line := check_line_is_coterm(p_line_id);
    print_info_log(' l_email_required        :=>'||l_email_required);
    print_info_log(' l_coterm_line           :=>'||l_coterm_line);
    
    --
    -- If the email is required then get he email sent date.
    --
    IF l_email_required ='Y'
    THEN
       l_email_sent_date := get_email_sent_date(l_header_id);
       IF l_email_sent_date is null
       THEN
          l_return_status := 'NR';
          p_err_msg := 'Transaction is waiting for Service Letter to be Sent.';
          RETURN l_return_status;
       END IF;
    END IF;
  	-- Check if the service items exists basaed on the line id.
    OPEN cur_services_exists_check(l_header_id);
	  FETCH cur_services_exists_check  INTO l_service_line_id,l_ordered_qty;
	  CLOSE cur_services_exists_check;
	  print_log('    - Service Line Id:=>'||l_service_line_id);
    --
    -- Check if the line is embedded support or subscription
    --
    l_embedded_sup_sub_line := check_line_is_embedded(l_service_line_id);
    print_info_log(' l_embedded_sup_sub_line :=>'||l_embedded_sup_sub_line);
    
	  IF l_service_line_id > 0
	  THEN
	     -- If exists, check if the contracts are created.
	     OPEN cur_check_contracts_created(l_service_line_id);
       FETCH cur_check_contracts_created INTO l_contract_qty;
       IF cur_check_contracts_created%NOTFOUND
	     THEN
	        --CLOSE cur_check_contracts_created;
		      l_return_status :=  'NR';
	     END IF;
	     print_log('    - l_ordered_qty:=>'||l_ordered_qty ||':=:'||'l_contract_qty:=>'||l_contract_qty);
	     CLOSE cur_check_contracts_created;
	     IF NVL(l_ordered_qty,-99) <> nvl(l_contract_qty,-99) -- Raise the concern when the contract qty is not matching. --ITS#573774
     --  IF nvl(l_contract_qty,-99) < NVL(l_ordered_qty,-99) 
	     THEN
	        l_return_status := 'NR';
		      p_err_msg := 'Transaction is waiting for Contracts to be created for service items.';
	     ELSE
          OPEN cur_check_invoice_created;
          FETCH cur_check_invoice_created INTO l_customer_trx_id;
	  	    CLOSE cur_check_invoice_created;
          print_log('    - Customer Trx id :=>'||l_customer_trx_id);
		      --print_log (   -)
		     IF l_customer_trx_id > 0
         THEN
		       l_return_status := 'R';
         ELSE
		 	     l_return_status :=  'NR';
			     p_err_msg := 'Transaction is waiting for invoice to be created.';
         END IF;
		     --
		     -- Get all contracts and serials against the line
		     --
		     IF l_return_status ='R'
		     THEN
            OPEN cur_get_all_contracts(l_service_line_id);
			     FETCH cur_get_all_contracts BULK COLLECT INTO l_serial_tbl1;
			     CLOSE cur_get_all_contracts;
			     IF l_serial_tbl1.count> 0
			     THEN
			        FOR cnt IN 1..l_serial_tbl1.COUNT
			        LOOP
                 l_cnt := g_serial_tbl.COUNT+1;
				         g_serial_tbl(l_cnt).line_id         := p_line_id;
				         g_serial_tbl(l_cnt).serial_number   := l_serial_tbl1(cnt).serial_number;
				         g_serial_tbl(l_cnt).contract_number := l_serial_tbl1(cnt).contract_number;
				         --g_serial_tbl(l_cnt).start_date      := l_serial_tbl1(cnt).start_date; --Defect #3239
				         --g_serial_tbl(l_cnt).end_date        := l_serial_tbl1(cnt).end_date;	--Defect #3239
                 --CR#85 --
                 IF l_coterm_line ='Y' and l_email_sent_date IS NOT NULL AND l_email_required ='Y'
                 THEN
                    g_serial_tbl(l_cnt).start_date := l_serial_tbl1(cnt).start_date; -- Ticket#508675
                    g_serial_tbl(l_cnt).end_date   := l_serial_tbl1(cnt).end_date;   --Ticket#508675
                 ELSIF l_embedded_sup_sub_line ='Y' and l_email_sent_date IS NOT NULL AND l_email_required ='Y' -- Ticket # 528351#
                 THEN
                    g_serial_tbl(l_cnt).start_date := l_serial_tbl1(cnt).start_date; -- Ticket#528351
                    g_serial_tbl(l_cnt).end_date   := l_serial_tbl1(cnt).end_date;   --Ticket#528351
				         ELSIF l_email_required ='Y' and l_email_sent_date IS NOT NULL and NVL(l_coterm_line,'N') ='N' AND l_embedded_sup_sub_line ='N'
                 THEN
                    g_serial_tbl(l_cnt).start_date := l_email_sent_date; -- CR#85
                    g_serial_tbl(l_cnt).end_date   := l_email_sent_date +(l_serial_tbl1(cnt).end_date-l_serial_tbl1(cnt).start_date); --CR#85
                 ELSE
                    g_serial_tbl(l_cnt).start_date      := l_serial_tbl1(cnt).start_date; --Defect #3239
				            g_serial_tbl(l_cnt).end_date        := l_serial_tbl1(cnt).end_date;	--Defect #3239
                 END IF;
                 --CR#85 --
                 g_serial_tbl(l_cnt).rank1           := l_serial_tbl1(cnt).rank1;

				         print_info_log(' line_id        :=>'||p_line_id);
				         print_info_log(' serial         :=>'||l_serial_tbl1(cnt).serial_number);
				         print_info_log(' Contract       :=>'|| l_serial_tbl1(cnt).contract_number);
				         print_info_log(' start_date     :=>'|| l_serial_tbl1(cnt).start_date);
				         print_info_log(' End Date       :=>'|| l_serial_tbl1(cnt).end_date);
				         print_info_log(' New start_date :=>'|| g_serial_tbl(l_cnt).start_date);
				         print_info_log(' new End Date   :=>'|| g_serial_tbl(l_cnt).end_date);
				         print_info_log(' rank :=>'||l_serial_tbl1(cnt).rank1);
			        END LOOP;
			     END IF;
        END IF;
	   END IF;
	ELSE
	   -- If yes retrieve the serial Numbers and return back.
	   --
       OPEN cur_check_invoice_created;
	   FETCH cur_check_invoice_created INTO l_customer_trx_id;
	   CLOSE cur_check_invoice_created;
	   print_log('    - Customer Trx id :=>'||l_customer_trx_id);
		 --print_log (   -)
	   IF l_customer_trx_id > 0
	   THEN
		  l_return_status := 'R';
	      --
	      -- Check if the serail numbers are available for the License products alone.
	      --
	      OPEN cur_get_fg_line;
	      FETCH cur_get_fg_line INTO l_serialized_line_id;
	      CLOSE cur_get_fg_line;
        IF l_serialized_line_id > 0 THEN
          OPEN cur_get_all_serials(l_serialized_line_id);
          FETCH cur_get_all_serials BULK COLLECT INTO l_serial_tbl1;
          CLOSE cur_get_all_serials;
          FOR cnt IN 1..l_serial_tbl1.COUNT
          LOOP
            l_cnt                               := g_serial_tbl.COUNT+1;
            g_serial_tbl(l_cnt).line_id         := p_line_id;
            g_serial_tbl(l_cnt).serial_number   := l_serial_tbl1(cnt).serial_number;
            g_serial_tbl(l_cnt).contract_number := l_serial_tbl1(cnt).contract_number;
            g_serial_tbl(l_cnt).start_date      := l_serial_tbl1(cnt).start_date;
            g_serial_tbl(l_cnt).end_date        := l_serial_tbl1(cnt).end_date;
            g_serial_tbl(l_cnt).rank1           := l_serial_tbl1(cnt).rank1;
            print_info_log(' line_id    :=>'||p_line_id);
            print_info_log(' serial     :=>'||l_serial_tbl1(cnt).serial_number);
            print_info_log(' Contract   :=>'|| l_serial_tbl1(cnt).contract_number);
            print_info_log(' start_date :=>'|| l_serial_tbl1(cnt).start_date);
            print_info_log(' End Date   :=>'|| l_serial_tbl1(cnt).end_date);
            print_info_log(' rank       :=>'||l_serial_tbl1(cnt).rank1);
          END LOOP;
        END IF;
	   ELSE
		    l_return_status :=  'NR';
		    p_err_msg := 'Transaction is waiting for invoice to be created.';
	   END IF;
	END IF;
	print_log('    - l_return_status:=>'||l_return_status);
	print_log('  - check_contracts_created End.');
	print_info_log('  - check_contracts_created End exit:=>'||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));
	return l_return_status;
 EXCEPTION
 WHEN OTHERS
 THEN
    print_exception_log('   - Unexpected Exception in check_contracts_created :=>'||SQLERRM);
	RETURN 'NR';
 END;

  --
  -- Process to get the serial and Contract against a Hardware/Hardware Bundle.
  --
  procedure get_serial_contract(p_line_id         IN NUMBER
                                ,p_rank            IN NUMBER
                                ,x_serial_number   OUT VARCHAR2
							                  ,x_contract_number OUT VARCHAR2
							                  ,x_start_date      OUT DATE
							                  ,x_end_date        OUT DATE
 )
 IS
	 l_service_line_id NUMBER;
	 l_ordered_item    oe_order_lines_all.ordered_item%TYPE;
	 l_ordered_qty     oe_order_lines_all.ordered_quantity%TYPE;
	 l_return_status   VARCHAR2(3);


	CURSOR cur_services_exists_check
	IS
	SELECT oola.line_id
	FROM mtl_system_items_b msi
		,oe_order_lines_all oola
	WHERE 1=1
	 AND (oola.top_model_line_id = p_line_id OR oola.line_id = p_line_id)
	 and msi.inventory_item_id = oola.inventory_item_id
	 and msi.organization_id = oola.ship_from_org_id
	 and msi.contract_item_type_code ='SERVICE'
	UNION
	SELECT oola.line_id
	FROM mtl_system_items_b msi
		   ,oe_order_lines_all oola
	WHERE oola.SERVICE_REFERENCE_LINE_ID = p_line_id
  and OOLA.SERVICE_REFERENCE_TYPE_CODE ='ORDER' /* ITS 550041 */
	 and msi.inventory_item_id = oola.inventory_item_id
	 and msi.organization_id = oola.ship_from_org_id
	 and msi.contract_item_type_code ='SERVICE';

	CURSOR cur_check_contracts_created(p_service_line_id NUMBER)
	IS
	SELECT Tab.serial_number,
		   Tab.contract_number
	 FROM (
		  SELECT csi.serial_number,
				 okh.contract_number,
				 row_number() over (order by kro.object1_id1) Rank1
		   FROM --oe_order_lines_all oola,
				OKC_K_REL_OBJS kro,
				okc_k_items okib,
				CSI.CSI_ITEM_INSTANCES csi,
			   okc_k_headers_all_b okh
		  WHERE 1                   =1 --line_id =3103
			--AND oola.header_id        =3276
			--and oola.line_id = p_service_line_id
			AND KRO.JTOT_OBJECT1_CODE ='OKX_ORDERLINE'
			AND kro.object1_id1       = p_service_line_id
			AND kro.object1_id2       ='#'
			  --and ok.id= kro.cle_id
			AND okib.cle_id            = kro.cle_id
			AND okib.jtot_object1_code ='OKX_CUSTPROD'
			AND csi.instance_id        = okib.object1_id1
			AND okh.id                 = KRO.CHR_ID
		  ) Tab
		  WHERE Tab.Rank1 = p_rank;

 CURSOR cur_get_fg_line
 IS
  SELECT oola.line_id
	FROM mtl_system_items_b msi
		,oe_order_lines_all oola
   WHERE 1=1
     AND (oola.top_model_line_id = p_line_id OR oola.line_id = p_line_id)
	 and msi.inventory_item_id = oola.inventory_item_id
	 and msi.organization_id = oola.ship_from_org_id
	 and msi.item_type ='FG'
   UNION
  SELECT oola.line_id
	FROM mtl_system_items_b msi
		   ,oe_order_lines_all oola
   WHERE oola.SERVICE_REFERENCE_LINE_ID = p_line_id
   and OOLA.SERVICE_REFERENCE_TYPE_CODE ='ORDER' /* ITS 550041 */
	 and msi.inventory_item_id = oola.inventory_item_id
	 and msi.organization_id = oola.ship_from_org_id
	 and msi.item_type ='FG';

 BEGIN
    -- simply check if the global pl sql table has the record and return.
    IF g_serial_tbl.COUNT > 0
	THEN
	   print_exception_log('   - g_serial_tbl.count :=>'|| g_serial_tbl.COUNT);
	   FOR cnt IN 1..g_serial_tbl.COUNT
	   LOOP
	      IF g_serial_tbl(cnt).line_id = p_line_id AND g_serial_tbl(cnt).rank1=p_rank
		  THEN
		     x_serial_number   := g_serial_tbl(cnt).serial_number;
			 x_contract_number := g_serial_tbl(cnt).contract_number;
			 x_start_date      := g_serial_tbl(cnt).start_date; -- Defect #3239
			 x_end_date        := g_serial_tbl(cnt).end_date; --Defect #3239.
			 exit;
		  END IF;
	   END LOOP;
	END IF;

	--print_log('    - l_return_status:=>'||l_return_status);
	--print_log('  - get_serial_contract End.');
   print_info_log('  - get_serial_contract End:=>'||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));
 EXCEPTION
 WHEN OTHERS
 THEN
    print_exception_log('   - Unexpected Exception in get_serial_contract :=>'||SQLERRM);
 END get_serial_contract;
 
 FUNCTION get_unit_sell_price(p_line_id in NUMBER)
 RETURN NUMBER
 IS
   l_sell_price NUMBER := 0;
 
 CURSOR cur_get_sell_price
 IS
 SELECT unit_selling_price
   FROM oe_order_lines_all
  WHERE line_id = p_line_id;
 
 BEGIN
   OPEN  cur_get_sell_price;
   FETCH cur_get_sell_price INTO l_sell_price;
   CLOSE cur_get_sell_price;
   RETURN l_sell_price;
   
 END;
 

  --
  -- Process to populate Incremental Load.
  --
  PROCEDURE Incremental_Populate(p_order_number IN NUMBER)
  IS
    l_cnt             NUMBER;
    l_serial_number   VARCHAR2(30);
    l_contract_number VARCHAR2(120);
	  l_start_date      DATE;
	  l_end_date        DATE;
    l_non_serialized VARCHAR2(2);
    l_unit_sell_price NUMBER;
    l_ord_qty         NUMBER;
    l_list_price      NUMBER;

    l_ch_batch_ln_tbl rpro_order_details_tbl_type;
    TYPE rec_parent_so_lines IS RECORD(
	   ORDER_NUMBER              NUMBER
	  ,HEADER_ID                 NUMBER
	  ,LINE_ID                   NUMBER
	  ,ordered_quantity          NUMBER
	  ,ordered_item              VARCHAR2(240)
	  ,cancelled_flag            VARCHAR2(1)
	  ,process_flag              VARCHAR2(4)
	  ,ERROR_MESSAGE             VARCHAR2(2000)
	  ,inv_process_flag          VARCHAR2(4)
	);

	TYPE rec_update_so_lines iS RECORD(
	   ORDER_NUMBER              NUMBER
	  ,HEADER_ID                 NUMBER
	  ,LINE_ID                   NUMBER
	  ,ordered_quantity          NUMBER
	  ,last_update_date          DATE
	  ,cancelled_flag            VARCHAR2(2)
	  ,flow_status_code          VARCHAR2(240)
	  ,attribute12               VARCHAR2(240)
	  ,attribute13               VARCHAR2(240)
	  ,ordered_item              VARCHAR2(240)
	  ,SALES_ORDER_LINE_BATCH_ID NUMBER
	  ,sales_order_line_id       NUMBER
	  ,Batch_Rank                NUMBER
	  ,process_flag              VARCHAR2(4)
	  ,ERROR_MESSAGE             VARCHAR2(2000)
    ,non_serial_flag           VARCHAR2(1)
    ,ext_sell_price            NUMBER -- ITS#566054
    ,unit_sell_price           NUMBER -- ITS#566054
    ,ext_list_price            NUMBER -- ITS#566054
		);

  TYPE parent_line_upd_tbl_type IS TABLE OF rec_update_so_lines
  INDEX BY BINARY_INTEGER;
  l_p_line_tbl_upd parent_line_upd_tbl_type;
  TYPE pre_valid_line_tbl_type IS TABLE OF rec_parent_so_lines
  INDEX BY BINARY_INTEGER;
  l_line_tbl       pre_valid_line_tbl_type;
  l_line_tbl2      pre_valid_line_tbl_type;
  TYPE update_batch_rec IS RECORD(
      batch_id          NUMBER
	    ,so_line_id       NUMBER
	    ,serial_number    VARCHAR2(30)
	    ,contract_number  VARCHAR2(120)
	    ,unit_cost        NUMBER
	    ,back_order_flag  VARCHAR2(2)
	    ,cancelled_flag   VARCHAR2(1)
	    ,flow_status_code VARCHAR2(240)
		  ,ship_date        DATE
		  ,rule_start_date  DATE
      ,rule_end_date    DATE
	   	,quantity_shipped NUMBER
      ,quantity_ordered NUMBER
      ,unit_sell_price  NUMBER
      ,ext_sell_price   NUMBER
      ,ext_list_price   NUMBER
	    ,process_flag     VARCHAR2(1)
	    ,Error_message    VARCHAR2(4000)
  );
  TYPE update_batch_tbl IS TABLE OF update_batch_rec
  INDEX BY BINARY_INTEGER;
  lt_update_batch_tbl  update_batch_tbl;
  lt_update_batch2_tbl update_batch_tbl;


	CURSOR cur_parent_lines
	IS
	SELECT xpoc.order_number order_number,
           xpoc.header_id,
		   xpoc.line_id,
		   oola.ordered_quantity,
		   oola.ordered_item,
		   oola.cancelled_flag,
		   'NULL' process_flag,
		   NULL  ERROR_MESSAGE,
       NULL inv_process_flag
	  FROM XXFT_RPRO_ORDER_CONTROL xpoc
	     , oe_order_lines_all oola
	 WHERE xpoc.processed_flag IN ('R','NR')
       AND xpoc.Status IN ('CLOSED','CANCELLED')
	   AND oola.line_id = xpoc.line_id
     AND oola.line_category_code='ORDER'
     AND xpoc.order_number     = NVL(p_order_number,xpoc.order_number)
    ;

	CURSOR cur_parent_lines1
	IS
	SELECT xpoc.order_number order_number,
           xpoc.header_id,
		   xpoc.line_id,
		   oola.ordered_quantity,
		   oola.ordered_item,
		   oola.cancelled_flag,
		   'NULL' process_flag,
		   NULL  ERROR_MESSAGE,
       NULL inv_process_flag
	  FROM XXFT_RPRO_ORDER_CONTROL xpoc
	     , oe_order_lines_all oola
	 WHERE xpoc.processed_flag = 'R'
       AND xpoc.Status IN ('CLOSED','CANCELLED')
	   AND oola.line_id = xpoc.line_id
       AND xpoc.order_number     = NVL(p_order_number,xpoc.order_number)
       AND oola.line_category_code='ORDER'
     ORDER BY xpoc.line_id
	;

	CURSOR cur_update_SO_lines(p_line_id IN NUMBER)
	IS
    SELECT
  /*+ PARALLEL(ord_details,DEFAULT) */
      xpoc.order_number order_number,
      xpoc.header_id,
      xpoc.line_id,
      oola.ordered_quantity,
      oola.last_update_date,
      oola.cancelled_flag,
      oola.flow_status_code,
      oola.attribute12,
      oola.attribute13,
      oola.ordered_item,
      SALES_ORDER_LINE_BATCH_ID,
      sales_order_line_id,
      dense_rank() OVER (PARTITION BY sales_order_line_id order by SALES_ORDER_LINE_BATCH_ID) Batch_Rank,
      xpoc.processed_flag,
      NULL ERROR_MESSAGE,
      ord_details.attribute60 non_serial_flag,
      Round(oola.unit_selling_price*oola.ordered_quantity, 2) ext_sell_price, -- ITS#566054
      oola.unit_selling_price unit_sell_price,                                -- ITS#566054
      Round(ord_details.unit_list_price*oola.ordered_quantity,2) ext_list_price -- ITS#566054
  FROM XXFT_RPRO_ORDER_CONTROL xpoc ,
      xxft_rpro_order_details ord_details ,
      oe_order_lines_all oola ,
      mtl_system_items_b msi ,
      mtl_parameters mp
  WHERE xpoc.line_id                  = p_line_id
  AND xpoc.processed_flag             ='R'
  AND xpoc.Status                    IN ('CLOSED','CANCELLED')
  AND ord_details.sales_order_line_id = xpoc.line_id
  AND oola.line_id                    = xpoc.line_id
  AND msi.inventory_item_id           = oola.inventory_item_id
  AND mp.organization_id              = mp.master_organization_id
  AND msi.organization_id             = mp.organization_id
  AND EXISTS
      (SELECT 1
      FROM apps.FND_LOOKUP_VALUES flv,
        oe_order_headers_all ooha,
        OE_TRANSACTION_TYPES_TL ott
      WHERE ooha.header_id          = xpoc.header_id
      AND ott.transaction_type_id   = ooha.order_type_id
      AND flv.language              = 'US'
      AND NVL(flv.enabled_flag,'N') = 'Y'
      AND sysdate BETWEEN NVL(flv.start_date_active,sysdate) AND NVL(flv.end_date_active,sysdate + 1)
      AND flv.lookup_type        = 'REVPRO_ORDER_TYPE'
      AND UPPER(flv.lookup_code) = UPPER(ott.name)
  )
    AND oola.line_category_code='ORDER'
  ORDER BY xpoc.line_id;

	CURSOR cur_batch_lines(p_batch_id       IN NUMBER
	                      ,p_parent_line_id IN NUMBER)
	IS
	SELECT *
	  FROM xxft.xxft_rpro_order_details
	 WHERE sales_order_line_batch_id = p_batch_id
       --AND sales_order_line_id <> 	p_parent_line_id
	   ;
     
  CURSOR cur_get_line_price(p_line_id NUMBER)
  IS
  SELECT unit_selling_price,
          ordered_quantity,
          XXFT_RPRO_DATA_EXTRACT_PKG.get_list_price(line_id,price_list_id,inventory_item_id,pricing_date) list_price
          --INTO l_unit_sell_price, l_ord_qty, l_list_price
    FROM oe_order_lines_all
   WHERE line_id =p_line_id;
     
  ex_dml_errors EXCEPTION;
  PRAGMA EXCEPTION_INIT(ex_dml_errors, -24381);

  BEGIN
	  print_log('    + Inside Incremental_Populate ');
	  print_info_log('   + Inside Incremental_Populate:=>'||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));
	  OPEN cur_parent_lines;
	  LOOP
	     EXIT WHEN cur_parent_lines%NOTFOUND;
	     FETCH cur_parent_lines BULK COLLECT INTO l_line_tbl LIMIT 1000;
        FOR valid_cnt IN 1..l_line_tbl.COUNT
        LOOP
          --
          -- Check if the Parent line ready for extraction.
          --
          IF NVL(l_line_tbl(valid_cnt).cancelled_flag,'N') <>'Y' THEN
            l_line_tbl(valid_cnt).process_flag             := check_contracts_created(l_line_tbl(valid_cnt).line_id ,l_line_tbl(valid_cnt).error_message);
          ELSE
            l_line_tbl(valid_cnt).process_flag := 'R';
          END IF;
          print_log( '  Incremental_Populate. line_id :=>'||l_line_tbl(valid_cnt).line_id);
          print_log( '  Incremental_Populate. process_flag :=>'||l_line_tbl(valid_cnt).process_flag);
        END LOOP;
         l_line_tbl2 := l_line_tbl;
	     IF l_line_tbl.COUNT > 0
	     THEN
	        print_log('Before Updating  the records for  '||l_line_tbl.COUNT);
	        BEGIN
		       FORALL X in l_line_tbl.FIRST..l_line_tbl.LAST SAVE EXCEPTIONS
			      UPDATE XXFT.XXFT_RPRO_ORDER_CONTROL
			         SET processed_flag   = l_line_tbl(X).process_flag
				           ,error_message    = l_line_tbl(X).error_message
				           ,last_update_date = SYSDATE
				           ,last_updated_by  = gn_user_id
				           ,request_id       = gn_request_id
			       WHERE line_id          = l_line_tbl2(X).line_id
               AND trx_type = 'ORD';
		        COMMIT;
	        EXCEPTION
		       WHEN ex_dml_errors THEN
			   --    l_error_count := SQL%BULK_EXCEPTIONS.count;
			      print_exception_log('Unexpected Exception when Updating validation status: ' || SQL%BULK_EXCEPTIONS.count);
					  FOR p IN 1 .. SQL%BULK_EXCEPTIONS.count LOOP
						print_exception_log('Error: ' || p ||
						   ' Array Index: ' || SQL%BULK_EXCEPTIONS(p).error_index ||
							 ' Message: ' || SQLERRM(-SQL%BULK_EXCEPTIONS(p).ERROR_CODE));
					  END LOOP;
					  ROLLBACK;
					  RAISE;
		       WHEN OTHERS
		       THEN
		          print_exception_log('  Unexpected Exception when Updating records #4:=>'||SQLERRM);
	              RAISE;
	        END;
		    l_line_tbl2.DELETE;
		END IF;
	  END LOOP;
	  CLOSE cur_parent_lines;
	  --
	  -- Actual Processing of the valid parent line. 5/23/2016 - Improve performance.
	  --
	  OPEN cur_parent_lines1;
	  LOOP
	     EXIT WHEN cur_parent_lines1%NOTFOUND;
	     FETCH cur_parent_lines1 BULK COLLECT INTO l_line_tbl LIMIT 1000;
		 FOR pa_cnt IN 1..l_line_tbl.COUNT
		 LOOP
		    print_info_log('   + l_line_tbl.line_id:=>'||l_line_tbl(pa_cnt).line_id);
        OPEN cur_update_SO_lines(l_line_tbl(pa_cnt).line_id);
	      LOOP
		       IF lt_update_batch_tbl.COUNT > 0
		       THEN
			     lt_update_batch_tbl.DELETE;
		       END IF;
		       IF l_p_line_tbl_upd.COUNT >0
		       THEN
			      l_p_line_tbl_upd.DELETE;
		       END IF;
		       -- Bulk collect the update records into a pl sql table.
           EXIT WHEN cur_update_SO_lines%NOTFOUND;
		       FETCH cur_update_SO_lines BULK COLLECT INTO l_p_line_tbl_upd LIMIT 500;
		       print_log('  Records fetched for Update :=>'||l_p_line_tbl_upd.COUNT);

		       FOR upd_cnt IN 1..l_p_line_tbl_upd.COUNT
		       LOOP
		          print_info_log( '  Incremental_Populate. Processing line_id :=>'||l_p_line_tbl_upd(upd_cnt).line_id);
			        print_info_log( '  Incremental_Populate. Processing line_id :=>'||l_p_line_tbl_upd(upd_cnt).process_flag);
			        print_info_log( '  Incremental_Populate. Processing sales_order_line_id :=>'||l_p_line_tbl_upd(upd_cnt).sales_order_line_id);
			        print_info_log( '  Incremental_Populate. Processing ordered_quantity :=>'||l_p_line_tbl_upd(upd_cnt).ordered_quantity);
			        print_info_log( '  Incremental_Populate. Processing batch_Rank :=>'||l_p_line_tbl_upd(upd_cnt).batch_Rank);
			        print_info_log( '  Incremental_Populate. Processing SALES_ORDER_LINE_BATCH_ID :=>'||l_p_line_tbl_upd(upd_cnt).SALES_ORDER_LINE_BATCH_ID);
              --
              -- Check the line is a consolidated line..#CR49.
					    --
              /*l_non_serialized := xxft_rpro_data_extract_pkg.check_non_serial_item(l_p_line_tbl_upd(upd_cnt).header_id
					                                        , l_p_line_tbl_upd(upd_cnt).line_id); */

					    print_info_log( '  Incremental_Populate. Non Serialized Flag :=>'||l_p_line_tbl_upd(upd_cnt).non_serial_flag);

			        IF l_p_line_tbl_upd(upd_cnt).process_flag = 'R'
			        THEN
			        --
			        -- Check if Item is a License? If it is License then don't wait for .
			        --
			          IF l_p_line_tbl_upd(upd_cnt).line_id = l_p_line_tbl_upd(upd_cnt).sales_order_line_id
                THEN
                   -- Get all Lines of the Batch and Update Status.
                   OPEN cur_batch_lines(l_p_line_tbl_upd(upd_cnt).SALES_ORDER_LINE_BATCH_ID
                                        ,l_p_line_tbl_upd(upd_cnt).sales_order_line_id);
                   FETCH cur_batch_lines BULK COLLECT INTO l_ch_batch_ln_tbl;
				           CLOSE cur_batch_lines;
                   print_log(l_ch_batch_ln_tbl.COUNT
					                           ||'  Number of lines collected from the batch id :=>'
							                       ||l_p_line_tbl_upd(upd_cnt).SALES_ORDER_LINE_BATCH_ID);

				          IF -- NVL(l_p_line_tbl_upd(upd_cnt).cancelled_flag,'N') ='Y'
                      l_p_line_tbl_upd(upd_cnt).flow_status_code ='CANCELLED'
				          THEN
                     FOR n in 1..l_ch_batch_ln_tbl.COUNT
					           LOOP
					             l_cnt := lt_update_batch_tbl.COUNT+1;
					             lt_update_batch_tbl(l_cnt).batch_id         := l_ch_batch_ln_tbl(n).SALES_ORDER_LINE_BATCH_ID;
					             lt_update_batch_tbl(l_cnt).so_line_id       := l_ch_batch_ln_tbl(n).sales_order_line_id;
                       lt_update_batch_tbl(l_cnt).serial_number    := NULL;
                       lt_update_batch_tbl(l_cnt).contract_number  := NULL;
                       lt_update_batch_tbl(l_cnt).back_order_flag  := NULL;
					             lt_update_batch_tbl(l_cnt).unit_cost        := NULL;
                       lt_update_batch_tbl(l_cnt).cancelled_flag   := l_p_line_tbl_upd(upd_cnt).cancelled_flag;
                       lt_update_batch_tbl(l_cnt).flow_status_code := l_p_line_tbl_upd(upd_cnt).flow_status_code;
                       lt_update_batch_tbl(l_cnt).ship_date        := NULL;
								       lt_update_batch_tbl(l_cnt).rule_start_date  := l_ch_batch_ln_tbl(n).rule_start_date;
								       lt_update_batch_tbl(l_cnt).rule_end_date    := l_ch_batch_ln_tbl(n).rule_end_date;
					             lt_update_batch_tbl(l_cnt).quantity_shipped := NULL;
					           END LOOP;
                     l_p_line_tbl_upd(upd_cnt).PROCESS_FLAG := 'P';--
                     l_p_line_tbl_upd(upd_cnt).ERROR_MESSAGE := NULL;--
                                --   l_p_line_tbl_upd(upd_cnt).Inv_process_flag := 'R';
				           ELSE
					            IF l_p_line_tbl_upd(upd_cnt).batch_Rank <= l_p_line_tbl_upd(upd_cnt).ordered_quantity
					            THEN
					               --
                         -- Get the serial and Contract Number.
					               --
					               get_serial_contract(p_line_id => l_p_line_tbl_upd(upd_cnt).line_id
								                            ,p_rank              => l_p_line_tbl_upd(upd_cnt).batch_Rank
								                            ,x_serial_number     => l_serial_number
								                            ,x_contract_number   => l_contract_number
															              ,x_start_date        => l_start_date
															              ,x_end_date          => l_end_date
						             );
                         print_info_log( '  Incremental_Populate.Serial Number   :=>'||l_serial_number);
								         print_info_log( '  Incremental_Populate.Contract Number :=>'||l_contract_number);
                         print_info_log( '  Incremental_Populate.Start Date      :=>'||l_start_date);
								         print_info_log( '  Incremental_Populate.End Date        :=>'||l_end_date);

                         FOR n in 1..l_ch_batch_ln_tbl.COUNT
                         LOOP
					                  l_cnt := lt_update_batch_tbl.COUNT+1;
						                lt_update_batch_tbl(l_cnt).batch_id         := l_ch_batch_ln_tbl(n).SALES_ORDER_LINE_BATCH_ID;
						                lt_update_batch_tbl(l_cnt).so_line_id       := l_ch_batch_ln_tbl(n).sales_order_line_id;
						                lt_update_batch_tbl(l_cnt).serial_number    := l_serial_number;
						                lt_update_batch_tbl(l_cnt).contract_number  := l_contract_number;
                            lt_update_batch_tbl(l_cnt).back_order_flag  :=  NULL;
                            lt_update_batch_tbl(l_cnt).cancelled_flag   := l_p_line_tbl_upd(upd_cnt).cancelled_flag;
						                lt_update_batch_tbl(l_cnt).flow_status_code := l_p_line_tbl_upd(upd_cnt).flow_status_code;
                            --lt_update_batch_tbl(l_cnt).unit_cost        := get_line_cost(l_ch_batch_ln_tbl(n).sales_order_line_id);
						                lt_update_batch_tbl(l_cnt).ship_date        := get_actual_ship_date(l_ch_batch_ln_tbl(n).sales_order_line_id);
								            -- Defect #3239.                            
								            IF l_ch_batch_ln_tbl(n).rule_start_date is not null OR l_ch_batch_ln_tbl(n).attribute44 is not null  --ITS#
								            THEN
								               lt_update_batch_tbl(l_cnt).rule_start_date  := l_start_date;
								            ELSE
                              lt_update_batch_tbl(l_cnt).rule_start_date  := l_ch_batch_ln_tbl(n).rule_start_date;
								            END IF;
								            IF l_ch_batch_ln_tbl(n).rule_end_date is not null OR l_ch_batch_ln_tbl(n).attribute44 is not null -- ITS#
                            THEN
								               lt_update_batch_tbl(l_cnt).rule_end_date    := l_end_date;
								            ELSE
                               lt_update_batch_tbl(l_cnt).rule_end_date    := l_ch_batch_ln_tbl(n).rule_end_date;
								            END IF;
								            --
								            -- Check for the consolidation
                            --
                            OPEN cur_get_line_price(l_ch_batch_ln_tbl(n).sales_order_line_id);
                            FETCH cur_get_line_price INTO l_unit_sell_price, l_ord_qty, l_list_price;
                            CLOSE cur_get_line_price;
                            
								           IF l_p_line_tbl_upd(upd_cnt).non_serial_flag ='Y'
								           THEN
						                  lt_update_batch_tbl(l_cnt).quantity_shipped   := l_ord_qty; --l_p_line_tbl_upd(upd_cnt).ordered_quantity;
                              lt_update_batch_tbl(l_cnt).unit_cost          := round(get_line_cost(l_ch_batch_ln_tbl(n).sales_order_line_id)*l_p_line_tbl_upd(upd_cnt).ordered_quantity,2);
                              lt_update_batch_tbl(l_cnt).quantity_ordered   := l_ord_qty;--l_p_line_tbl_upd(upd_cnt).ordered_quantity; --ITS#566054
                              lt_update_batch_tbl(l_cnt).ext_sell_price     := round(l_unit_sell_price*l_ord_qty, 2) ;               --l_p_line_tbl_upd(upd_cnt).ext_sell_price;  --ITS#566054
                              lt_update_batch_tbl(l_cnt).unit_sell_price    := l_unit_sell_price;               --l_p_line_tbl_upd(upd_cnt).unit_sell_price;  --ITS#566054
                              lt_update_batch_tbl(l_cnt).ext_list_price     := round(l_list_price*l_ord_qty,2); --l_p_line_tbl_upd(upd_cnt).ext_list_price;  --ITS#566054
                              --
                              -- ITS#566054 - Split Line did not get transferred properly to Revpro.
                              --
                              --Ext_list_price
                              --Ext_sell_price
                              --Quantity_ordered
                              --Unit list price.
                              
                           ELSE
								             lt_update_batch_tbl(l_cnt).quantity_shipped  := 1;
                             lt_update_batch_tbl(l_cnt).unit_cost         := get_line_cost(l_ch_batch_ln_tbl(n).sales_order_line_id);
                             
                             --
                             -- Need to add the ext sell Price and unit sell price calculation after the order line is closed.ITS#
                             --
                              lt_update_batch_tbl(l_cnt).ext_sell_price     := l_unit_sell_price;  --ITS#566054
                              lt_update_batch_tbl(l_cnt).unit_sell_price    := l_unit_sell_price;  --ITS#566054                              
                           END IF;
                       END LOOP;
					          ELSE
                       FOR n in 1..l_ch_batch_ln_tbl.COUNT
                       LOOP
                          l_cnt := lt_update_batch_tbl.COUNT+1;
                          lt_update_batch_tbl(l_cnt).batch_id         := l_ch_batch_ln_tbl(n).SALES_ORDER_LINE_BATCH_ID;
                          lt_update_batch_tbl(l_cnt).so_line_id       := l_ch_batch_ln_tbl(n).sales_order_line_id;
                          lt_update_batch_tbl(l_cnt).serial_number    := NULL;
                          lt_update_batch_tbl(l_cnt).contract_number  := NULL;
                          lt_update_batch_tbl(l_cnt).back_order_flag  := 'BO';
                          lt_update_batch_tbl(l_cnt).cancelled_flag   := l_p_line_tbl_upd(upd_cnt).cancelled_flag;
                          lt_update_batch_tbl(l_cnt).flow_status_code := 'BACKORDER';
                          lt_update_batch_tbl(l_cnt).unit_cost        := get_line_cost(l_ch_batch_ln_tbl(n).sales_order_line_id);
                          lt_update_batch_tbl(l_cnt).ship_date        :=  NULL;
                          lt_update_batch_tbl(l_cnt).quantity_shipped := NULL;
								          -- Defect #3239.
									        lt_update_batch_tbl(l_cnt).rule_start_date  := l_ch_batch_ln_tbl(n).rule_start_date;
									        lt_update_batch_tbl(l_cnt).rule_end_date    := l_ch_batch_ln_tbl(n).rule_end_date;
                       END LOOP;
                    END IF; -- quantity  rank if else.
				          END IF; --cancelled flag if else.
				          l_p_line_tbl_upd(upd_cnt).PROCESS_FLAG := 'P';--
                  l_p_line_tbl_upd(upd_cnt).ERROR_MESSAGE := NULL;--
		                   --l_p_line_tbl_upd(upd_cnt).Inv_process_flag := 'R';
			         END IF; --line id check
            END IF;
		    END LOOP;
		    print_log('Number of records in the lt_update_batch_tbl:=>'||lt_update_batch_tbl.COUNT);

		    lt_update_batch2_tbl := lt_update_batch_tbl;
		    print_log('---Printing the Update Batch Table---');
		    IF lt_update_batch_tbl.COUNT > 0
		    THEN
           FOR b in lt_update_batch_tbl.FIRST..lt_update_batch_tbl.LAST
           LOOP
		          print_exception_log(' ----------------------------------------------------');
			        print_exception_log(' batch_id:=>        '||lt_update_batch_tbl(b).batch_id);
			        print_exception_log(' so_line_id:=>      '||lt_update_batch_tbl(b).so_line_id);
			        print_exception_log(' flow_status_code:=>'||lt_update_batch_tbl(b).flow_status_code);
			        print_exception_log(' cancelled_flag:=>  '||lt_update_batch_tbl(b).cancelled_flag);
			        print_exception_log(' back_order_flag:=> '||lt_update_batch_tbl(b).back_order_flag);
			        print_exception_log(' serial_number:=>   '||lt_update_batch_tbl(b).serial_number);
			        print_exception_log(' contract_number:=> '||lt_update_batch_tbl(b).contract_number);
			        print_exception_log(' flow_status_code:=>'||lt_update_batch_tbl(b).flow_status_code);
				      print_exception_log(' Rule Start Date:=> '||lt_update_batch_tbl(b).rule_start_date);
				      print_exception_log(' Rule End Date  :=> '||lt_update_batch_tbl(b).rule_end_date);
              print_exception_log(' Ordered Quantity  :=> '||lt_update_batch_tbl(b).quantity_ordered);
              print_exception_log(' Ext Sell Price    :=> '||lt_update_batch_tbl(b).ext_sell_price);
              print_exception_log(' Ext List Price    :=> '||lt_update_batch_tbl(b).ext_list_price);
              
			        print_exception_log(' ----------------------------------------------------');
           END LOOP;
		    END IF;
		    print_log('---Printing the Update Batch Table---');

		    IF lt_update_batch_tbl.COUNT > 0
		    THEN
			   print_log('Before Updating  the records for  '||lt_update_batch_tbl.COUNT);
			   BEGIN
				  FORALL X in lt_update_batch_tbl.FIRST..lt_update_batch_tbl.LAST SAVE EXCEPTIONS
				    UPDATE XXFT.XXFT_RPRO_ORDER_DETAILS
					  SET cancelled_flag         = lt_update_batch_tbl(X).cancelled_flag
               ,processing_attribute5 = lt_update_batch_tbl(X).back_order_flag
						   ,attribute33       = lt_update_batch_tbl(X).serial_number
						   ,attribute45       = lt_update_batch_tbl(X).contract_number
						   ,order_line_status = lt_update_batch_tbl(X).flow_status_code
						   ,last_update_date   = SYSDATE
						   ,last_updated_by  = gn_user_id
						   ,cost_amount      = lt_update_batch_tbl(X).unit_cost
						   ,ship_date        = lt_update_batch_tbl(X).ship_date
						   ,quantity_shipped = lt_update_batch_tbl(X).quantity_shipped
						   ,rule_start_date  = lt_update_batch_tbl(X).rule_start_date --Defect #3239
						   ,rule_end_date    = lt_update_batch_tbl(X).rule_end_date   --Defect #3239
						   ,processed_flag   = 'N' -- to take care of the delta.
               ,return_flag      = lt_update_batch_tbl(X).cancelled_flag -- ITS#561763               
               ,quantity_ordered = nvl(lt_update_batch_tbl(X).quantity_ordered,quantity_ordered) --ITS#566054
               ,unit_sell_price  = nvl(lt_update_batch_tbl(X).ext_sell_price,unit_sell_price)   --ITS#566054
               ,ext_sell_price   = nvl(lt_update_batch_tbl(X).ext_sell_price,ext_sell_price)   --ITS#566054
               ,ext_list_price   = nvl(lt_update_batch_tbl(X).ext_list_price,ext_list_price)   --ITS#566054
				    WHERE sales_order_line_batch_id = lt_update_batch2_tbl(X).batch_id
				      AND SALES_ORDER_LINE_ID       = lt_update_batch2_tbl(X).so_line_id;
				  COMMIT;
			   EXCEPTION
				  WHEN ex_dml_errors THEN
				  --    l_error_count := SQL%BULK_EXCEPTIONS.count;
					  fnd_file.put_line(fnd_file.LOG,'Unexpected Exception when Updating records #2: ' || SQL%BULK_EXCEPTIONS.count);
					  FOR p IN 1 .. SQL%BULK_EXCEPTIONS.count LOOP
						fnd_file.put_line(fnd_file.LOG,'Error: ' || p ||
						   ' Array Index: ' || SQL%BULK_EXCEPTIONS(p).error_index ||
							 ' Message: ' || SQLERRM(-SQL%BULK_EXCEPTIONS(p).ERROR_CODE));
					    lt_update_batch_tbl(p).process_flag := 'E';
						lt_update_batch_tbl(p).error_message := SQLERRM(-SQL%BULK_EXCEPTIONS(p).ERROR_CODE);
					  END LOOP;
					  ROLLBACK;
				  WHEN OTHERS
			      THEN
				     FND_FILE.PUT_LINE(FND_FILE.LOG,'  Unexpected Exception when Updating records #2:=>'||SQLERRM);
				     RAISE;
			     END;
			     lt_update_batch2_tbl.DELETE;
		       END IF;
		      END LOOP; -- Loop through the Update SO Lines.
	        CLOSE cur_update_SO_lines;
	  --
	  -- Completed processing all the batches of the specific line and hence update status.
	  --
	    l_line_tbl(pa_cnt).process_flag    := 'P';
	    l_line_tbl(pa_cnt).error_message     := NULL;
	    l_line_tbl(pa_cnt).inv_process_flag  := 'R';
	  END LOOP;
         l_line_tbl2 := l_line_tbl;
	     IF l_line_tbl.COUNT > 0
	     THEN
	        print_log('Before Updating  the records for  '||l_line_tbl.COUNT);
	        BEGIN
		       FORALL X in l_line_tbl.FIRST..l_line_tbl.LAST SAVE EXCEPTIONS
			      UPDATE XXFT.XXFT_RPRO_ORDER_CONTROL
			         SET processed_flag     = l_line_tbl(X).process_flag
				        ,error_message      = l_line_tbl(X).error_message
				        ,last_update_date   = SYSDATE
				        ,last_updated_by    = gn_user_id
				        ,request_id         = gn_request_id
						    ,inv_processed_flag = l_line_tbl(X).inv_process_flag
			       WHERE line_id            = l_line_tbl2(X).line_id
               AND trx_type = 'ORD';
		        COMMIT;
	        EXCEPTION
		       WHEN ex_dml_errors THEN
			   --    l_error_count := SQL%BULK_EXCEPTIONS.count;
			      print_exception_log('Unexpected Exception when Updating validation status: ' || SQL%BULK_EXCEPTIONS.count);
					  FOR p IN 1 .. SQL%BULK_EXCEPTIONS.count LOOP
						print_exception_log('Error: ' || p ||
						   ' Array Index: ' || SQL%BULK_EXCEPTIONS(p).error_index ||
							 ' Message: ' || SQLERRM(-SQL%BULK_EXCEPTIONS(p).ERROR_CODE));
					  END LOOP;
					  ROLLBACK;
					  RAISE;
		       WHEN OTHERS
		       THEN
		          print_exception_log('  Unexpected Exception when Updating records #4:=>'||SQLERRM);
	              RAISE;
	        END;
		    l_line_tbl2.DELETE;
		END IF;
	  END LOOP;
	  CLOSE cur_parent_lines1;
	  print_log('    - Inside Incremental_Populate ');
	  print_info_log('   + Inside Incremental_Populate exit:=>'||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));
  EXCEPTION
     WHEN OTHERS
	 THEN
	    print_exception_log(' Unexpected Exception in the Incremental Load :=>'||SQLERRM);
  END;


PROCEDURE get_order_details_multi(
    p_from_date    IN VARCHAR2,
    p_order_number IN NUMBER )
IS
  l_quantity NUMBER;
  CURSOR cur_orders_process(p_quantity IN NUMBER)
  IS
    SELECT DISTINCT order_number
    FROM XXFT_RPRO_ORDER_CONTROL a ,
      oe_order_lines_all b
    WHERE a.processed_flag    = ANY('R','NR')
    AND b.line_id             = a.line_id
    AND b.ordered_quantity    < p_quantity
    AND b.line_category_code  ='ORDER'
    AND NVL(a.trx_type,'ORD') = 'ORD'
    AND a.order_number        = NVL(p_order_number,a.order_number)
  UNION
  SELECT DISTINCT order_number
  FROM XXFT_RPRO_ORDER_CONTROL a ,
    oe_order_lines_all b
  WHERE a.processed_flag    =ANY('R','NR')
  AND b.line_id             = a.line_id
  AND NVL(a.trx_type,'ORD') = 'ORD'
    --and b.ordered_quantity <100
  AND a.order_number = p_order_number ;
BEGIN
  --get_order_details_multi
  print_log('   + Inside Procedure get_order_details_multi');
  print_info_log('  + Inside Procedure get_order_details_multi:=>'||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));
  print_info_log('  + ORDER NUMBER  :=>'||p_order_number);
  print_output('  ORDER NUMBER : '||p_order_number);
  print_log('ORDER NUMBER : '||p_order_number);
  IF TO_NUMBER(TO_CHAR(SYSDATE,'HH24')) > 17 THEN
    l_quantity                         := 100000000;
  ELSE
    l_quantity := 300;
  END IF;
  --
  -- Calling Validate Data Load.
  --
  validate_intial_load(p_order_number);
  -- Added lately.
  FOR rec_orders_process IN cur_orders_process(l_quantity)
  LOOP
    --
    -- Calling Initial Populate
    --
    Initial_populate(rec_orders_process.order_number);
    --
    -- Incremental Populate.
    --
    Incremental_Populate(rec_orders_process.order_number);
    --
    -- Get Invoice details by Order.
    --
    get_invoice_details_new(rec_orders_process.order_number);
  END LOOP;
  print_info_log('  + Inside Procedure get_order_details_multi exit:=>'||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));
  lc_r_return_status:= 'S';
EXCEPTION
WHEN OTHERS THEN
  lc_r_return_status:= 'E';
  print_exception_log('Error get_order_details:'||SQLERRM);
END get_order_details_multi;

  PROCEDURE update_license_numbers_line(p_order_lineid IN NUMBER)
  IS
  CURSOR cur_lic
  IS
   select rownum rown,
        v.SALES_ORDER_LINE_ID,
        c.Attribute3
	  from
	      XXFT_RPRO_OE_ORDER_DETAILS_V v,
	      CSI_ITEM_INSTANCES c,
        mtl_system_items_b m
	where v.SALES_ORDER_LINE_ID     = c.LAST_OE_ORDER_LINE_ID
	  and v.ITEM_ID                 = m.INVENTORY_ITEM_ID
    and m.ORGANIZATION_ID         = v.SHIP_FROM_ORG_ID
	  and v.PRODUCT_CATEGORY        = 'SOFTWARE'
	  and m.COMMS_NL_TRACKABLE_FLAG = 'Y'
	  and v.SALES_ORDER_LINE_ID     = p_order_lineid
	ORDER BY rownum;


		l_line_id   NUMBER :=0;

		begin
		BEGIN
		 select DISTINCT min(SALES_ORDER_NEW_LINE_ID)
		 INTO l_line_id
		 from XXFT_RPRO_ORDER_DETAILS
		 where SALES_ORDER_LINE_ID = p_order_lineid;
		END;


		FOR i in cur_lic
		LOOP
				BEGIN
				UPDATE XXFT_RPRO_ORDER_DETAILS
				SET attribute45 = i.Attribute3
				WHERE
					SALES_ORDER_LINE_ID = i.SALES_ORDER_LINE_ID
				and SALES_ORDER_NEW_LINE_ID = l_line_id;
				l_line_id:=l_line_id+1;
				END;

		END LOOP;
    COMMIT;
    END update_license_numbers_line;

--
-- CR95 - To Keep polling to check for Grace Period Changes and push to Revpro.
--
PROCEDURE get_grace_period_event(
      errbuff           OUT VARCHAR2 ,
      retcode           OUT NUMBER,
      p_contract_number IN  VARCHAR2)
IS

  l_contract_number     VARCHAR2(240);
  l_start_date          DATE;
  l_end_date            DATE;
  l_registration_date   DATE;
  l_old_auto_start_date DATE;
  i                     NUMBER;
  j                     NUMBER;
  l_err_msg             VARCHAR2(2000);
  l_return_msg          VARCHAR2(2000);
  l_orig_order_exists   NUMBER;
  l_chr_id              OKC_K_HEADERS_ALL_B.id%type;
  l_orig_grace_period   NUMBER;
  l_sales_order         NUMBER;


TYPE contract_fc_rec_type
  IS
    RECORD
    (
      contract_number     VARCHAR2(240),
      process_flag        VARCHAR2(2),
      error_message       VARCHAR2(2000)
      );
  TYPE contract_fc_tbl_type
  IS
    TABLE OF contract_fc_rec_type INDEX BY BINARY_INTEGER;

TYPE contract_lines_rec_type
  IS
    RECORD
    (
      contract_number     VARCHAR2(240),
      grace_period        NUMBER,
      sc_start_date       DATE, -- Defect #3239
      sc_end_date         DATE, -- Defect #3239
      registration_dt     DATE,
      old_auto_start_dt   DATE,
      new_auto_start_dt   DATE,
      new_rule_start_dt   DATE,
      new_rule_end_dt     DATE,
      process_flag        VARCHAR2(2),
      error_message       VARCHAR2(2000)
      );

  TYPE contract_hdr_tbl_type
  IS
    TABLE OF contract_lines_rec_type INDEX BY BINARY_INTEGER;


    l_contract_hdr_tbl    contract_hdr_tbl_type;
    l_contract_fc_tbl     contract_fc_tbl_type;
    l_contract_fc_tbl1    contract_fc_tbl_type;


CURSOR cur_fc_contract_details
IS
  SELECT oracle_contractnumber,
          autostart_date,update_id
  FROM XXFT.XXFT_FR_AUTOSTARTDATEUPDATE a
  WHERE NVL(process_flag,'N') = ANY('N')
  AND update_id               =
    (SELECT MAX(update_id)
    FROM XXFT.XXFT_FR_AUTOSTARTDATEUPDATE b
    WHERE b.oracle_contractNumber= a.oracle_contractNumber
    AND NVL(process_flag,'N')    = ANY('N')
    )
  AND a.oracle_contractNumber = NVL(p_contract_number,a.oracle_contractNumber);

CURSOR cur_oracle_contract_details(p_contract_num IN VARCHAR2)
IS
  SELECT DISTINCT
    c.contract_number,
    b.start_date Start_Date,
    B.end_date End_Date,
    B.ATTRIBUTE3 Registration_Date,
    B.ATTRIBUTE4 Auto_Start_Date,
    c.id
  FROM okc_k_lines_b a ,
    OKC_K_LINES_B b,
    okc_k_headers_all_b c
  WHERE a.chr_id        = c.id
  AND b.cle_id          = a.id
  --AND b.sts_code        ='ACTIVE'
  AND c.contract_number = p_contract_num;



  CURSOR cur_contract_details
  IS
  SELECT RECORD_ID,
	        CONTRACT_NUMBER,
          NEW_AUTO_START_DT,
	        NEW_GRACE_PERIOD,
          NEW_RULE_START_DT,
          NEW_RULE_END_DT,
          OLD_AUTO_START_DT
    from XXFT.XXFT_RPRO_GRACE_PD_CHG_DETAILS
   WHERE processed_flag = 'N'
     AND CONTRACT_NUMBER = NVL(p_contract_number,CONTRACT_NUMBER);

  CURSOR cur_get_order(p_contract_number IN VARCHAR2)
  IS
  SELECT ooha.order_number
  FROM OKC_K_REL_OBJS REL,
    okc_k_headers_all_b okh,
    OE_ORDER_HEADERS_ALL ooha
  WHERE REL.jtot_object1_code LIKE 'OKX_ORDERHEAD%'
  AND REL.CHR_ID          = okh.ID
  AND REL.OBJECT1_ID1     = OOHA.HEADER_ID
  AND okh.contract_number = p_contract_number;

ex_dml_errors EXCEPTION;
  PRAGMA EXCEPTION_INIT(ex_dml_errors, -24381);

BEGIN
    retcode := 0;
    errbuff :=  null;
    print_info_log('  + Inside Procedure get_grace_period_event :=>'||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));
    FOR fc_contract_details_rec in cur_fc_contract_details
    LOOP
       l_contract_number     := NULL;
       l_start_date          := NULL;
       l_end_date            := NULL;
       l_registration_date   := NULL;
       l_old_auto_start_date := NULL;
       l_err_msg             := NULL;
       l_chr_id              := NULL;
       l_orig_grace_period   := NULL;

       OPEN cur_oracle_contract_details(fc_contract_details_rec.oracle_contractnumber);
       FETCH cur_oracle_contract_details INTO l_contract_number,l_start_date,l_end_date, l_registration_date, l_old_auto_start_date,l_chr_id;
       IF cur_oracle_contract_details%NOTFOUND
       THEN
          l_err_msg := ' Contract Number is  not found in Oracle:=>'||fc_contract_details_rec.oracle_contractnumber;
       END IF;
       CLOSE cur_oracle_contract_details;
       /*SELECT COUNT(1)
         INTO l_orig_order_exists
         FROM OKC_K_REL_OBJS
        WHERE chr_id          = l_chr_id
          AND JTOT_OBJECT1_CODE='OKX_ORDERHEAD';

       IF l_orig_order_exists = 0
       THEN
          l_err_msg := l_err_msg||' This contract is not originated from Sales Order :=>'||fc_contract_details_rec.oracle_contractnumber;
       END IF; */
       IF l_registration_date is not null and fc_contract_details_rec.autostart_date > l_registration_date
       THEN
          l_err_msg := l_err_msg||' Registration has already been completed for the contract :=>'||fc_contract_details_rec.oracle_contractnumber;
       END IF;
       IF l_err_msg is not null
       THEN
          i:= l_contract_fc_tbl.COUNT+1;
          l_contract_fc_tbl(i).contract_number := fc_contract_details_rec.oracle_contractnumber;
          l_contract_fc_tbl(i).process_flag    := 'E';
          l_contract_fc_tbl(i).error_message   := l_err_msg;
          continue;
       ELSIF trunc(l_old_auto_start_date) = trunc(fc_contract_details_rec.autostart_date)
       THEN
          i:= l_contract_fc_tbl.COUNT+1;
          l_contract_fc_tbl(i).contract_number := fc_contract_details_rec.oracle_contractnumber;
          l_contract_fc_tbl(i).process_flag    := 'P';
          l_contract_fc_tbl(i).error_message   := 'No Change in Auto Start Date hence no event to Revpro.Marking as Processed';
          continue;
       ELSIF trunc(l_start_date) = trunc(fc_contract_details_rec.autostart_date) -- ITS#552383.
       THEN
          i:= l_contract_fc_tbl.COUNT+1;
          l_contract_fc_tbl(i).contract_number := fc_contract_details_rec.oracle_contractnumber;
          l_contract_fc_tbl(i).process_flag    := 'P';
          l_contract_fc_tbl(i).error_message   := 'No Change in Auto Start Date hence no event to Revpro.Marking as Processed';
          continue;       
       ELSE
          i := l_contract_hdr_tbl.COUNT+1;
          l_contract_hdr_tbl(i).contract_number   := l_contract_number;
          l_contract_hdr_tbl(i).sc_start_date     := l_start_date;
          l_contract_hdr_tbl(i).sc_end_date       := l_end_date;
          l_contract_hdr_tbl(i).registration_dt   := l_registration_date;
          l_contract_hdr_tbl(i).old_auto_start_dt := l_old_auto_start_date;
          l_contract_hdr_tbl(i).new_auto_start_dt := fc_contract_details_rec.autostart_date;
          l_contract_hdr_tbl(i).grace_period      := round(fc_contract_details_rec.autostart_date-l_start_date); --l_orig_grace_period+ round(fc_contract_details_rec.autostart_date-l_old_auto_start_date,0);
          l_contract_hdr_tbl(i).new_rule_start_dt := l_start_date+l_contract_hdr_tbl(i).grace_period;
          l_contract_hdr_tbl(i).new_rule_end_dt   := l_end_date+l_contract_hdr_tbl(i).grace_period;
          l_contract_hdr_tbl(i).process_flag := 'N';
          l_contract_hdr_tbl(i).error_message := NULL;
          i:= l_contract_fc_tbl.COUNT+1;
          l_contract_fc_tbl(i).contract_number := fc_contract_details_rec.oracle_contractnumber;
          l_contract_fc_tbl(i).process_flag    := 'P';
          l_contract_fc_tbl(i).error_message   := NULL;

       END IF;
    END LOOP;

    FOR i in 1..l_contract_hdr_tbl.COUNT
    LOOP
       print_info_log('  -----------------');
       print_info_log('  contract_number  :=>'||l_contract_hdr_tbl(i).contract_number);
       print_info_log('  sc_start_date    :=>'||l_contract_hdr_tbl(i).sc_start_date);
       print_info_log('  sc_end_date      :=>'||l_contract_hdr_tbl(i).sc_end_date);
       print_info_log('  registration_dt  :=>'||l_contract_hdr_tbl(i).registration_dt);
       print_info_log('  old_auto_start_dt:=>'||l_contract_hdr_tbl(i).old_auto_start_dt);
       print_info_log('  new_auto_start_dt:=>'||l_contract_hdr_tbl(i).new_auto_start_dt);
       print_info_log('  grace_period     :=>'||l_contract_hdr_tbl(i).grace_period);
       print_info_log('  new_rule_start_dt:=>'||l_contract_hdr_tbl(i).new_rule_start_dt);
       print_info_log('  new_rule_end_dt  :=>'||l_contract_hdr_tbl(i).new_rule_end_dt);
       print_info_log('  process_flag     :=>'||l_contract_hdr_tbl(i).process_flag);
       print_info_log('  error_message    :=>'||l_contract_hdr_tbl(i).error_message);
       print_info_log('  -----------------');
    END LOOP;


    FOR X IN 1..l_contract_hdr_tbl.COUNT --..l_contract_hdr_tbl.LAST SAVE EXCEPTIONS
    LOOP
       BEGIN
          INSERT
          INTO XXFT.XXFT_RPRO_GRACE_PD_CHG_DETAILS
            (
              RECORD_ID,
               CONTRACT_NUMBER,
               NEW_AUTO_START_DT,
               NEW_GRACE_PERIOD,
               NEW_RULE_START_DT,
               NEW_RULE_END_DT,
               OLD_AUTO_START_DT,
               CREATED_BY,
               CREATION_DATE,
               LAST_UPDATED_BY,
               LAST_UPDATE_DATE,
               ERROR_MESSAGE,
               PROCESSED_FLAG,
               REQUEST_ID
               ) VALUES
              (
                XXFT_RPRO_GRACE_PD_CHG_DTL_S.NEXTVAL,
                l_contract_hdr_tbl(X).contract_number,
                l_contract_hdr_tbl(X).new_auto_start_dt,
                l_contract_hdr_tbl(X).grace_period,
                l_contract_hdr_tbl(X).new_rule_start_dt,
                l_contract_hdr_tbl(X).new_rule_end_dt,
                l_contract_hdr_tbl(X).old_auto_start_dt,
                gn_user_id,
                SYSDATE,
                gn_user_id,
                SYSDATE,
                NULL,
                'N',
                gn_request_id
              );
            COMMIT;
        EXCEPTION
        WHEN OTHERS THEN
          print_info_log('  Unexpected Exception :=>'||SQLERRM);
          RAISE;
        END;
      END LOOP;

    FOR i in 1..l_contract_fc_tbl.COUNT
    LOOP
       print_info_log('  -----------------');
       print_info_log('  contract_number:=>'||l_contract_fc_tbl(i).contract_number);
       print_info_log('  process_flag   :=>'||l_contract_fc_tbl(i).process_flag);
       print_info_log('  error message   :=>'||l_contract_fc_tbl(i).error_message);
       print_info_log('  -----------------');
    END LOOP;

       l_contract_fc_tbl1:= l_contract_fc_tbl;


   BEGIN
		  FORALL X in l_contract_fc_tbl1.FIRST..l_contract_fc_tbl1.LAST SAVE EXCEPTIONS
			UPDATE XXFT.XXFT_FR_AUTOSTARTDATEUPDATE
			   SET process_flag = l_contract_fc_tbl(X).process_flag
				  ,error_message    = l_contract_fc_tbl(X).error_message
				  ,lastupdate_date = SYSDATE
				  ,lastupdate_by  = gn_user_id
				  ,request_id       = gn_request_id
			 WHERE oracle_contractnumber  = l_contract_fc_tbl1(X).contract_number
         AND  NVL(process_flag,'N') ='N';
		   COMMIT;
	   EXCEPTION
		  WHEN ex_dml_errors THEN
			   --    l_error_count := SQL%BULK_EXCEPTIONS.count;
         print_log('Unexpected Exception when Updating XXFT_FR_AUTOSTARTDATEUPDATE: ' || SQL%BULK_EXCEPTIONS.count);
			   FOR p IN 1 .. SQL%BULK_EXCEPTIONS.count LOOP
			     print_log('Error: ' || p ||
						   ' Array Index: ' || SQL%BULK_EXCEPTIONS(p).error_index ||
							 ' Message: ' || SQLERRM(-SQL%BULK_EXCEPTIONS(p).ERROR_CODE));
         END LOOP;
			   ROLLBACK;
		  WHEN OTHERS
		  THEN
		     print_log('  Unexpected Exception when Updating XXFT_FR_AUTOSTARTDATEUPDATE:=>'||SQLERRM);
	         RAISE;
	   END;
    print_info_log('  Number of Records in Fc Table:=>'||l_contract_fc_tbl.COUNT);
    print_info_log('  Number of Records in Sc Table:=>'||l_contract_hdr_tbl.COUNT);

    --
    -- Call the API for all the new transactions from the table.
    --
    FOR contract_details_rec IN cur_contract_details
    LOOP
       l_sales_order := NULL;
       OPEN cur_get_order(contract_details_rec.contract_number);
       FETCH cur_get_order INTO l_sales_order;
       CLOSE cur_get_order;
       --
       -- Call the API for Revpro.
       --
       REVPRO.RPRO_FN_UPD_TRX_ATTR (
         ---input paramenters for where clause
         p_sales_order         => l_sales_order,
         p_sales_order_line_id => NULL,
         p_contract_number     => contract_details_rec.contract_number,
         ---input parameters for update on the lines
         p_grace_period       =>  contract_details_rec.NEW_GRACE_PERIOD,
         p_new_start_date    =>   NULL,
         p_new_end_date      =>   NULL,
         p_registration_date =>   NULL,
         p_region            =>   NULL,
         p_besp_or_vsoe      =>   NULL,
         p_created_by        =>   'ORACLE-ERP',
         P_reason            =>   'Change in Grace Period from Forticare',
         p_return_msg        => l_return_msg);

      UPDATE XXFT_RPRO_GRACE_PD_CHG_DETAILS
         SET processed_flag     = decode(l_return_msg, NULL,'P','E'),
             error_message    = l_return_msg,
             REQUEST_ID       = gn_request_id,
             last_update_Date = SYSDATE
        WHERE record_id = contract_details_rec.record_id;
        COMMIT;
    END LOOP;
    print_info_log('  + Inside Procedure get_grace_period_event exit:=>'||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));
EXCEPTION
   WHEN OTHERS
   THEN
      print_info_log('  Unexpected error in get_grace_period_event:=>'||SQLERRM);
      retcode := 2;
      errbuff := 'Unexpected Exception in the Procedure get_grace_period_event :=>'||SQLERRM;
END;

-- Bulk Processing of Orders first time.
PROCEDURE RPRO_BULK_ORDERS_PROCESS(
      errbuff      OUT VARCHAR2 ,
      retcode      OUT NUMBER,
      p_size_limit IN  NUMBER,
      p_from_date  IN  VARCHAR2, --523105
      p_to_date    IN  VARCHAR2  --523105
)
IS
ln_request_id NUMBER;
l_exists_cnt   NUMBER;

CURSOR cur_submit_orders
IS
SELECT order_number
FROM
  (SELECT distinct order_number
  FROM xxft_rpro_order_control
  WHERE 1           =1
  AND processed_flag= any('R','E','NR')
  AND trunc(creation_date) between NVL(to_date(p_from_date,'YYYY/MM/DD HH24:MI:SS'),trunc(creation_date)) and nvl(to_date(p_to_date,'YYYY/MM/DD HH24:MI:SS'),trunc(creation_date))
  ) tab --where --order_number=7010002281;
WHERE rownum< p_size_limit
  ;


BEGIN
  retcode:=0;
  errbuff := NULL;

  fnd_file.put_line(fnd_file.log,'  + Starting Parallel Processing of the Orders.');
  fnd_file.put_line(fnd_file.log,'  + Size Limit:=>'||p_size_limit);
  fnd_file.put_line(fnd_file.log,'  + From Date:=>'||p_from_date);
  fnd_file.put_line(fnd_file.log,'  + To   Date:=>'||p_to_date);
  FOR submit_orders_rec IN cur_submit_orders
  LOOP
    SELECT COUNT(1)
    INTO l_exists_cnt
    FROM fnd_concurrent_requests fcr,
      fnd_concurrent_programs fcp
    WHERE 1                         =1
    AND fcp.CONCURRENT_PROGRAM_NAME = 'FTNT_REVPRO_DATA_EXTRACT'
    AND fcr.concurrent_program_id   = fcp.concurrent_program_id
    AND argument2                   = TO_CHAR(submit_orders_rec.order_number)
    and fcr.phase_code IN ('P','R');
    --
    -- Checking if the request was already placed.
    --
    IF l_exists_cnt                 > 0 THEN
      fnd_file.put_line(fnd_file.log,'Request was already placed for the Order Number:=>'||submit_orders_rec.order_number);
      CONTINUE;
    END IF;
    ln_request_id := Fnd_Request.SUBMIT_REQUEST('AR' -- Application
                                                ,'FTNT_REVPRO_DATA_EXTRACT'                      -- Conc Program Short Name
                                                ,'FTNT RevPro Data Extract'                      --Description
                                                ,NULL                                            -- start time
                                                ,FALSE                                           -- sub request
                                                ,NULL                                            -- Parameter - Date
                                                ,submit_orders_rec.order_number                  -- parameter - Order Number
                                                ,'ALL'                                           -- Parameter - Transaction Type
                                                ,CHR (0)                                         -- End of arguments
                                                );
    COMMIT;
    fnd_file.put_line(fnd_file.log,'Request Id :=>'||ln_request_id||' For the Order Number:=>'||submit_orders_rec.order_number);
  END LOOP;
EXCEPTION
   WHEN OTHERS
   THEN
      errbuff := 'Unexpected exception in the process RPRO_BULK_ORDERS_PROCESS'||SQLERRM;
      retcode :=2;
END;

/*
-- Bulk Processing of Orders first time.
PROCEDURE RPRO_BULK_GP_PROCESS(
      errbuff      OUT VARCHAR2 ,
      retcode      OUT NUMBER,
      p_size_limit IN  NUMBER
)
IS
ln_request_id NUMBER;
l_exists_cnt   NUMBER;

CURSOR cur_submit_orders
IS
SELECT order_number
FROM
  (SELECT distinct order_number
  FROM xxft_rpro_order_control
  WHERE 1           =1
  AND processed_flag= any('R','E','NR')
  ) tab --where --order_number=7010002281;
WHERE rownum< p_size_limit;


BEGIN
  retcode:=0;
  errbuff := NULL;

  fnd_file.put_line(fnd_file.log,'  + Starting Parallel Processing of the Orders.');
  FOR submit_orders_rec IN cur_submit_orders
  LOOP
    SELECT COUNT(1)
    INTO l_exists_cnt
    FROM fnd_concurrent_requests fcr,
      fnd_concurrent_programs fcp
    WHERE 1                         =1
    AND fcp.CONCURRENT_PROGRAM_NAME = 'FTNT_REVPRO_DATA_EXTRACT'
    AND fcr.concurrent_program_id   = fcp.concurrent_program_id
    AND argument2                   = TO_CHAR(submit_orders_rec.order_number)
    and fcr.phase_code IN ('P','R');
    --
    -- Checking if the request was already placed.
    --
    IF l_exists_cnt                 > 0 THEN
      fnd_file.put_line(fnd_file.log,'Request was already placed for the Order Number:=>'||submit_orders_rec.order_number);
      CONTINUE;
    END IF;
    ln_request_id := Fnd_Request.SUBMIT_REQUEST('AR' -- Application
                                                ,'FTNT_REVPRO_DATA_EXTRACT'                      -- Conc Program Short Name
                                                ,'FTNT RevPro Data Extract'                      --Description
                                                ,NULL                                            -- start time
                                                ,FALSE                                           -- sub request
                                                ,NULL                                            -- Parameter - Date
                                                ,submit_orders_rec.order_number                  -- parameter - Order Number
                                                ,'ALL'                                           -- Parameter - Transaction Type
                                                ,CHR (0)                                         -- End of arguments
                                                );
    COMMIT;
    fnd_file.put_line(fnd_file.log,'Request Id :=>'||ln_request_id||' For the Order Number:=>'||submit_orders_rec.order_number);
  END LOOP;
EXCEPTION
   WHEN OTHERS
   THEN
      errbuff := 'Unexpected exception in the process RPRO_BULK_ORDERS_PROCESS'||SQLERRM;
      retcode :=2;
END;
*/
   --ITS 523169
   PROCEDURE validate_cm_trx(p_order_number IN NUMBER)
   IS
     l_error_msg     VARCHAR2(4000);
	   l_ch_error_msg  VARCHAR2(4000);
     ex_dml_errors   EXCEPTION;
     PRAGMA          EXCEPTION_INIT(ex_dml_errors, -24381);
     --
     -- Picking the CRA/NON CRA Credit Memos created for Historical Orders.
     --
     CURSOR cur_rma_lines
     IS
     SELECT xpoc.order_number
             ,xpoc.header_id
             ,xpoc.line_id
             ,oola.ordered_item
             ,RV.ITEM_NUMBER
             ,rv.item_desc
             ,rv.unit_list_price
             ,rv.ext_list_price
             ,rv.PRODUCT_FAMILY
             ,rv.PRODUCT_CATEGORY
             ,rv.PRODUCT_LINE
             ,rv.PRODUCT_CLASS
             ,rv.attribute15
             ,XXFT_RPRO_DATA_EXTRACT_PKG.get_rma_cost(xpoc.line_id)
             ,oola.ATTRIBUTE13
             ,oola.attribute14
             ,oola.attribute12
             ,ooha.PACKING_INSTRUCTIONS
             ,rv.attribute1 region
        FROM xxft_rpro_order_control xpoc
             ,oe_order_lines_all oola
             ,xxft_rpro_oe_return_details_v rv,oe_order_headers_all ooha
        WHERE xpoc.processed_flag= any('R','E')
          AND xpoc.trx_type='ORD'
          AND oola.line_id = xpoc.line_id
          AND rv.sales_order_line_id= oola.line_id
          and oola.flow_status_code='CLOSED'
          and oola.reference_line_id IS NULL and oola.ATTRIBUTE14 is null --and oola.attribute12 is not null
          and ooha.header_id = oola.header_id
          and xpoc.order_number = nvl(p_order_number,xpoc.order_number);
   BEGIN
      print_info_log('  + Inside Procedure validate_cm_trx exit:=>'||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));
      FOR rma_lines_rec IN cur_rma_lines
      LOOP
         l_error_msg := NULL;
         
        IF rma_lines_rec.product_family IS NULL THEN
          l_error_msg                   :=l_error_msg||':=:'||' Product Family';
        END IF;
        IF rma_lines_rec.product_category IS NULL THEN
          l_error_msg                     := l_error_msg||':=:'||'Product Category';
        END IF;
        IF rma_lines_rec.product_line IS NULL THEN
          l_error_msg                 := l_error_msg||':=:'||'Product Line';
        END IF;
        IF rma_lines_rec.product_class IS NULL THEN
          l_error_msg                  := l_error_msg||':=:'||'Product Class ';
        END IF;
        IF rma_lines_rec.attribute15 IS NULL THEN
          l_error_msg                := l_error_msg||':=:'||' Product Group';
        END IF;
        IF rma_lines_rec.unit_list_price IS NULL THEN
          l_error_msg                    := l_error_msg||':=:'||' Unit List Price';
        END IF;
        IF rma_lines_rec.region IS NULL THEN
          l_error_msg           := l_error_msg||':=:'||' Region'; -- added for the ticket #508676
        END IF;
        
        IF l_error_msg IS NOT NULL
        THEN
            l_error_msg := l_error_msg||' missing for Return Order Line :=>'||rma_lines_rec.item_number||CHR(13);
        END IF;
    
			UPDATE XXFT.XXFT_RPRO_ORDER_CONTROL
			   SET processed_flag = decode(sign(length(l_error_msg)-0),1,'E','R')
				  ,error_message    = l_error_msg
				  ,last_update_date = SYSDATE
				  ,last_updated_by  = gn_user_id
				  ,request_id       = gn_request_id
			 WHERE line_id       = rma_lines_rec.line_id
         AND trx_type ='ORD';
      COMMIT;
    END LOOP;
   
      print_info_log('  - Inside Procedure validate_cm_trx exit:=>'||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));
  EXCEPTION
     WHEN OTHERS
	   THEN
	    print_exception_log(' Unexpected exception in validate_cm_trx :=>'||SQLERRM);
		RAISE;
         
   END;
   
   --523169
   PROCEDURE extract_cm_trx(p_order_number IN NUMBER)
   IS
      l_trx_type         VARCHAR2(240);
      l_name             VARCHAR2(240);
      l_invoice_id       NUMBER;
      l_invoice_number   varchar2(40);
      l_invoice_date     DATE;
      l_invoice_line     VARCHAR2(20);
      l_invoice_qty      NUMBER;
      l_invoice_line_id  NUMBER;
      l_so_new_line_id   NUMBER;
      l_so_line_bacth_id NUMBER;
      l_batch_id         NUMBER;
      l_new_line_id      NUMBER;
      l_rma_cost         NUMBER;
      l_segment1         VARCHAR2(240);
      l_segment2         VARCHAR2(240);
      l_segment3         VARCHAR2(240);
      l_segment4         VARCHAR2(240);
      l_segment5         VARCHAR2(240);
      l_segment6         VARCHAR2(240);
      l_segment7         VARCHAR2(240);
      
      
     --
     -- Picking the CRA/NON CRA Credit Memos created for Historical Orders.
     --
     CURSOR cur_rma_lines
     IS
      SELECT RV.ITEM_ID ,
      RV.ITEM_NUMBER ,
      RV.ITEM_DESC ,
      RV.PRODUCT_FAMILY ,
      RV.PRODUCT_CATEGORY ,
      RV.PRODUCT_LINE ,
      RV.PRODUCT_CLASS ,
      RV.PRICE_LIST_NAME ,
      RV.UNIT_LIST_PRICE ,
      RV.UNIT_SELL_PRICE ,
      RV.EXT_SELL_PRICE
      --DECODE(invoice_rec.invoice_type,'CM',invoice_rec.extended_amount,lcu_rpro.EXT_SELL_PRICE) -- invoice_rec.extended_amount
      ,
      RV.EXT_LIST_PRICE ,
      RV.REC_AMT ,
      RV.DEF_AMT ,
      RV.COST_AMOUNT -- Cost Amount
      ,
      RV.COST_REC_AMT ,
      RV.COST_DEF_AMT ,
      RV.TRANS_CURR_CODE ,
      RV.EX_RATE ,
      RV.BASE_CURR_CODE ,
      RV.COST_CURR_CODE ,
      RV.COST_EX_RATE ,
      RV.RCURR_EX_RATE ,
      RV.ACCOUNTING_PERIOD ,
      RV.ACCOUNTING_RULE,
      RV.RULE_START_DATE,
      RV.RULE_END_DATE,
      RV.VAR_RULE_ID ,
      RV.PO_NUM ,
      RV.QUOTE_NUM ,
      RV.SALES_ORDER ,
      RV.SALES_ORDER_LINE ,
      RV.SALES_ORDER_ID ,
      RV.SALES_ORDER_LINE_ID,
      RV.SHIP_DATE ,
      RV.SO_BOOK_DATE ,
      RV.TRANS_DATE ,
      RV.SCHEDULE_SHIP_DATE ,
      RV.QUANTITY_SHIPPED ,
      RV.QUANTITY_ORDERED ,
      RV.QUANTITY_CANCELED ,
      RV.SALESREP_NAME ,
      RV.SALES_REP_ID ,
      RV.ORDER_TYPE ,
      RV.ORDER_LINE_TYPE ,
      RV.SERVICE_REFERENCE_LINE_ID
      --                  ,l_INVOICE_NUMBER
      --                  ,l_INVOICE_TYPE
      --                  ,l_INVOICE_LINE
      --  ,l_INVOICE_ID
      -- ,l_INVOICE_ID||'-'||l_SALES_ORDER_NEW_LINE_ID --invoice_rec.INVOICE_LINE_ID
      --  ,l_QUANTITY_CREDITED--1 --invoice_rec.QUANTITY_INVOICED
      --  ,l_INVOICE_DATE
      --  ,l_DUE_DATE
      ,
      NULL ,
      RV.CUSTOMER_ID ,
      RV.CUSTOMER_NAME ,
      RV.CUSTOMER_CLASS ,
      RV.BILL_TO_ID ,
      RV.BILL_TO_CUSTOMER_NAME ,
      RV.BILL_TO_CUSTOMER_NUMBER ,
      RV.BILL_TO_COUNTRY ,
      RV.SHIP_TO_ID ,
      RV.SHIP_TO_CUSTOMER_NAME ,
      RV.SHIP_TO_CUSTOMER_NUMBER ,
      RV.SHIP_TO_COUNTRY ,
      RV.BUSINESS_UNIT ,
      RV.ORG_ID ,
      RV.SOB_ID ,
      RV.SEC_ATTR_VALUE ,
      'Y' RETURN_FLAG,
      RV.CANCELLED_FLAG ,
      RV.FLAG_97_2 ,
      RV.PCS_FLAG ,
      RV.UNDELIVERED_FLAG ,
      RV.STATED_FLAG ,
      RV.ELIGIBLE_FOR_CV ,
      RV.ELIGIBLE_FOR_FV ,
      RV.DEFERRED_REVENUE_FLAG ,
      RV.NON_CONTINGENT_FLAG ,
      RV.UNBILLED_ACCOUNTING_FLAG ,
      RV.DEAL_ID ,
      RV.LAG_DAYS ,
      RV.ATTRIBUTE1 ,
      RV.ATTRIBUTE2 ,
      RV.ATTRIBUTE3 ,
      RV.ATTRIBUTE4 ,
      RV.ATTRIBUTE5 ,
      RV.ATTRIBUTE6 ,
      RV.ATTRIBUTE7 ,
      RV.ATTRIBUTE8 ,
      RV.ATTRIBUTE9 ,
      RV.ATTRIBUTE10 ,
      RV.ATTRIBUTE11 ,
      RV.ATTRIBUTE12 ,
      RV.ATTRIBUTE13 ,
      RV.ATTRIBUTE14 ,
      RV.ATTRIBUTE15 ,
      RV.ATTRIBUTE16 ,
      RV.ATTRIBUTE17 ,
      RV.ATTRIBUTE18 ,
      RV.ATTRIBUTE19 ,
      RV.ATTRIBUTE20 ,
      RV.ATTRIBUTE21 ,
      RV.ATTRIBUTE22 ,
      RV.ATTRIBUTE23 ,
      RV.ATTRIBUTE24 ,
      RV.ATTRIBUTE25 ,
      RV.ATTRIBUTE26 ,
      RV.ATTRIBUTE27 ,
      RV.ATTRIBUTE28 ,
      RV.ATTRIBUTE29 ,
      RV.ATTRIBUTE30 ,
      RV.ATTRIBUTE31 ,
      RV.ATTRIBUTE32 ,
      RV.ATTRIBUTE33 ,
      RV.ATTRIBUTE34 ,
      RV.ATTRIBUTE35 ,
      RV.ATTRIBUTE36 ,
      RV.ATTRIBUTE37 ,
      RV.ATTRIBUTE38 ,
      RV.ATTRIBUTE39 ,
      RV.ATTRIBUTE40 ,
      RV.ATTRIBUTE41 ,
      RV.ATTRIBUTE42 ,
      RV.ATTRIBUTE43 ,
      RV.ATTRIBUTE44 ,
      RV.ATTRIBUTE45 ,
      RV.ATTRIBUTE46 ,
      'RETURN' attribute47 ,
      RV.ATTRIBUTE48 ,
      RV.ATTRIBUTE49 ,
      RV.ATTRIBUTE50 ,
      RV.ATTRIBUTE51 ,
      RV.ATTRIBUTE52 ,
      RV.ATTRIBUTE53 ,
      RV.ATTRIBUTE54 ,
      RV.ATTRIBUTE55 ,
      RV.ATTRIBUTE56 ,
      RV.ATTRIBUTE57 ,
      RV.ATTRIBUTE58 ,
      RV.ATTRIBUTE59 ,
      RV.ATTRIBUTE60 ,
      RV.DATE1 ,
      RV.DATE2 ,
      RV.DATE3 ,
      RV.DATE4 ,
      RV.DATE5 ,
      RV.NUMBER1 ,
      --'l_INVOICE_LINE_ID' NUMBER2,
      RV.NUMBER3 ,
      RV.NUMBER4 ,
      RV.NUMBER5 ,
      RV.NUMBER6 ,
      RV.NUMBER7 ,
      RV.NUMBER8 ,
      RV.NUMBER9 ,
      RV.NUMBER10 ,
      RV.NUMBER11 ,
      RV.NUMBER12 ,
      RV.NUMBER13 ,
      RV.NUMBER14 ,
      NULL NUMBER15,
      RV.REV_ACCTG_SEG1 ,
      RV.REV_ACCTG_SEG2 ,
      RV.REV_ACCTG_SEG3 ,
      RV.REV_ACCTG_SEG4 ,
      RV.REV_ACCTG_SEG5 ,
      RV.REV_ACCTG_SEG6 ,
      RV.REV_ACCTG_SEG7 ,
      RV.REV_ACCTG_SEG8 ,
      RV.REV_ACCTG_SEG9 ,
      RV.REV_ACCTG_SEG10 ,
      RV.DEF_ACCTG_SEG1 ,
      RV.DEF_ACCTG_SEG2 ,
      RV.DEF_ACCTG_SEG3 ,
      RV.DEF_ACCTG_SEG4 ,
      RV.DEF_ACCTG_SEG5 ,
      RV.DEF_ACCTG_SEG6 ,
      RV.DEF_ACCTG_SEG7 ,
      RV.DEF_ACCTG_SEG8 ,
      RV.DEF_ACCTG_SEG9 ,
      RV.DEF_ACCTG_SEG10 ,
      RV.COGS_R_SEG1 ,
      RV.COGS_R_SEG2 ,
      RV.COGS_R_SEG3 ,
      RV.COGS_R_SEG4 ,
      RV.COGS_R_SEG5 ,
      RV.COGS_R_SEG6 ,
      RV.COGS_R_SEG7 ,
      RV.COGS_R_SEG8 ,
      RV.COGS_R_SEG9 ,
      RV.COGS_R_SEG10 ,
      RV.COGS_D_SEG1 ,
      RV.COGS_D_SEG2 ,
      RV.COGS_D_SEG3 ,
      RV.COGS_D_SEG4 ,
      RV.COGS_D_SEG5 ,
      RV.COGS_D_SEG6 ,
      RV.COGS_D_SEG7 ,
      RV.COGS_D_SEG8 ,
      RV.COGS_D_SEG9 ,
      RV.COGS_D_SEG10 ,
      RV.LT_DEFERRED_ACCOUNT ,
      RV.LT_DCOGS_ACCOUNT,
      RV.BOOK_ID ,
      RV.BNDL_CONFIG_ID
    FROM xxft_rpro_order_control xpoc ,
      oe_order_lines_all oola ,
      xxft_rpro_oe_return_details_v rv,
      oe_order_headers_all ooha
    WHERE xpoc.processed_flag   = ANY('R','E')
    AND xpoc.trx_type           ='ORD'
    AND oola.line_id            = xpoc.line_id
    AND rv.sales_order_line_id  = oola.line_id
    AND oola.flow_status_code   ='CLOSED'
    AND oola.reference_line_id IS NULL
    AND oola.ATTRIBUTE14       IS NULL --and oola.attribute12 is not null
    AND ooha.header_id          = oola.header_id
    AND xpoc.order_number       = NVL(p_order_number,xpoc.order_number)
    ORDER BY 1,2 ASC;
    
    CURSOR cur_get_cm_details(p_line_id IN NUMBER)
    IS
    SELECT trxt.type trx_type,
      trxt.name trx_name,
      trx.trx_number trx_number,
      trx.customer_trx_id,
      TO_CHAR(trxl.customer_trx_line_id),
      trx.trx_date,
      trxl.line_number,
      trxl.quantity_credited
    FROM ra_customer_trx_all trx,
      ra_cust_trx_types_all trxt,
      ra_customer_trx_lines_all trxl,
      oe_order_lines_all oola2
    WHERE oola2.line_id                     =p_line_id
    AND TO_CHAR(oola2.line_id)              = TRXL.interface_line_attribute6
    AND trx.CUSTOMER_TRX_ID                 = trxl.CUSTOMER_TRX_ID
    AND trx.cust_trx_type_id                = trxt.cust_trx_type_id
    AND trx.org_id                          = trxt.org_id
    AND trxt.name                          <> 'FTNT SPR Claim CM'
  --  AND trxl.PREVIOUS_CUSTOMER_TRX_LINE_ID IS NULL
    --AND trxl.attribute13                   IS NULL
    AND trxl.line_type                      = 'LINE'
    AND trxt.type                           = 'CM'
    and not exists (SELECT 1 FROM xxft_rpro_invoice_details where tran_type='CM' and invoice_id = trx.customer_trx_id and number2=trxl.customer_trx_line_id);
  
    CURSOR cur_get_actual_cogs(p_line_id IN NUMBER)
    IS
    SELECT A.BASE_TRANSACTION_VALUE,    
            segment1,
            segment2,
            segment3,
            segment4,
            segment5,
            segment6,
            segment7
      FROM  oe_order_lines_all oola,
            mtl_material_transactions mmt,
            MTL_TRANSACTION_ACCOUNTS a,
            mfg_lookups b,
            gl_code_combinations gcc1
     WHERE 1                       = 1 --order_number = any(7090000008)
       AND oola.line_id              = p_line_id
       AND mmt.trx_source_line_id (+)= oola.line_id
       AND a.transaction_id (+)      = mmt.transaction_id
       AND b.lookup_type             = 'CST_ACCOUNTING_LINE_TYPE'
       AND b.lookup_code             = a.ACCOUNTING_LINE_TYPE
      --AND b.meaning     (+)           = any('Cost of Goods Sold'--, 'Deferred Cost of Goods Sold')
       AND b.meaning                 =  ANY('Cost of Goods Sold', 'Deferred Cost of Goods Sold')
       AND gcc1.code_combination_id (+)= a.reference_account;
          
      CURSOR cur_get_segment3(P_BILL_TO_COUNTRY IN VARCHAR2)
      IS
      SELECT flv.attribute14
        FROM apps.fnd_lookup_values flv
      WHERE 1=1
      AND flv.lookup_code =P_BILL_TO_COUNTRY
      AND flv.lookup_type      = 'FTNT_REVPRO_REGION'
      and flv.enabled_flag ='Y'
      ;

   BEGIN
      print_info_log('  + Inside Procedure extract_cm_trx exit:=>'||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));
      
      FOR rec_rma_lines IN cur_rma_lines
      LOOP
          l_invoice_id      := null;
          l_trx_type        := null;
          l_invoice_line_id := null;
          l_invoice_date    := null;
          l_invoice_line    := null;
          l_invoice_qty     := null;
          
          --
          -- Derive the invoice details.
          --
          OPEN cur_get_cm_details(rec_rma_lines.sales_order_line_id);
          FETCH cur_get_cm_details INTO l_trx_type
                                        ,l_name
                                        ,l_invoice_number
                                        ,l_invoice_id
                                        ,l_invoice_line_id
                                        ,l_invoice_date
                                        ,l_invoice_line
                                        ,l_invoice_qty;
          CLOSE cur_get_cm_details;
          l_rma_cost         :=0;
          l_segment1         :=NULL;
          l_segment2         :=NULL;
          l_segment3         :=NULL;
          l_segment4         :=NULL;
          l_segment5         :=NULL;
          l_segment6         :=NULL;
          l_segment7         :=NULL;
          --
          -- Dervice the actual cost of goods sold for the order. ITS#524256
          --
          OPEN cur_get_actual_cogs(rec_rma_lines.sales_order_line_id);
          FETCH cur_get_actual_cogs INTO l_rma_cost,
                                          l_segment1,
                                          l_segment2,
                                          l_segment3,
                                          l_segment4,
                                          l_segment5,
                                          l_segment6,
                                          l_segment7;
          CLOSE cur_get_actual_cogs;
          
          IF l_segment1 is not null -- ITS#524256
          THEN
             rec_rma_lines.COGS_R_SEG1 :=l_segment1; 
             rec_rma_lines.COGS_D_SEG1 :=l_segment1;
             rec_rma_lines.COGS_R_SEG2 :=l_segment2;
             rec_rma_lines.COGS_D_SEG2 :=l_segment2;
             --
             -- Get Segment3 based on the bill to country.
             --
             OPEN cur_get_segment3(rec_rma_lines.BILL_TO_COUNTRY);
             FETCH cur_get_segment3 INTO l_segment3;
             CLOSE cur_get_segment3;             
             rec_rma_lines.COGS_R_SEG3 :=l_segment3;   
             rec_rma_lines.COGS_D_SEG3 :=l_segment3;
             rec_rma_lines.COGS_R_SEG4 :=l_segment4;
             rec_rma_lines.COGS_D_SEG4 :=l_segment4;
             rec_rma_lines.COGS_R_SEG5 :=l_segment5;
             rec_rma_lines.COGS_D_SEG5 :=l_segment5;
             rec_rma_lines.COGS_R_SEG6 :=l_segment6;
             rec_rma_lines.COGS_D_SEG6 :=l_segment6;
             rec_rma_lines.COGS_R_SEG7 :=l_segment7;
             rec_rma_lines.COGS_D_SEG7 :=l_segment7;
          END IF;
          
          IF l_invoice_id is not null
          THEN
             print_info_log('  + extract_cm_trx l_invoice_id:=>'||TO_CHAR(l_invoice_id));
             print_info_log('  + extract_cm_trx SALES_ORDER:=>'||TO_CHAR(rec_rma_lines.SALES_ORDER));
               l_batch_id    := XXFT_RPRO_LINE_BATCH_ID_S.NEXTVAL;
             l_new_line_id := NULL;
              SELECT --XXFT_RPRO_NEW_LINE_ID_S.NEXTVAL --ITS#569702
             XXFT_RPRO_LINE_BATCH_ID_S.NEXTVAL--, XXFT_RPRO_NEW_LINE_ID_S.NEXTVAL
             INTO --l_new_line_id,
             l_batch_id 
             FROM DUAL;
             
             INSERT INTO XXFT_RPRO_INVOICE_DETAILS
                  (
                   TRAN_TYPE
                  ,ITEM_ID
                  ,ITEM_NUMBER
                  ,ITEM_DESC
                  ,PRODUCT_FAMILY
                  ,PRODUCT_CATEGORY
                  ,PRODUCT_LINE
                  ,PRODUCT_CLASS
                  ,PRICE_LIST_NAME
                  ,UNIT_LIST_PRICE
                  ,UNIT_SELL_PRICE
                  ,EXT_SELL_PRICE
                  ,EXT_LIST_PRICE
                  ,REC_AMT
                  ,DEF_AMT
                  ,COST_AMOUNT
                  ,COST_REC_AMT
                  ,COST_DEF_AMT
                  ,TRANS_CURR_CODE
                  ,EX_RATE
                  ,BASE_CURR_CODE
                  ,COST_CURR_CODE
                  ,COST_EX_RATE
                  ,RCURR_EX_RATE
                  ,ACCOUNTING_PERIOD
                  ,ACCOUNTING_RULE
                  ,RULE_START_DATE
                  ,RULE_END_DATE
                  ,VAR_RULE_ID
                  ,PO_NUM
                  ,QUOTE_NUM
                  ,SALES_ORDER
                  ,SALES_ORDER_LINE
                  ,SALES_ORDER_ID
                  ,SALES_ORDER_LINE_ID
                  ,SALES_ORDER_NEW_LINE_ID
                  ,SALES_ORDER_LINE_BATCH_ID
                  ,SHIP_DATE
                  ,SO_BOOK_DATE
                  ,TRANS_DATE
                  ,SCHEDULE_SHIP_DATE
                  ,QUANTITY_SHIPPED
                  ,QUANTITY_ORDERED
                  ,QUANTITY_CANCELED
                  ,SALESREP_NAME
                  ,SALES_REP_ID
                  ,ORDER_TYPE
                  ,ORDER_LINE_TYPE
                  ,SERVICE_REFERENCE_LINE_ID
                  ,INVOICE_NUMBER
                  ,INVOICE_TYPE
                  ,INVOICE_LINE
                  ,INVOICE_ID
                  ,INVOICE_LINE_ID
                  ,QUANTITY_INVOICED
                  ,INVOICE_DATE
                  ,DUE_DATE
                  ,ORIG_INV_LINE_ID
                  ,CUSTOMER_ID
                  ,CUSTOMER_NAME
                  ,CUSTOMER_CLASS
                  ,BILL_TO_ID
                  ,BILL_TO_CUSTOMER_NAME
                  ,BILL_TO_CUSTOMER_NUMBER
                  ,BILL_TO_COUNTRY
                  ,SHIP_TO_ID
                  ,SHIP_TO_CUSTOMER_NAME
                  ,SHIP_TO_CUSTOMER_NUMBER
                  ,SHIP_TO_COUNTRY
                  ,BUSINESS_UNIT
                  ,ORG_ID
                  ,SOB_ID
                  ,SEC_ATTR_VALUE
                  ,RETURN_FLAG
                  ,CANCELLED_FLAG
                  ,FLAG_97_2
                  ,PCS_FLAG
                  ,UNDELIVERED_FLAG
                  ,STATED_FLAG
                  ,ELIGIBLE_FOR_CV
                  ,ELIGIBLE_FOR_FV
                  ,DEFERRED_REVENUE_FLAG
                  ,NON_CONTINGENT_FLAG
                  ,UNBILLED_ACCOUNTING_FLAG
                  ,DEAL_ID
                  ,LAG_DAYS
                  ,ATTRIBUTE1
                  ,ATTRIBUTE2
                  ,ATTRIBUTE3
                  ,ATTRIBUTE4
                  ,ATTRIBUTE5
                  ,ATTRIBUTE6
                  ,ATTRIBUTE7
                  ,ATTRIBUTE8
                  ,ATTRIBUTE9
                  ,ATTRIBUTE10
                  ,ATTRIBUTE11
                  ,ATTRIBUTE12
                  ,ATTRIBUTE13
                  ,ATTRIBUTE14
                  ,ATTRIBUTE15
                  ,ATTRIBUTE16
                  ,ATTRIBUTE17
                  ,ATTRIBUTE18
                  ,ATTRIBUTE19
                  ,ATTRIBUTE20
                  ,ATTRIBUTE21
                  ,ATTRIBUTE22
                  ,ATTRIBUTE23
                  ,ATTRIBUTE24
                  ,ATTRIBUTE25
                  ,ATTRIBUTE26
                  ,ATTRIBUTE27
                  ,ATTRIBUTE28
                  ,ATTRIBUTE29
                  ,ATTRIBUTE30
                  ,ATTRIBUTE31
                  ,ATTRIBUTE32
                  ,ATTRIBUTE33
                  ,ATTRIBUTE34
                  ,ATTRIBUTE35
                  ,ATTRIBUTE36
                  ,ATTRIBUTE37
                  ,ATTRIBUTE38
                  ,ATTRIBUTE39
                  ,ATTRIBUTE40
                  ,ATTRIBUTE41
                  ,ATTRIBUTE42
                  ,ATTRIBUTE43
                  ,ATTRIBUTE44
                  ,ATTRIBUTE45
                  ,ATTRIBUTE46
                  ,ATTRIBUTE47
                  ,ATTRIBUTE48
                  ,ATTRIBUTE49
                  ,ATTRIBUTE50
                  ,ATTRIBUTE51
                  ,ATTRIBUTE52
                  ,ATTRIBUTE53
                  ,ATTRIBUTE54
                  ,ATTRIBUTE55
                  ,ATTRIBUTE56
                  ,ATTRIBUTE57
                  ,ATTRIBUTE58
                  ,ATTRIBUTE59
                  ,ATTRIBUTE60
                  ,DATE1
                  ,DATE2
                  ,DATE3
                  ,DATE4
                  ,DATE5
                  ,NUMBER1
                  ,NUMBER2
                  ,NUMBER3
                  ,NUMBER4
                  ,NUMBER5
                  ,NUMBER6
                  ,NUMBER7
                  ,NUMBER8
                  ,NUMBER9
                  ,NUMBER10
                  ,NUMBER11
                  ,NUMBER12
                  ,NUMBER13
                  ,NUMBER14
                  ,NUMBER15
                  ,REV_ACCTG_SEG1
                  ,REV_ACCTG_SEG2
                  ,REV_ACCTG_SEG3
                  ,REV_ACCTG_SEG4
                  ,REV_ACCTG_SEG5
                  ,REV_ACCTG_SEG6
                  ,REV_ACCTG_SEG7
                  ,REV_ACCTG_SEG8
                  ,REV_ACCTG_SEG9
                  ,REV_ACCTG_SEG10
                  ,DEF_ACCTG_SEG1
                  ,DEF_ACCTG_SEG2
                  ,DEF_ACCTG_SEG3
                  ,DEF_ACCTG_SEG4
                  ,DEF_ACCTG_SEG5
                  ,DEF_ACCTG_SEG6
                  ,DEF_ACCTG_SEG7
                  ,DEF_ACCTG_SEG8
                  ,DEF_ACCTG_SEG9
                  ,DEF_ACCTG_SEG10
                  ,COGS_R_SEG1
                  ,COGS_R_SEG2
                  ,COGS_R_SEG3
                  ,COGS_R_SEG4
                  ,COGS_R_SEG5
                  ,COGS_R_SEG6
                  ,COGS_R_SEG7
                  ,COGS_R_SEG8
                  ,COGS_R_SEG9
                  ,COGS_R_SEG10
                  ,COGS_D_SEG1
                  ,COGS_D_SEG2
                  ,COGS_D_SEG3
                  ,COGS_D_SEG4
                  ,COGS_D_SEG5
                  ,COGS_D_SEG6
                  ,COGS_D_SEG7
                  ,COGS_D_SEG8
                  ,COGS_D_SEG9
                  ,COGS_D_SEG10
                  ,LT_DEFERRED_ACCOUNT
                  ,LT_DCOGS_ACCOUNT
                  ,REV_DIST_ID
                  ,COST_DIST_ID
                  ,BOOK_ID
                  ,BNDL_CONFIG_ID
                  ,so_last_update_date
                  ,so_line_creation_date
                  ,PROCESSED_FLAG
               --   ,ERROR_MESSAGE
                  ,CREATION_DATE
                  ,LAST_UPDATE_DATE                
                 )
              VALUES
                  (
                      'CM', --lcu_rpro.TRAN_TYPE
                      rec_rma_lines.ITEM_ID ,
                      rec_rma_lines.ITEM_NUMBER ,
                      rec_rma_lines.ITEM_DESC ,
                      rec_rma_lines.PRODUCT_FAMILY ,
                      rec_rma_lines.PRODUCT_CATEGORY ,
                      rec_rma_lines.PRODUCT_LINE ,
                      rec_rma_lines.PRODUCT_CLASS ,
                      rec_rma_lines.PRICE_LIST_NAME ,
                      rec_rma_lines.UNIT_LIST_PRICE ,
                      rec_rma_lines.UNIT_SELL_PRICE ,
                      rec_rma_lines.EXT_SELL_PRICE
                      --DECODE(invoice_rec.invoice_type,'CM',invoice_rec.extended_amount,lcu_rpro.EXT_SELL_PRICE) -- invoice_rec.extended_amount
                      ,
                      rec_rma_lines.EXT_LIST_PRICE ,
                      rec_rma_lines.REC_AMT ,
                      rec_rma_lines.DEF_AMT ,
                      rec_rma_lines.COST_AMOUNT -- Cost Amount
                      ,
                      rec_rma_lines.COST_REC_AMT ,
                      rec_rma_lines.COST_DEF_AMT ,
                      rec_rma_lines.TRANS_CURR_CODE ,
                      rec_rma_lines.EX_RATE ,
                      rec_rma_lines.BASE_CURR_CODE ,
                      rec_rma_lines.COST_CURR_CODE ,
                      rec_rma_lines.COST_EX_RATE ,
                      rec_rma_lines.RCURR_EX_RATE ,
                      rec_rma_lines.ACCOUNTING_PERIOD ,
                      rec_rma_lines.ACCOUNTING_RULE,
                      rec_rma_lines.RULE_START_DATE,
                      rec_rma_lines.RULE_END_DATE,
                      rec_rma_lines.VAR_RULE_ID ,
                      rec_rma_lines.PO_NUM ,
                      rec_rma_lines.QUOTE_NUM ,
                      rec_rma_lines.SALES_ORDER ,
                      rec_rma_lines.SALES_ORDER_LINE ,
                      rec_rma_lines.SALES_ORDER_ID ,
                      rec_rma_lines.SALES_ORDER_LINE_ID,
                      l_new_line_id ,
                      l_batch_id ,
                      rec_rma_lines.SHIP_DATE ,
                      rec_rma_lines.SO_BOOK_DATE ,
                      rec_rma_lines.TRANS_DATE ,
                      rec_rma_lines.SCHEDULE_SHIP_DATE ,
                      rec_rma_lines.QUANTITY_SHIPPED ,
                      rec_rma_lines.QUANTITY_ORDERED ,
                      rec_rma_lines.QUANTITY_CANCELED ,
                      rec_rma_lines.SALESREP_NAME ,
                      rec_rma_lines.SALES_REP_ID ,
                      rec_rma_lines.ORDER_TYPE ,
                      rec_rma_lines.ORDER_LINE_TYPE ,
                      rec_rma_lines.SERVICE_REFERENCE_LINE_ID
                      ,l_INVOICE_NUMBER
                      ,l_trx_TYPE
                      ,l_INVOICE_LINE
                      ,l_INVOICE_ID
                      ,to_char(l_INVOICE_LINE_ID) --l_INVOICE_ID||'-'||l_NEW_LINE_ID 
                      ,abs(l_invoice_qty)
                      ,l_INVOICE_DATE
                      ,NULL 
                      ,NULL 
                      ,rec_rma_lines.CUSTOMER_ID ,
                      rec_rma_lines.CUSTOMER_NAME ,
                      rec_rma_lines.CUSTOMER_CLASS ,
                      rec_rma_lines.BILL_TO_ID ,
                      rec_rma_lines.BILL_TO_CUSTOMER_NAME ,
                      rec_rma_lines.BILL_TO_CUSTOMER_NUMBER ,
                      rec_rma_lines.BILL_TO_COUNTRY ,
                      rec_rma_lines.SHIP_TO_ID ,
                      rec_rma_lines.SHIP_TO_CUSTOMER_NAME ,
                      rec_rma_lines.SHIP_TO_CUSTOMER_NUMBER ,
                      rec_rma_lines.SHIP_TO_COUNTRY ,
                      rec_rma_lines.BUSINESS_UNIT ,
                      rec_rma_lines.ORG_ID ,
                      rec_rma_lines.SOB_ID ,
                      rec_rma_lines.SEC_ATTR_VALUE ,
                      'N'--lcu_rpro.RETURN_FLAG --ITS#575305
                      ,
                      rec_rma_lines.CANCELLED_FLAG ,
                      rec_rma_lines.FLAG_97_2 ,
                      rec_rma_lines.PCS_FLAG ,
                      rec_rma_lines.UNDELIVERED_FLAG ,
                      rec_rma_lines.STATED_FLAG ,
                      rec_rma_lines.ELIGIBLE_FOR_CV ,
                      rec_rma_lines.ELIGIBLE_FOR_FV ,
                      rec_rma_lines.DEFERRED_REVENUE_FLAG ,
                      rec_rma_lines.NON_CONTINGENT_FLAG ,
                      rec_rma_lines.UNBILLED_ACCOUNTING_FLAG ,
                      rec_rma_lines.DEAL_ID ,
                      rec_rma_lines.LAG_DAYS ,
                      rec_rma_lines.ATTRIBUTE1 ,
                      rec_rma_lines.ATTRIBUTE2 ,
                      rec_rma_lines.ATTRIBUTE3 ,
                      rec_rma_lines.ATTRIBUTE4 ,
                      rec_rma_lines.ATTRIBUTE5 ,
                      rec_rma_lines.ATTRIBUTE6 ,
                      rec_rma_lines.ATTRIBUTE7 ,
                      rec_rma_lines.ATTRIBUTE8 ,
                      rec_rma_lines.ATTRIBUTE9 ,
                      rec_rma_lines.ATTRIBUTE10 ,
                      rec_rma_lines.ATTRIBUTE11 ,
                      rec_rma_lines.ATTRIBUTE12 ,
                      rec_rma_lines.ATTRIBUTE13 ,
                      rec_rma_lines.ATTRIBUTE14 ,
                      rec_rma_lines.ATTRIBUTE15 ,
                      rec_rma_lines.ATTRIBUTE16 ,
                      rec_rma_lines.ATTRIBUTE17 ,
                      rec_rma_lines.ATTRIBUTE18 ,
                      rec_rma_lines.ATTRIBUTE19 ,
                      rec_rma_lines.ATTRIBUTE20 ,
                      rec_rma_lines.ATTRIBUTE21 ,
                      rec_rma_lines.ATTRIBUTE22 ,
                      rec_rma_lines.ATTRIBUTE23 ,
                      rec_rma_lines.ATTRIBUTE24 ,
                      rec_rma_lines.ATTRIBUTE25 ,
                      rec_rma_lines.ATTRIBUTE26 ,
                      rec_rma_lines.ATTRIBUTE27 ,
                      rec_rma_lines.ATTRIBUTE28 ,
                      rec_rma_lines.ATTRIBUTE29 ,
                      rec_rma_lines.ATTRIBUTE30 ,
                      rec_rma_lines.ATTRIBUTE31 ,
                      rec_rma_lines.ATTRIBUTE32 ,
                      rec_rma_lines.ATTRIBUTE33 ,
                      rec_rma_lines.ATTRIBUTE34 ,
                      rec_rma_lines.ATTRIBUTE35 ,
                      rec_rma_lines.ATTRIBUTE36 ,
                      rec_rma_lines.ATTRIBUTE37 ,
                      rec_rma_lines.ATTRIBUTE38 ,
                      rec_rma_lines.ATTRIBUTE39 ,
                      rec_rma_lines.ATTRIBUTE40 ,
                      rec_rma_lines.ATTRIBUTE41 ,
                      rec_rma_lines.ATTRIBUTE42 ,
                      rec_rma_lines.ATTRIBUTE43 ,
                      rec_rma_lines.ATTRIBUTE44 ,
                      rec_rma_lines.ATTRIBUTE45 ,
                      rec_rma_lines.ATTRIBUTE46 ,
                      'RETURN'--lcu_rpro.ATTRIBUTE47
                      ,
                      rec_rma_lines.ATTRIBUTE48 ,
                      rec_rma_lines.ATTRIBUTE49 ,
                      rec_rma_lines.ATTRIBUTE50 ,
                      rec_rma_lines.ATTRIBUTE51 ,
                      rec_rma_lines.ATTRIBUTE52 ,
                      rec_rma_lines.ATTRIBUTE53 ,
                      rec_rma_lines.ATTRIBUTE54 ,
                      rec_rma_lines.ATTRIBUTE55 ,
                      rec_rma_lines.ATTRIBUTE56 ,
                      rec_rma_lines.ATTRIBUTE57 ,
                      rec_rma_lines.ATTRIBUTE58 ,
                      rec_rma_lines.ATTRIBUTE59 ,
                      rec_rma_lines.ATTRIBUTE60 ,
                      rec_rma_lines.DATE1 ,
                      rec_rma_lines.DATE2 ,
                      rec_rma_lines.DATE3 ,
                      rec_rma_lines.DATE4 ,
                      rec_rma_lines.DATE5 ,
                      rec_rma_lines.NUMBER1 ,
                      l_INVOICE_LINE_ID --lcu_rpro.NUMBER2
                      ,
                      NVL(rec_rma_lines.NUMBER3,0) ,
                      rec_rma_lines.NUMBER4 ,
                      rec_rma_lines.NUMBER5 ,
                      (rec_rma_lines.rule_end_date- rec_rma_lines.rule_start_date)+1 ,
                      rec_rma_lines.NUMBER7 ,
                      rec_rma_lines.NUMBER8 ,
                      rec_rma_lines.NUMBER9 ,
                      rec_rma_lines.NUMBER10 ,
                      rec_rma_lines.NUMBER11 ,
                      rec_rma_lines.NUMBER12 ,
                      rec_rma_lines.NUMBER13 ,
                      rec_rma_lines.NUMBER14 ,
                      gn_request_id -- lcu_rpro.NUMBER15      --Request Id.
                      ,
                      rec_rma_lines.REV_ACCTG_SEG1 ,
                      rec_rma_lines.REV_ACCTG_SEG2 ,
                      rec_rma_lines.REV_ACCTG_SEG3 ,
                      rec_rma_lines.REV_ACCTG_SEG4 ,
                      rec_rma_lines.REV_ACCTG_SEG5 ,
                      rec_rma_lines.REV_ACCTG_SEG6 ,
                      rec_rma_lines.REV_ACCTG_SEG7 ,
                      rec_rma_lines.REV_ACCTG_SEG8 ,
                      rec_rma_lines.REV_ACCTG_SEG9 ,
                      rec_rma_lines.REV_ACCTG_SEG10 ,
                      rec_rma_lines.DEF_ACCTG_SEG1 ,
                      rec_rma_lines.DEF_ACCTG_SEG2 ,
                      rec_rma_lines.DEF_ACCTG_SEG3 ,
                      rec_rma_lines.DEF_ACCTG_SEG4 ,
                      rec_rma_lines.DEF_ACCTG_SEG5 ,
                      rec_rma_lines.DEF_ACCTG_SEG6 ,
                      rec_rma_lines.DEF_ACCTG_SEG7 ,
                      rec_rma_lines.DEF_ACCTG_SEG8 ,
                      rec_rma_lines.DEF_ACCTG_SEG9 ,
                      rec_rma_lines.DEF_ACCTG_SEG10 ,
                      rec_rma_lines.COGS_R_SEG1 ,
                      rec_rma_lines.COGS_R_SEG2 ,
                      rec_rma_lines.COGS_R_SEG3 ,
                      rec_rma_lines.COGS_R_SEG4 ,
                      rec_rma_lines.COGS_R_SEG5 ,
                      rec_rma_lines.COGS_R_SEG6 ,
                      rec_rma_lines.COGS_R_SEG7 ,
                      rec_rma_lines.COGS_R_SEG8 ,
                      rec_rma_lines.COGS_R_SEG9 ,
                      rec_rma_lines.COGS_R_SEG10 ,
                      rec_rma_lines.COGS_D_SEG1 ,
                      rec_rma_lines.COGS_D_SEG2 ,
                      rec_rma_lines.COGS_D_SEG3 ,
                      rec_rma_lines.COGS_D_SEG4 ,
                      rec_rma_lines.COGS_D_SEG5 ,
                      rec_rma_lines.COGS_D_SEG6 ,
                      rec_rma_lines.COGS_D_SEG7 ,
                      rec_rma_lines.COGS_D_SEG8 ,
                      rec_rma_lines.COGS_D_SEG9 ,
                      rec_rma_lines.COGS_D_SEG10 ,
                      rec_rma_lines.LT_DEFERRED_ACCOUNT ,
                      rec_rma_lines.LT_DCOGS_ACCOUNT,
                      NULL ,
                      NULL ,
                      rec_rma_lines.BOOK_ID ,
                      rec_rma_lines.BNDL_CONFIG_ID ,
                      SYSDATE --lcu_rpro.so_last_update_date
                      ,
                      SYSDATE --lcu_rpro.so_line_creation_date
                      ,
                      'N' --lcu_rpro.PROCESSED_FLAG
                      --,lcu_rpro.ERROR_MESSAGE
                      ,
                      sysdate--lcu_rpro.CREATION_DATE
                      ,
                      sysdate--lcu_rpro.LAST_UPDATE_DATE
                   );
            --
            -- Update the Status back to the table.
            --
            UPDATE xxft_rpro_order_control 
               SET processed_flag='P'
                  ,error_message = NULL
			            ,last_update_date = SYSDATE
			            ,last_updated_by = fnd_global.user_id
			            ,request_id       = gn_request_id
		         WHERE line_id = rec_rma_lines.sales_order_line_id
               AND trx_type = 'ORD'
		           --AND status NOT IN ('CLOSED')
               ;
              
         END IF;
      END LOOP;
      
      print_info_log('  - Inside Procedure extract_cm_trx exit:=>'||TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS SSSSS.FF'));
   EXCEPTION
      WHEN OTHERS
	    THEN
	       print_exception_log(' Unexpected exception in extract_cm_trx :=>'||SQLERRM);
		     RAISE;
   END;
   
  
END XXFT_RPRO_DATA_PROCESS_PKG;
/
 show errors;