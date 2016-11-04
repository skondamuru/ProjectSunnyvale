--------------------------------------------------------
--  File created - Thursday-November-03-2016   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Package Body XXFT_POS_UPLOADLINK_INT_PKG
--------------------------------------------------------

  CREATE OR REPLACE PACKAGE BODY "APPS"."XXFT_POS_UPLOADLINK_INT_PKG" 
AS
  -- +=========================================================================
  -- ==================================================+
  -- |                               Fortinet, Inc.
  -- |
  -- |                                Sunnyvale, CA
  -- |
  -- +=========================================================================
  -- ==================================================+
  -- |
  -- |
  -- |Program Name   : XXFT_POS_UPLOADLINK_INT_PKG.plb
  -- |
  -- |
  -- |
  -- |Description    : Fortinet POS UPLOAD LINK interface Package.
  -- |
  -- |                 This package has been written to provide the
  -- |
  -- |                 functionality for uploading POS                        |
  -- |                                 |
  -- |
  -- |
  -- |Documents:
  -- |
  -- |==========
  -- |
  -- |    MD050_POS_UPLOAD_LINKING_INTERFACE_v1.0.docx
  -- |
  -- |    MD070_POS_UPLOAD_LINKING_INTERFACE_v1.0.docx
  -- |
  -- |
  -- |
  -- |Change Record:
  -- |
  -- |===============
  -- |
  -- |Version   Date         Author                 Remarks
  -- |
  -- |=======   ==========   ===================    ===========================
  -- ==================================================+
  -- |1.0       09-DEC-2015  Moreshwarr            Initial code version
  -- |
  -- |1.1    24-Dec-2015  Moreshwarr   Change the code remove the mapping for
  -- RESELLER_ADDRESS1,        |
  -- |           RESELLER_ADDRESS2 and ENDUSER_ADDRESS1,ENDUSER_ADDRESS2  for
  -- interface table   |                |
  -- |1.2    15-SEP-2016     Obireddy Kamireddy    Modified for the performance issue. ITS#543628
  -- |1.3    31-OCT-2016     Sathish Gopinathan    Coterm and Drop Ship. ITS#575976 and 599807
  -- +=========================================================================
  -- ==================================================+
  /*########################################
  Declaring Required Global variables
  ##########################################*/
  -- Define GLOBAL Variables
  --
  gn_user_id      NUMBER := apps.FND_GLOBAL.USER_ID;
  gn_login_id     NUMBER := APPS.FND_GLOBAL.LOGIN_ID;
  gn_request_id   NUMBER := fnd_profile.VALUE('CONC_REQUEST_ID');
  gn_resp_id      NUMBER := apps.fnd_global.resp_id;
  gn_resp_appl_id NUMBER := apps.fnd_global.resp_appl_id;
  gn_org_id       NUMBER := apps.fnd_global.org_id;
  --
  --Declaring Variables
  --
  ln_pl_accepted NUMBER:=0;
  ln_pl_total    NUMBER:=0;
  ln_pl_rejected NUMBER:=0;
  -- +=========================================================================
  -- +
  -- | Name            : qp_pos_upload_main_proc
  -- | Type            : Procedure
  -- | Description     : Main procedure to  load data from flat file to Staging
  -- table  and validate and process the records
  -- |                   and prints errors/success records
  -- |
  -- | Parameters      : p_path
  -- |
  -- |
  -- | Returns         : errbuf
  -- |                   retcode
  -- +=========================================================================
  -- +

  PROCEDURE qp_pos_upload_main_proc(
      errbuff OUT VARCHAR2 ,
      retcode OUT NUMBER ,
      p_path     IN VARCHAR2 ,
      p_run_mode IN VARCHAR2
      --  ,p_filename   IN VARCHAR2
    )
  IS
    lc_return_status VARCHAR2(1);
    p_batch_number   NUMBER;
    lc_status        VARCHAR2(1);
    lc_int_status    VARCHAR2(1);
    ln_batch_id      NUMBER;
    lc_link_status   VARCHAR2(1);
  BEGIN
    print_output(
    '***************** POS Upload link Interface *******************');
    print_log('***************** POS Upload link Interface *******************'
    );
    print_log('SQL Loader program to insert data from flat file ');

    /* Delete old Data */

    DELETE
    FROM XXFT.XXFT_PO_UPLOAD_LINKING_STG;

    DELETE
    FROM XXFT.XXFT_POS_UPLOAD_LINKING_INT;

    DELETE
    FROM XXFT.XXFT_POS_UPLOAD_TBL;
    COMMIT;
    --

    IF p_run_mode ='L' OR p_run_mode IS NULL THEN

      IF p_path IS NOT NULL THEN
        POS_data_loader(p_path,p_batch_number);

      END IF;

    END IF;
    PRINT_LOG('Step 1: End Loader Program  Return Batcha number '||
    p_batch_number);
    --
    print_log(
    'Procedure to validate data based on the filename and insert into custom interface table: '
    );

    IF p_run_mode ='AS' OR p_run_mode IS NULL THEN
      PRINT_LOG('Step2 : Start Calling  POS_ASSIGN_PROC Procedure....' );
      POS_ASSIGN_PROC(p_batch_number,lc_return_status);
      PRINT_LOG('Step 2: End POS_ASSIGN_PROC Procedure Return Batch number.. '
      || p_batch_number);
      print_log('POS_ASSIGN_PROC Status :'|| lc_return_status||
      ' and batch Number '||p_batch_number );
      PRINT_LOG('Step3 : Start Calling insert_serial_qty_proc Procedure' );
      --Calling split Procedure to split record based on qty and serial Number
      insert_serial_qty_proc(p_batch_number);
      PRINT_LOG('Step3 : End Calling insert_serial_qty_proc Procedure..' );

    END IF; --- Load from sql loader and insert into custom interface table
    --

    IF p_run_mode ='V' OR p_run_mode IS NULL THEN --Added if condition as
      -- discuss with satish  on 14-Jan-215
      print_log('Procedure to validate data in staging table: ');
      PRINT_LOG('Step4 : Start Calling validate_pos_upload_proc Procedure..' );
      validate_pos_upload_proc(p_batch_number,lc_status);
      PRINT_LOG('Step4 : End Calling validate_pos_upload_proc Procedure..' );
      Print_log('validate_pos_upload_proc  lc_status'||lc_status);

      IF lc_status = 'S' THEN
        Print_log(
        'Step 5:Start Calling  insert_interface_table Procedure.....lc_status:'
        ||lc_status);
        insert_interface_table(p_batch_number,ln_batch_id,lc_int_status);
        PRINT_LOG('Step5 : End Calling insert_interface_table Procedure..' );
        Print_log('insert_interface_table return Parameter ln_batch_id :' ||
        ln_batch_id||' and lc_int_status :' ||lc_int_status);

        IF lc_int_status ='S' THEN
          PRINT_LOG('Step6 : Start Calling pos_link_proc Procedure..' );
          pos_link_proc (ln_batch_id , lc_link_status);

          IF lc_link_status='S' THEN
            Print_log('POS Link Process Complete successfully');

          ELSE
            Print_log('POS Link Process error out....');

          END IF;
          PRINT_LOG('Step6 : End Calling pos_link_proc Procedure..' );

        ELSE
          Print_log('Interface Process is not complete successfully..!');

        END IF;

      ELSE
        --Calling Exception Program
        PRINT_LOG('Step7 : Start Calling pos_exception_report Procedure..' );
        pos_exception_report(NULL,'E');

      END IF;

    END IF;
    ---Calling insert_interface_table procedure
    --

    IF p_run_mode ='VI' THEN
      Print_log('Calling  insert_interface_table Procedure.....');
      insert_interface_table(p_batch_number,ln_batch_id,lc_int_status);
      Print_log('insert_interface_table return Parameter ln_batch_id :' ||
      ln_batch_id||' and lc_int_status :' ||lc_int_status);

      IF lc_int_status ='S' THEN
        pos_link_proc (ln_batch_id , lc_link_status);

        IF lc_link_status='S' THEN
          Print_log('Pos Link Process Complete successfully');

        ELSE
          Print_log('Pos Link Process error out....');

        END IF;

      ELSE
        Print_log('Interface Process is not complete successfully..!');

      END IF;

    END IF;
    --

    IF p_run_mode ='R' THEN
      --Calling Exception Report Procedure
      Print_log('Calling  pos_exception_report Procedure.....');
      pos_exception_report(NULL,'E');

    END IF;

  END qp_pos_upload_main_proc;
---
-- +===========================================================================
-- =================================================+
-- | Name            : POS_ASSIGN_PROC
-- |
-- | Type            : Procedure
-- |
-- | Description     : Procedure to validated the data based on lookup and
-- filename and assign  to custom interface table  |
-- |
-- |
-- | Parameters      : p_path
-- |
-- |                                                                    |
-- |
-- |
-- | Returns         :                                                       |
-- |
-- |
-- +===========================================================================
-- =================================================+

  PROCEDURE POS_ASSIGN_PROC(
      p_batch_number IN NUMBER,
      p_status OUT VARCHAR2)
  IS
    --Variable Declaration
    lc_lookup_type          VARCHAR2(100);
    lc_filename             VARCHAR2(240);
    lc_agreement_name       VARCHAR2(240);
    ld_date_invoiced        DATE ;
    ld_date_shipped         DATE ;
    lc_disti_cust_number    VARCHAR2(240);
    lc_disti_name           VARCHAR2(240);
    lc_end_cust_addr_2      VARCHAR2(240);
    lc_enduser_address1     VARCHAR2(240);
    lc_enduser_name         VARCHAR2(240);
    lc_end_cust_city        VARCHAR2(240);
    lc_end_cust_country     VARCHAR2(240);
    lc_end_cust_postal_code VARCHAR2(240);
    lc_end_cust_state       VARCHAR2(240);
    lc_end_cust_type        VARCHAR2(240);
    lc_ext_pos_amt          VARCHAR2(240);
    lc_ft_item_number       VARCHAR2(240);
    lc_invoice_line_number  VARCHAR2(240);
    lc_invoice_number       VARCHAR2(240);
    lc_msrp                 VARCHAR2(240);
    lc_order_line_number    VARCHAR2(240);
    lc_order_number         VARCHAR2(240);
    ld_pos_report_date      DATE ;
    ld_po_date              DATE ;
    lc_po_number            VARCHAR2(240);
    lc_purchase_price       VARCHAR2(240);
    lc_quantity             NUMBER ;
    lc_reseller_address1    VARCHAR2(240);
    lc_reseller_address2    VARCHAR2(240);
    lc_reseller_city        VARCHAR2(240);
    lc_reseller_country     VARCHAR2(240);
    lc_reseller_name        VARCHAR2(240);
    lc_reseller_postal_code VARCHAR2(240);
    lc_reseller_state       VARCHAR2(240);
    lc_selling_price        NUMBER ;
    lc_serial_number        VARCHAR2(240);
    lc_vendor_item_number   VARCHAR2(240);
    lc_return_status        VARCHAR2(3);
    lc_error_msg            VARCHAR2(4000);
    lc_column_name          VARCHAR2(30);
    lr_arrowus_rec ARROW_US_REC;
    ln_accept_cnt   NUMBER :=0;
    ln_rejected_cnt NUMBER :=0;
    ln_total_cnt    NUMBER :=0;
    --Cursor to get unique file name

    CURSOR filename_cur
    IS

      SELECT DISTINCT pou.filename,
        pou.batch_number
      FROM XXFT_PO_UPLOAD_LINKING_STG pou
      WHERE 1=1 --
        --and pou.filename =nvl(p_filename,pou.filename) --'FINETECUS'
      AND status          IN ('N','E')
      AND pou.BATCH_NUMBER = p_batch_number;
    --Cursor to get column mapping based on the lookup type

    CURSOR staging_cur (p_fname IN VARCHAR2)
    IS

      SELECT pos.*,
        pos.rowid
      FROM XXFT.XXFT_POS_UPLOAD_TBL pos
      WHERE filename       =p_fname
      AND pos.batch_number = p_batch_number;
  BEGIN

    FOR lcu_filename IN filename_cur --1
    LOOP
      --Variable Initialization
      lc_lookup_type  := NULL;
      lc_filename     :=NULL;
      ln_accept_cnt   := 0;
      ln_rejected_cnt := 0;
      ln_total_cnt    := 0;
      lc_filename     :=lcu_filename.filename;
      --Get the lookup in order to match the column Mapping
      BEGIN

        /* Sathish G */

        SELECT DISTINCT lkp.meaning
        INTO lc_lookup_type
        FROM XXFT_PO_UPLOAD_LINKING_STG pou,
          FND_LOOKUP_VALUES_VL lkp
        WHERE 1=1 --
          --AND pou.filename =nvl(p_filename,pou.filename) --'FINETECUS'
        AND status IN ('N','E')
        AND upper(pou.filename) LIKE '%'
          ||lkp.LOOKUP_CODE
          ||'%'
        AND lkp.LOOKUP_TYPE='FTNT_CHRM_UPLOADS_LKP'
        AND lkp.attribute1 ='POS' --Added on 12-Jan-15
        AND pou.filename   = lcu_filename.filename;

      EXCEPTION

      WHEN OTHERS THEN
        Print_log('Error While Geting lookup for filename :'||
        lcu_filename.filename);

      END;

      --get total number of records

      SELECT COUNT(*)
      INTO ln_total_cnt
      FROM XXFT_PO_UPLOAD_LINKING_STG pos
      WHERE filename =lcu_filename.filename
      AND status     ='N';
      BEGIN
        XXFT_POS_UPLOADLINK_INT_PKG.POS_LOAD_TBL(p_batch_number,lc_lookup_type,
        p_status);

      EXCEPTION

      WHEN OTHERS THEN
        Print_log('Error While calling :POS_LOAD_TBL '|| lcu_filename.filename
        ||lc_lookup_type);

      END;
      -- get column mapping

      FOR lcu_mapping IN staging_cur(lc_filename) --2
      LOOP
        --rec type variable initialization
        lr_arrowus_rec.DATE_INVOICED        :=NULL;
        lr_arrowus_rec.INVOICE_NUMBER       :=NULL;
        lr_arrowus_rec.RESELLER_NAME        :=NULL;
        lr_arrowus_rec.RESELLER_ADDRESS1    :=NULL;
        lr_arrowus_rec.RESELLER_ADDRESS2    :=NULL;
        lr_arrowus_rec.RESELLER_CITY        :=NULL;
        lr_arrowus_rec.RESELLER_STATE       :=NULL;
        lr_arrowus_rec.RESELLER_COUNTRY     :=NULL;
        lr_arrowus_rec.RESELLER_POSTAL_CODE :=NULL;
        lr_arrowus_rec.ENDUSER_NAME         :=NULL;
        lr_arrowus_rec.ENDUSER_ADDRESS1     :=NULL;
        lr_arrowus_rec.END_CUST_NAME        :=NULL;
        lr_arrowus_rec.END_CUST_ADDR_1      :=NULL;
        lr_arrowus_rec.END_CUST_ADDR_2      :=NULL;
        lr_arrowus_rec.END_CUST_CITY        :=NULL;
        lr_arrowus_rec.END_CUST_STATE       :=NULL;
        lr_arrowus_rec.END_CUST_POSTAL_CODE :=NULL;
        lr_arrowus_rec.END_CUST_COUNTRY     :=NULL;
        lr_arrowus_rec.END_CUST_TYPE        :=NULL;
        lr_arrowus_rec.VENDOR_ITEM_NUMBER   :=NULL;
        lr_arrowus_rec.FT_ITEM_NUMBER       :=NULL;
        lr_arrowus_rec.DISTI_NAME           :=NULL;
        lr_arrowus_rec.QUANTITY             :=NULL;
        lr_arrowus_rec.SERIAL_NUMBER        :=NULL;
        lr_arrowus_rec.PURCHASE_PRICE       :=NULL;
        lr_arrowus_rec.SELLING_PRICE        :=NULL;
        lr_arrowus_rec.MSRP                 :=NULL;
        lr_arrowus_rec.EXT_POS_AMT          :=NULL;
        lr_arrowus_rec.ORDER_LINE_NUMBER    :=NULL;
        lr_arrowus_rec.DISTI_CUST_NUMBER    :=NULL;
        lr_arrowus_rec.ORDER_NUMBER         :=NULL;
        lr_arrowus_rec.DATE_SHIPPED         :=NULL;
        lr_arrowus_rec.INVOICE_LINE_NUMBER  :=NULL;
        lr_arrowus_rec.AGREEMENT_NAME       :=NULL;
        lr_arrowus_rec.POS_REPORT_DATE      :=NULL;
        lr_arrowus_rec.PO_DATE              :=NULL;
        lr_arrowus_rec.PO_NUMBER            :=NULL;
        lc_error_msg                        :=NULL;
        lc_return_status                    :='N';
        BEGIN
          --        lc_return_status                    :='Y';
          --        lr_arrowus_rec.DATE_INVOICED        :=to_date(
          -- lcu_mapping.column1,'MM-DD-RRRR');--'DD-MM-RRRR');
          --        lr_arrowus_rec.INVOICE_NUMBER       :=lcu_mapping.column2;
          --        lr_arrowus_rec.RESELLER_NAME        :=lcu_mapping.column3;
          --        lr_arrowus_rec.RESELLER_ADDRESS1    :=lcu_mapping.column4;
          --        lr_arrowus_rec.RESELLER_CITY        :=lcu_mapping.column5;
          --        lr_arrowus_rec.RESELLER_STATE       :=lcu_mapping.column6;
          --        lr_arrowus_rec.RESELLER_POSTAL_CODE :=lcu_mapping.column7;
          --        lr_arrowus_rec.ENDUSER_NAME         :=lcu_mapping.column9;
          --        lr_arrowus_rec.ENDUSER_ADDRESS1     :=lcu_mapping.column10;
          --        lr_arrowus_rec.END_CUST_ADDR_2      :=lcu_mapping.column11;
          --        lr_arrowus_rec.END_CUST_CITY        :=lcu_mapping.column12;
          --        lr_arrowus_rec.END_CUST_STATE       :=lcu_mapping.column13;
          --        lr_arrowus_rec.END_CUST_POSTAL_CODE :=lcu_mapping.column14;
          --        lr_arrowus_rec.END_CUST_TYPE        :=lcu_mapping.column15;
          --        lr_arrowus_rec.VENDOR_ITEM_NUMBER   :=lcu_mapping.column16;
          --        lr_arrowus_rec.FT_ITEM_NUMBER       :=lcu_mapping.column17;
          --        lr_arrowus_rec.DISTI_NAME           :=NULL;
          --        lr_arrowus_rec.QUANTITY             :=to_number(
          -- lcu_mapping.column19);
          --        lr_arrowus_rec.SERIAL_NUMBER        :=lcu_mapping.column20;
          --        lr_arrowus_rec.PURCHASE_PRICE       :=REPLACE(
          -- lcu_mapping.column21,',','') ;
          --        lr_arrowus_rec.SELLING_PRICE        :=to_number(REPLACE(
          -- lcu_mapping.column22,',',''));
          --        lr_arrowus_rec.MSRP                 :=REPLACE(
          -- lcu_mapping.column23,',','') ;
          --        lr_arrowus_rec.EXT_POS_AMT          :=REPLACE(
          -- lcu_mapping.column24,',','') ;
          --        lr_arrowus_rec.ORDER_LINE_NUMBER    :=NULL;
          --        lr_arrowus_rec.DISTI_CUST_NUMBER    :=NULL;
          --        lr_arrowus_rec.ORDER_NUMBER         :=lcu_mapping.column27;
          --        lr_arrowus_rec.DATE_SHIPPED         :=NULL;
          --        lr_arrowus_rec.END_CUST_COUNTRY     :=NULL;
          --        lr_arrowus_rec.INVOICE_LINE_NUMBER  :=NULL;
          --        lr_arrowus_rec.AGREEMENT_NAME       :=lcu_mapping.column31;
          --        lr_arrowus_rec.POS_REPORT_DATE      :=NULL;
          --        lr_arrowus_rec.PO_DATE              :=NULL;
          --        lr_arrowus_rec.RESELLER_ADDRESS2    :=NULL;
          --        lr_arrowus_rec.PO_NUMBER            :=lcu_mapping.column35;
          --        lr_arrowus_rec.RESELLER_COUNTRY     :=NULL;
          lc_return_status              :='Y';
          lr_arrowus_rec.AGREEMENT_NAME := lcu_mapping.AGREEMENT_NAME;
          --                lr_arrowus_rec.DATE_INVOICED        := to_date(
          -- lcu_mapping.DATE_INVOICED,'MM-DD-RRRR');--'DD-MM-RRRR');
          --                lr_arrowus_rec.DATE_SHIPPED         := to_date(
          -- lcu_mapping.DATE_SHIPPED,'MM-DD-RRRR');--'DD-MM-RRRR');
          Print_log('Before lcu_mapping.DATE_INVOICED '||
          lcu_mapping.DATE_INVOICED);
          lr_arrowus_rec.DATE_INVOICED :=

          CASE
          WHEN REGEXP_LIKE(lcu_mapping.DATE_INVOICED, '\/') THEN
            TO_DATE(lcu_mapping.DATE_INVOICED, 'MM-DD-RRRR')
          WHEN REGEXP_LIKE(lcu_mapping.DATE_INVOICED, '\-') THEN
            TO_DATE(lcu_mapping.DATE_INVOICED, 'DD-MON-YY')
          END;
          Print_log('Before lcu_mapping.DATE_SHIPPED '||
          lcu_mapping.DATE_SHIPPED);
          lr_arrowus_rec.DATE_SHIPPED :=

          CASE
          WHEN REGEXP_LIKE(lcu_mapping.DATE_SHIPPED, '\/') THEN
            TO_DATE(lcu_mapping.DATE_SHIPPED, 'MM-DD-RRRR')
          WHEN REGEXP_LIKE(lcu_mapping.DATE_SHIPPED, '\-') THEN
            TO_DATE(lcu_mapping.DATE_SHIPPED, 'DD-MON-YY')
          END;
          lr_arrowus_rec.DISTI_CUST_NUMBER    := lcu_mapping.DISTI_CUST_NUMBER;
          lr_arrowus_rec.DISTI_NAME           := lcu_mapping.DISTI_NAME;
          lr_arrowus_rec.END_CUST_ADDR_2      := lcu_mapping.END_CUST_ADDR_2;
          lr_arrowus_rec.ENDUSER_ADDRESS1     := lcu_mapping.ENDUSER_ADDRESS1;
          lr_arrowus_rec.ENDUSER_NAME         := lcu_mapping.ENDUSER_NAME;
          lr_arrowus_rec.END_CUST_CITY        := lcu_mapping.END_CUST_CITY;
          lr_arrowus_rec.END_CUST_COUNTRY     := lcu_mapping.END_CUST_COUNTRY;
          lr_arrowus_rec.END_CUST_POSTAL_CODE :=
          lcu_mapping.END_CUST_POSTAL_CODE;
          lr_arrowus_rec.END_CUST_STATE := lcu_mapping.END_CUST_STATE;
          lr_arrowus_rec.END_CUST_TYPE  := lcu_mapping.END_CUST_TYPE;
          lr_arrowus_rec.EXT_POS_AMT    := to_number(REPLACE(REPLACE(
          lcu_mapping.EXT_POS_AMT,',',''),'$',''));
          lr_arrowus_rec.FT_ITEM_NUMBER      := lcu_mapping.FT_ITEM_NUMBER;
          lr_arrowus_rec.INVOICE_LINE_NUMBER := lcu_mapping.INVOICE_LINE_NUMBER
          ;
          lr_arrowus_rec.INVOICE_NUMBER := lcu_mapping.INVOICE_NUMBER;
          lr_arrowus_rec.MSRP           := to_number(REPLACE(REPLACE(
          lcu_mapping.MSRP,',',''),'$',''));
          lr_arrowus_rec.ORDER_LINE_NUMBER := lcu_mapping.ORDER_LINE_NUMBER;
          lr_arrowus_rec.ORDER_NUMBER      := lcu_mapping.ORDER_NUMBER;
          --                lr_arrowus_rec.POS_REPORT_DATE      := to_date(
          -- lcu_mapping.POS_REPORT_DATE,'MM-DD-RRRR');--'DD-MM-RRRR');
          --                lr_arrowus_rec.PO_DATE              := to_date(
          -- lcu_mapping.PO_DATE,'MM-DD-RRRR');--'DD-MM-RRRR');
          lr_arrowus_rec.POS_REPORT_DATE:=

          CASE
          WHEN REGEXP_LIKE(lcu_mapping.POS_REPORT_DATE, '\/') THEN
            TO_DATE(lcu_mapping.POS_REPORT_DATE, 'MM-DD-RRRR')
          WHEN REGEXP_LIKE(lcu_mapping.POS_REPORT_DATE, '\-') THEN
            TO_DATE(lcu_mapping.POS_REPORT_DATE, 'DD-MON-YY')
          END;
          lr_arrowus_rec.PO_DATE :=

          CASE
          WHEN REGEXP_LIKE(lcu_mapping.PO_DATE, '\/') THEN
            TO_DATE(lcu_mapping.PO_DATE, 'MM-DD-RRRR')
          WHEN REGEXP_LIKE(lcu_mapping.PO_DATE, '\-') THEN
            TO_DATE(lcu_mapping.PO_DATE, 'DD-MON-YY')
          END;
          lr_arrowus_rec.PO_NUMBER      := lcu_mapping.PO_NUMBER;
          lr_arrowus_rec.PURCHASE_PRICE := to_number(REPLACE(REPLACE(
          lcu_mapping.PURCHASE_PRICE,',',''),'$',''));
          lr_arrowus_rec.QUANTITY             := to_number(lcu_mapping.QUANTITY);
          lr_arrowus_rec.RESELLER_ADDRESS1    := lcu_mapping.RESELLER_ADDRESS1;
          lr_arrowus_rec.RESELLER_ADDRESS2    := lcu_mapping.RESELLER_ADDRESS2;
          lr_arrowus_rec.RESELLER_CITY        := lcu_mapping.RESELLER_CITY;
          lr_arrowus_rec.RESELLER_COUNTRY     := lcu_mapping.RESELLER_COUNTRY;
          lr_arrowus_rec.RESELLER_NAME        := lcu_mapping.RESELLER_NAME;
          lr_arrowus_rec.RESELLER_POSTAL_CODE :=
          lcu_mapping.RESELLER_POSTAL_CODE;
          lr_arrowus_rec.RESELLER_STATE := lcu_mapping.RESELLER_STATE;
          lr_arrowus_rec.SELLING_PRICE  := to_number(REPLACE(REPLACE(
          lcu_mapping.SELLING_PRICE,',',''),'$',''));
          lr_arrowus_rec.SERIAL_NUMBER      := lcu_mapping.SERIAL_NUMBER;
          lr_arrowus_rec.VENDOR_ITEM_NUMBER := lcu_mapping.VENDOR_ITEM_NUMBER;

        EXCEPTION

        WHEN OTHERS THEN
          lc_return_status:='E';
          lc_error_msg    :='Error while filed assignment'||'-'||SQLCODE||'-'||
          SQLERRM;
          Print_log(lc_error_msg ||lcu_mapping.filename||lcu_mapping.rowid);

          UPDATE XXFT_PO_UPLOAD_LINKING_STG
          SET STATUS         = 'E' ,
            ERROR_MSG        =lc_error_msg ,
            request_id       =gn_request_id ,
            last_update_date =sysdate ,
            last_updated_by  = gn_user_id
          WHERE record_id    = lcu_mapping.record_id
          AND filename       =lcu_mapping.filename
          AND batch_number   = p_batch_number;
          COMMIT;

        END;

        IF lc_return_status ='Y' THEN
          BEGIN

            -- Insert records into custom interface table
            --  Print_log('Inserting Mapping columns into
            -- XXFT_POS_UPLOAD_LINKING_INT Table');

            INSERT
            INTO XXFT_POS_UPLOAD_LINKING_INT
              (
                FILENAME ,
                DATE_INVOICED ,
                INVOICE_NUMBER ,
                RESELLER_NAME ,
                RESELLER_ADDRESS1 ,
                RESELLER_CITY ,
                RESELLER_STATE ,
                RESELLER_POSTAL_CODE ,
                ENDUSER_NAME ,
                ENDUSER_ADDRESS1 ,
                END_CUST_ADDR_2 ,
                END_CUST_CITY ,
                END_CUST_STATE ,
                END_CUST_POSTAL_CODE ,
                END_CUST_TYPE ,
                VENDOR_ITEM_NUMBER ,
                FT_ITEM_NUMBER ,
                DISTI_NAME ,
                QUANTITY ,
                SERIAL_NUMBER ,
                PURCHASE_PRICE ,
                SELLING_PRICE ,
                MSRP ,
                EXT_POS_AMT ,
                ORDER_LINE_NUMBER ,
                DISTI_CUST_NUMBER ,
                ORDER_NUMBER ,
                DATE_SHIPPED ,
                END_CUST_COUNTRY ,
                INVOICE_LINE_NUMBER ,
                AGREEMENT_NAME ,
                POS_REPORT_DATE ,
                PO_DATE ,
                RESELLER_ADDRESS2 ,
                PO_NUMBER ,
                RESELLER_COUNTRY ,
                STATUS ,
                ERROR_MSG ,
                REQUEST_ID ,
                LAST_UPDATE_DATE ,
                LAST_UPDATED_BY ,
                LAST_UPDATE_LOGIN ,
                CREATION_DATE ,
                CREATED_BY ,
                RECORD_ID,
                BATCH_NUMBER
              )
              VALUES
              (
                lcu_mapping.FILENAME ,
                lr_arrowus_rec.DATE_INVOICED ,
                lr_arrowus_rec.INVOICE_NUMBER ,
                lr_arrowus_rec.RESELLER_NAME ,
                lr_arrowus_rec.RESELLER_ADDRESS1 ,
                lr_arrowus_rec.RESELLER_CITY ,
                lr_arrowus_rec.RESELLER_STATE ,
                lr_arrowus_rec.RESELLER_POSTAL_CODE ,
                lr_arrowus_rec.ENDUSER_NAME ,
                lr_arrowus_rec.ENDUSER_ADDRESS1 ,
                lr_arrowus_rec.END_CUST_ADDR_2 ,
                lr_arrowus_rec.END_CUST_CITY ,
                lr_arrowus_rec.END_CUST_STATE ,
                lr_arrowus_rec.END_CUST_POSTAL_CODE ,
                lr_arrowus_rec.END_CUST_TYPE ,
                lr_arrowus_rec.VENDOR_ITEM_NUMBER ,
                lr_arrowus_rec.FT_ITEM_NUMBER ,
                lr_arrowus_rec.DISTI_NAME ,
                lr_arrowus_rec.QUANTITY ,
                lr_arrowus_rec.SERIAL_NUMBER ,
                lr_arrowus_rec.PURCHASE_PRICE ,
                lr_arrowus_rec.SELLING_PRICE ,
                lr_arrowus_rec.MSRP ,
                lr_arrowus_rec.EXT_POS_AMT ,
                lr_arrowus_rec.ORDER_LINE_NUMBER ,
                lr_arrowus_rec.DISTI_CUST_NUMBER ,
                lr_arrowus_rec.ORDER_NUMBER ,
                lr_arrowus_rec.DATE_SHIPPED ,
                lr_arrowus_rec.END_CUST_COUNTRY ,
                lr_arrowus_rec.INVOICE_LINE_NUMBER ,
                lr_arrowus_rec.AGREEMENT_NAME ,
                lr_arrowus_rec.POS_REPORT_DATE ,
                lr_arrowus_rec.PO_DATE ,
                lr_arrowus_rec.RESELLER_ADDRESS2 ,
                lr_arrowus_rec.PO_NUMBER ,
                lr_arrowus_rec.RESELLER_COUNTRY ,
                lcu_mapping.STATUS ,
                lcu_mapping.ERROR_MSG ,
                gn_request_id ,
                SYSDATE ,
                FND_GLOBAL.USER_ID ,
                FND_GLOBAL.LOGIN_ID ,
                SYSDATE ,
                FND_GLOBAL.USER_ID ,
                lcu_mapping.RECORD_ID,
                lcu_mapping.BATCH_NUMBER
              );
            COMMIT;
            lc_return_status:='S';

          EXCEPTION

          WHEN OTHERS THEN
            lc_return_status:='E';
            lc_error_msg    :=(
            'Error while inserting into XXFT_POS_UPLOAD_LINKING_INT'||SQLERRM);
            print_log(lc_error_msg);

          END;

          IF lc_return_status='S' THEN

            UPDATE XXFT_PO_UPLOAD_LINKING_STG
            SET STATUS         = 'S' ,
              ERROR_MSG        =lc_error_msg ,
              request_id       =gn_request_id ,
              last_update_date =sysdate ,
              last_updated_by  = gn_user_id
            WHERE record_id    = lcu_mapping.RECORD_ID
            AND filename       =lcu_mapping.filename
            AND batch_number   = p_batch_number;
            COMMIT;
            ln_accept_cnt :=ln_accept_cnt+1;

          ELSIF lc_return_status='E' THEN

            UPDATE XXFT_PO_UPLOAD_LINKING_STG
            SET STATUS         = 'E' ,
              ERROR_MSG        =lc_error_msg ,
              request_id       =gn_request_id ,
              last_update_date =sysdate ,
              last_updated_by  = gn_user_id
            WHERE record_id    = lcu_mapping.RECORD_ID
            AND filename       =lcu_mapping.filename
            AND batch_number   = p_batch_number;
            COMMIT;
            ln_rejected_cnt:=ln_rejected_cnt+1;

          END IF;

        END IF;
        --p_status:=lc_return_status;

      END LOOP;
      PRINT_OUTPUT(
      '***************XXFT_POS_UPLOAD_LINKING_INT Status Report*********************'
      );
      PRINT_OUTPUT('Process FileName             :'|| lc_filename);
      PRINT_OUTPUT('Total Number of Record Processed :'||ln_total_cnt);
      PRINT_OUTPUT('Total Number of Record Accepted  :'||ln_accept_cnt);
      PRINT_OUTPUT('Total Number of Record Rejected  :'||ln_rejected_cnt);
      PRINT_OUTPUT('***************Status Report*********************');

    END LOOP; --end 1
    p_status:='S';

  END POS_ASSIGN_PROC;
---
-- +===========================================================================
-- =================================================+
-- | Name            : pos_data_loader
-- |
-- | Type            : Procedure
-- |
-- | Description     : Procedure to load data from flat file to staging table
-- and update filename and request id in stg table   |
-- |
-- |
-- | Parameters      : p_path
-- |
-- |                                                                    |
-- |
-- |
-- | Returns         :                                                       |
-- |
-- |
-- +===========================================================================
-- =================================================+

  PROCEDURE pos_data_loader(
      p_path IN VARCHAR2,
      p_batch_number OUT NUMBER)
  IS
    ln_req_id           NUMBER;
    l_req_return_status BOOLEAN;
    lc_phase            VARCHAR2(100);
    lc_status           VARCHAR2(100);
    lc_dev_phase        VARCHAR2(100);
    lc_dev_status       VARCHAR2(100);
    lc_message          VARCHAR2(100);
    lc_file_name        VARCHAR2(240);
    ln_batch_number     NUMBER;
    v_layout            BOOLEAN;
  BEGIN
    BEGIN
      fnd_global.apps_initialize( user_id => gn_user_id ,resp_id => gn_resp_id
      ,resp_appl_id => gn_resp_appl_id);
      mo_global.set_policy_context('S',gn_org_id );
      ln_req_id := fnd_request.submit_request (application => 'XXFT', program
      => 'XXFTPOUPLOADLINKLDR', description =>
      'Fortinet POS Upload Linking Interface Loader Program', start_time =>
      SYSDATE, sub_request => FALSE, argument1 => p_path );
      print_log ('ln_req_id: '||ln_req_id);
      print_log('p_path :'||p_path);
      COMMIT;

    EXCEPTION

    WHEN OTHERS THEN
      print_log(
      'Fortinet POS Upload Linking Interface Loader Program error : ' ||
      SQLERRM);

    END;

    IF ln_req_id = 0 --1 if
      THEN
      print_log (
      'Request submission For SQL Loader program to load data into staging table is FAILED'
      ||SQLERRM);

    ELSE
      print_log (
      'Request submission For SQL Loader program to load data into staging table is SUCCESS'
      ||'Request id :'||ln_req_id);
      --Get File name
      BEGIN

        SELECT SUBSTR(p_path,INSTR(p_path,'/', -1)+1)
        INTO lc_file_name
        FROM dual;

      EXCEPTION

      WHEN OTHERS THEN
        lc_file_name:=p_path;

      END;

      IF lc_file_name IS NULL THEN
        lc_file_name  :=p_path;

      END IF;
      print_log('File Name :'||lc_file_name);
      /* UPDATE xxft_po_upload_linking_stg
      SET FILENAME = lc_file_name
      ,request_id =ln_req_id
      ,LAST_UPDATE_DATE =sysdate
      ,LAST_UPDATE_LOGIN = apps.fnd_global.conc_login_id
      ,CREATED_BY        =apps.fnd_global.user_id
      ,LAST_UPDATED_BY   =apps.fnd_global.user_id
      WHERE FILENAME = '$%FILE';
      COMMIT;
      print_log(' update complete at 1');
      */

    END IF; --end 1 if

    IF ln_req_id > 0 THEN --2 if
      LOOP
        BEGIN
          l_req_return_status := fnd_concurrent.wait_for_request (request_id =>
          ln_req_id ,interval => 2 ,max_wait => 60 ,phase => lc_phase ,status
          => lc_status ,dev_phase => lc_dev_phase ,dev_status => lc_dev_status
          ,MESSAGE => lc_message );
          COMMIT;
          --print_log('l_req_return_status '|| l_req_return_status);

        EXCEPTION

        WHEN OTHERS THEN
          print_log(
          'Fortinet POS Upload Linking Interface Loader Program wait_for_request error: '
          || SQLERRM);

        END;
        EXIT
      WHEN UPPER (lc_phase) = 'COMPLETED' OR UPPER (lc_status) IN ('CANCELLED',
        'ERROR', 'TERMINATED');

      END LOOP;

      IF UPPER (lc_phase) = 'COMPLETED' AND UPPER (lc_status) = 'ERROR' --3 if
        THEN
        print_log(
        'Fortinet POS Upload Linking Interface Loader Program completed in error:'
        ||ln_req_id ||' '||SQLERRM);

      ELSIF UPPER (lc_phase) = 'COMPLETED' AND UPPER (lc_status) = 'NORMAL' OR
        UPPER (lc_status)    ='WARNING' THEN
        print_log(
        'The Fortinet POS Upload Linking Interface Loader Program is successful with request id: '
        || ln_req_id);

        SELECT xxft_batch_number_s.NEXTVAL
        INTO ln_batch_number
        FROM dual;

        UPDATE xxft_po_upload_linking_stg --2
        SET FILENAME   = NVL(lc_file_name,'PATH') ,
          batch_number = ln_batch_number --Added  on 12-Jan-15 batch_number to
          -- track each file loader program run this is unique number
          ,
          request_id        = ln_req_id ,
          LAST_UPDATE_DATE  =sysdate ,
          LAST_UPDATE_LOGIN = apps.fnd_global.conc_login_id ,
          CREATED_BY        =apps.fnd_global.user_id ,
          LAST_UPDATED_BY   =apps.fnd_global.user_id
        WHERE --FILENAME      = '$FILE';
       -- creation_date = sysdate
       -- and
        FILENAME  is null;
        -- AND  batch_number is null;     --added on 12-Jan-15
        COMMIT;
        p_batch_number := ln_batch_number;
        print_log(' update complete at 2');

      END IF; -- end 3 if

    END IF; --end 2 if

  END pos_data_loader;
------------------------------------------------------------------------------
-- +===========================================================================
-- =================================================+
-- | Name            : pos_exception_report
-- |
-- | Type            : Procedure
-- |
-- | Description     : Procedure to display error and Success record based on
-- parameter value passed          |
-- |
-- |
-- | Parameters      : p_filename                        |
-- |     : p_status                                                     |
-- |                                                                    |
-- |
-- |
-- | Returns         :                                                       |
-- |
-- |
-- +===========================================================================
-- =================================================+

  PROCEDURE pos_exception_report(
      p_filename IN VARCHAR2,
      p_status   IN VARCHAR2)
  IS
    ln_req_id           NUMBER;
    l_req_return_status BOOLEAN;
    lc_phase            VARCHAR2(100);
    lc_status           VARCHAR2(100);
    lc_dev_phase        VARCHAR2(100);
    lc_dev_status       VARCHAR2(100);
    lc_message          VARCHAR2(100);
    lc_file_name        VARCHAR2(240);
    v_layout            BOOLEAN;
  BEGIN
    BEGIN
      fnd_global.apps_initialize( user_id => gn_user_id ,resp_id => gn_resp_id
      ,resp_appl_id => gn_resp_appl_id);
      mo_global.set_policy_context('S',gn_org_id);
      v_layout:=fnd_request.add_layout(template_appl_name =>'XXFT',
      template_code => 'XXFTPOSEXCEPTIONREPORT',
      ----template_language =>'EN',
      template_language =>'en', template_territory => NULL, output_format =>
      'EXCEL');
      ln_req_id := fnd_request.submit_request (application => 'XXFT', program
      => 'XXFTPOSEXCEPTIONREPORT', description =>
      ': Fortinet POS Upload Link Exception Report', start_time => SYSDATE,
      sub_request => FALSE, argument1 => p_filename, argument2 => p_status );
      print_log ('ln_req_id: '||ln_req_id);
      COMMIT;

    EXCEPTION

    WHEN OTHERS THEN
      print_log( 'Fortinet POS Upload Link Exception Report error : ' ||
      SQLERRM);

    END;

    IF ln_req_id = 0 --1 if
      THEN
      print_log ('Request submission For Exception Report is FAILED'||SQLERRM);

    ELSE
      print_log ('Request submission For Exception Report is SUCCESS'||
      'Request id :'||ln_req_id);

    END IF; --end 1 if

    IF ln_req_id > 0 THEN --2 if
      LOOP
        BEGIN
          l_req_return_status := fnd_concurrent.wait_for_request (request_id =>
          ln_req_id ,interval => 2 ,max_wait => 60 ,phase => lc_phase ,status
          => lc_status ,dev_phase => lc_dev_phase ,dev_status => lc_dev_status
          ,MESSAGE => lc_message );
          COMMIT;
          --print_log('l_req_return_status '|| l_req_return_status);

        EXCEPTION

        WHEN OTHERS THEN
          print_log(
          'Fortinet POS Upload Link Exception Report wait_for_request error: '
          || SQLERRM);

        END;
        EXIT
      WHEN UPPER (lc_phase) = 'COMPLETED' OR UPPER (lc_status) IN ('CANCELLED',
        'ERROR', 'TERMINATED');

      END LOOP;

      IF UPPER (lc_phase) = 'COMPLETED' AND UPPER (lc_status) = 'ERROR' --3 if
        THEN
        print_log(
        'Fortinet POS Upload Link Exception Report completed in error:'||
        ln_req_id ||' '||SQLERRM);

      ELSIF UPPER (lc_phase) = 'COMPLETED' AND UPPER (lc_status) = 'NORMAL'
        THEN
        print_log(
        'Fortinet POS Upload Link Exception Report is successful with request id: '
        || ln_req_id);

      END IF; -- end 3 if

    END IF; --end 2 if

  END pos_exception_report;
-- +=========================================================================+
-- | Name            : validate_pos_upload_proc
-- | Type            : Procedure
-- | Description     : Procedure to validate data and update the same with
-- change status from 'N' to 'V'
-- |                   in order to process further.
-- | Parameters      :
-- |
-- | Returns         :
-- |
-- +=========================================================================+

  PROCEDURE validate_pos_upload_proc(
      p_batch_number IN NUMBER,
      o_status OUT VARCHAR2)
  IS
    --Declare Variables
    ln_inventory_item_id NUMBER;
    ln_party_id          NUMBER;
    ln_party_site_id     NUMBER;
    ln_cust_account_id   NUMBER;
    lc_party_type        VARCHAR2(100);
    lc_error_msg         VARCHAR2(4000);
    lc_return_status1    VARCHAR2(3);
    lc_return_status     VARCHAR2(1);
    lc_posed_y_n         VARCHAR2(1);
    lc_period_y_n        VARCHAR2(1);
    ln_validated         NUMBER :=0;
    ln_rejected          NUMBER :=0;
    ln_total_records     NUMBER :=0;
    l_file               VARCHAR2(500);
    --

    CURSOR filemass_cur
    IS

      SELECT UNIQUE pou.filename,
        pou.DISTI_CUST_NUMBER
      FROM XXFT_POS_UPLOAD_LINKING_INT pou
      WHERE 1=1 --
        --AND pou.filename ='FINETECUS' --'TECHDATAUS'
      AND batch_number = p_batch_number
      AND status      IN ('N','E');
    --define cursor

    CURSOR posupmass_cur(p_filename IN VARCHAR2,p_disti_number IN VARCHAR2)
    IS

      SELECT pos.* ,
        pos.rowid
      FROM XXFT_POS_UPLOAD_LINKING_INT pos
      WHERE 1               =1
      AND FILENAME          = p_filename
      AND batch_number      = p_batch_number
      AND DISTI_CUST_NUMBER = p_disti_number
      AND pos.quantity      =1
      OR pos.quantity       = -1
      AND pos.STATUS         IN ('N','E');
    --Cursor to get unique file name

    CURSOR filename_cur
    IS

      SELECT UNIQUE pou.filename
      FROM XXFT_POS_UPLOAD_LINKING_INT pou
      WHERE 1=1 --
        --AND pou.filename ='FINETECUS' --'TECHDATAUS'
      AND batch_number = p_batch_number
      AND status      IN ('N','E');
    --define cursor

    CURSOR posupload_cur(p_filename IN VARCHAR2)
    IS

      SELECT pos.* ,
        pos.rowid
      FROM XXFT_POS_UPLOAD_LINKING_INT pos
      WHERE 1          =1
      AND FILENAME     = p_filename
      AND batch_number = p_batch_number
      AND pos.quantity =1
      OR pos.quantity  = -1
      AND pos.STATUS    IN ('N','E');
  BEGIN

    SELECT DISTINCT FILENAME
    INTO l_file
    FROM XXFT_POS_UPLOAD_LINKING_INT
    WHERE batch_number = p_batch_number;

    IF l_file LIKE '%FORTINET_POS%' THEN

      FOR lcu_file IN filemass_cur
      LOOP
        ln_total_records   :=0;
        ln_validated       :=0;
        ln_rejected        :=0;
        ln_party_id        :=NULL;
        ln_party_site_id   :=NULL;
        ln_cust_account_id :=NULL;
        lc_party_type      :=NULL;
        lc_return_status   :=NULL;
        --
        BEGIN

          --

          SELECT COUNT(*)
          INTO ln_total_records
          FROM XXFT_POS_UPLOAD_LINKING_INT pos
          WHERE 1          =1
          AND FILENAME     = lcu_file.filename --'FINETECUS'
          AND pos.quantity =1
          OR pos.quantity  = -1
          AND pos.STATUS   ='N';
          --

        END;
        --Derive customer Account id
        --ln_cust_account_id := get_customer_id(lcu_file.filename ,'POS');
        BEGIN

          SELECT cust_account_id
          INTO ln_cust_account_id
          FROM hz_cust_accounts_all
          WHERE SALES_CHANNEL_CODE = 'CHANNEL_PARTNER'
          and account_number = lcu_file.DISTI_CUST_NUMBER;

        EXCEPTION

        WHEN NO_DATA_FOUND THEN
          print_log('Mass update customer not found '||
          lcu_file.DISTI_CUST_NUMBER);
          ln_cust_account_id := NULL;
          PRINT_OUTPUT('Customer Incorrect or Not a Valid Channel Partner  : # '||
          lcu_file.DISTI_CUST_NUMBER);

        WHEN TOO_MANY_ROWS THEN
          print_log('Mass update customer not found '||
          lcu_file.DISTI_CUST_NUMBER);
          ln_cust_account_id := NULL;
          PRINT_OUTPUT('Customer Incorrect or Not a Valid Channel Partner  : # '||
          lcu_file.DISTI_CUST_NUMBER);

        WHEN OTHERS THEN
          print_log('Mass update customer not found '||
          lcu_file.DISTI_CUST_NUMBER||' '||SQLCODE||' '||SQLERRM);
          ln_cust_account_id := NULL;
          PRINT_OUTPUT('Customer Incorrect or Not a Valid Channel Partner  : # '||
          lcu_file.DISTI_CUST_NUMBER);

        END;

        IF ln_cust_account_id IS NULL THEN
          lc_return_status    :='E';
          --lc_error_msg:= '1. Error while geting customer account id from file
          -- '||lcu_pos.filename;
          Print_log('1. Error while geting customer account id from file '||
          lcu_file.filename);

        ELSE
          lc_return_status:='Y';
          ln_party_id     := get_party_id(ln_cust_account_id);

          IF ln_party_id    IS NULL THEN
            lc_return_status:='E';
            PRINT_LOG('1. Party id does not found for customer id'||
            ln_cust_account_id);

          END IF;
          ln_party_site_id := get_party_site_id( ln_cust_account_id,'SHIP_TO');

          IF ln_party_site_id IS NULL THEN
            --lc_return_status:='E';
            PRINT_LOG('1. Party site id does not found  for customer id'||
            ln_cust_account_id);

          END IF;
          lc_party_type :=get_party_type(ln_cust_account_id);

          IF lc_party_type  IS NULL THEN
            lc_return_status:='E';
            PRINT_LOG('1. Party type does not found for customer id'||
            ln_cust_account_id);

          END IF;
          Print_log('1. Customer Account Id  '||ln_cust_account_id ||
          'For customer ' ||lcu_file.filename);
          Print_log('1. ln_party_id       '||ln_party_id);
          Print_log('1. ln_party_site_id  '||ln_party_site_id);
          Print_log('1. lc_party_type      '||lc_party_type);

        END IF;
        Print_log('lc_return_status =' ||lc_return_status);
        --
        Print_log('****Start Validation for File: '||lcu_file.filename||
        '******');

        IF lc_return_status ='Y' THEN

          FOR lcu_pos IN posupmass_cur(lcu_file.filename,
          lcu_file.DISTI_CUST_NUMBER)
          LOOP
            ln_inventory_item_id :=NULL;
            lc_party_type        :=NULL;
            lc_return_status1    :='Y';
            lc_error_msg         :=NULL;
            lc_posed_y_n         :=NULL;
            lc_period_y_n        :=NULL;
            -- Print_log('Start Validation for file '||lcu_file.filename);
            --1 Validation for item - Look for item in the EBS item master
            --Get item id from EBS Item Master

            IF lcu_pos.ft_item_number IS NOT NULL THEN
              ln_inventory_item_id    := get_item_id(lcu_pos.ft_item_number);
              --

              IF ln_inventory_item_id IS NULL AND lcu_pos.serial_number IS NOT
                NULL THEN
                --lc_error_msg :='Valid2: Item does not exists in EBS item
                -- master '||' '||lcu_pos.ft_item_number;
                Print_log('Valid2: Item does not exists in EBS item master'||
                ' '||lcu_pos.ft_item_number);
                -- Checking item based on serial Number
                ln_inventory_item_id:=get_serial_item_id(lcu_pos.serial_number)
                ;

                IF ln_inventory_item_id IS NULL THEN
                  lc_error_msg          :=
                  'Valid3: Item does not exists for serial item  '||
                  lcu_pos.serial_number;
                  Print_log('Valid3 :  Item does not exists for serial item  '
                  ||lcu_pos.serial_number);
                  -- check the item in ozf_code
                  ln_inventory_item_id:=get_item_frm_ozf_code(
                  ln_cust_account_id,lcu_pos.ft_item_number);

                END IF;

              END IF;

            ELSIF lcu_pos.serial_number IS NOT NULL THEN
              ln_inventory_item_id      :=get_serial_item_id(
              lcu_pos.serial_number);

            END IF;
            --

            IF ln_inventory_item_id IS NULL THEN
              lc_return_status1     :='E';
              lc_error_msg          := lc_error_msg||
              'Valid4: Error while getting Item from Oracle for item '||
              lcu_pos.ft_item_number;
              Print_log( 'valid4: Error while getting item id  for item '||
              lcu_pos.ft_item_number ||' and for serial_number'||
              lcu_pos.serial_number);

            ELSE
              Print_log('Valid4:Item id for Item  ' ||lcu_pos.ft_item_number||
              ' ' ||'or serial Number'||lcu_pos.serial_number ||'is :'||
              'ln_inventory_item_id :'||ln_inventory_item_id);

            END IF;
            --
            -- Validate  if the line is already POS'ed or NOT

            IF lcu_pos.quantity > 0 THEN
              lc_posed_y_n     := get_line_posed(lcu_pos.serial_number);

            END IF;

            IF lc_posed_y_n     ='Y' THEN
           --   lc_return_status1:='E'; -- Removed based on business
              lc_error_msg     := lc_error_msg||
              'Valid5: Line Already Posed for serial Number : '||
              lcu_pos.serial_number||'!!!';
              Print_log('Valid5: Line Already Posed for Serial Number : '||
              lcu_pos.serial_number||'!!!');

            ELSE
              Print_log('Valid5: Line is ready to Posed...for Serial Number :'
              ||lcu_pos.serial_number);

            END IF;
            --Check for Period Open or NOT
            lc_period_y_n:=get_period_status(TRUNC(NVL(lcu_pos.date_invoiced,
            lcu_pos.date_shipped)));

            IF lc_period_y_n ='Y' THEN
              Print_log('valid6: Period is Open for the date '||
              lcu_pos.date_invoiced ||' :'||lc_period_y_n);

            ELSE
              lc_return_status1 :='E';
              lc_error_msg      := lc_error_msg||
              'Valid6: Period is not open for the date '||lcu_pos.date_invoiced
              ||'!!!';

            END IF;
            --updating custom interface table if all validation get success...

            IF lc_return_status1 = 'Y' THEN

              UPDATE XXFT_POS_UPLOAD_LINKING_INT
              SET inventory_item_id =ln_inventory_item_id ,
                cust_account_id     =ln_cust_account_id ,
                party_id            =ln_party_id ,
                party_site_id       =ln_party_site_id ,
                party_type          =lc_party_type ,
                last_update_date    =sysdate ,
                last_updated_by     =gn_user_id ,
                last_update_login   =gn_login_id ,
                request_id          =gn_request_id ,
                status              ='V' ,
                error_msg           = 'Successfully Validated ....!!!'
              WHERE record_id       = lcu_pos.record_id --Added on 12-Jan-15
              AND filename          =lcu_pos.filename
              AND status           <> 'S';
              COMMIT;
              ln_validated :=ln_validated+1;

            ELSIF lc_return_status1 = 'E' THEN

              --

              UPDATE XXFT_POS_UPLOAD_LINKING_INT
              SET status      ='E' ,
                ERROR_MSG     = lc_error_msg
              WHERE record_id = lcu_pos.record_id --ROWID = lcu_pos.ROWID
              AND filename    =lcu_pos.filename
              AND status     <> 'S';
              COMMIT;
              ln_rejected :=ln_rejected+1;
              PRINT_OUTPUT('Validation Failed because : '||
              lc_error_msg);

            END IF;

          END LOOP;

        END IF;
        --Added for if one of the record is rejected then should not proceed
        -- to Interface

        IF ln_rejected = 0 THEN
          o_status    :='S';

        ELSE
          o_status :='E';

        END IF;
        PRINT_OUTPUT('Validation Procedure status :'||o_status);
        PRINT_OUTPUT(
        '*******************Validation Status******************************');
        PRINT_OUTPUT('File Name   '||lcu_file.filename);
        PRINT_OUTPUT('Total Number of Record Processed for validation  '||
        ln_total_records);
        PRINT_OUTPUT('Total Number of Record Successfully Validated  '||
        ln_validated);
        PRINT_OUTPUT('Total Number of Record Rejected     '||ln_rejected);
        PRINT_OUTPUT(
        '******************End Validation Status***************************');

      END LOOP;

    ELSE
      --

      FOR lcu_file IN filename_cur
      LOOP
        ln_total_records   :=0;
        ln_validated       :=0;
        ln_rejected        :=0;
        ln_party_id        :=NULL;
        ln_party_site_id   :=NULL;
        ln_cust_account_id :=NULL;
        lc_party_type      :=NULL;
        lc_return_status   :=NULL;
        --
        BEGIN

          --

          SELECT COUNT(*)
          INTO ln_total_records
          FROM XXFT_POS_UPLOAD_LINKING_INT pos
          WHERE 1          =1
          AND FILENAME     = lcu_file.filename --'FINETECUS'
          AND pos.quantity =1
          OR pos.quantity  = -1
          AND pos.STATUS   ='N';
          --

        END;
        --Derive customer Account id
        ln_cust_account_id := get_customer_id(lcu_file.filename ,'POS');

        IF ln_cust_account_id IS NULL THEN
          lc_return_status    :='E';
          --lc_error_msg:= '1. Error while geting customer account id from file
          -- '||lcu_pos.filename;
          Print_log('1. Error while geting customer account id from file '||
          lcu_file.filename);

        ELSE
          lc_return_status:='Y';
          ln_party_id     := get_party_id(ln_cust_account_id);

          IF ln_party_id    IS NULL THEN
            lc_return_status:='E';
            PRINT_LOG('1. Party id does not found for customer id'||
            ln_cust_account_id);

          END IF;
          ln_party_site_id := get_party_site_id( ln_cust_account_id,'SHIP_TO');

          IF ln_party_site_id IS NULL THEN
            --lc_return_status:='E';
            PRINT_LOG('1. Party site id does not found  for customer id'||
            ln_cust_account_id);

          END IF;
          lc_party_type :=get_party_type(ln_cust_account_id);

          IF lc_party_type  IS NULL THEN
            lc_return_status:='E';
            PRINT_LOG('1. Party type does not found for customer id'||
            ln_cust_account_id);

          END IF;
          Print_log('1. Customer Account Id  '||ln_cust_account_id ||
          'For customer ' ||lcu_file.filename);
          Print_log('1. ln_party_id       '||ln_party_id);
          Print_log('1. ln_party_site_id  '||ln_party_site_id);
          Print_log('1. lc_party_type      '||lc_party_type);

        END IF;
        Print_log('lc_return_status =' ||lc_return_status);
        --
        Print_log('****Start Validation for File: '||lcu_file.filename||
        '******');

        IF lc_return_status ='Y' THEN

          FOR lcu_pos IN posupload_cur(lcu_file.filename)
          LOOP
            ln_inventory_item_id :=NULL;
            lc_party_type        :=NULL;
            lc_return_status1    :='Y';
            lc_error_msg         :=NULL;
            lc_posed_y_n         :=NULL;
            lc_period_y_n        :=NULL;
            -- Print_log('Start Validation for file '||lcu_file.filename);
            --1 Validation for item - Look for item in the EBS item master
            --Get item id from EBS Item Master

            IF lcu_pos.ft_item_number IS NOT NULL THEN
              ln_inventory_item_id    := get_item_id(lcu_pos.ft_item_number);
              --

              IF ln_inventory_item_id IS NULL AND lcu_pos.serial_number IS NOT
                NULL THEN
                --lc_error_msg :='Valid2: Item does not exists in EBS item
                -- master '||' '||lcu_pos.ft_item_number;
                Print_log('Valid2: Item does not exists in EBS item master'||
                ' '||lcu_pos.ft_item_number);
                -- Checking item based on serial Number
                ln_inventory_item_id:=get_serial_item_id(lcu_pos.serial_number)
                ;

                IF ln_inventory_item_id IS NULL THEN
                  lc_error_msg          :=
                  'Valid3: Item does not exists for serial item  '||
                  lcu_pos.serial_number;
                  Print_log('Valid3 :  Item does not exists for serial item  '
                  ||lcu_pos.serial_number);
                  -- check the item in ozf_code
                  ln_inventory_item_id:=get_item_frm_ozf_code(
                  ln_cust_account_id,lcu_pos.ft_item_number);

                END IF;

              END IF;

            END IF;
            --

            IF ln_inventory_item_id IS NULL THEN
              lc_return_status1     :='E';
              lc_error_msg          := lc_error_msg||
              'Valid4: Error while getting Item from Oracle for item '||
              lcu_pos.ft_item_number;
              Print_log( 'valid4: Error while getting item id  for item '||
              lcu_pos.ft_item_number ||' and for serial_number'||
              lcu_pos.serial_number);

            ELSE
              Print_log('Valid4:Item id for Item  ' ||lcu_pos.ft_item_number||
              ' ' ||'or serial Number'||lcu_pos.serial_number ||'is :'||
              'ln_inventory_item_id :'||ln_inventory_item_id);

            END IF;

            IF lcu_pos.quantity > 0 THEN
              --
              -- Validate  if the line is already POS'ed or NOT
              lc_posed_y_n := get_line_posed(lcu_pos.serial_number);

            END IF;

            IF lc_posed_y_n     ='Y' THEN
           --   lc_return_status1:='E';
              lc_error_msg     := lc_error_msg||
              'Valid5: Line Already Posed for serial Number : '||
              lcu_pos.serial_number||'!!!';
              Print_log('Valid5: Line Already Posed for Serial Number : '||
              lcu_pos.serial_number||'!!!');

            ELSE
              Print_log('Valid5: Line is ready to Posed...for Serial Number :'
              ||lcu_pos.serial_number);

            END IF;
            --Check for Period Open or NOT
            lc_period_y_n:=get_period_status(TRUNC(NVL(lcu_pos.date_invoiced,
            lcu_pos.date_shipped)));

            IF lc_period_y_n ='Y' THEN
              Print_log('valid6: Period is Open for the date '||
              lcu_pos.date_invoiced ||' :'||lc_period_y_n);

            ELSE
              lc_return_status1 :='E';
              lc_error_msg      := lc_error_msg||
              'Valid6: Period is not open for the date '||lcu_pos.date_invoiced
              ||'!!!';

            END IF;
            --updating custom interface table if all validation get success...

            IF lc_return_status1 = 'Y' THEN

              UPDATE XXFT_POS_UPLOAD_LINKING_INT
              SET inventory_item_id =ln_inventory_item_id ,
                cust_account_id     =ln_cust_account_id ,
                party_id            =ln_party_id ,
                party_site_id       =ln_party_site_id ,
                party_type          =lc_party_type ,
                last_update_date    =sysdate ,
                last_updated_by     =gn_user_id ,
                last_update_login   =gn_login_id ,
                request_id          =gn_request_id ,
                status              ='V' ,
                error_msg           = 'Successfully Validated ....!!!'
              WHERE record_id       = lcu_pos.record_id --Added on 12-Jan-15
              AND filename          =lcu_pos.filename
              AND status           <> 'S';
              COMMIT;
              ln_validated :=ln_validated+1;

            ELSIF lc_return_status1 = 'E' THEN

              --

              UPDATE XXFT_POS_UPLOAD_LINKING_INT
              SET status      ='E' ,
                ERROR_MSG     = lc_error_msg
              WHERE record_id = lcu_pos.record_id --ROWID = lcu_pos.ROWID
              AND filename    =lcu_pos.filename
              AND status     <> 'S';
              COMMIT;
              ln_rejected :=ln_rejected+1;

            END IF;

          END LOOP;

        END IF;
        --Added for if one of the record is rejected then should not proceed
        -- to Interface

        IF ln_rejected = 0 THEN
          o_status    :='S';

        ELSE
          o_status :='E';

        END IF;
        PRINT_OUTPUT('Validation Procedure status :'||o_status);
        PRINT_OUTPUT(
        '*******************Validation Status******************************');
        PRINT_OUTPUT('File Name   '||lcu_file.filename);
        PRINT_OUTPUT('Total Number of Record Processed for validation  '||
        ln_total_records);
        PRINT_OUTPUT('Total Number of Record Successfully Validated  '||
        ln_validated);
        PRINT_OUTPUT('Total Number of Record Rejected     '||ln_rejected);
        PRINT_OUTPUT(
        '******************End Validation Status***************************');

      END LOOP;

    END IF;

  EXCEPTION

  WHEN OTHERS THEN
    lc_error_msg:='Error in procedure validate_POS_Upload_proc '||SQLCODE ||
    SQLERRM;
    Print_log(lc_error_msg);

  END validate_POS_Upload_proc;
-- +===========================================================================
-- ==============================================+
-- | Name            : pos_validate_upload_proc
-- | Type            : Procedure
-- | Description     : This procedure will be use run the concurrenpt program
-- seperately for validation to validate data
-- |     and update the same with change status from 'N' to 'V'.
-- |                   in order to process further.
-- | Parameters      :
-- |
-- | Returns         :
-- |
-- +===========================================================================
-- ===========================================+

  PROCEDURE pos_validate_upload_proc(
      errbuff OUT VARCHAR2 ,
      retcode OUT NUMBER ,
      p_filename IN VARCHAR2)
  IS
    --Declare Variables
    ln_inventory_item_id NUMBER;
    ln_party_id          NUMBER;
    ln_party_site_id     NUMBER;
    ln_cust_account_id   NUMBER;
    lc_party_type        VARCHAR2(100);
    lc_error_msg         VARCHAR2(4000);
    lc_return_status1    VARCHAR2(3);
    lc_return_status     VARCHAR2(1);
    lc_posed_y_n         VARCHAR2(1);
    lc_period_y_n        VARCHAR2(1);
    ln_validated         NUMBER :=0;
    ln_rejected          NUMBER :=0;
    ln_total_records     NUMBER :=0;

    CURSOR filemass_cur(p_filename IN VARCHAR2)
    IS

      SELECT UNIQUE pou.filename,
        pou.DISTI_CUST_NUMBER
      FROM XXFT_POS_UPLOAD_LINKING_INT pou
      WHERE 1          =1                            --
      AND pou.filename =NVL(p_filename,pou.filename) --'FINETECUS' --'
        -- TECHDATAUS'
      AND status IN ('N','E');
    --define cursor

    CURSOR posmassload_cur(p_filename IN VARCHAR2,p_cust_number IN VARCHAR2)
    IS

      SELECT pos.* ,
        pos.rowid
      FROM XXFT_POS_UPLOAD_LINKING_INT pos
      WHERE 1               =1
      AND FILENAME          = p_filename
      AND DISTI_CUST_NUMBER = p_cust_number
      AND pos.quantity      =1
      OR pos.quantity       = -1
      AND pos.STATUS         IN ('N','E');
    --
    --Cursor to get unique file name

    CURSOR filename_cur(p_filename IN VARCHAR2)
    IS

      SELECT UNIQUE pou.filename
      FROM XXFT_POS_UPLOAD_LINKING_INT pou
      WHERE 1          =1                            --
      AND pou.filename =NVL(p_filename,pou.filename) --'FINETECUS' --'
        -- TECHDATAUS'
      AND status IN ('N','E');
    --define cursor

    CURSOR posupload_cur(p_filename IN VARCHAR2)
    IS

      SELECT pos.* ,
        pos.rowid
      FROM XXFT_POS_UPLOAD_LINKING_INT pos
      WHERE 1          =1
      AND FILENAME     = p_filename
      AND pos.quantity =1
      OR pos.quantity  = -1
      AND pos.STATUS    IN ('N','E');
  BEGIN

    IF upper(p_filename) LIKE '%FORTINET_POS%' THEN

      FOR lcu_file IN filemass_cur(p_filename)
      LOOP
        ln_total_records   :=0;
        ln_validated       :=0;
        ln_rejected        :=0;
        ln_party_id        :=NULL;
        ln_party_site_id   :=NULL;
        ln_cust_account_id :=NULL;
        lc_party_type      :=NULL;
        lc_return_status   :=NULL;
        --
        BEGIN

          --

          SELECT COUNT(*)
          INTO ln_total_records
          FROM XXFT_POS_UPLOAD_LINKING_INT pos
          WHERE 1               =1
          AND FILENAME          = lcu_file.filename --'FINETECUS'
          AND DISTI_CUST_NUMBER = lcu_file.DISTI_CUST_NUMBER
          AND pos.quantity      =1
          OR pos.quantity       = -1
          AND pos.STATUS         IN ('N','E');
          --

        END;
        --Derive customer Account id
        --ln_cust_account_id := get_customer_id(lcu_file.filename ,'POS');
        BEGIN

          SELECT cust_account_id
          INTO ln_cust_account_id
          FROM hz_cust_accounts_all
          WHERE SALES_CHANNEL_CODE = 'CHANNEL_PARTNER'
          and account_number = lcu_file.DISTI_CUST_NUMBER;

        EXCEPTION

        WHEN NO_DATA_FOUND THEN
          print_log('Customer not found '||
          lcu_file.DISTI_CUST_NUMBER);
          ln_cust_account_id := NULL;
          PRINT_OUTPUT('Customer Incorrect or Not a Valid Channel Partner  : # '||
          lcu_file.DISTI_CUST_NUMBER);

        WHEN TOO_MANY_ROWS THEN
          print_log('Mass update customer not found '||
          lcu_file.DISTI_CUST_NUMBER);
          ln_cust_account_id := NULL;
           PRINT_OUTPUT('Customer Incorrect or Not a Valid Channel Partner  : # '||
          lcu_file.DISTI_CUST_NUMBER);

        WHEN OTHERS THEN
          print_log('Mass update customer not found '||
          lcu_file.DISTI_CUST_NUMBER||' '||SQLCODE||' '||SQLERRM);
          ln_cust_account_id := NULL;
          PRINT_OUTPUT('Customer Incorrect or Not a Valid Channel Partner  : # '||
          lcu_file.DISTI_CUST_NUMBER);
        END;

        IF ln_cust_account_id IS NULL THEN
          lc_return_status    :='E';
          --lc_error_msg:= '1. Error while geting customer account id from file
          -- '||lcu_pos.filename;
          Print_log('1. Error while geting customer from file '||
          lcu_file.filename);

        ELSE
          lc_return_status:='Y';
          ln_party_id     := get_party_id(ln_cust_account_id);

          IF ln_party_id    IS NULL THEN
            lc_return_status:='E';
            PRINT_LOG('1. Party id does not found for customer id'||
            ln_cust_account_id);

          END IF;
          ln_party_site_id := get_party_site_id( ln_cust_account_id,'SHIP_TO');

          IF ln_party_site_id IS NULL THEN
            --lc_return_status:='E';
            PRINT_LOG('1. Party site id does not found  for customer id'||
            ln_cust_account_id);

          END IF;
          lc_party_type :=get_party_type(ln_cust_account_id);

          IF lc_party_type  IS NULL THEN
            lc_return_status:='E';
            PRINT_LOG('1. Party type does not found for customer id'||
            ln_cust_account_id);

          END IF;
          Print_log('1. Customer Account Id  '||ln_cust_account_id ||
          'For customer ' ||lcu_file.filename);
          Print_log('1. ln_party_id       '||ln_party_id);
          Print_log('1. ln_party_site_id  '||ln_party_site_id);
          Print_log('1. lc_party_type      '||lc_party_type);

        END IF;
        Print_log('lc_return_status =' ||lc_return_status);
        --
        Print_log('****Start Validation for File: '||lcu_file.filename||
        '******');

        IF lc_return_status ='Y' THEN

          FOR lcu_pos IN posmassload_cur(lcu_file.filename,
          lcu_file.DISTI_CUST_NUMBER)
          LOOP
            ln_inventory_item_id :=NULL;
            lc_party_type        :=NULL;
            lc_return_status1    :='Y';
            lc_error_msg         :=NULL;
            lc_posed_y_n         :=NULL;
            lc_period_y_n        :=NULL;
            -- Print_log('Start Validation for file '||lcu_file.filename);
            --1 Validation for item - Look for item in the EBS item master
            --Get item id from EBS Item Master

            IF lcu_pos.ft_item_number IS NOT NULL THEN
              ln_inventory_item_id    := get_item_id(lcu_pos.ft_item_number);
              --

              IF ln_inventory_item_id IS NULL THEN
                lc_error_msg          :=
                'Valid2: Item does not exists in EBS item master '||' '||
                lcu_pos.ft_item_number;
                Print_log('Valid2: Item does not exists in EBS item master'||
                ' '||lcu_pos.ft_item_number);
                -- Checking item based on serial Number
                ln_inventory_item_id:=get_serial_item_id(lcu_pos.serial_number)
                ;

                IF ln_inventory_item_id IS NULL THEN
                  lc_error_msg          :=
                  'Valid3: Item does not exists for serial item  '||
                  lcu_pos.serial_number;
                  Print_log('Valid3 :  Item does not exists for serial item  '
                  ||lcu_pos.serial_number);
                  -- check the item in ozf_code
                  ln_inventory_item_id:=get_item_frm_ozf_code(
                  ln_cust_account_id,lcu_pos.ft_item_number);

                END IF;

              END IF;

            END IF;
            --

            IF ln_inventory_item_id IS NULL THEN
              lc_return_status1     :='E';
              lc_error_msg          := lc_error_msg||
              'Valid4: Error while getting item id  for item '||
              lcu_pos.ft_item_number;
              Print_log( 'valid4: Error while getting item id  for item '||
              lcu_pos.ft_item_number ||' and for serial_number'||
              lcu_pos.serial_number);

            ELSE
              Print_log('Valid4:Item id for Item  ' ||lcu_pos.ft_item_number||
              ' ' ||'or serial Number'||lcu_pos.serial_number ||'is :'||
              'ln_inventory_item_id :'||ln_inventory_item_id);

            END IF;

            IF lcu_pos.quantity > 0 THEN
              --
              -- Validate  if the line is already POS'ed or NOT
              lc_posed_y_n := get_line_posed(lcu_pos.serial_number);

            END IF;

            IF lc_posed_y_n     ='Y' THEN
           --   lc_return_status1:='E';
              lc_error_msg     := lc_error_msg||
              'Valid5: Line Already Posed for serial Number : '||
              lcu_pos.serial_number||'!!!';
              Print_log('Valid5: Line Already Posed for Serial Number : '||
              lcu_pos.serial_number||'!!!');

            ELSE
              Print_log('Valid5: Line is ready to Posed...for Serial Number :'
              ||lcu_pos.serial_number);

            END IF;
            --Check for Period Open or NOT
            lc_period_y_n:=get_period_status(TRUNC(NVL(lcu_pos.date_invoiced,
            lcu_pos.date_shipped))); -- added NVL for Synnex file

            IF lc_period_y_n ='Y' THEN
              Print_log('valid6: Period is Open for the date '||
              lcu_pos.date_invoiced ||' :'||lc_period_y_n);

            ELSE
              lc_return_status1 :='E';
              lc_error_msg      := lc_error_msg||
              'Valid6: Period is not open for the date '||lcu_pos.date_invoiced
              ||'!!!';

            END IF;
            --updating custom interface table if all validation get success...

            IF lc_return_status1 = 'Y' THEN

              UPDATE XXFT_POS_UPLOAD_LINKING_INT
              SET inventory_item_id =ln_inventory_item_id ,
                cust_account_id     =ln_cust_account_id ,
                party_id            =ln_party_id ,
                party_site_id       =ln_party_site_id ,
                party_type          =lc_party_type ,
                last_update_date    =sysdate ,
                last_updated_by     =gn_user_id ,
                last_update_login   =gn_login_id ,
                request_id          =gn_request_id ,
                status              ='V' ,
                error_msg           = 'Successfully Validated ....!!!'
              WHERE record_id       = lcu_pos.record_id --ROWID = lcu_pos.ROWID
              AND filename          =lcu_pos.filename
              AND DISTI_CUST_NUMBER = lcu_pos.DISTI_CUST_NUMBER;
              COMMIT;
              ln_validated :=ln_validated+1;

            ELSIF lc_return_status1 = 'E' THEN

              --

              UPDATE XXFT_POS_UPLOAD_LINKING_INT
              SET status            ='E' ,
                ERROR_MSG           = lc_error_msg
              WHERE record_id       = lcu_pos.record_id --ROWID = lcu_pos.ROWID
              AND filename          =lcu_pos.filename
              AND DISTI_CUST_NUMBER = lcu_pos.DISTI_CUST_NUMBER;
              COMMIT;
              ln_rejected :=ln_rejected+1;

            END IF;

          END LOOP;

        END IF;
        PRINT_OUTPUT(
        '*******************Validation Status******************************');
        PRINT_OUTPUT('File Name   '||lcu_file.filename);
        PRINT_OUTPUT('Total Number of Record Processed for validation  '||
        ln_total_records);
        PRINT_OUTPUT('Total Number of Record Successfully Validated  '||
        ln_validated);
        PRINT_OUTPUT('Total Number of Record Rejected     '||ln_rejected);
        PRINT_OUTPUT(
        '******************End Validation Status***************************');

      END LOOP;

    ELSE
      --

      FOR lcu_file IN filename_cur(p_filename)
      LOOP
        ln_total_records   :=0;
        ln_validated       :=0;
        ln_rejected        :=0;
        ln_party_id        :=NULL;
        ln_party_site_id   :=NULL;
        ln_cust_account_id :=NULL;
        lc_party_type      :=NULL;
        lc_return_status   :=NULL;
        --
        BEGIN

          --

          SELECT COUNT(*)
          INTO ln_total_records
          FROM XXFT_POS_UPLOAD_LINKING_INT pos
          WHERE 1          =1
          AND FILENAME     = lcu_file.filename --'FINETECUS'
          AND pos.quantity =1
          OR pos.quantity  = -1
          AND pos.STATUS    IN ('N','E');
          --

        END;
        --Derive customer Account id
        ln_cust_account_id := get_customer_id(lcu_file.filename ,'POS');

        IF ln_cust_account_id IS NULL THEN
          lc_return_status    :='E';
          --lc_error_msg:= '1. Error while geting customer account id from file
          -- '||lcu_pos.filename;
          Print_log('1. Error while geting customer account id from file '||
          lcu_file.filename);

        ELSE
          lc_return_status:='Y';
          ln_party_id     := get_party_id(ln_cust_account_id);

          IF ln_party_id    IS NULL THEN
            lc_return_status:='E';
            PRINT_LOG('1. Party id does not found for customer id'||
            ln_cust_account_id);

          END IF;
          ln_party_site_id := get_party_site_id( ln_cust_account_id,'SHIP_TO');

          IF ln_party_site_id IS NULL THEN
            --lc_return_status:='E';
            PRINT_LOG('1. Party site id does not found  for customer id'||
            ln_cust_account_id);

          END IF;
          lc_party_type :=get_party_type(ln_cust_account_id);

          IF lc_party_type  IS NULL THEN
            lc_return_status:='E';
            PRINT_LOG('1. Party type does not found for customer id'||
            ln_cust_account_id);

          END IF;
          Print_log('1. Customer Account Id  '||ln_cust_account_id ||
          'For customer ' ||lcu_file.filename);
          Print_log('1. ln_party_id       '||ln_party_id);
          Print_log('1. ln_party_site_id  '||ln_party_site_id);
          Print_log('1. lc_party_type      '||lc_party_type);

        END IF;
        Print_log('lc_return_status =' ||lc_return_status);
        --
        Print_log('****Start Validation for File: '||lcu_file.filename||
        '******');

        IF lc_return_status ='Y' THEN

          FOR lcu_pos IN posupload_cur(lcu_file.filename)
          LOOP
            ln_inventory_item_id :=NULL;
            lc_party_type        :=NULL;
            lc_return_status1    :='Y';
            lc_error_msg         :=NULL;
            lc_posed_y_n         :=NULL;
            lc_period_y_n        :=NULL;
            -- Print_log('Start Validation for file '||lcu_file.filename);
            --1 Validation for item - Look for item in the EBS item master
            --Get item id from EBS Item Master

            IF lcu_pos.ft_item_number IS NOT NULL THEN
              ln_inventory_item_id    := get_item_id(lcu_pos.ft_item_number);
              --

              IF ln_inventory_item_id IS NULL THEN
                lc_error_msg          :=
                'Valid2: Item does not exists in EBS item master '||' '||
                lcu_pos.ft_item_number;
                Print_log('Valid2: Item does not exists in EBS item master'||
                ' '||lcu_pos.ft_item_number);
                -- Checking item based on serial Number
                ln_inventory_item_id:=get_serial_item_id(lcu_pos.serial_number)
                ;

                IF ln_inventory_item_id IS NULL THEN
                  lc_error_msg          :=
                  'Valid3: Item does not exists for serial item  '||
                  lcu_pos.serial_number;
                  Print_log('Valid3 :  Item does not exists for serial item  '
                  ||lcu_pos.serial_number);
                  -- check the item in ozf_code
                  ln_inventory_item_id:=get_item_frm_ozf_code(
                  ln_cust_account_id,lcu_pos.ft_item_number);

                END IF;

              END IF;

            END IF;
            --

            IF ln_inventory_item_id IS NULL THEN
              lc_return_status1     :='E';
              lc_error_msg          := lc_error_msg||
              'Valid4: Error while getting item id  for item '||
              lcu_pos.ft_item_number;
              Print_log( 'valid4: Error while getting item id  for item '||
              lcu_pos.ft_item_number ||' and for serial_number'||
              lcu_pos.serial_number);

            ELSE
              Print_log('Valid4:Item id for Item  ' ||lcu_pos.ft_item_number||
              ' ' ||'or serial Number'||lcu_pos.serial_number ||'is :'||
              'ln_inventory_item_id :'||ln_inventory_item_id);

            END IF;

            IF lcu_pos.quantity > 0 THEN
              --
              -- Validate  if the line is already POS'ed or NOT
              lc_posed_y_n := get_line_posed(lcu_pos.serial_number);

            END IF;

            IF lc_posed_y_n     ='Y' THEN
           --   lc_return_status1:='E';
              lc_error_msg     := lc_error_msg||
              'Valid5: Line Already Posed for serial Number : '||
              lcu_pos.serial_number||'!!!';
              Print_log('Valid5: Line Already Posed for Serial Number : '||
              lcu_pos.serial_number||'!!!');

            ELSE
              Print_log('Valid5: Line is ready to Posed...for Serial Number :'
              ||lcu_pos.serial_number);

            END IF;
            --Check for Period Open or NOT
            lc_period_y_n:=get_period_status(TRUNC(NVL(lcu_pos.date_invoiced,
            lcu_pos.date_shipped))); -- added NVL for Synnex file

            IF lc_period_y_n ='Y' THEN
              Print_log('valid6: Period is Open for the date '||
              lcu_pos.date_invoiced ||' :'||lc_period_y_n);

            ELSE
              lc_return_status1 :='E';
              lc_error_msg      := lc_error_msg||
              'Valid6: Period is not open for the date '||lcu_pos.date_invoiced
              ||'!!!';

            END IF;
            --updating custom interface table if all validation get success...

            IF lc_return_status1 = 'Y' THEN

              UPDATE XXFT_POS_UPLOAD_LINKING_INT
              SET inventory_item_id =ln_inventory_item_id ,
                cust_account_id     =ln_cust_account_id ,
                party_id            =ln_party_id ,
                party_site_id       =ln_party_site_id ,
                party_type          =lc_party_type ,
                last_update_date    =sysdate ,
                last_updated_by     =gn_user_id ,
                last_update_login   =gn_login_id ,
                request_id          =gn_request_id ,
                status              ='V' ,
                error_msg           = 'Successfully Validated ....!!!'
              WHERE record_id       = lcu_pos.record_id --ROWID = lcu_pos.ROWID
              AND filename          =lcu_pos.filename ;
              COMMIT;
              ln_validated :=ln_validated+1;

            ELSIF lc_return_status1 = 'E' THEN

              --

              UPDATE XXFT_POS_UPLOAD_LINKING_INT
              SET status      ='E' ,
                ERROR_MSG     = lc_error_msg
              WHERE record_id = lcu_pos.record_id --ROWID = lcu_pos.ROWID
              AND filename    =lcu_pos.filename ;
              COMMIT;
              ln_rejected :=ln_rejected+1;

            END IF;

          END LOOP;

        END IF;
        PRINT_OUTPUT(
        '*******************Validation Status******************************');
        PRINT_OUTPUT('File Name   '||lcu_file.filename);
        PRINT_OUTPUT('Total Number of Record Processed for validation  '||
        ln_total_records);
        PRINT_OUTPUT('Total Number of Record Successfully Validated  '||
        ln_validated);
        PRINT_OUTPUT('Total Number of Record Rejected     '||ln_rejected);
        PRINT_OUTPUT(
        '******************End Validation Status***************************');

      END LOOP;

    END IF;

  EXCEPTION

  WHEN OTHERS THEN
    lc_error_msg:='Error in procedure validate_POS_Upload_proc '||SQLCODE ||
    SQLERRM;
    Print_log(lc_error_msg);

  END pos_validate_Upload_proc;
-- +=========================================================================+
-- | Name            : print_log
-- | Type            : Procedure
-- | Description     : To print the errors/success messages in Log file
-- |
-- | Parameters      :
-- |
-- | Returns         :
-- |
-- +=========================================================================+

  PROCEDURE print_log(
      pc_message VARCHAR2 )
  AS
    ln_user_id NUMBER;
  BEGIN

    SELECT fnd_global.user_id
    INTO ln_user_id
    FROM DUAL;
    --Condition for backend comparability

    IF (ln_user_id = -1) THEN
      dbms_output.put_line (pc_message);

    ELSE
      fnd_file.put_line (fnd_file.LOG, pc_message);
      dbms_output.put_line (pc_message);

    END IF;

  END print_log;
--
-- +================================================================+
-- | Name            : print_output                                 |
-- | Type            : Procedure                                    |
-- | Description     : This is the print output procedure to print  |
-- |                   the output file                              |
-- |                |
-- | Parameters      : pc_message                              |
-- |                                                                |
-- |                                                                |
-- | Returns         :                                        |
-- |                                                          |
-- +================================================================+

  PROCEDURE print_output(
      pc_message VARCHAR2)
    --
  IS
    ln_user_id NUMBER;
  BEGIN

    SELECT fnd_global.user_id
    INTO ln_user_id
    FROM DUAL;

    IF (ln_user_id = -1) THEN
      DBMS_OUTPUT.PUT_LINE (pc_message);

    ELSE
      FND_FILE.PUT_LINE (FND_FILE.OUTPUT, pc_message);

    END IF;

  END print_output;
--
-- +================================================================+
-- | Name            : get_item_id                                 |
-- | Type            : FUNCTION                                     |
-- | Description     : This function used to get item id    |
-- |                                                |
-- |                |
-- | Parameters      : p_item_number                          |
-- |                                                                |
-- |                                                                |
-- | Returns         : Item_id                               |
-- |                                                          |
-- +================================================================+

  FUNCTION get_item_id(
      p_item_number IN VARCHAR2)
    RETURN NUMBER
  IS
    ln_item_id NUMBER;
  BEGIN
    ln_item_id:=NULL;
    BEGIN

    BEGIN

      SELECT msi.inventory_item_id
      INTO ln_item_id
      FROM MTL_system_items_b msi
      WHERE msi.segment1      = p_item_number
      AND msi.organization_id =
        (
          SELECT mp.organization_id
          FROM mtl_parameters mp
          WHERE mp.organization_code ='FIM'
        );

    EXCEPTION

        WHEN NO_DATA_FOUND THEN
          print_log('FUNC1. Item Does not exist in system '|| p_item_number);

        WHEN TOO_MANY_ROWS THEN
          print_log('FUNC1. More Than One Item found for item '|| p_item_number
          );

        WHEN OTHERS THEN
          print_log('FUNC1. Error while deriving item derive '||p_item_number||
          ' '||SQLCODE||' '||SQLERRM);

        END;

      IF ln_item_id IS NULL THEN
        BEGIN

          SELECT msi.inventory_item_id
          INTO ln_item_id
          FROM MTL_system_items_b msi
          WHERE REPLACE(msi.segment1,'-','') LIKE REPLACE(REPLACE(rtrim(ltrim(
            p_item_number)),'-','%'),' ','%')
          AND msi.organization_id =
            (
              SELECT mp.organization_id
              FROM mtl_parameters mp
              WHERE mp.organization_code ='FIM'
            );

        EXCEPTION

        WHEN NO_DATA_FOUND THEN
          print_log('FUNC1. Item Does not exist in system '|| p_item_number);

        WHEN TOO_MANY_ROWS THEN
          print_log('FUNC1. More Than One Item found for item '|| p_item_number
          );

        WHEN OTHERS THEN
          print_log('FUNC1. Error while deriving item derive '||p_item_number||
          ' '||SQLCODE||' '||SQLERRM);

        END;

      END IF;

      IF ln_item_id IS NULL THEN
        BEGIN

          SELECT msi.inventory_item_id
          INTO ln_item_id
          FROM MTL_system_items_b msi
          WHERE UPPER(msi.segment1)||'-US' = UPPER(p_item_number)
          AND msi.organization_id =
            (
              SELECT mp.organization_id
              FROM mtl_parameters mp
              WHERE mp.organization_code ='FIM'
            );

        EXCEPTION

        WHEN NO_DATA_FOUND THEN
          print_log('FUNC1. Item Does not exist in system '|| p_item_number);

        WHEN TOO_MANY_ROWS THEN
          print_log('FUNC1. More Than One Item found for item '|| p_item_number
          );

        WHEN OTHERS THEN
          print_log('FUNC1. Error while deriving item derive '||p_item_number||
          ' '||SQLCODE||' '||SQLERRM);

        END;

      END IF;

      IF ln_item_id IS NOT NULL THEN

        RETURN(ln_item_id);

      ELSE

        RETURN(NULL);

      END IF;

    EXCEPTION

    WHEN NO_DATA_FOUND THEN
      print_log('FUNC1. Item Does not exist in system '|| p_item_number);

      RETURN(NULL);

    WHEN TOO_MANY_ROWS THEN
      print_log('FUNC1. More Than One Item found for item '|| p_item_number);

      RETURN(NULL);

    WHEN OTHERS THEN
      print_log('FUNC1. Error while deriving item derive '||p_item_number||' '
      ||SQLCODE||' '||SQLERRM);

      RETURN(NULL);

    END;

  END get_item_id;
--
-- +================================================================+
-- | Name            : get_serial_item_id                         |
-- | Type            : FUNCTION                                     |
-- | Description     : This function used to get item id    |
-- |                                                    |
-- |                |
-- | Parameters      : p_item_number                          |
-- |                                                                |
-- |                                                                |
-- | Returns         : Item_id                               |
-- |                                                          |
-- +================================================================+

  FUNCTION get_serial_item_id(
      p_serial_num IN VARCHAR2)
    RETURN NUMBER
  IS
    ln_item_id NUMBER;
  BEGIN
    ln_item_id:=NULL;
    BEGIN

      SELECT msn.inventory_item_id
      INTO ln_item_id
      FROM MTL_SERIAL_NUMBERS msn
      WHERE serial_number         = rtrim(ltrim(p_serial_num))
      AND current_organization_id =
        (
          SELECT mp.organization_id
          FROM mtl_parameters mp
          WHERE mp.organization_code ='FIM'
        );

    EXCEPTION

    WHEN NO_DATA_FOUND THEN
      Print_log ('FUNC2: NO Data found exp for serial Number'||p_serial_num);
      ln_item_id:=NULL;

    WHEN TOO_MANY_ROWS THEN
      Print_log ('FUNC2: More than One Item found for serial Number'||
      p_serial_num);
      ln_item_id:=NULL;

    WHEN OTHERS THEN
      Print_log ('FUNC2: Error while geting item from serial Number'||
      p_serial_num ||SQLCODE ||SQLERRM);
      ln_item_id:=NULL;

    END;

    IF ln_item_id IS NULL THEN
      BEGIN

        SELECT msn.inventory_item_id
        INTO ln_item_id
        FROM MTL_SERIAL_NUMBERS msn
        WHERE serial_number LIKE REPLACE(REPLACE(rtrim(ltrim(p_serial_num)),'-'
          ,'%'),' ','%')
        AND current_organization_id =
          (
            SELECT mp.organization_id
            FROM mtl_parameters mp
            WHERE mp.organization_code ='FIM'
          );

      EXCEPTION

      WHEN NO_DATA_FOUND THEN
        Print_log ('FUNC2: NO Data found exp for serial Number'||p_serial_num);
        ln_item_id:=NULL;

      WHEN TOO_MANY_ROWS THEN
        Print_log ('FUNC2: More than One Item found for serial Number'||
        p_serial_num);
        ln_item_id:=NULL;

      WHEN OTHERS THEN
        Print_log ('FUNC2: Error while geting item from serial Number'||
        p_serial_num ||SQLCODE ||SQLERRM);
        ln_item_id:=NULL;

      END;

    END IF;

    IF ln_item_id IS NULL THEN
      BEGIN

        SELECT DISTINCT ozf.inventory_item_id
        INTO ln_item_id
        FROM OZF_SALES_TRANSACTIONS_ALL ozf
        WHERE rtrim(ltrim(ATTRIBUTE8)) LIKE REPLACE(REPLACE(rtrim(ltrim(
          p_serial_num)),'-','%'),' ','%');

      EXCEPTION

      WHEN NO_DATA_FOUND THEN
        Print_log ('FUNC2: NO Data found exp for serial Number'||p_serial_num);
        ln_item_id:=NULL;

      WHEN TOO_MANY_ROWS THEN
        Print_log ('FUNC2: More than One Item found for serial Number'||
        p_serial_num);
        ln_item_id:=NULL;

      WHEN OTHERS THEN
        Print_log ('FUNC2: Error while geting item from serial Number'||
        p_serial_num ||SQLCODE ||SQLERRM);
        ln_item_id:=NULL;

      END;

    END IF;

    IF ln_item_id IS NOT NULL THEN

      RETURN(ln_item_id);

    ELSE

      RETURN(NULL);

    END IF;

  EXCEPTION

  WHEN NO_DATA_FOUND THEN
    print_log('FUNC2. Item Does not exist in system  for Serial Number :'||
    p_serial_num);

    RETURN(NULL);

  WHEN TOO_MANY_ROWS THEN
    print_log('FUNC2. More Than One Item found for item for Serial Number :'||
    p_serial_num);

    RETURN(NULL);

  WHEN OTHERS THEN
    print_log('FUNC2. Error while deriving item derive for Serial Number :'||
    p_serial_num||' '||SQLCODE||' '||SQLERRM);

    RETURN(NULL);

  END get_serial_item_id;
-- +===========================================================================
-- ==+
-- | Name            : get_column                                     |
-- | Type            : FUNCTION                                         |
-- | Description     : This function used to get column name based on tag  and
-- |
-- |                   the output file                                  |
-- |                    |
-- | Parameters      : p_number                                   |
-- |                                                                    |
-- |                                                                    |
-- | Returns         :                                            |
-- |                                                              |
-- +===========================================================================
-- ==+

  FUNCTION get_column(
      p_number IN NUMBER)
    RETURN VARCHAR2
  IS
    lc_field_name VARCHAR2(30) :='COLUMN';
    lc_name       VARCHAR2(30);
  BEGIN

    SELECT lc_field_name
      ||p_number
    INTO lc_name
    FROM dual;

    RETURN(lc_name);

  EXCEPTION

  WHEN OTHERS THEN
    PRINT_LOG('Error while geting column name for tag'||p_number);

    RETURN(NULL);

  END get_column;
----------------------
-- +===========================================================================
-- ============+
-- | Name            : get_customer_id                                        |
-- | Type            : FUNCTION                                            |
-- | Description     : This Function used to get customer account id based on
-- filename     |
-- |                   and attribute 1 from lookup FTNT_CHRM_UPLOADS_LKP
-- |
-- |                                                            |
-- |                          |
-- +===========================================================================
-- ============+

  FUNCTION get_customer_id(
      p_filename IN VARCHAR2,
      p_type     IN VARCHAR2 )
    RETURN NUMBER
  IS
    ln_cust_acct_id NUMBER;
  BEGIN

    SELECT to_number(attribute2) -- Cust_account_id
    INTO ln_cust_acct_id
    FROM FND_LOOKUP_VALUES
    WHERE LOOKUP_TYPE = 'FTNT_CHRM_UPLOADS_LKP'
    AND ATTRIBUTE1    = p_type --'POS'
    AND p_filename LIKE '%'
      ||LOOKUP_CODE
      ||'%'; --'ARROWUS' ;

    IF ln_cust_acct_id IS NULL THEN
      print_log( 'Customer account id does not exist for looup code :'||
      p_filename || 'and ATTRIBUTE1  :' ||p_type);

    END IF;

    RETURN(ln_cust_acct_id);

  EXCEPTION

  WHEN OTHERS THEN
    PRINT_LOG('FUNC3 get_customer_id : Error while geting cust acct id'||
    SQLCODE||'  '||SQLERRM);

    RETURN(NULL);

  END get_customer_id;
-- +===========================================================================
-- ============+
-- | Name            : get_item_frm_ozf_code
-- |
-- | Type            : FUNCTION                                            |
-- | Description     : This Function used to get  item id from
-- OZF_CODE_CONVERSIONS_ALL    |
-- |                                       |
-- |                                                            |
-- |                          |
-- +===========================================================================
-- ============+

  FUNCTION get_item_frm_ozf_code(
      p_cust_acct_id IN NUMBER,
      p_item_name    IN VARCHAR2)
    RETURN NUMBER
  IS
    ln_inventory_item_id NUMBER;
  BEGIN

    SELECT INTERNAL_CODE
    INTO ln_inventory_item_id -- MTL -- inventory item id
    FROM OZF_CODE_CONVERSIONS_ALL
    WHERE CODE_CONVERSION_TYPE = 'OZF_PRODUCT_CODES'
    AND CUST_ACCOUNT_ID        = p_cust_acct_id --6042 -- pass disti
      -- cust_account_id
    AND EXTERNAL_CODE = p_item_name --'FG500DC' -- Item from POS
    AND SYSDATE BETWEEN NVL(start_date_active,SYSDATE) AND NVL(END_DATE_ACTIVE,
      SYSDATE);

    IF ln_inventory_item_id IS NULL THEN
      print_log(
      'Item id does not exist in OZF_CODE_CONVERSIONS_ALL for item  :'||
      p_item_name || 'and customer account id  :' ||p_cust_acct_id);

      RETURN(NULL);

    ELSE

      RETURN(ln_inventory_item_id);

    END IF;

  EXCEPTION

  WHEN NO_DATA_FOUND THEN
    PRINT_LOG(
    'FUNC4 get_item_frm_ozf_code : No Data Found exp for item  in OZF_CODE_CONVERSIONS_ALL'
    ||p_item_name );

    RETURN(NULL);

  WHEN TOO_MANY_ROWS THEN
    PRINT_LOG(
    'FUNC4 get_item_frm_ozf_code : More than one item found for item in OZF_CODE_CONVERSIONS_ALL'
    ||p_item_name);

    RETURN(NULL);

  WHEN OTHERS THEN
    PRINT_LOG(
    'FUNC4 get_item_frm_ozf_code : Error while getting item id in OZF_CODE_CONVERSIONS_ALL'
    ||SQLCODE||'  '||SQLERRM);

    RETURN(NULL);

  END get_item_frm_ozf_code;
-- +===========================================================================
-- ============+
-- | Name            : get_line_posed
-- |
-- | Type            : FUNCTION                                            |
-- | Description     : This Function used to get  line POS'ed yes or not
-- |
-- |                                       |
-- |                                                            |
-- |                          |
-- +===========================================================================
-- ============+

  FUNCTION get_line_posed(
      p_serial_number IN VARCHAR2)
    RETURN VARCHAR2
  IS
    lc_yes_no VARCHAR2(1);
    ln_count  NUMBER ;
  BEGIN

    SELECT COUNT(*)
    INTO ln_count
    FROM OZF_RESALE_LINES_INT_ALL
    WHERE STATUS_CODE    IN ('PROCESSED','CLOSED')
    AND UPC_CODE          = p_serial_number --Pass Serial Number
    AND line_attribute15 IS NOT NULL;

    IF ln_count  >0 THEN
      lc_yes_no :='Y';

    ELSE
      lc_yes_no :='N';

    END IF;

    RETURN(lc_yes_no);

  EXCEPTION

  WHEN OTHERS THEN

    RETURN('N');
    PRINT_LOG('FUNC5 get_line_posed : Error while geting linse POS ed or not '
    ||SQLCODE||'  '||SQLERRM);

  END get_line_posed;
-- +===========================================================================
-- ============+
-- | Name            : get_period_status
-- |
-- | Type            : FUNCTION                                            |
-- | Description     : This Function used to get period open or CLOSED        |
-- |                                                            |
-- |                          |
-- +===========================================================================
-- ============+

  FUNCTION get_period_status(
      p_date IN DATE)
    RETURN VARCHAR2
  IS
    lc_date   VARCHAR2 (100);
    lc_status VARCHAR2(30);
  BEGIN
    print_log ('FUNC6 get_period_status  p_date ' || p_date);
    BEGIN

      --  SELECT attribute1
      --  INTO lc_status
      --  FROM AP.AP_OTHER_PERIODS
      --  WHERE PERIOD_TYPE = 'FTNT POS'
      --  AND  period_name = to_char(to_date(p_date,'DD/MON/RRRR '),'MON-RR');

      SELECT closing_status
      INTO lc_status
      FROM GL_PERIOD_STATUSES_v
      WHERE APPLICATION_ID = 101
      AND ledger_id        = 2021
      AND p_date BETWEEN START_DATE AND END_DATE
      AND rownum < 2;
      --order by START_DATE;
      --
      print_log ('FUNC6 get_period_status  lc_status' || lc_status);
      --

    END;
    --

    IF lc_status = 'O' THEN

      RETURN ('Y');

    ELSE

      RETURN ('N');

    END IF;

  EXCEPTION

  WHEN NO_DATA_FOUND THEN

    RETURN ('N');
    print_log('FUNC6 get_period_status  no data found exp' || lc_status );

  WHEN OTHERS THEN

    RETURN ('N');
    print_log ('FUNC6 get_period_status  others exp' || lc_status || SQLERRM );

  END get_period_status;
---
-- +===========================================================================
-- ============+
-- | Name            : get_party_id
-- |
-- | Type            : FUNCTION                                            |
-- | Description     : This Function used to get party id by passing cust
-- account id     |
-- |                                                            |
-- |                          |
-- +===========================================================================
-- ============+

  FUNCTION get_party_id(
      p_cust_acct_id IN NUMBER)
    RETURN NUMBER
  IS
    ln_party_id NUMBER :=NULL;
  BEGIN

    SELECT ca.party_id
    INTO ln_party_id
    FROM hz_cust_accounts ca
    WHERE ca.cust_account_id =p_cust_acct_id ; -- 6042

    IF ln_party_id IS NOT NULL THEN

      RETURN(ln_party_id);

    ELSE

      RETURN(NULL);

    END IF;

  EXCEPTION

  WHEN NO_DATA_FOUND THEN

    RETURN (NULL);
    print_log('FUNC7 get_party_id:  no data found exp' || p_cust_acct_id );

  WHEN OTHERS THEN
    print_log(
    'FUNC7 get_party_id: Erorr while geting party id for cust_account_id '||
    p_cust_acct_id ||' '||SQLCODE||' '||SQLERRM);

    RETURN(NULL);

  END get_party_id;
-- +===========================================================================
-- =============+
-- | Name            : get_party_site_id
-- |
-- | Type            : FUNCTION
-- |
-- | Description     : This Function used to get party site id by passing cust
-- account id      |
-- |                                                                |
-- |                              |
-- +===========================================================================
-- ================+

  FUNCTION get_party_site_id(
      p_cust_acct_id IN NUMBER,
      p_site         IN VARCHAR2)
    RETURN NUMBER
  IS
    ln_party_site_id NUMBER ;
  BEGIN

    SELECT hca.party_site_id
    INTO ln_party_site_id
    FROM hz_cust_accounts ca,
      hz_cust_acct_sites_all hca,
      hz_cust_site_uses_all hcs
    WHERE ca.cust_account_id  = hca.cust_account_id
    AND hca.cust_acct_site_id =hcs.cust_acct_site_id
    AND hcs.PRIMARY_FLAG      = 'Y'
    AND hcs.site_use_code     =p_site --'BILL_TO'
    AND ca.cust_account_id    = p_cust_acct_id;

    RETURN(ln_party_site_id);

  EXCEPTION

  WHEN NO_DATA_FOUND THEN
    print_log('FUNC8 get_party_site_id:  no data found exp' || p_cust_acct_id )
    ;
    ln_party_site_id:=NULL;

    RETURN (ln_party_site_id);

  WHEN TOO_MANY_ROWS THEN
    print_log('FUNC8 get_party_site_id:  Too Many Rows found exp' ||
    p_cust_acct_id );
    ln_party_site_id:=NULL;

    RETURN (ln_party_site_id);

  WHEN OTHERS THEN
    print_log(
    'FUNC8 get_party_site_id:Error while getting party site id for cust_account_id ='
    ||p_cust_acct_id || 'For Site ='||p_site||'  '||SQLCODE||' '||SQLERRM);
    ln_party_site_id:=NULL;

    RETURN (ln_party_site_id);

  END get_party_site_id;
-- +===========================================================================
-- =============+
-- | Name            : get_party_type                                         |
-- | Type            : FUNCTION
-- |
-- | Description     : This Function used to get party type by passing cust
-- account id      |
-- |                                                                |
-- |                              |
-- +===========================================================================
-- ================+

  FUNCTION get_party_type(
      p_cust_acct_id IN NUMBER)
    RETURN VARCHAR2
  IS
    ln_party_type VARCHAR2(50):=NULL;
  BEGIN
    --   SELECT nvl(ca.customer_class_code,'DS')
    --   INTO ln_party_type
    --  FROM hz_cust_accounts ca
    --  WHERE ca.cust_account_id =p_cust_acct_id;
    ln_party_type := 'DS';

    IF ln_party_type IS NOT NULL THEN

      RETURN(ln_party_type);

    ELSE

      RETURN(NULL);

    END IF;

  EXCEPTION

  WHEN NO_DATA_FOUND THEN

    RETURN (NULL);
    print_log('FUNC9 get_party_type:  no data found exp' || p_cust_acct_id );

  WHEN TOO_MANY_ROWS THEN

    RETURN (NULL);
    print_log('FUNC8 get_party_type:  Too Many Rows found exp' ||
    p_cust_acct_id );

  WHEN OTHERS THEN
    print_log(
    'FUNC9 get_party_type:Erorr while geting party site id for cust_account_id '
    ||p_cust_acct_id ||' '||SQLCODE||' '||SQLERRM);

    RETURN(NULL);

  END get_party_type;
-- +===========================================================================
-- =============+
-- | Name            : get_min_invoice_date
-- |
-- | Type            : FUNCTION
-- |
-- | Description     : This Function used to get max invoice date from pos file
-- |
-- |                                                                |
-- |                              |
-- +===========================================================================
-- ================+

  FUNCTION get_min_invoice_date(
      p_filename     IN VARCHAR2,
      p_cust_acct_id IN NUMBER)
    RETURN DATE
  IS
    ld_min_invoice_date DATE;
  BEGIN
    BEGIN

      SELECT MIN(date_invoiced)
      INTO ld_min_invoice_date
      FROM XXFT_POS_UPLOAD_LINKING_INT
      WHERE filename      =p_filename
      AND CUST_ACCOUNT_ID = p_cust_acct_id;

    EXCEPTION

    WHEN OTHERS THEN
      Print_log('FUN10 ERROR while geting min invoice date for file '||
      p_filename||SQLCODE||' '||SQLERRM);

    END;

    IF ld_min_invoice_date IS NULL THEN
      BEGIN

        SELECT MIN(date_shipped)
        INTO ld_min_invoice_date
        FROM XXFT_POS_UPLOAD_LINKING_INT
        WHERE filename      =p_filename
        AND CUST_ACCOUNT_ID = p_cust_acct_id;

      EXCEPTION

      WHEN OTHERS THEN
        Print_log('FUN10 ERROR while geting min date_shipped for file '||
        p_filename||SQLCODE||' '||SQLERRM);

      END;

    END IF;

    IF ld_min_invoice_date IS NOT NULL THEN

      RETURN(ld_min_invoice_date);

    ELSE

      RETURN(NULL);

    END IF;

  EXCEPTION

  WHEN NO_DATA_FOUND THEN

    RETURN (NULL);
    print_log('FUNC10 get_min_invoice_date:  no data found exp' || p_filename )
    ;

  WHEN OTHERS THEN
    print_log(
    'FUNC10 get_min_invoice_date:Erorr while geting min invoice date for file'
    ||p_filename ||' '||SQLCODE||' '||SQLERRM);

    RETURN(NULL);

  END get_min_invoice_date;
--
-- +===========================================================================
-- =============+
-- | Name            : get_max_invoice_date
-- |
-- | Type            : FUNCTION
-- |
-- | Description     : This Function used to get max invoice date from pos file
-- |
-- |                                                                |
-- |                              |
-- +===========================================================================
-- ================+

  FUNCTION get_max_invoice_date(
      p_filename     IN VARCHAR2,
      p_cust_acct_id IN NUMBER)
    RETURN DATE
  IS
    ld_max_invoice_date DATE;
  BEGIN
    BEGIN

      SELECT MAX(date_invoiced)
      INTO ld_max_invoice_date
      FROM XXFT_POS_UPLOAD_LINKING_INT
      WHERE filename      =p_filename
      AND CUST_ACCOUNT_ID = p_cust_acct_id;

    EXCEPTION

    WHEN OTHERS THEN
      Print_log('FUN10 ERROR while geting min invoice date for file '||
      p_filename||SQLCODE||' '||SQLERRM);

    END;

    IF ld_max_invoice_date IS NULL THEN
      BEGIN

        SELECT MAX(date_shipped)
        INTO ld_max_invoice_date
        FROM XXFT_POS_UPLOAD_LINKING_INT
        WHERE filename      =p_filename
        AND CUST_ACCOUNT_ID = p_cust_acct_id;

      EXCEPTION

      WHEN OTHERS THEN
        Print_log('FUN11: ERROR while geting min date_shipped for file '||
        p_filename||SQLCODE||' '||SQLERRM);

      END;

    END IF;
    --

    IF ld_max_invoice_date IS NOT NULL THEN

      RETURN(ld_max_invoice_date);

    ELSE

      RETURN(NULL);

    END IF;
    --

  EXCEPTION

  WHEN NO_DATA_FOUND THEN

    RETURN (NULL);
    print_log('FUNC11: get_max_invoice_date:  no data found exp' || p_filename
    );

  WHEN OTHERS THEN
    print_log(
    'FUNC11: get_max_invoice_date:Error while getting min invoice date for file'
    ||p_filename ||' '||SQLCODE||' '||SQLERRM);

    RETURN(NULL);

  END get_max_invoice_date;
-- +===========================================================================
-- =============+
-- | Name            : check_serialized_item                              |
-- | Type            : FUNCTION                                           |
-- | Description     : This function used to check item is serialized item or
-- not   |
-- |                                                          |
-- |                      |
-- | Parameters      : p_item_number                                |
-- |                                                                      |
-- |                                                                      |
-- | Returns         : 'Y' or 'N'                                    |
-- |                                                                |
-- +===========================================================================
-- =============+

  FUNCTION check_serialized_item(
      p_item_number IN NUMBER)
    RETURN VARCHAR2
  IS
    lc_yes_no VARCHAR2(1);
    ln_count  NUMBER;
    l_bdl     VARCHAR2(240);
    l_item    VARCHAR2(240);
  BEGIN

    SELECT item_type ,
      segment1
    INTO l_bdl ,
      l_item
    FROM mtl_system_items_b
    WHERE INVENTORY_ITEM_ID = p_item_number
    AND organization_id     =
      (
        SELECT organization_id
        FROM mtl_parameters
        WHERE organization_code ='FIM'
      );

    IF l_bdl     = 'PTO' AND l_item NOT LIKE 'FTK%' THEN
      lc_yes_no :='Y';

    ELSE

      SELECT COUNT(*)
      INTO ln_count
      FROM mtl_system_items_b
      WHERE inventory_item_id LIKE p_item_number--REPLACE(REPLACE(p_item_number
        -- ,'-','%'),' ','%')--p_item_number --in('FAP-221C-A','FC-10-P0223-311
        -- -02-12')
      AND organization_id =
        (
          SELECT organization_id
          FROM mtl_parameters
          WHERE organization_code ='FIM'
        )
      AND SERIAL_NUMBER_CONTROL_CODE !=1;

      IF ln_count  >0 THEN
        lc_yes_no :='Y';

      ELSE
        lc_yes_no :='N';

      END IF;

    END IF;

    RETURN(lc_yes_no);

  EXCEPTION

  WHEN OTHERS THEN
    PRINT_LOG('FUNC13: Error While checking Serialized item :'||p_item_number
    ||' '||SQLCODE||' '||SQLERRM);

    RETURN('N');

  END check_serialized_item;
-- +===========================================================================
-- =============+
-- | Name            : check_valid_serial_num                             |
-- | Type            : FUNCTION                                           |
-- | Description     : This function used to check serial Number is valid or
-- not   |
-- |                                                          |
-- |                      |
-- | Parameters      : p_serial_number                                |
-- |                                                                      |
-- |                                                                      |
-- | Returns         : 'Y' or 'N'                                    |
-- |                                                                |
-- +===========================================================================
-- =============+

  FUNCTION check_valid_serial_num(
      p_serial_number IN VARCHAR2)
    RETURN VARCHAR2
  IS
    lc_yes_no VARCHAR2(1);
    ln_count  NUMBER;
    ln_count1 NUMBER;
  BEGIN

    SELECT COUNT(*)
    INTO ln_count
    FROM MTL_SERIAL_NUMBERS msn
    WHERE serial_number         = rtrim(ltrim(p_serial_number))
    AND current_organization_id =
      (
        SELECT mp.organization_id
        FROM mtl_parameters mp
        WHERE mp.organization_code ='FIM'
      );

    IF ln_count =0 THEN

      SELECT COUNT(*)
      INTO ln_count1
      FROM MTL_SERIAL_NUMBERS msn
      WHERE serial_number LIKE REPLACE(REPLACE(rtrim(ltrim(p_serial_number)),
        '-','%'),' ','%')
      AND current_organization_id =
        (
          SELECT mp.organization_id
          FROM mtl_parameters mp
          WHERE mp.organization_code ='FIM'
        );

    END IF;

    IF ln_count  >0 OR ln_count1>0 THEN
      lc_yes_no :='Y';

    ELSE
      lc_yes_no :='N';

    END IF;

    RETURN(lc_yes_no);

  EXCEPTION

  WHEN OTHERS THEN
    PRINT_LOG('FUNC14: Error While checking valid serial Number :'||
    p_serial_number ||' '||SQLCODE||' '||SQLERRM);

    RETURN('N');

  END check_valid_serial_num;
-- +===========================================================================
-- =============+
-- | Name            : check_valid_po_number                              |
-- | Type            : FUNCTION                                           |
-- | Description     : This function used to check PO Number is valid or not
-- |
-- |                                                          |
-- |                      |
-- | Parameters      : p_serial_number                                |
-- |                                                                      |
-- |                                                                      |
-- | Returns         : 'Y' or 'N'                                    |
-- |                                                                |
-- +===========================================================================
-- =============+

  FUNCTION check_valid_po_number(
      p_po_number IN VARCHAR2,
      p_item_id VARCHAR2,
      p_cust_account_id IN NUMBER)
    RETURN VARCHAR2
  IS
    lc_yes_no VARCHAR2(1);
    ln_count  NUMBER;
  BEGIN

    SELECT COUNT(*) --OOH.CUST_PO_NUMBER
    INTO ln_count
    FROM WSH_DELIVERY_DETAILS WDD,
      WSH_SERIAL_NUMBERS wsn,
      OE_ORDER_HEADERS_ALL OOH,
      OE_ORDER_LINES_ALL OOL
    WHERE WDD.SOURCE_LINE_ID  =OOL.LINE_ID
    AND OOH.HEADER_ID         =OOL.HEADER_ID
    AND OOH.ORDER_NUMBER      =WDD.SOURCE_HEADER_NUMBER
    AND WDD.DELIVERY_DETAIL_ID=WSN.DELIVERY_DETAIL_ID
      --AND WSN.FM_SERIAL_NUMBER  =ln_serial_number
    AND OOH.CUST_PO_NUMBER    = p_po_number
    AND WDD.inventory_item_id =p_item_id
    AND WDD.CUSTOMER_ID       =p_cust_account_id;

    IF ln_count  >0 THEN
      lc_yes_no :='Y';

    ELSE
      lc_yes_no :='N';

    END IF;

    RETURN(lc_yes_no);

  EXCEPTION

  WHEN OTHERS THEN
    PRINT_LOG('FUNC15: Error While checking valid PO Number :'||p_po_number ||
    ' '||SQLCODE||' '||SQLERRM);

    RETURN('N');

  END check_valid_po_number;
-- +===========================================================================
-- ============================================+
-- | Name            : get_valid_serial_number
-- |
-- | Type            : FUNCTION
-- |
-- | Description     : This FUNCTION is used to get valid serial number with
-- Fortinet                |
-- |                   Serial number, Customer AND Product
-- |
-- |
-- |
-- | Parameters      : p_serial_number,p_cust_id ,p_item_id
-- |
-- |
-- |
-- | Returns         : lc_serial_number
-- |
-- +===========================================================================
-- ============================================+

  FUNCTION get_valid_serial_number(
      p_serial_number IN VARCHAR2,
      p_cust_id       IN NUMBER,
      p_item_id       IN VARCHAR2)
    RETURN VARCHAR2
  IS
    lc_serial_num VARCHAR2(100);
  BEGIN
    lc_serial_num:=NULL;
    BEGIN

      SELECT OZF.ATTRIBUTE8
      INTO lc_serial_num
      FROM OZF_SALES_TRANSACTIONS_ALL OZF,
        HZ_CUST_ACCOUNTS_ALL HZC
      WHERE OZF.SOURCE_CODE     = 'MA'
      AND OZF.transfer_type     = 'IN'
      AND OZF.reason_code       = 'ENDINVCONV'
      AND OZF.SOLD_TO_PARTY_ID  = HZC.PARTY_ID
      AND OZF.INVENTORY_ITEM_ID = p_item_id
      AND Upper(OZF.ATTRIBUTE8) = upper(p_serial_number)
      AND HZC.CUST_ACCOUNT_ID   = p_cust_id;

    EXCEPTION

    WHEN NO_DATA_FOUND THEN
      print_log('FUNC3. Error Serial Number does not exists in system for:'||
      p_cust_id);
      -- RETURN(NULL);

    WHEN TOO_MANY_ROWS THEN
      print_log('FUNC3.Serial number :'||p_serial_number||
      ' exists multiple times');
      -- RETURN(NULL);

    WHEN OTHERS THEN
      print_log(
      'FUNC3. Error while validating for Serial Number with Fortinet serial numbers :'
      ||p_serial_number||' '||SQLCODE||' '||SQLERRM);
      -- RETURN(NULL);

    END;

    IF lc_serial_num IS NULL THEN
      BEGIN

 /*       SELECT wsn.fm_serial_number
        INTO lc_serial_num
        FROM OE_ORDER_HEADERS_ALL OOH,
          WSH_DELIVERY_DETAILS WDD,
          OE_ORDER_LINES_ALL OOL,
          WSH_SERIAL_NUMBERS WSN
        WHERE OOH.HEADER_ID             = OOL.HEADER_ID
        AND TO_CHAR(OOH.HEADER_ID)      = TO_CHAR(WDD.SOURCE_HEADER_ID)
        AND TO_CHAR(OOL.LINE_ID)        = TO_CHAR(Wdd.SOURCE_LINE_ID)
        AND WDD.DELIVERY_DETAIL_ID      = WSN.DELIVERY_DETAIL_ID
        AND UPPER(WSN.FM_SERIAL_NUMBER) = UPPER(p_serial_number)
        AND
          (
            WDD.inventory_item_id  = p_item_id
          OR WDD.inventory_item_id =
            (
              SELECT HW.inventory_item_id
              FROM MTL_SYSTEM_ITEMS_B BD ,
                MTL_SYSTEM_ITEMS_B HW
              WHERE BD.inventory_item_id = p_item_id
              AND BD.ORGANIZATION_ID     =fnd_profile.value(
                'AMS_ITEM_ORGANIZATION_ID')
              AND BD.ORGANIZATION_ID = HW.ORGANIZATION_ID
              AND BD.attribute15     = HW.segment1
            )
          )
        AND OOH.sold_to_org_id = p_cust_id
        AND rownum             < 2 ;
        --
        --
*/

                SELECT attribute33
                INTO lc_serial_num
                FROM XXFT.XXFT_RPRO_ORDER_DETAILS
                WHERE item_id          = p_item_id
                AND CUSTOMER_ID        = p_cust_id
                AND upper(attribute33) = upper(p_serial_number)
                AND PARENT_LINE = 'Y'
                AND attribute37 = 'P' --ITS#575976 and 599807
                AND rownum             < 2;

      EXCEPTION

      WHEN NO_DATA_FOUND THEN
        print_log(
        'FUNC16. Error Serial Number does not exists in system for:p_cust_id '
        || p_cust_id||' '|| 'p_item_id :' || p_item_id );
        --RETURN(NULL);

      WHEN TOO_MANY_ROWS THEN
        print_log('FUNC16.Item  :'||p_item_id||' exists multiple times');
        -- RETURN(NULL);

      WHEN OTHERS THEN
        print_log('FUNC16. Error while validating for item Serial Number :'||
        p_item_id||' '||SQLCODE||' '||SQLERRM);
        --RETURN(NULL);

      END;

    END IF;

    IF lc_serial_num IS NOT NULL THEN

      RETURN(lc_serial_num);

    ELSE

      RETURN(NULL);

    END IF;

  END get_valid_serial_number;
-- +===========================================================================
-- =============================================+
-- | Name            : insert_int_table
-- |
-- | Type            : PROCEDURE
-- |
-- | Description     : This Procedure used to insert record into custom
-- interface table                             |
-- |
-- |
-- |
-- |
-- +===========================================================================
-- =============================================+

  PROCEDURE insert_int_table(
      lr_arrowus_rec IN xxft_pos_upload_linking_int%ROWTYPE,
      p_status OUT VARCHAR2,
      p_msg OUT VARCHAR2)
  IS
    ln_accept_cnt NUMBER :=0;
  BEGIN

    INSERT
    INTO XXFT_POS_UPLOAD_LINKING_INT
      (
        FILENAME ,
        DATE_INVOICED ,
        INVOICE_NUMBER ,
        RESELLER_NAME ,
        RESELLER_ADDRESS1 ,
        RESELLER_CITY ,
        RESELLER_STATE ,
        RESELLER_POSTAL_CODE ,
        ENDUSER_NAME ,
        ENDUSER_ADDRESS1 ,
        END_CUST_ADDR_2 ,
        END_CUST_CITY ,
        END_CUST_STATE ,
        END_CUST_POSTAL_CODE ,
        END_CUST_TYPE ,
        VENDOR_ITEM_NUMBER ,
        FT_ITEM_NUMBER ,
        DISTI_NAME ,
        QUANTITY ,
        SERIAL_NUMBER ,
        PURCHASE_PRICE ,
        SELLING_PRICE ,
        MSRP ,
        EXT_POS_AMT ,
        ORDER_LINE_NUMBER ,
        DISTI_CUST_NUMBER ,
        ORDER_NUMBER ,
        DATE_SHIPPED ,
        END_CUST_COUNTRY ,
        INVOICE_LINE_NUMBER ,
        AGREEMENT_NAME ,
        POS_REPORT_DATE ,
        PO_DATE ,
        RESELLER_ADDRESS2 ,
        PO_NUMBER ,
        RESELLER_COUNTRY ,
        SPLIT_LINE_ID ,
        SPLIT_HDR_ID ,
        STATUS ,
        ERROR_MSG ,
        REQUEST_ID ,
        LAST_UPDATE_DATE ,
        LAST_UPDATED_BY ,
        LAST_UPDATE_LOGIN ,
        CREATION_DATE ,
        CREATED_BY ,
        RECORD_ID ,
        BATCH_NUMBER
      )
      VALUES
      (
        lr_arrowus_rec.FILENAME ,
        lr_arrowus_rec.DATE_INVOICED ,
        lr_arrowus_rec.INVOICE_NUMBER ,
        lr_arrowus_rec.RESELLER_NAME ,
        lr_arrowus_rec.RESELLER_ADDRESS1 ,
        lr_arrowus_rec.RESELLER_CITY ,
        lr_arrowus_rec.RESELLER_STATE ,
        lr_arrowus_rec.RESELLER_POSTAL_CODE ,
        lr_arrowus_rec.ENDUSER_NAME ,
        lr_arrowus_rec.ENDUSER_ADDRESS1 ,
        lr_arrowus_rec.END_CUST_ADDR_2 ,
        lr_arrowus_rec.END_CUST_CITY ,
        lr_arrowus_rec.END_CUST_STATE ,
        lr_arrowus_rec.END_CUST_POSTAL_CODE ,
        lr_arrowus_rec.END_CUST_TYPE ,
        lr_arrowus_rec.VENDOR_ITEM_NUMBER ,
        lr_arrowus_rec.FT_ITEM_NUMBER ,
        lr_arrowus_rec.DISTI_NAME ,
        lr_arrowus_rec.QUANTITY ,
        lr_arrowus_rec.SERIAL_NUMBER ,
        lr_arrowus_rec.PURCHASE_PRICE ,
        lr_arrowus_rec.SELLING_PRICE ,
        lr_arrowus_rec.MSRP ,
        lr_arrowus_rec.EXT_POS_AMT ,
        lr_arrowus_rec.ORDER_LINE_NUMBER ,
        lr_arrowus_rec.DISTI_CUST_NUMBER ,
        lr_arrowus_rec.ORDER_NUMBER ,
        lr_arrowus_rec.DATE_SHIPPED ,
        lr_arrowus_rec.END_CUST_COUNTRY ,
        lr_arrowus_rec.INVOICE_LINE_NUMBER ,
        lr_arrowus_rec.AGREEMENT_NAME ,
        lr_arrowus_rec.POS_REPORT_DATE ,
        lr_arrowus_rec.PO_DATE ,
        lr_arrowus_rec.RESELLER_ADDRESS2 ,
        lr_arrowus_rec.PO_NUMBER ,
        lr_arrowus_rec.RESELLER_COUNTRY ,
        lr_arrowus_rec.split_line_id ,
        lr_arrowus_rec.split_hdr_id ,
        lr_arrowus_rec.STATUS ,
        lr_arrowus_rec.ERROR_MSG ,
        gn_request_id ,
        SYSDATE ,
        FND_GLOBAL.USER_ID ,
        FND_GLOBAL.LOGIN_ID ,
        SYSDATE ,
        FND_GLOBAL.USER_ID ,
        lr_arrowus_rec.RECORD_ID ,
        lr_arrowus_rec.BATCH_NUMBER
      );
    COMMIT;
    --ln_accept_cnt :=ln_accept_cnt+1;

  EXCEPTION

  WHEN OTHERS THEN
    p_status:='E';
    p_msg   :=SQLERRM ;

  END insert_int_table;
--
-- +===========================================================================
-- =============================================+
-- | Name            : insert_serial_qty_proc
-- |
-- | Type            : PROCEDURE
-- |
-- | Description     : This Procedure used to split line based on serial number
-- if serial number is null      |
-- |       then based on quantity if qty >1                     |
-- |                                                                     |
-- |                                   |
-- +===========================================================================
-- =============================================+

  PROCEDURE insert_serial_qty_proc
    (
      p_batch_number IN NUMBER
    )
  IS
    --Declare Variables
    ln_quantity      NUMBER;
    lc_serial_number VARCHAR2(4000);
    lc_error_msg     VARCHAR2(4000);
    lc_status        VARCHAR2(3);
    lr_serial_rec XXFT_POS_UPLOAD_LINKING_INT%rowtype;--lr_pos_data_rec;
    lr_qty_rec XXFT_POS_UPLOAD_LINKING_INT%rowtype;
    ln_line_id  NUMBER;
    ln_hdr_id   NUMBER;
    ln_qty_loop NUMBER;
    --define cursor

    CURSOR posupload_cur
    IS

      SELECT pos.* ,
        pos.rowid
      FROM XXFT_POS_UPLOAD_LINKING_INT pos
      WHERE 1          =1 --pos.filename ='INGRAM'
      AND batch_number =p_batch_number
      AND pos.quantity > 1
      OR pos.quantity  < -1 --Added  OR condition to get -ve value more than -1
        -- qty
      AND pos.STATUS ='N';
    --- cursor to get separated serial number  from comma separated serial
    -- number

    CURSOR pos_serialnum_cur(p_serial_number IN VARCHAR2)
    IS

      SELECT REPLACE(REPLACE(trim(regexp_substr(REPLACE(p_serial_number,',',' '
        ),'[^,][^ / ]([^[[:space:]])+', 1, level)),',',''),';','')
        serial_number
      FROM dual
        CONNECT BY REGEXP_SUBSTR(REPLACE(p_serial_number,',',' '),
        '[^,][^ / ]([^[[:space:]])+', 1, level) IS NOT NULL;
  BEGIN

    FOR lcu_pos IN posupload_cur
    LOOP
      --
      ln_quantity      :=NULL;
      lc_serial_number :=NULL;
      lc_status        :=NULL;
      lc_error_msg     :=NULL;
      --

      IF lcu_pos.serial_number IS NOT NULL THEN
        lc_serial_number       :=lcu_pos.serial_number;
        Print_log('FILENAME :'||lcu_pos.FILENAME ||' '||'SERIAL_NUMBER :'||
        lc_serial_number);

        IF lcu_pos.quantity < -1 THEN --added this condition for  qty value
          -- less than -1 13 -Jan-2015
          ln_quantity:=-1;

        ELSIF lcu_pos.quantity > 1 THEN --added this condition for qty greater
          -- than 1
          ln_quantity:=1;

        END IF;
        ln_hdr_id:=NULL;

        SELECT xxft_split_hdr_qty_id_s.nextval
        INTO ln_hdr_id
        FROM DUAL;
        BEGIN

          FOR lcu_serialnum IN pos_serialnum_cur(lcu_pos.serial_number)
          LOOP
            ln_line_id                         :=NULL;
            lr_serial_rec.FILENAME             :=NULL;
            lr_serial_rec.QUANTITY             :=NULL;
            lr_serial_rec.SERIAL_NUMBER        :=NULL;
            lr_serial_rec.DATE_INVOICED        :=NULL;
            lr_serial_rec.INVOICE_NUMBER       :=NULL;
            lr_serial_rec.RESELLER_NAME        :=NULL;
            lr_serial_rec.RESELLER_ADDRESS1    :=NULL;
            lr_serial_rec.RESELLER_ADDRESS2    :=NULL;
            lr_serial_rec.RESELLER_CITY        :=NULL;
            lr_serial_rec.RESELLER_STATE       :=NULL;
            lr_serial_rec.RESELLER_COUNTRY     :=NULL;
            lr_serial_rec.RESELLER_POSTAL_CODE :=NULL;
            lr_serial_rec.ENDUSER_NAME         :=NULL;
            lr_serial_rec.ENDUSER_ADDRESS1     :=NULL;
            lr_serial_rec.END_CUST_NAME        :=NULL;
            lr_serial_rec.END_CUST_ADDR_1      :=NULL;
            lr_serial_rec.END_CUST_ADDR_2      :=NULL;
            lr_serial_rec.END_CUST_CITY        :=NULL;
            lr_serial_rec.END_CUST_STATE       :=NULL;
            lr_serial_rec.END_CUST_POSTAL_CODE :=NULL;
            lr_serial_rec.END_CUST_COUNTRY     :=NULL;
            lr_serial_rec.END_CUST_TYPE        :=NULL;
            lr_serial_rec.VENDOR_ITEM_NUMBER   :=NULL;
            lr_serial_rec.FT_ITEM_NUMBER       :=NULL;
            lr_serial_rec.DISTI_NAME           :=NULL;
            lr_serial_rec.PURCHASE_PRICE       :=NULL;
            lr_serial_rec.SELLING_PRICE        :=NULL;
            lr_serial_rec.MSRP                 :=NULL;
            lr_serial_rec.EXT_POS_AMT          :=NULL;
            lr_serial_rec.ORDER_LINE_NUMBER    :=NULL;
            lr_serial_rec.DISTI_CUST_NUMBER    :=NULL;
            lr_serial_rec.ORDER_NUMBER         :=NULL;
            lr_serial_rec.DATE_SHIPPED         :=NULL;
            lr_serial_rec.INVOICE_LINE_NUMBER  :=NULL;
            lr_serial_rec.AGREEMENT_NAME       :=NULL;
            lr_serial_rec.POS_REPORT_DATE      :=NULL;
            lr_serial_rec.PO_DATE              :=NULL;
            lr_serial_rec.STATUS               :=NULL;
            lr_serial_rec.PO_NUMBER            :=NULL;
            lr_serial_rec.RECORD_ID            :=NULL;
            lr_serial_rec.split_line_id        :=NULL;
            lr_serial_rec.split_hdr_id         :=NULL;
            lr_serial_rec.batch_number         :=NULL;

            --
            --Get line id

            SELECT xxft_split_qty_id_s.nextval
            INTO ln_line_id
            FROM DUAL;

            Print_log('FILENAME :'||lcu_pos.FILENAME ||' '||'SERIAL_NUMBER :'||
            lcu_serialnum.SERIAL_NUMBER);
            lr_serial_rec.FILENAME             :=lcu_pos.FILENAME;
            lr_serial_rec.QUANTITY             :=ln_quantity;
            lr_serial_rec.SERIAL_NUMBER        :=lcu_serialnum.SERIAL_NUMBER;
            lr_serial_rec.DATE_INVOICED        :=lcu_pos.DATE_INVOICED;
            lr_serial_rec.INVOICE_NUMBER       :=lcu_pos.INVOICE_NUMBER;
            lr_serial_rec.RESELLER_NAME        :=lcu_pos.RESELLER_NAME;
            lr_serial_rec.RESELLER_ADDRESS1    :=lcu_pos.RESELLER_ADDRESS1;
            lr_serial_rec.RESELLER_ADDRESS2    :=lcu_pos.RESELLER_ADDRESS2;
            lr_serial_rec.RESELLER_CITY        :=lcu_pos.RESELLER_CITY;
            lr_serial_rec.RESELLER_STATE       :=lcu_pos.RESELLER_STATE;
            lr_serial_rec.RESELLER_COUNTRY     :=lcu_pos.RESELLER_COUNTRY;
            lr_serial_rec.RESELLER_POSTAL_CODE :=lcu_pos.RESELLER_POSTAL_CODE;
            lr_serial_rec.ENDUSER_NAME         :=lcu_pos.RESELLER_POSTAL_CODE;
            lr_serial_rec.ENDUSER_ADDRESS1     :=lcu_pos.ENDUSER_NAME;
            lr_serial_rec.END_CUST_NAME        :=lcu_pos.END_CUST_NAME;
            lr_serial_rec.END_CUST_ADDR_1      :=lcu_pos.END_CUST_ADDR_1;
            lr_serial_rec.END_CUST_ADDR_2      :=lcu_pos.END_CUST_ADDR_2;
            lr_serial_rec.END_CUST_CITY        :=lcu_pos.END_CUST_CITY;
            lr_serial_rec.END_CUST_STATE       :=lcu_pos.END_CUST_STATE;
            lr_serial_rec.END_CUST_POSTAL_CODE :=lcu_pos.END_CUST_POSTAL_CODE;
            lr_serial_rec.END_CUST_COUNTRY     :=lcu_pos.END_CUST_COUNTRY;
            lr_serial_rec.END_CUST_TYPE        :=lcu_pos.END_CUST_TYPE;
            lr_serial_rec.VENDOR_ITEM_NUMBER   :=lcu_pos.VENDOR_ITEM_NUMBER;
            lr_serial_rec.FT_ITEM_NUMBER       :=lcu_pos.FT_ITEM_NUMBER;
            lr_serial_rec.DISTI_NAME           :=lcu_pos.DISTI_NAME;
            lr_serial_rec.PURCHASE_PRICE       :=lcu_pos.PURCHASE_PRICE ;
            lr_serial_rec.SELLING_PRICE        :=lcu_pos.SELLING_PRICE ;
            lr_serial_rec.MSRP                 :=lcu_pos.MSRP;
            lr_serial_rec.EXT_POS_AMT          :=lcu_pos.EXT_POS_AMT ;
            lr_serial_rec.ORDER_LINE_NUMBER    :=lcu_pos.ORDER_LINE_NUMBER;
            lr_serial_rec.DISTI_CUST_NUMBER    :=lcu_pos.DISTI_CUST_NUMBER;
            lr_serial_rec.ORDER_NUMBER         :=lcu_pos.ORDER_NUMBER;
            lr_serial_rec.DATE_SHIPPED         :=lcu_pos.DATE_SHIPPED;
            lr_serial_rec.INVOICE_LINE_NUMBER  :=lcu_pos.INVOICE_LINE_NUMBER;
            lr_serial_rec.AGREEMENT_NAME       :=lcu_pos.AGREEMENT_NAME;
            lr_serial_rec.POS_REPORT_DATE      :=lcu_pos.POS_REPORT_DATE;
            lr_serial_rec.PO_DATE              :=lcu_pos.PO_DATE;
            lr_serial_rec.STATUS               :='N' ;
            lr_serial_rec.PO_NUMBER            :=lcu_pos.PO_NUMBER;
            lr_serial_rec.RECORD_ID            :=lcu_pos.RECORD_ID;
            lr_serial_rec.split_line_id        :=ln_line_id;
            lr_serial_rec.split_hdr_id         :=ln_hdr_id ;
            lr_serial_rec.batch_number         :=lcu_pos.batch_number;
            --
            --calling insert table procedure
            --
            insert_int_table ( lr_serial_rec, lc_status,lc_error_msg);
            --

            IF lc_status   ='E' THEN
              lc_error_msg:=lc_error_msg;
              PRINT_LOG('Error msg for Serial Number:'||lc_error_msg||
              'FILENAME :'||lcu_pos.filename ||'Serial Number :'||
              lcu_pos.serial_number);

            END IF;

          END LOOP;

          UPDATE XXFT_POS_UPLOAD_LINKING_INT
          SET status         ='S' ,
            error_msg        ='Split Record based on serial number ..Success..!!' ,
            split_hdr_id     =ln_hdr_id
          WHERE 1            =1 --rowid = lcu_pos.rowid
          AND record_id      = lcu_pos.record_id
          AND batch_number   =p_batch_number
          AND serial_number IS NOT NULL --lcu_pos.serial_number
          AND quantity       > 1
          OR quantity        < -1
          AND filename       =lcu_pos.filename
          AND batch_number   = lcu_pos.batch_number;
          COMMIT;

        EXCEPTION

        WHEN OTHERS THEN
          lc_status   :='E';
          lc_error_msg:='1 ERROR :'||SQLCODE ||' '||SQLERRM;
          PRINT_LOG(lc_error_msg);

        END;
        --

      ELSIF lcu_pos.quantity IS NOT NULL AND lcu_pos.serial_number IS NULL THEN
        ln_qty_loop          :=0;
        --ln_quantity:=1;

        IF lcu_pos.quantity < -1 THEN --added this condition for  qty value
          -- less than -1  on 13-Jan-2016
          ln_quantity :=  -1;
          ln_qty_loop := (-1)*(lcu_pos.quantity);

        ELSIF lcu_pos.quantity > 1 THEN --added this condition for qty greater
          -- than 1 on 13-Jan-2016
          ln_quantity :=1;
          ln_qty_loop :=lcu_pos.quantity;

        END IF;
        ln_hdr_id :=NULL;

        SELECT xxft_split_hdr_qty_id_s.nextval
        INTO ln_hdr_id
        FROM DUAL;
        BEGIN

          FOR lcu_qty IN 1.. ln_qty_loop --lcu_pos.quantity
          LOOP
            ln_line_id                      :=NULL;
            lr_qty_rec.FILENAME             :=NULL;
            lr_qty_rec.QUANTITY             :=NULL;
            lr_qty_rec.SERIAL_NUMBER        :=NULL;
            lr_qty_rec.DATE_INVOICED        :=NULL;
            lr_qty_rec.INVOICE_NUMBER       :=NULL;
            lr_qty_rec.RESELLER_NAME        :=NULL;
            lr_qty_rec.RESELLER_ADDRESS1    :=NULL;
            lr_qty_rec.RESELLER_ADDRESS2    :=NULL;
            lr_qty_rec.RESELLER_CITY        :=NULL;
            lr_qty_rec.RESELLER_STATE       :=NULL;
            lr_qty_rec.RESELLER_COUNTRY     :=NULL;
            lr_qty_rec.RESELLER_POSTAL_CODE :=NULL;
            lr_qty_rec.ENDUSER_NAME         :=NULL;
            lr_qty_rec.ENDUSER_ADDRESS1     :=NULL;
            lr_qty_rec.END_CUST_NAME        :=NULL;
            lr_qty_rec.END_CUST_ADDR_1      :=NULL;
            lr_qty_rec.END_CUST_ADDR_2      :=NULL;
            lr_qty_rec.END_CUST_CITY        :=NULL;
            lr_qty_rec.END_CUST_STATE       :=NULL;
            lr_qty_rec.END_CUST_POSTAL_CODE :=NULL;
            lr_qty_rec.END_CUST_COUNTRY     :=NULL;
            lr_qty_rec.END_CUST_TYPE        :=NULL;
            lr_qty_rec.VENDOR_ITEM_NUMBER   :=NULL;
            lr_qty_rec.FT_ITEM_NUMBER       :=NULL;
            lr_qty_rec.DISTI_NAME           :=NULL;
            lr_qty_rec.PURCHASE_PRICE       :=NULL;
            lr_qty_rec.SELLING_PRICE        :=NULL;
            lr_qty_rec.MSRP                 :=NULL;
            lr_qty_rec.EXT_POS_AMT          :=NULL;
            lr_qty_rec.ORDER_LINE_NUMBER    :=NULL;
            lr_qty_rec.DISTI_CUST_NUMBER    :=NULL;
            lr_qty_rec.ORDER_NUMBER         :=NULL;
            lr_qty_rec.DATE_SHIPPED         :=NULL;
            lr_qty_rec.INVOICE_LINE_NUMBER  :=NULL;
            lr_qty_rec.AGREEMENT_NAME       :=NULL;
            lr_qty_rec.POS_REPORT_DATE      :=NULL;
            lr_qty_rec.PO_DATE              :=NULL;
            lr_qty_rec.STATUS               :=NULL;
            lr_qty_rec.PO_NUMBER            :=NULL;
            lr_qty_rec.RECORD_ID            :=NULL;
            lr_qty_rec.split_line_id        :=NULL;
            lr_qty_rec.split_hdr_id         :=NULL;
            lr_qty_rec.batch_number         :=NULL;

            --
            --Get line id

            SELECT xxft_split_qty_id_s.nextval
            INTO ln_line_id
            FROM DUAL;
            -- PRINT_LOG('qty>1' ||'Split_line_id ='|| ln_line_id);
            lr_qty_rec.FILENAME             :=lcu_pos.FILENAME;
            lr_qty_rec.QUANTITY             :=ln_quantity;
            lr_qty_rec.SERIAL_NUMBER        :=lcu_pos.SERIAL_NUMBER;
            lr_qty_rec.DATE_INVOICED        :=lcu_pos.DATE_INVOICED;
            lr_qty_rec.INVOICE_NUMBER       :=lcu_pos.INVOICE_NUMBER;
            lr_qty_rec.RESELLER_NAME        :=lcu_pos.RESELLER_NAME;
            lr_qty_rec.RESELLER_ADDRESS1    :=lcu_pos.RESELLER_ADDRESS1;
            lr_qty_rec.RESELLER_ADDRESS2    :=lcu_pos.RESELLER_ADDRESS2;
            lr_qty_rec.RESELLER_CITY        :=lcu_pos.RESELLER_CITY;
            lr_qty_rec.RESELLER_STATE       :=lcu_pos.RESELLER_STATE;
            lr_qty_rec.RESELLER_COUNTRY     :=lcu_pos.RESELLER_COUNTRY;
            lr_qty_rec.RESELLER_POSTAL_CODE :=lcu_pos.RESELLER_POSTAL_CODE;
            lr_qty_rec.ENDUSER_NAME         :=lcu_pos.ENDUSER_NAME;
            lr_qty_rec.ENDUSER_ADDRESS1     :=lcu_pos.ENDUSER_ADDRESS1;
            lr_qty_rec.END_CUST_NAME        :=lcu_pos.END_CUST_NAME;
            lr_qty_rec.END_CUST_ADDR_1      :=lcu_pos.END_CUST_ADDR_1;
            lr_qty_rec.END_CUST_ADDR_2      :=lcu_pos.END_CUST_ADDR_2;
            lr_qty_rec.END_CUST_CITY        :=lcu_pos.END_CUST_CITY;
            lr_qty_rec.END_CUST_STATE       :=lcu_pos.END_CUST_STATE;
            lr_qty_rec.END_CUST_POSTAL_CODE :=lcu_pos.END_CUST_POSTAL_CODE;
            lr_qty_rec.END_CUST_COUNTRY     :=lcu_pos.END_CUST_COUNTRY;
            lr_qty_rec.END_CUST_TYPE        :=lcu_pos.END_CUST_TYPE;
            lr_qty_rec.VENDOR_ITEM_NUMBER   :=lcu_pos.VENDOR_ITEM_NUMBER;
            lr_qty_rec.FT_ITEM_NUMBER       :=lcu_pos.FT_ITEM_NUMBER;
            lr_qty_rec.DISTI_NAME           :=lcu_pos.DISTI_NAME;
            lr_qty_rec.PURCHASE_PRICE       :=lcu_pos.PURCHASE_PRICE ;
            lr_qty_rec.SELLING_PRICE        :=lcu_pos.SELLING_PRICE ;
            lr_qty_rec.MSRP                 :=lcu_pos.MSRP;
            lr_qty_rec.EXT_POS_AMT          :=lcu_pos.EXT_POS_AMT ;
            lr_qty_rec.ORDER_LINE_NUMBER    :=lcu_pos.ORDER_LINE_NUMBER;
            lr_qty_rec.DISTI_CUST_NUMBER    :=lcu_pos.DISTI_CUST_NUMBER;
            lr_qty_rec.ORDER_NUMBER         :=lcu_pos.ORDER_NUMBER;
            lr_qty_rec.DATE_SHIPPED         :=lcu_pos.DATE_SHIPPED;
            lr_qty_rec.INVOICE_LINE_NUMBER  :=lcu_pos.INVOICE_LINE_NUMBER;
            lr_qty_rec.AGREEMENT_NAME       :=lcu_pos.AGREEMENT_NAME;
            lr_qty_rec.POS_REPORT_DATE      :=lcu_pos.POS_REPORT_DATE;
            lr_qty_rec.PO_DATE              :=lcu_pos.PO_DATE;
            lr_qty_rec.STATUS               :='N' ;
            lr_qty_rec.PO_NUMBER            :=lcu_pos.PO_NUMBER;
            lr_qty_rec.RECORD_ID            :=lcu_pos.RECORD_ID;
            lr_qty_rec.split_line_id        :=ln_line_id;
            lr_qty_rec.split_hdr_id         :=ln_hdr_id;
            lr_qty_rec.batch_number         :=lcu_pos.batch_number;
            --
            --calling insert table procedure
            --
            insert_int_table ( lr_qty_rec, lc_status,lc_error_msg);
            --

            IF lc_status   ='E' THEN
              lc_error_msg:=lc_error_msg;
              PRINT_LOG('FILENAME :'||lcu_pos.filename ||'Error msg for qty :'
              ||lc_error_msg||'   '||'QUANTITY :'||lcu_pos.quantity );

            END IF;
            --
            --

          END LOOP;

          UPDATE XXFT_POS_UPLOAD_LINKING_INT
          SET status       ='S' ,
            error_msg      ='Split Record based on Qty..Success..!!' ,
            split_hdr_id   =ln_hdr_id
          WHERE 1          =1 --rowid = lcu_pos.rowid
          AND record_id    = lcu_pos.record_id
          AND batch_number =p_batch_number
          AND quantity     = lcu_pos.QUANTITY
          AND quantity     >1
          OR quantity      < -1
          AND filename     =lcu_pos.filename
          AND batch_number = lcu_pos.batch_number;
          COMMIT;

        EXCEPTION

        WHEN OTHERS THEN
          lc_status   :='E';
          lc_error_msg:='2 ERROR :'||SQLCODE ||' '||SQLERRM;
          PRINT_LOG(lc_error_msg);

          UPDATE XXFT_POS_UPLOAD_LINKING_INT
          SET status  ='E' ,
            error_msg =
            'Split Record based on serial number and Qty..Failed..!!' ,
            split_hdr_id   =ln_hdr_id
          WHERE record_id  = lcu_pos.record_id --rowid = lcu_pos.rowid
          AND quantity     = lcu_pos.QUANTITY
          AND quantity     >1
          OR quantity      < -1
          AND filename     =lcu_pos.filename
          AND batch_number = lcu_pos.batch_number;
          COMMIT;

        END;

      END IF;

    END LOOP;

  EXCEPTION

  WHEN OTHERS THEN
    lc_error_msg:='3 ERROR :'||SQLCODE ||' '||SQLERRM;
    PRINT_LOG(lc_error_msg);

  END insert_serial_qty_proc;
--
-- +===========================================================================
-- =============================================+
-- | Name            : exception_report_proc
-- |
-- | Type            : PROCEDURE
-- |
-- | Description     : This Procedure used to get all the the error records
-- |
-- |                                   |
-- |                                                                     |
-- |                                   |
-- +===========================================================================
-- =============================================+

  PROCEDURE exception_report_proc(
      errbuff OUT VARCHAR2 ,
      retcode OUT NUMBER ,
      p_filename IN VARCHAR2 ,
      p_value    IN VARCHAR2 )
  IS

    CURSOR filename_cur
    IS

      SELECT UNIQUE filename
      FROM XXFT_POS_UPLOAD_LINKING_INT pu
      WHERE pu.FILENAME =NVL(P_filename,pu.FILENAME)
      AND pu.status     =NVL(P_value,pu.status);

    CURSOR exp_cur(p_filename IN VARCHAR2,p_value IN VARCHAR2)
    IS

      SELECT DISTI_NAME ,
        POS_REPORT_DATE REPORT_DATE ,
        DATE_INVOICED ,
        INVOICE_NUMBER ,
        INVOICE_LINE_NUMBER ,
        ORDER_NUMBER SO_NUMBER ,
        ORDER_LINE_NUMBER SO_LINE_NUMBER ,
        DATE_SHIPPED ,
        po_number ,
        po_date ,
        RESELLER_NAME ,
        RESELLER_ADDRESS1 Reseller_Addr_1 ,
        RESELLER_ADDRESS2 Reseller_Addr_2 ,
        RESELLER_CITY ,
        RESELLER_STATE ,
        RESELLER_POSTAL_CODE ,
        RESELLER_COUNTRY ,
        END_CUST_NAME ,
        END_CUST_ADDR_1 ,
        END_CUST_ADDR_2 ,
        END_CUST_CITY ,
        END_CUST_STATE ,
        END_CUST_POSTAL_CODE ,
        END_CUST_COUNTRY ,
        END_CUST_TYPE ,
        VENDOR_ITEM_NUMBER Vendor_part_number ,
        FT_ITEM_NUMBER Fortinet_Part_Number ,
        QUANTITY ,
        SERIAL_NUMBER ,
        PURCHASE_PRICE ,
        SELLING_PRICE ,
        MSRP ,
        EXT_POS_AMT Exdended_POS_Amt ,
        AGREEMENT_NAME ,
        CREATION_DATE Load_date ,
        (
          SELECT user_name
          FROM fnd_user
          WHERE user_id =pu.CREATED_BY
        )
      Load_by_user ,
      filename ,
      ERROR_MSG
    FROM XXFT_POS_UPLOAD_LINKING_INT pu
    WHERE pu.FILENAME =NVL(P_filename,pu.FILENAME)
    AND pu.status     =NVL(P_value,pu.status);
  BEGIN

    FOR lcu_filename IN filename_cur
    LOOP
      PRINT_OUTPUT('Request id '||gn_request_id ||'--------'||'FileName :'||
      lcu_filename.filename ||'---------------'||'Date :'||sysdate);
      print_output(
      '-----------------------------------------------------------------------------------------------------------'
      );
      print_output('DISTI_NAME'||'    ' ||'REPORT_DATE' || '    ' ||
      'DATE_INVOICED' ||'    ' ||'INVOICE_NUMBER' || '    ' ||
      'INVOICE_LINE_NUMBER' || '    ' ||'SO_NUMBER' || '    ' ||
      'SO_LINE_NUMBER' || '    ' ||'DATE_SHIPPED' || '    ' ||'po_number' ||
      '    ' ||'po_date' || '    ' ||'RESELLER_NAME' || '    ' ||
      'Reseller_Addr_1' || '    ' ||'Reseller_Addr_2' || '    ' ||
      'RESELLER_CITY' || '    ' ||'RESELLER_STATE' || '    ' ||
      'RESELLER_POSTAL_CODE' || '    ' ||'RESELLER_COUNTRY' || '    ' ||
      'END_CUST_NAME' || '    ' ||'END_CUST_ADDR_1' || '    ' ||
      'END_CUST_ADDR_2' || '    ' ||'END_CUST_CITY' || '    ' ||
      'END_CUST_STATE' || '    ' ||'END_CUST_POSTAL_CODE' || '    ' ||
      'END_CUST_COUNTRY' || '    ' ||'END_CUST_TYPE' || '    ' ||
      'Vendor_part_number' || '    ' ||'Fortinet_Part_Number' || '    ' ||
      'QUANTITY' || '    ' ||'SERIAL_NUMBER' || '    ' ||'PURCHASE_PRICE' ||
      '    ' ||'SELLING_PRICE' || '    ' ||'MSRP' || '    ' ||
      'Exdended_POS_Amt' || '    ' ||'AGREEMENT_NAME' || '    ' ||'Load_date'
      || '    ' ||'Load_by_user' || '    ' ||'Filename' || '    ' ||'ERROR_MSG'
      );
      PRINT_OUTPUT(
      '-------------------------------------------------------------------------------------------------------------------------------------------'
      );

      FOR lcu_exp IN exp_cur(lcu_filename.filename,p_value)
      LOOP
        PRINT_OUTPUT(lcu_exp.disti_name ||'    ' ||lcu_exp.REPORT_DATE ||'    '
        ||lcu_exp.DATE_INVOICED ||'    ' ||lcu_exp.INVOICE_NUMBER || '    ' ||
        lcu_exp.INVOICE_LINE_NUMBER || '    ' ||lcu_exp.SO_NUMBER || '    ' ||
        lcu_exp.SO_LINE_NUMBER || '    ' ||lcu_exp.DATE_SHIPPED || '    ' ||
        lcu_exp.po_number || '    ' ||lcu_exp.po_date || '    ' ||'"'||
        lcu_exp.RESELLER_NAME ||'"' || '    ' ||'"'||lcu_exp.Reseller_Addr_1 ||
        '"' || '    ' ||'"'||lcu_exp.Reseller_Addr_2 ||'"' || '    ' ||'"'||
        lcu_exp.RESELLER_CITY ||'"' || '    ' ||lcu_exp.RESELLER_STATE ||
        '    ' ||lcu_exp.RESELLER_POSTAL_CODE || '    ' ||
        lcu_exp.RESELLER_COUNTRY || '    ' ||'"'||lcu_exp.END_CUST_NAME ||'"'
        || '    ' ||'"'||lcu_exp.END_CUST_ADDR_1 ||'"' || '    ' ||'"'||
        lcu_exp.END_CUST_ADDR_2 ||'"' || '    ' ||lcu_exp.END_CUST_CITY ||
        '    ' ||lcu_exp.END_CUST_STATE || '    ' ||
        lcu_exp.END_CUST_POSTAL_CODE || '    ' ||lcu_exp.END_CUST_COUNTRY ||
        '    ' ||lcu_exp.END_CUST_TYPE || '    ' ||lcu_exp.Vendor_part_number
        || '    ' ||lcu_exp.Fortinet_Part_Number || '    ' ||lcu_exp.QUANTITY
        || '    ' ||lcu_exp.SERIAL_NUMBER || '    ' ||lcu_exp.PURCHASE_PRICE ||
        '    ' ||lcu_exp.SELLING_PRICE || '    ' ||lcu_exp.MSRP || '    ' ||
        lcu_exp.Exdended_POS_Amt || '    ' ||lcu_exp.AGREEMENT_NAME || '    '
        ||lcu_exp.Load_date || '    ' ||lcu_exp.Load_by_user || '    ' ||
        lcu_exp.Filename || '    ' ||lcu_exp.ERROR_MSG);

      END LOOP;

    END LOOP;

  END exception_report_proc;
--
-- +===========================================================================
-- =============================================+
-- | Name            : insert_interface_table
-- |
-- | Type            : PROCEDURE
-- |
-- | Description     : This Procedure used to insert heade and line interface
-- table and call API to process record into base table      |
-- |                           |
-- |                                                                     |
-- |                                   |
-- +===========================================================================
-- =============================================+

  PROCEDURE insert_interface_table(
      p_batch_number IN NUMBER,
      p_batch_id OUT NUMBER,
      o_status OUT VARCHAR2)
  IS
    ld_sysdate              DATE := SYSDATE;
    ln_org_id               NUMBER; --fnd_profile.VALUE('ORG_ID');
    ln_obeject_version      NUMBER;
    ln_resale_batch_id      NUMBER;
    lc_batch_number         VARCHAR2(300);
    ln_order_number         NUMBER;
    lc_partner_claim_number VARCHAR2(300);
    lx_msg_data             VARCHAR2(3000);
    lc_msg_dummy            VARCHAR2(3000);
    lx_return_status        VARCHAR2(300);
    ld_min_invoice_date     DATE;
    ld_max_invoice_date     DATE;
    lx_msg_count            NUMBER;
    ln_line_success_cnt     NUMBER;
    ln_line_rejected_cnt    NUMBER;
    ln_line_total_cnt       NUMBER;
    ln_hdr_success_cnt      NUMBER;
    ln_hdr_total_cnt        NUMBER;
    ln_hdr_rejected_cnt     NUMBER;
    ln_party_id             NUMBER;
    ln_party_site_id        NUMBER;
    ln_cust_account_id      NUMBER;
    lc_party_type           VARCHAR2(100);
    lc_return_status        VARCHAR2(1);
    lc_error_msg            VARCHAR2(4000);
    ln_bill_to_site_id      NUMBER;
    ln_resale_line_int_id   NUMBER;
    lc_link_status          VARCHAR2(1);
    --Cursor to get unique file name

    CURSOR filename_cur
    IS

      SELECT UNIQUE pou.filename,
        pou.CUST_ACCOUNT_ID
      FROM XXFT_POS_UPLOAD_LINKING_INT pou
      WHERE 1=1
        --AND pou.filename ='FINETECUS'
      AND status        ='V'
      AND batch_number  = p_batch_number
      AND filename NOT IN
        (
          SELECT DISTINCT filename
          FROM XXFT_POS_UPLOAD_LINKING_INT
          WHERE status = 'E'
        ); --checked this code working fine  will add bach number as additional
    -- condition
    --Define cursor

    CURSOR upload_cur(p_filename IN VARCHAR2,p_CUST_ACCOUNT_ID IN NUMBER)
    IS

      SELECT pos.* ,
        pos.rowid
      FROM XXFT_POS_UPLOAD_LINKING_INT pos
      WHERE 1             =1
      AND FILENAME        = p_filename
      AND batch_number    = p_batch_number
      AND CUST_ACCOUNT_ID = p_CUST_ACCOUNT_ID
      AND pos.STATUS      ='V'
      AND filename NOT   IN
        (
          SELECT DISTINCT filename
          FROM XXFT_POS_UPLOAD_LINKING_INT
          WHERE status = 'E'
        ); --checked this working fine will add bach number as addition
    -- condition
  BEGIN

    FOR lcu_file IN filename_cur
    LOOP
      Print_log( ' FILE NAME : '|| lcu_file.filename);
      ln_resale_batch_id   :=NULL;
      ln_line_success_cnt  :=0;
      ln_line_rejected_cnt :=0;
      ln_line_total_cnt    :=0;
      ln_hdr_success_cnt   :=0;
      ln_hdr_rejected_cnt  :=0;
      ln_hdr_total_cnt     :=0;
      ln_party_id          :=NULL;
      ln_party_site_id     :=NULL;
      ln_obeject_version   :=0;
      ld_min_invoice_date  :=NULL;
      ld_max_invoice_date  :=NULL;
      ln_bill_to_site_id   :=NULL;
      ln_order_number      :=NULL;
      BEGIN

        SELECT COUNT(*)
        INTO ln_line_total_cnt
        FROM XXFT_POS_UPLOAD_LINKING_INT pou
        WHERE 1          =1
        AND pou.filename =lcu_file.filename --'FINETECUS'
        AND status       ='V';

      END;
      BEGIN

        SELECT COUNT(*)
        INTO ln_hdr_total_cnt
        FROM
          (
            SELECT UNIQUE filename
            FROM XXFT_POS_UPLOAD_LINKING_INT
            WHERE filename =lcu_file.filename
          );

      END;

      --fnd_global.APPS_INITIALIZE (1002795,22371,682); -- 1177,51387,682 (KR)/
      -- 1178,51690,682(AP)
      --MO_GLOBAL.INIT('OZF');
      --MO_GLOBAL.SET_POLICY_CONTEXT(p_access_mode => 'S',  p_org_id =>81); --
      -- SINGLE OU Setting
      -- get resale batch id

      SELECT ozf_resale_batches_all_s.nextval
      INTO ln_resale_batch_id
      FROM DUAL;

      print_output('POS Batch Number : '||ln_resale_batch_id);
      lc_batch_number         := ln_resale_batch_id;
      lc_partner_claim_number := lc_batch_number;
      ln_cust_account_id      :=lcu_file.CUST_ACCOUNT_ID;--get_customer_id(
      -- lcu_file.filename,'POS');
      ln_party_id         :=get_party_id (ln_cust_account_id);
      ln_party_site_id    :=get_party_site_id(ln_cust_account_id,'SHIP_TO');
      ln_bill_to_site_id  :=get_party_site_id(ln_cust_account_id,'BILL_TO');
      lc_party_type       :=get_party_type(ln_cust_account_id);
      ln_org_id           :=FND_PROFILE.VALUE('ORG_ID');
      ld_min_invoice_date :=get_min_invoice_date(lcu_file.filename,
      lcu_file.CUST_ACCOUNT_ID);
      ld_max_invoice_date :=get_max_invoice_date(lcu_file.filename,
      lcu_file.CUST_ACCOUNT_ID);
      ln_obeject_version :=1;
      ln_order_number    := ln_resale_batch_id;
      print_log('ln_party_site_id '||ln_party_site_id||' '||
      'ln_bill_to_site_id '||ln_bill_to_site_id||' '||'lc_batch_number'||
      lc_batch_number);
      print_log('ld_min_invoice_date '||ld_min_invoice_date|| '  '||
      'ld_max_invoice_date' ||ld_max_invoice_date);
      -- IF (ln_cust_account_id is not null and ln_party_id is not null and
      -- ln_party_site_id is not null  and lc_party_type is not null)

      IF (ln_cust_account_id IS NOT NULL AND ln_party_id IS NOT NULL ) THEN
        lc_return_status     :='Y';

      ELSE
        lc_return_status :='N';

      END IF;

      IF lc_return_status ='Y' THEN
        BEGIN

          INSERT
          INTO OZF_RESALE_BATCHES_ALL
            (
              resale_batch_id ,
              batch_number ,
              object_version_number ,
              last_update_date ,
              last_updated_by ,
              creation_date ,
              created_by ,
              batch_type ,
              report_date ,
              report_start_date ,
              report_end_date ,
              status_code ,
              comments ,
              org_id ,
              currency_code ,
              partner_claim_number ,
              partner_party_id ,
              partner_cust_account_id ,
              partner_site_id ,
              partner_type
            )
            VALUES
            (
              ln_resale_batch_id -- ozf_resale_batches_all_s.nextval for
              -- RESALE_BATCH_ID
              ,
              lc_batch_number -- BATCH_NUMBER
              ,
              ln_obeject_version --1                                  --
              -- OBJECT_VERSION_NUMBER
              ,
              TRUNC(sysdate) -- LAST_UPDATE_DATE
              ,
              gn_user_id
              /*Trademgr*/
              -- LAST_UPDATEd_BY
              ,
              TRUNC(sysdate) -- CREATION_DATE
              ,
              gn_user_id
              /*Trademgr*/
              -- CREATED_BY
              ,
              'TRACING' -- BATCH_TYPE
              ,
              TRUNC(sysdate) -- REPORT_DATE
              ,
              TRUNC(ld_min_invoice_date) --trunc(sysdate)-1   --
              -- REPORT_START_DATE -- this would be min invoice date/ship date
              -- in pos file
              ,
              TRUNC(ld_max_invoice_date) -- REPORT_END_DATE   -- this would be
              -- max invoice date/ship date in pos file
              ,
              'OPEN' -- STATUS_CODE
              ,
              lcu_file.filename
              ||' '
              || 'Case' -- COMMENTS
              ,
              ln_org_id --81                                -- ORG_ID
              ,
              'USD' -- CURRENCY_CODE
              ,
              lc_partner_claim_number -- PARTNER_CLAIM_NUMBER
              ,
              ln_party_id -- PARTNER_PARTY_ID   CDS
              ,
              ln_cust_account_id --6042                              --
              -- PARTNER_CUST_ACCOUNT_ID
              ,
              ln_party_site_id --9037                             --
              -- PARTNER_SITE_ID
              ,
              lc_party_type --'DS'                               --
              -- PARTNER_TYPE
            );
          COMMIT;
          ln_hdr_success_cnt:=ln_hdr_success_cnt+1;

        EXCEPTION

        WHEN OTHERS THEN
          lc_return_status :='N';
          Print_log('Error while inserting records into OZF_RESALE_BATCHES_ALL'
          ||SQLCODE ||' '||SQLERRM);
          lc_error_msg :=
          'Error while inserting records into OZF_RESALE_BATCHES_ALL'||SQLCODE
          ||' '||SQLERRM;

        END;
        --

        IF lc_return_status ='N' THEN

          UPDATE XXFT_POS_UPLOAD_LINKING_INT
          SET Status          ='E' ,
            error_msg         =lc_error_msg ,
            request_id        =gn_request_id ,
            last_update_date  =sysdate ,
            last_updated_by   =gn_user_id ,
            last_update_login = gn_login_id
          WHERE filename      =lcu_file.filename ;
          COMMIT;
          ln_hdr_rejected_cnt:=ln_hdr_rejected_cnt+1;

        END IF;
        PRINT_OUTPUT(
        '************Interface Header Table OZF_RESALE_BATCHES_ALL insertion status*******************'
        );
        PRINT_OUTPUT('FILE NAME      '||lcu_file.filename);
        PRINT_OUTPUT('Total Number of record Processed  '||ln_hdr_total_cnt);
        PRINT_OUTPUT('Total Number of record successfuly Processed' ||
        ln_hdr_success_cnt);
        PRINT_OUTPUT('Total Number of record Rejected' ||ln_hdr_rejected_cnt);
        PRINT_OUTPUT(
        '***********************************************************************************'
        );

        IF lc_return_status ='Y' THEN

          FOR lcu_upload IN upload_cur(lcu_file.filename,
          lcu_file.CUST_ACCOUNT_ID)
          LOOP
            lc_return_status :='Y';

            --Get line id

            SELECT ozf_resale_lines_int_all_s.nextval
            INTO ln_resale_line_int_id
            FROM dual;
            BEGIN

              INSERT
              INTO OZF_RESALE_LINES_INT_ALL
                (
                  resale_line_int_id ,
                  object_version_number ,
                  last_update_date ,
                  last_updated_by ,
                  creation_date ,
                  created_by ,
                  status_code ,
                  resale_batch_id ,
                  product_transfer_movement_type ,
                  product_transfer_date ,
                  tracing_flag ,
                  ship_from_cust_account_id ,
                  ship_from_site_id ,
                  sold_from_cust_account_id ,
                  sold_from_site_id ,
                  bill_to_cust_account_id ,
                  bill_to_site_use_id ,
                  bill_to_party_id ,
                  bill_to_party_site_id
                  --,bill_to_party_name
                  ,
                  ship_to_cust_account_id ,
                  ship_to_site_use_id ,
                  direct_customer_flag ,
                  order_type_id ,
                  order_type ,
                  order_category ,
                  agreement_type ,
                  agreement_id ,
                  agreement_name ,
                  price_list_id ,
                  currency_code ,
                  date_invoiced ,
                  order_number ,
                  date_ordered ,
                  date_shipped ,
                  purchase_price ,
                  selling_price ,
                  uom_code ,
                  quantity ,
                  credit_code ,
                  inventory_item_id ,
                  UPC_CODE ,
                  org_id ,
                  data_source_code ,
                  invoice_number ,
                  invoice_line_number ,
                  order_line_number ,
                  po_number ,
                  line_attribute1 ,
                  line_attribute2 ,
                  line_attribute3 ,
                  line_attribute4 ,
                  line_attribute5 ,
                  line_attribute6 ,
                  line_attribute7 ,
                  line_attribute8 ,
                  line_attribute9 ,
                  line_attribute10 ,
                  line_attribute11 ,
                  line_attribute12 ,
                  LINE_ATTRIBUTE_CATEGORY
                  --, line_attribute13
                  --, line_attribute14
                  --, line_attribute15
                  ,
                  orig_system_item_number ,
                  item_number ,
                  orig_system_purchase_price ,
                  orig_system_selling_price ,
                  orig_system_agreement_name ,
                  CLAIMED_AMOUNT,
                  TOTAL_CLAIMED_AMOUNT,
                  request_id
                )
                VALUES
                (
                  ln_resale_line_int_id --ozf_resale_lines_int_all_s.nextval
                  -- /*resale_line_int_id*/
                  ,
                  1
                  /*object_version_number*/
                  ,
                  TRUNC(sysdate)
                  /*last_update_date*/
                  ,
                  gn_user_id
                  /*trademgr*/
                  /*last_updated_by*/
                  ,
                  TRUNC(sysdate)
                  /*creation_date*/
                  ,
                  gn_user_id
                  /*trademgr*/
                  /*created_by*/
                  ,
                  'OPEN'
                  /*status_code*/
                  ,
                  ln_resale_batch_id
                  /*resale_batch_id */
                  ,
                  'DC'
                  /*product_transfer_movement_type*/
                  ,
                  TRUNC(sysdate)
                  /*product_transfer_date*/
                  ,
                  'T'
                  /*tracing_flag*/
                  ,
                  ln_cust_account_id --6042
                  -- /*ship_from_cust_account_id*/
                  ,
                  ln_party_site_id --9037                                    /*
                  -- ship_from_site_id*/
                  ,
                  ln_cust_account_id --6042
                  -- /*sold_from_cust_account_id tech data*/
                  ,
                  ln_party_site_id --9037                                    /*
                  -- sold_from_site_id*/
                  ,
                  NULL --4951                                    /*
                  -- bill_to_cust_account_id */
                  ,
                  NULL
                  /*bill_to_site_use_id*/
                  ,
                  ln_party_id --20065                                    /*
                  -- bill_to_party_id*/
                  ,
                  ln_bill_to_site_id
                  /*bill_to_party_site_id*/
                  ,
                  NULL
                  /*ship_to_cust_account_id*/
                  ,
                  NULL
                  /*ship_to_site_use_id*/
                  ,
                  'F'
                  /*direct_customer_flag*/
                  ,
                  NULL
                  /*order_type_id*/
                  ,
                  NULL
                  /*'standard (line invoicing' --order_type*/
                  ,
                  'ORDER'
                  /*order_category*/
                  ,
                  NULL --'PL'                                    /*
                  -- agreement_type*/
                  ,
                  NULL --1000                                    /*agreement_id
                  -- */
                  ,
                  NULL --lcu_upload.agreement_name --'Corporate'
                  -- /*agreement_name*/
                  ,
                  NULL --1000                                    /*20808
                  -- price_list_id*/
                  ,
                  'USD'
                  /*currency_code*/
                  ,
                  TRUNC(NVL(lcu_upload.date_invoiced,sysdate)) --trunc(sysdate)
                  -- /*date_invoiced*/-- from POS
                  ,
                  NVL(lcu_upload.order_number,ln_order_number) --v_order_number
                  -- /*order_number*/-- from POS
                  ,
                  TRUNC(NVL(lcu_upload.po_date,ld_min_invoice_date))--trunc(
                  -- sysdate)                          /*date_ordered*/-- from
                  -- POS
                  ,
                  TRUNC(NVL(lcu_upload.date_shipped,ld_min_invoice_date)) --
                  -- trunc(sysdate)                          /*date_shipped*/--
                  -- from POS
                  ,
                  lcu_upload.purchase_price --1699
                  -- /*purchase_price*/-- from POS
                  ,
                  lcu_upload.selling_price -- 1599
                  -- /*selling_price*/-- from POS
                  ,
                  'EA'
                  /*uom_code*/
                  ,
                  lcu_upload.quantity
                  /*quantity*/
                  -- from POS
                  ,
                  NULL
                  /*credit_code*/
                  ,
                  lcu_upload.inventory_item_id --2004
                  -- /*inventory_item_id sj-item01 */-- from POS
                  ,
                  lcu_upload.SERIAL_NUMBER ,
                  ln_org_id --81                                    /*org_id*/
                  ,
                  NULL
                  /*data_source_code*/
                  ,
                  lcu_upload.INVOICE_NUMBER ,
                  lcu_upload.INVOICE_LINE_NUMBER ,
                  lcu_upload.ORDER_LINE_NUMBER ,
                  lcu_upload.PO_NUMBER ,
                  lcu_upload.RESELLER_NAME
                  --  ,lcu_upload.RESELLER_ADDRESS1         --Commented on 24-
                  -- Dec-15
                  --  ,lcu_upload.RESELLER_ADDRESS2         --Commented on 24-
                  -- Dec-15
                  ,
                  lcu_upload.RESELLER_CITY ,
                  lcu_upload.RESELLER_STATE ,
                  lcu_upload.RESELLER_POSTAL_CODE ,
                  lcu_upload.RESELLER_COUNTRY ,
                  lcu_upload.ENDUSER_NAME
                  --  ,lcu_upload.ENDUSER_ADDRESS1   --Commented on 24-Dec-15
                  --  ,lcu_upload.ENDUSER_ADDRESS2   --Commented on 24-Dec-15
                  ,
                  lcu_upload.END_CUST_CITY ,
                  lcu_upload.END_CUST_STATE ,
                  lcu_upload.END_CUST_POSTAL_CODE ,
                  lcu_upload.END_CUST_COUNTRY ,
                  lcu_upload.END_CUST_TYPE ,
                  ln_resale_line_int_id --Added for Duplicate issue to resolve
                  -- on 24 Dec 15
                  ,
                  'POS' ,
                  lcu_upload.VENDOR_ITEM_NUMBER ,
                  lcu_upload.FT_ITEM_NUMBER ,
                  lcu_upload.MSRP ,
                  lcu_upload.EXT_POS_AMT ,
                  lcu_upload.AGREEMENT_NAME ,
                  0,
                  0,
                  gn_request_id
                );
              COMMIT;

            EXCEPTION

            WHEN OTHERS THEN
              lc_return_status :='E';
              PRINT_LOG ('ERROR  while inserting into OZF_RESALE_LINES_INT_ALL'
              ||SQLCODE ||' '||SQLERRM);
              lc_error_msg :=
              'ERROR  while inserting into OZF_RESALE_LINES_INT_ALL'||SQLCODE
              ||' '||SQLERRM;

            END;

            IF lc_return_status='Y' THEN

              UPDATE XXFT_POS_UPLOAD_LINKING_INT pos
              SET Status  ='P' ,
                error_msg =
                'Successfully inserted into OZF_RESALE_LINES_INT_ALL .....!!!'
                ,
                request_id        =gn_request_id ,
                last_update_date  =sysdate ,
                last_updated_by   =gn_user_id ,
                last_update_login = gn_login_id
              WHERE record_id     = lcu_upload.record_id --pos.rowid =
                -- lcu_upload.rowid
              AND pos.filename= lcu_upload.filename;
              COMMIT;
              ln_line_success_cnt:=ln_line_success_cnt+1;

            ELSE

              UPDATE XXFT_POS_UPLOAD_LINKING_INT pos
              SET Status          ='E' ,
                error_msg         =lc_error_msg ,
                request_id        =gn_request_id ,
                last_update_date  =sysdate ,
                last_updated_by   =gn_user_id ,
                last_update_login = gn_login_id
              WHERE record_id     = lcu_upload.record_id --pos.rowid =
                -- lcu_upload.rowid
              AND pos.filename= lcu_upload.filename;
              COMMIT;
              ln_line_rejected_cnt :=ln_line_rejected_cnt+1;

            END IF;

          END LOOP; --line loop

        END IF;

        IF ln_line_success_cnt>0 THEN --Added on 14-Jan-2016
          p_batch_id         := ln_resale_batch_id;
          o_status           :='S';

        ELSE
          p_batch_id := NULL;
          o_status   :='E';

        END IF;
        --
        PRINT_OUTPUT(
        '************Interface table OZF_RESALE_LINES_INT_ALL insertion status*******************'
        );
        PRINT_OUTPUT('FILE NAME      '||lcu_file.filename);
        PRINT_OUTPUT('Total Number of record Processed  '||ln_line_total_cnt);
        PRINT_OUTPUT('Total Number of record successful Processed' ||
        ln_line_success_cnt);
        PRINT_OUTPUT('Total Number of record Rejected' ||ln_line_rejected_cnt);
        PRINT_OUTPUT(
        '****************************************************************************************'
        );
        --
        --
        PRINT_LOG('Calling API ozf_resale_pub.start_process_iface');
        -- Call the procedure
        ozf_resale_pub.start_process_iface( p_api_version => 1, p_init_msg_list
        => '', p_commit => '', p_validation_level => '', p_resale_batch_id =>
        ln_resale_batch_id, x_return_status => lx_return_status, x_msg_data =>
        lx_msg_data, x_msg_count => lx_msg_count);
        PRINT_LOG('x_return_status = ' || lx_return_status );
        PRINT_LOG('x_msg_data = ' || lx_msg_data );
        PRINT_LOG('x_msg_count = ' || lx_msg_count );

        FOR i IN 1 .. fnd_msg_pub.count_msg
        LOOP
          fnd_msg_pub.get(i, fnd_api.g_false, lx_msg_data, lc_msg_dummy);
          PRINT_LOG('I : ' || i || ' MSG : ' || lx_msg_data);

        END LOOP;
        PRINT_LOG(
        '***************END API calling OZF_RESALE_PUB.start_process_iface*********** '
        );
        /* Commented as per Discussion With Satish on 7-Jan-2015
        PRINT_LOG('***************Calling API calling
        ozf_resale_pub.start_payment*********** ');
        ozf_resale_pub.start_payment(
        p_api_version       => 1,
        p_init_msg_list     => '',
        p_commit            => '',
        p_validation_level  => '',
        p_resale_batch_id   => ln_resale_batch_id,
        x_return_status     => lx_return_status,
        x_msg_data          => lx_msg_data,
        x_msg_count         => lx_msg_count);
        PRINT_LOG('x_return_status = ' || lx_return_status );
        PRINT_LOG('x_msg_data = ' || lx_msg_data );
        PRINT_LOG('x_msg_count = ' || lx_msg_count );
        FOR i IN 1 .. fnd_msg_pub.count_msg LOOP
        fnd_msg_pub.get(i, fnd_api.g_false, lx_msg_data, lc_msg_dummy);
        PRINT_LOG('I : ' || i || ' MSG : ' || lx_msg_data);
        END LOOP;
        */

        IF o_status ='S' THEN
          pos_link_proc (p_batch_id , lc_link_status);

          IF lc_link_status='S' THEN
            Print_log('Pos Link Process Complete successfully');

          ELSE
            Print_log('Pos Link Process error out....');

          END IF;

        ELSE
          Print_log('Interface Process is not complete successfully..!');

        END IF;

      END IF;

    END LOOP; -- Main loop

  EXCEPTION

  WHEN OTHERS THEN
    PRINT_LOG ('ERROR  while inserting into OZF_RESALE_LINES_INT_ALL'||SQLCODE
    ||' '||SQLERRM);

  END insert_interface_table;
--
-- +===========================================================================
-- =============================================+
-- | Name            : pos_link_proc
-- |
-- | Type            : PROCEDURE
-- |
-- | Description     : This Procedure used to linkinking interface
-- |
-- |                                                                     |
-- | Parameter    : errbuff         OUT VARCHAR2                 |
-- |                   retcode          OUT NUMBER                  |
-- |       p_filename            IN VARCHAR2                 |
-- |                   p_batch_id    IN NUMBER                  |
-- +===========================================================================
-- =============================================+

  PROCEDURE pos_link_proc(
      errbuff OUT VARCHAR2 ,
      retcode OUT NUMBER ,
      p_batch_id IN NUMBER )
  IS
    --Variable Declaration
    lc_serial_num            VARCHAR2(100);
    lc_message               VARCHAR2(3000);
    lc_valid                 VARCHAR2(1);
    lc_serailized_item       VARCHAR2(1);
    lc_valid_po_num          VARCHAR2(1);
    lc_order_number          VARCHAR2(50);
    ln_order_line_id         NUMBER;
    ln_order_line_batch_id   NUMBER ;
    lc_valid_contract        VARCHAR2(1);
    ln_cust_account_id       NUMBER;
    lc_valid_serial_number   VARCHAR2(100);
    lc_return_flag           VARCHAR2(1);
    lc_invoice_num           VARCHAR2(50);
    lc_sequence_num          VARCHAR2(50);
    lc_link_order_inv        VARCHAR2(50);
    lc_link_batch_line_seqno VARCHAR2(50);
    lc_link_status           VARCHAR2(1);
    --Cursor declaration

/*    CURSOR pos_link_cur
    IS

      SELECT ORL.rowid,
        ORL.*
      FROM OZF_RESALE_LINES_INT_ALL ORL
      WHERE resale_batch_id    =p_batch_id
      AND ORL.STATUS_CODE NOT IN ('PROCESSED','CLOSED') ; */
  BEGIN
    pos_link_proc (p_batch_id , lc_link_status);

  EXCEPTION

  WHEN OTHERS THEN
    Print_log('Error while linking interface lines for Batch id  ' ||P_batch_id
    ||' '||SQLCODE||' '||SQLERRM);

  END pos_link_proc;
-- +===========================================================================
-- =============================================+
-- | Name            : pos_link_proc
-- |
-- | Type            : PROCEDURE
-- |
-- | Description     : This Procedure used to linkinking interface
-- |
-- |                                                                     |
-- | Parameter    : errbuff         OUT VARCHAR2                 |
-- |                   retcode          OUT NUMBER                  |
-- |       p_filename            IN VARCHAR2                 |
-- |                   p_batch_id    IN NUMBER                  |
-- +===========================================================================
-- =============================================+

  PROCEDURE pos_link_proc(
      p_batch_id IN NUMBER,
      o_status OUT VARCHAR2 )
  IS
    --Variable Declaration
    lc_serial_num            VARCHAR2(100);
    lc_message               VARCHAR2(3000);
    lc_valid                 VARCHAR2(1);
    lc_serailized_item       VARCHAR2(1);
    lc_valid_po_num          VARCHAR2(1);
    lc_order_number          VARCHAR2(50);
    ln_order_line_id         NUMBER;
    ln_order_line_batch_id   NUMBER ;
    lc_valid_contract        VARCHAR2(1);
    ln_cust_account_id       NUMBER;
    lc_valid_serial_number   VARCHAR2(100);
    lc_return_flag           VARCHAR2(1);
    lc_invoice_num           VARCHAR2(50);
    lc_sequence_num          VARCHAR2(50);
    lc_link_order_inv        VARCHAR2(50);
    lc_link_batch_line_seqno VARCHAR2(50);
    ln_pos_line_cnt          NUMBER :=0;
    ln_pos_reject_cnt        NUMBER :=0;
    --Cursor declaration

    CURSOR pos_link_cur
    IS

      SELECT ORL.rowid,
        ORL.*
      FROM OZF_RESALE_LINES_INT_ALL ORL
      WHERE resale_batch_id =p_batch_id
      AND ORL.STATUS_CODE  IN ('PROCESSED')
      AND line_attribute15 IS NULL
      ORDER by ORL.UPC_CODE,ORL.PO_NUMBER;
    --AND ORL.STATUS_CODE NOT IN ('PROCESSED','CLOSED') ;
  BEGIN

    FOR lcu_link_line IN pos_link_cur
    LOOP
      lc_serial_num            :=NULL;
      lc_valid                 :=NULL;
      lc_message               :=NULL;
      lc_serailized_item       :=NULL;
      lc_valid_po_num          :=NULL;
      lc_order_number          :=NULL;
      ln_order_line_id         :=NULL;
      ln_order_line_batch_id   :=NULL;
      lc_valid_contract        :=NULL;
      ln_cust_account_id       :=NULL;
      lc_return_flag           :=NULL;
      lc_invoice_num           :=NULL;
      lc_sequence_num          :=NULL;
      lc_link_order_inv        :=NULL;
      lc_link_batch_line_seqno :=NULL;
      ln_cust_account_id       := lcu_link_line.ship_from_cust_account_id;
      lc_serailized_item       := check_serialized_item(
      lcu_link_line.inventory_item_id);
      --lc_valid_po_num    := check_valid_po_number(lcu_link_line.po_number,
      -- lcu_link_line.inventory_item_id,ln_cust_account_id);
      PRINT_LOG('Customer ' ||lcu_link_line.ship_from_cust_account_id);
      PRINT_LOG('Item Id ' ||lcu_link_line.item_number);
      PRINT_LOG('PO Number ' ||lcu_link_line.po_number);
      PRINT_LOG('serailized_item ' ||lc_serailized_item);
      --1. Validation for Serialized item and Serial Number
      PRINT_LOG('Resale Line Int Id ' ||lcu_link_line.resale_line_int_id);

      IF lcu_link_line.item_number = 'COTERM' THEN
        PRINT_LOG('Inside Coterm Linking ' ||lcu_link_line.po_number);
        PRINT_LOG('Customer ' ||lcu_link_line.ship_from_cust_account_id);
        BEGIN
          lc_return_flag:='Y';

          SELECT DISTINCT OZF.ATTRIBUTE5
          INTO lc_link_batch_line_seqno
          FROM ozf_sales_transactions_all OZF ,
            HZ_CUST_ACCOUNTS_ALL HZC,
            mtl_system_items_b mtl
          WHERE OZF.SOURCE_CODE     = 'MA'
          AND OZF.transfer_type     = 'IN'
          AND OZF.reason_code       = 'ENDINVCONV'
          AND mtl.INVENTORY_ITEM_ID = OZF.INVENTORY_ITEM_ID
          AND MTL.ORGANIZATION_ID         = FND_PROFILE.value('AMS_ITEM_ORGANIZATION_ID')
          AND OZF.SOLD_TO_PARTY_ID  = HZC.PARTY_ID
          AND HZC.CUST_ACCOUNT_ID   = ln_cust_account_id
          AND upper(OZF.ATTRIBUTE5) = upper(lcu_link_line.po_number)
          AND (mtl.segment1 LIKE 'FC-1%'
          OR mtl.segment1 LIKE 'FC%Z%')
          AND NOT EXISTS --ITS#543628
            (

                SELECT /*+ PARALLEL (ozfa, 500) */  NVL(REPLACE(LINE_ATTRIBUTE15,'-H',''),'X')
                FROM OZF_RESALE_LINES_INT_ALL ozfa
                WHERE status_code IN ('PROCESSED','CLOSED')
                AND LINE_ATTRIBUTE15 like '%H'
                AND NVL(REPLACE(LINE_ATTRIBUTE15,'-H',''),'X') = OZF.ATTRIBUTE15
                AND quantity > 0
              )
          AND NOT EXISTS --ITS#543628
            (
                  SELECT /*+ PARALLEL (ozfa, 500) */ NVL(LINE_ATTRIBUTE15,'X')
                  FROM OZF_RESALE_LINES_INT_ALL ozfa
                  WHERE status_code IN ('PROCESSED','CLOSED')
                  AND NVL(LINE_ATTRIBUTE15,'X') = OZF.ATTRIBUTE5
                  AND quantity > 0
                );
       /*       OZF.ATTRIBUTE15 NOT IN
              (

                SELECT NVL(REPLACE(LINE_ATTRIBUTE15,'-H',''),'X')
                FROM OZF_RESALE_LINES_INT_ALL
                WHERE status_code IN ('PROCESSED','CLOSED')
                AND LINE_ATTRIBUTE15 like '%H'
                AND quantity > 0
              )
            AND

                OZF.ATTRIBUTE5 NOT IN -- ITS# 534682
                (
                  SELECT NVL(LINE_ATTRIBUTE15,'X')
                  FROM OZF_RESALE_LINES_INT_ALL
                  WHERE status_code IN ('PROCESSED','CLOSED')
                  AND quantity > 0
                );
                
                */

        EXCEPTION

        WHEN NO_DATA_FOUND THEN
          PRINT_LOG(' NO Data Found');
          lc_return_flag:='E';

        WHEN TOO_MANY_ROWS THEN
          PRINT_LOG(' More Than One Record Found');
          lc_return_flag:='E';

        WHEN OTHERS THEN
          PRINT_LOG('Error while geting Coterm '||SQLCODE ||' '||SQLERRM);
          lc_link_order_inv        :=NULL;
          lc_link_batch_line_seqno :=NULL;
          lc_return_flag           :='E';

        END;

        IF lc_link_batch_line_seqno IS NULL THEN
          lc_return_flag            :='Y';
          BEGIN

            SELECT po_num
            INTO lc_link_batch_line_seqno
            FROM XXFT_RPRO_ORDER_DETAILS XPOD
            WHERE 1               =1
            AND order_line_status = 'CLOSED'
            AND PARENT_LINE       = 'Y'
            AND attribute37 = 'P' --ITS#575976 and 599807
            AND attribute28 = 'Y'
            AND upper(po_num)     = upper(lcu_link_line.po_number) --
              -- lcu_link_line.order_number --1100175
              --AND item_id     =lcu_link_line.inventory_item_id --57977
            AND customer_id                    =ln_cust_account_id
            AND NOT EXISTS --ITS#543628
             (
                SELECT /*+ PARALLEL (ozfa, 500) */ NVL(LINE_ATTRIBUTE15,'X')
                FROM OZF_RESALE_LINES_INT_ALL ozfa
                WHERE status_code IN ('PROCESSED','CLOSED')
                AND  NVL(LINE_ATTRIBUTE15,'X') = to_char(XPOD.SALES_ORDER_LINE_BATCH_ID)
                AND quantity > 0
              )
            
            /* to_char(SALES_ORDER_LINE_BATCH_ID) NOT IN
              (
                SELECT NVL(LINE_ATTRIBUTE15,'X')
                FROM OZF_RESALE_LINES_INT_ALL
                WHERE status_code IN ('PROCESSED','CLOSED')
                AND quantity > 0
              )
              
              */
            AND NOT EXISTS --ITS#543628
            (
                    SELECT /*+ PARALLEL (ozfa, 500) */ NVL(LINE_ATTRIBUTE15,'X')
                    FROM OZF_RESALE_LINES_INT_ALL ozfa
                    WHERE status_code IN ('PROCESSED','CLOSED')
                    AND NVL(LINE_ATTRIBUTE15,'X') = UPPER(XPOD.po_num) 
                    AND quantity > 0
                  )
            /*      po_num NOT IN    -- ITS# 534682
                  (
                    SELECT NVL(LINE_ATTRIBUTE15,'X')
                    FROM OZF_RESALE_LINES_INT_ALL
                    WHERE status_code IN ('PROCESSED','CLOSED')
                    AND quantity > 0
                  ) */
            AND rownum < 2;

          EXCEPTION

          WHEN NO_DATA_FOUND THEN
            PRINT_LOG(' NO Data Found');
            lc_return_flag:='E';

          WHEN TOO_MANY_ROWS THEN
            PRINT_LOG(' More Than One Record Found');
            lc_return_flag:='E';

          WHEN OTHERS THEN
            PRINT_LOG('Error while geting Coterm '||SQLCODE ||' '||SQLERRM);
            lc_link_order_inv        :=NULL;
            lc_link_batch_line_seqno :=NULL;
            lc_return_flag           :='E';

          END;

        END IF;

      ELSE

        IF lcu_link_line.quantity > 0 THEN

          IF lc_serailized_item ='Y' THEN
            --get the  valid serial number , sales order , sales order line
            -- batch id from rev pro table
            lc_valid_serial_number:= get_valid_serial_number(
            lcu_link_line.UPC_CODE, ln_cust_account_id,
            lcu_link_line.inventory_item_id);
            PRINT_LOG('serail number ' ||lc_valid_serial_number);
            --IF lc_valid_serial_number IS NULL THEN--IS NOT NULL THEN
            lc_return_flag:='Y';
            BEGIN

              SELECT OZF.ATTRIBUTE1 invoice_number ,
                OZF.ATTRIBUTE15
                ||'-H' seq_number
              INTO lc_link_order_inv ,
                lc_link_batch_line_seqno
              FROM OZF_SALES_TRANSACTIONS_ALL OZF,
                HZ_CUST_ACCOUNTS_ALL HZC
              WHERE OZF.SOURCE_CODE     = 'MA'
              AND OZF.transfer_type     = 'IN'
              AND OZF.reason_code       = 'ENDINVCONV'
              AND OZF.SOLD_TO_PARTY_ID  = HZC.PARTY_ID
              AND OZF.INVENTORY_ITEM_ID = lcu_link_line.inventory_item_id
              AND UPPER(OZF.ATTRIBUTE8) = UPPER(lcu_link_line.UPC_CODE)--
                -- lc_valid_serial_number--
              AND HZC.CUST_ACCOUNT_ID  = ln_cust_account_id
              AND NOT EXISTS --ITS#543628
               (
                  SELECT /*+ PARALLEL (ozfa, 500) */  NVL(REPLACE(LINE_ATTRIBUTE15,'-H',''),'X')
                  FROM OZF_RESALE_LINES_INT_ALL ozfa
                  WHERE status_code IN ('PROCESSED','CLOSED')
                  AND LINE_ATTRIBUTE15 like '%H'
                  AND NVL(REPLACE(LINE_ATTRIBUTE15,'-H',''),'X') = OZF.ATTRIBUTE15
                  AND quantity > 0
                );
              
             /* AND OZF.ATTRIBUTE15 NOT IN
                (
                  SELECT NVL(REPLACE(LINE_ATTRIBUTE15,'-H',''),'X')
                  FROM OZF_RESALE_LINES_INT_ALL
                  WHERE status_code IN ('PROCESSED','CLOSED')
                  AND LINE_ATTRIBUTE15 like '%H'
                  AND quantity > 0
                ); */

              PRINT_LOG('lc_link_order_inv ' ||lc_link_order_inv);
              PRINT_LOG('lc_link_batch_line_seqno ' ||lc_link_batch_line_seqno)
              ;

            EXCEPTION

            WHEN NO_DATA_FOUND THEN
              PRINT_LOG(' NO Data Found');

            WHEN TOO_MANY_ROWS THEN
              PRINT_LOG(' More Than One Record Found');

            WHEN OTHERS THEN
              PRINT_LOG('Error while geting order_line_batch_id '||SQLCODE ||
              ' '||SQLERRM);
              lc_link_order_inv        :=NULL;
              lc_link_batch_line_seqno :=NULL;

            END;
            --

            IF lc_link_batch_line_seqno IS NULL THEN

              IF lcu_link_line.item_number LIKE '%BDL%' THEN
                PRINT_LOG('Inside Bundle ' ||lcu_link_line.item_number);
                PRINT_LOG('Item Id ' ||lcu_link_line.inventory_item_id);
                PRINT_LOG('Cust Id ' ||ln_cust_account_id );
                PRINT_LOG('SN : ' ||lcu_link_line.UPC_CODE );
                BEGIN

                  SELECT XR.SALES_ORDER,
                    XR.SALES_ORDER_LINE_BATCH_ID
                  INTO lc_link_order_inv,
                    lc_link_batch_line_seqno
                  FROM XXFT_RPRO_ORDER_DETAILS XR
                  WHERE 1                  =1
                  AND XR.order_line_status = 'CLOSED'
                  AND PARENT_LINE          = 'Y'
                  AND attribute37 = 'P'--ITS#575976 and 599807
                  AND XR.item_id           =lcu_link_line.inventory_item_id --
                    -- 57977
                  AND XR.customer_id                    =ln_cust_account_id -- 6042;
                  AND NOT EXISTS --ITS#543628
                  (
                      SELECT /*+ PARALLEL (ozfa, 500) */ NVL(LINE_ATTRIBUTE15,'X')
                      FROM OZF_RESALE_LINES_INT_ALL ozfa
                      WHERE status_code IN ('PROCESSED','CLOSED')
                      AND  NVL(LINE_ATTRIBUTE15,'X') = to_char(XR.SALES_ORDER_LINE_BATCH_ID) 
                      AND quantity > 0
                    )
                  /* AND to_char(XR.SALES_ORDER_LINE_BATCH_ID) NOT IN
                    (
                      SELECT NVL(LINE_ATTRIBUTE15,'X')
                      FROM OZF_RESALE_LINES_INT_ALL
                      WHERE status_code IN ('PROCESSED','CLOSED')
                      AND quantity > 0
                    ) */
                  AND XR.SALES_ORDER_LINE_BATCH_ID IN
                    (
                      SELECT SALES_ORDER_LINE_BATCH_ID
                      FROM XXFT_RPRO_ORDER_DETAILS
                      WHERE UPPER(attribute33) = UPPER(lcu_link_line.UPC_CODE)
                      AND attribute37 = 'P'--ITS#575976 and 599807
                      AND order_line_status    = 'CLOSED'
                    ) 
                    AND NOT EXISTS (SELECT RPRO.SALES_ORDER_LINE_BATCH_ID -- Performance Fix
                      FROM XXFT.XXFT_RPRO_INVOICE_DETAILS RPRO,
                        ra_customer_trx_all RA ,
                        FND_LOOKUP_VALUES_VL VL,
                        RA_CUST_TRX_TYPES_ALL RT
                      WHERE RPRO.TRAN_TYPE               = 'CM'
                      AND RA.CUSTOMER_TRX_ID             = RPRO.INVOICE_ID
                      AND RT.CUST_TRX_TYPE_ID            = RA.CUST_TRX_TYPE_ID
                      AND RT.TYPE                        = 'CM'
                      AND RPRO.SALES_ORDER_LINE_BATCH_ID = XR.SALES_ORDER_LINE_BATCH_ID
                      AND VL.lookup_type                 = 'CREDIT_MEMO_REASON'
                      AND VL.ATTRIBUTE_CATEGORY          = 'CREDIT_MEMO_REASON'
                      AND VL.VIEW_APPLICATION_ID         =222
                      AND VL.ATTRIBUTE1                  = 'Y'
                      AND RA.REASON_CODE                 = VL.LOOKUP_CODE);
            /*      AND XR.SALES_ORDER_LINE_BATCH_ID NOT IN
                    (
                      SELECT RPRO.SALES_ORDER_LINE_BATCH_ID
                      FROM XXFT.XXFT_RPRO_INVOICE_DETAILS RPRO,
                        ra_customer_trx_all RA ,
                        RA_CUST_TRX_TYPES_ALL RT
                      WHERE RPRO.TRAN_TYPE                = 'CM'
                      AND RA.CUSTOMER_TRX_ID              = RPRO.INVOICE_ID
                      AND RT.CUST_TRX_TYPE_ID             = RA.CUST_TRX_TYPE_ID
                      AND RPRO.SALES_ORDER_LINE_BATCH_ID IS NOT NULL
                      AND RPRO.sales_order               IS NOT NULL
                      AND RT.NAME                        <> 'FTNT SPR Claim CM'
                    ); */

                EXCEPTION

                WHEN NO_DATA_FOUND THEN
                  Print_log('No Data Found for BDL');

                WHEN TOO_MANY_ROWS THEN
                  Print_log('Too Many Rows Found BDL');

                WHEN OTHERS THEN
                  lc_return_flag:='E';
                  lc_message    :=
                  'Error While geting batch id from RPRO for BDL'||SQLCODE ||
                  ' '||SQLERRM;
                  Print_log(lc_message);

                END;

              ELSE
                --Check
                BEGIN

                  SELECT SALES_ORDER,
                    SALES_ORDER_LINE_BATCH_ID
                  INTO lc_link_order_inv,
                    lc_link_batch_line_seqno
                  FROM XXFT_RPRO_ORDER_DETAILS xpod
                  WHERE 1               =1
                  AND order_line_status = 'CLOSED'
                  AND PARENT_LINE       = 'Y'
                  AND attribute37 = 'P'--ITS#575976 and 599807
                    --AND po_num      = lcu_link_line.po_number --
                    -- lcu_link_line.order_number --1100175
                  AND UPPER(attribute33) =UPPER(lcu_link_line.UPC_CODE)--
                    -- lc_valid_serial_number
                  AND item_id                        =lcu_link_line.inventory_item_id --57977
                  AND customer_id                    =ln_cust_account_id              -- 6042;
                  AND NOT EXISTS --ITS#543628
                   (
                      SELECT /*+ PARALLEL (ozfa, 500) */ NVL(LINE_ATTRIBUTE15,'X')
                      FROM OZF_RESALE_LINES_INT_ALL ozfa
                      WHERE status_code IN ('PROCESSED','CLOSED')
                      AND NVL(LINE_ATTRIBUTE15,'X') = to_char(XPOD.SALES_ORDER_LINE_BATCH_ID)
                      AND quantity > 0
                    ) 
                 /* AND to_char(SALES_ORDER_LINE_BATCH_ID) NOT IN
                    (
                      SELECT NVL(LINE_ATTRIBUTE15,'X')
                      FROM OZF_RESALE_LINES_INT_ALL
                      WHERE status_code IN ('PROCESSED','CLOSED')
                      AND quantity > 0
                    )   */
                    AND NOT EXISTS (SELECT RPRO.SALES_ORDER_LINE_BATCH_ID -- Performance Fix
                      FROM XXFT.XXFT_RPRO_INVOICE_DETAILS RPRO,
                        ra_customer_trx_all RA ,
                        FND_LOOKUP_VALUES_VL VL,
                        RA_CUST_TRX_TYPES_ALL RT
                      WHERE RPRO.TRAN_TYPE               = 'CM'
                      AND RA.CUSTOMER_TRX_ID             = RPRO.INVOICE_ID
                      AND RT.CUST_TRX_TYPE_ID            = RA.CUST_TRX_TYPE_ID
                      AND RT.TYPE                        = 'CM'
                      AND RPRO.SALES_ORDER_LINE_BATCH_ID = XPOD.SALES_ORDER_LINE_BATCH_ID
                      AND VL.lookup_type                 = 'CREDIT_MEMO_REASON'
                      AND VL.ATTRIBUTE_CATEGORY          = 'CREDIT_MEMO_REASON'
                      AND VL.VIEW_APPLICATION_ID         =222
                      AND VL.ATTRIBUTE1                  = 'Y'
                      AND RA.REASON_CODE                 = VL.LOOKUP_CODE);
                  --AND SALES_ORDER_LINE_BATCH_ID NOT IN
               /*   AND NOT EXISTS
                    (
                      SELECT RPRO.SALES_ORDER_LINE_BATCH_ID
                      FROM XXFT.XXFT_RPRO_INVOICE_DETAILS RPRO,
                        ra_customer_trx_all RA ,
                        RA_CUST_TRX_TYPES_ALL RT
                      WHERE RPRO.TRAN_TYPE                = 'CM'
                      AND RA.CUSTOMER_TRX_ID              = RPRO.INVOICE_ID
                      AND RT.CUST_TRX_TYPE_ID             = RA.CUST_TRX_TYPE_ID
                      AND RPRO.SALES_ORDER_LINE_BATCH_ID = xpod.SALES_ORDER_LINE_BATCH_ID
                      AND RPRO.sales_order               IS NOT NULL
                      AND RT.NAME                        <> 'FTNT SPR Claim CM'
                    );*/

                EXCEPTION

                WHEN NO_DATA_FOUND THEN
                  Print_log('No Data Found');

                WHEN TOO_MANY_ROWS THEN
                  Print_log('Too Many Rows Found');

                WHEN OTHERS THEN
                  lc_return_flag:='E';
                  lc_message    :=
                  'Error While geting invoice num from OZF_SALES_TRANSACTIONS_ALL'
                  ||SQLCODE ||' '||SQLERRM;
                  Print_log(lc_message);

                END;

              END IF;

            END IF;
            --

            IF lc_link_batch_line_seqno IS NULL THEN
              lc_return_flag            :='E';
              lc_message                :='Serialized Item '||
              lcu_link_line.item_number||' does not have valid Link';

            END IF;

          ELSE
            lc_return_flag:='Y';
            -- Get the valid sales order , sales order line batch id from rev
            -- pro table in the combination of PO , SKU and Qty
            BEGIN

              SELECT OZF.ATTRIBUTE1 invoice_number ,
                OZF.ATTRIBUTE15
                ||'-H' seq_number
              INTO lc_link_order_inv ,
                lc_link_batch_line_seqno
              FROM OZF_SALES_TRANSACTIONS_ALL OZF,
                HZ_CUST_ACCOUNTS_ALL HZC
              WHERE OZF.SOURCE_CODE     = 'MA'
              AND OZF.transfer_type     = 'IN'
              AND OZF.reason_code       = 'ENDINVCONV'
              AND OZF.SOLD_TO_PARTY_ID  = HZC.PARTY_ID
              AND OZF.INVENTORY_ITEM_ID = lcu_link_line.inventory_item_id
              AND upper(OZF.ATTRIBUTE5) = upper(lcu_link_line.po_number)
              AND HZC.CUST_ACCOUNT_ID   = ln_cust_account_id
              AND OZF.ATTRIBUTE15 NOT  IN --ITS#543628
                (
                  SELECT /*+ PARALLEL (ozfa, 500) */  NVL(REPLACE(LINE_ATTRIBUTE15,'-H',''),'X')
                  FROM OZF_RESALE_LINES_INT_ALL ozfa
                  WHERE status_code IN ('PROCESSED','CLOSED')
                  AND LINE_ATTRIBUTE15 like '%H'
                  AND quantity > 0
                )
              AND rownum < 2;
              --order by OZF.TRANSACTION_DATE;

            EXCEPTION

            WHEN NO_DATA_FOUND THEN
              lc_message:='No Data found for Non Serialized Item';

            WHEN TOO_MANY_ROWS THEN
              lc_message:='Too Many Rows Found for Non Serialized Item';

            WHEN OTHERS THEN
              lc_return_flag:='E';
              lc_message    :=
              'error while getting link  for for Non Serialized Item '||SQLCODE
              ||' '||SQLERRM;

            END ;

            IF lc_link_batch_line_seqno IS NULL THEN
              Print_log('Inside Non Ser Item RPRO Table');
              Print_log('lcu_link_line.po_number '||lcu_link_line.po_number);
              Print_log('lcu_link_line.inventory_item_id '||
              lcu_link_line.inventory_item_id);
              BEGIN

                SELECT SALES_ORDER,
                  SALES_ORDER_LINE_BATCH_ID
                INTO lc_link_order_inv,
                  lc_link_batch_line_seqno
                FROM XXFT_RPRO_ORDER_DETAILS xpod
                WHERE 1               =1
                AND order_line_status = 'CLOSED'
                AND PARENT_LINE       = 'Y'
                AND attribute37 = 'P'--ITS#575976 and 599807
                AND upper(po_num)     = upper(lcu_link_line.po_number) --
                  -- lcu_link_line.order_number --1100175
                AND item_id                        =lcu_link_line.inventory_item_id --57977
                AND customer_id                    =ln_cust_account_id
                AND NOT EXISTS --ITS#543628
                 ( 
                    SELECT /*+ PARALLEL (ozfa, 500) */  NVL(LINE_ATTRIBUTE15,'X')
                    FROM OZF_RESALE_LINES_INT_ALL ozfa
                    WHERE status_code IN ('PROCESSED','CLOSED')
                    AND NVL(LINE_ATTRIBUTE15,'X') = to_char(XPOD.SALES_ORDER_LINE_BATCH_ID)
                    AND quantity > 0
                  )
                /* AND to_char(SALES_ORDER_LINE_BATCH_ID) NOT IN
                  (
                    SELECT NVL(LINE_ATTRIBUTE15,'X')
                    FROM OZF_RESALE_LINES_INT_ALL
                    WHERE status_code IN ('PROCESSED','CLOSED')
                    AND quantity > 0
                  ) */
                      AND NOT EXISTS (SELECT RPRO.SALES_ORDER_LINE_BATCH_ID -- Performance Fix
                      FROM XXFT.XXFT_RPRO_INVOICE_DETAILS RPRO,
                        ra_customer_trx_all RA ,
                        FND_LOOKUP_VALUES_VL VL,
                        RA_CUST_TRX_TYPES_ALL RT
                      WHERE RPRO.TRAN_TYPE               = 'CM'
                      AND RA.CUSTOMER_TRX_ID             = RPRO.INVOICE_ID
                      AND RT.CUST_TRX_TYPE_ID            = RA.CUST_TRX_TYPE_ID
                      AND RT.TYPE                        = 'CM'
                      AND RPRO.SALES_ORDER_LINE_BATCH_ID = XPOD.SALES_ORDER_LINE_BATCH_ID
                      AND VL.lookup_type                 = 'CREDIT_MEMO_REASON'
                      AND VL.ATTRIBUTE_CATEGORY          = 'CREDIT_MEMO_REASON'
                      AND VL.VIEW_APPLICATION_ID         =222
                      AND VL.ATTRIBUTE1                  = 'Y'
                      AND RA.REASON_CODE                 = VL.LOOKUP_CODE)
               -- AND SALES_ORDER_LINE_BATCH_ID NOT IN
              /*   AND NOT EXISTS
                  (
                    SELECT RPRO.SALES_ORDER_LINE_BATCH_ID
                    FROM XXFT.XXFT_RPRO_INVOICE_DETAILS RPRO,
                      ra_customer_trx_all RA ,
                      RA_CUST_TRX_TYPES_ALL RT
                    WHERE RPRO.TRAN_TYPE                = 'CM'
                    AND RA.CUSTOMER_TRX_ID              = RPRO.INVOICE_ID
                    AND RT.CUST_TRX_TYPE_ID             = RA.CUST_TRX_TYPE_ID
                    AND RPRO.SALES_ORDER_LINE_BATCH_ID = xpod.SALES_ORDER_LINE_BATCH_ID
                    AND RPRO.sales_order               IS NOT NULL
                    AND RT.NAME                        <> 'FTNT SPR Claim CM'
                  ) */
                AND rownum < 2;

                Print_log('lc_link_batch_line_seqno '||lc_link_batch_line_seqno
                );
                --order by so_book_date;

              EXCEPTION

              WHEN NO_DATA_FOUND THEN
                lc_message:='No Data found for Non Serialized Item';

              WHEN TOO_MANY_ROWS THEN
                lc_message:='Too Many Rows Found for Non Serialized Item';

              WHEN OTHERS THEN
                lc_return_flag:='E';
                lc_message    :=
                'Error while getting link for Non Serialized Item '||SQLCODE ||
                ' '||SQLERRM;

              END ;

            END IF;


                        IF lc_link_batch_line_seqno IS NULL THEN
              BEGIN

                SELECT invoice_number,
                  seq_number
                INTO lc_link_order_inv ,
                  lc_link_batch_line_seqno
                FROM
                  (
                    SELECT OZF.ATTRIBUTE1 invoice_number ,
                      OZF.ATTRIBUTE15
                      ||'-H' seq_number
                      --   INTO lc_link_order_inv ,
                      --     lc_link_batch_line_seqno
                    FROM OZF_SALES_TRANSACTIONS_ALL OZF,
                      HZ_CUST_ACCOUNTS_ALL HZC
                    WHERE OZF.SOURCE_CODE     = 'MA'
                    AND OZF.transfer_type     = 'IN'
                    AND OZF.reason_code       = 'ENDINVCONV'
                    AND OZF.SOLD_TO_PARTY_ID  = HZC.PARTY_ID
                    AND OZF.INVENTORY_ITEM_ID = lcu_link_line.inventory_item_id
                   -- and upper(OZF.ATTRIBUTE5) = upper(lcu_link_line.po_number) -- Like
                    AND  upper(OZF.ATTRIBUTE5) like upper(lcu_link_line.po_number)||'%'
                    AND HZC.CUST_ACCOUNT_ID  = ln_cust_account_id
                    AND NOT EXISTS --ITS#543628
                    
                     (
                        SELECT /*+ PARALLEL (ozfa, 500) */  NVL(REPLACE(LINE_ATTRIBUTE15,'-H',''),'X')
                        FROM OZF_RESALE_LINES_INT_ALL ozfa
                        WHERE status_code IN ('PROCESSED','CLOSED')
                        AND LINE_ATTRIBUTE15 like '%H'
                        AND  NVL(REPLACE(LINE_ATTRIBUTE15,'-H',''),'X') = OZF.ATTRIBUTE15
                        AND quantity > 0
                      )
                  /*  AND OZF.ATTRIBUTE15 NOT IN
                      (
                        SELECT NVL(REPLACE(LINE_ATTRIBUTE15,'-H',''),'X')
                        FROM OZF_RESALE_LINES_INT_ALL
                        WHERE status_code IN ('PROCESSED','CLOSED')
                        AND LINE_ATTRIBUTE15 like '%H'
                        AND quantity > 0
                      ) */
                    ORDER BY OZF.TRANSACTION_DATE
                  )
                WHERE rownum <2;

              EXCEPTION

              WHEN NO_DATA_FOUND THEN
                lc_message:='No Data found for Non Serialized Item';

              WHEN TOO_MANY_ROWS THEN
                lc_message:='Too Many Rows Found for Non Serialized Item';

              WHEN OTHERS THEN
                lc_return_flag:='E';
                lc_message    :=
                'error while getting link  for for Non Serialized Item '||
                SQLCODE ||' '||SQLERRM;

              END ;

            END IF;

            IF lc_link_batch_line_seqno IS NULL THEN
              BEGIN

                SELECT SALES_ORDER,
                  SALES_ORDER_LINE_BATCH_ID
                INTO lc_link_order_inv ,
                  lc_link_batch_line_seqno
                FROM
                  (
                    SELECT SALES_ORDER,
                      SALES_ORDER_LINE_BATCH_ID
                      --            INTO lc_link_order_inv,
                      --              lc_link_batch_line_seqno
                    FROM XXFT_RPRO_ORDER_DETAILS xpod
                    WHERE 1               =1
                    AND order_line_status = 'CLOSED'
                    AND PARENT_LINE       = 'Y'
                    AND attribute37 = 'P'--ITS#575976 and 599807
                  --  AND upper(po_num)      = upper(lcu_link_line.po_number)-- Like
                  AND upper(po_num) like upper(lcu_link_line.po_number)||'%'
                    --lcu_link_line.order_number --1100175
                    AND item_id                        =lcu_link_line.inventory_item_id --57977
                    AND customer_id                    =ln_cust_account_id
                    AND NOT EXISTS --ITS#543628
                     (
                        SELECT /*+ PARALLEL (ozfa, 500) */  NVL(LINE_ATTRIBUTE15,'X')
                        FROM OZF_RESALE_LINES_INT_ALL ozfa
                        WHERE status_code IN ('PROCESSED','CLOSED')
                        AND NVL(LINE_ATTRIBUTE15,'X') = to_char(XPOD.SALES_ORDER_LINE_BATCH_ID)
                        AND quantity > 0
                      )
                 /*   AND to_char(SALES_ORDER_LINE_BATCH_ID) NOT IN
                      (
                        SELECT NVL(LINE_ATTRIBUTE15,'X')
                        FROM OZF_RESALE_LINES_INT_ALL
                        WHERE status_code IN ('PROCESSED','CLOSED')
                        AND quantity > 0
                      )
                      */
                      AND NOT EXISTS (SELECT RPRO.SALES_ORDER_LINE_BATCH_ID -- Performance Fix
                      FROM XXFT.XXFT_RPRO_INVOICE_DETAILS RPRO,
                        ra_customer_trx_all RA ,
                        FND_LOOKUP_VALUES_VL VL,
                        RA_CUST_TRX_TYPES_ALL RT
                      WHERE RPRO.TRAN_TYPE               = 'CM'
                      AND RA.CUSTOMER_TRX_ID             = RPRO.INVOICE_ID
                      AND RT.CUST_TRX_TYPE_ID            = RA.CUST_TRX_TYPE_ID
                      AND RT.TYPE                        = 'CM'
                      AND RPRO.SALES_ORDER_LINE_BATCH_ID = XPOD.SALES_ORDER_LINE_BATCH_ID
                      AND VL.lookup_type                 = 'CREDIT_MEMO_REASON'
                      AND VL.ATTRIBUTE_CATEGORY          = 'CREDIT_MEMO_REASON'
                      AND VL.VIEW_APPLICATION_ID         =222
                      AND VL.ATTRIBUTE1                  = 'Y'
                      AND RA.REASON_CODE                 = VL.LOOKUP_CODE)
                    --AND SALES_ORDER_LINE_BATCH_ID NOT IN
                     /* AND NOT EXISTS
                      (
                        SELECT RPRO.SALES_ORDER_LINE_BATCH_ID
                        FROM XXFT.XXFT_RPRO_INVOICE_DETAILS RPRO,
                          ra_customer_trx_all RA ,
                          RA_CUST_TRX_TYPES_ALL RT
                        WHERE RPRO.TRAN_TYPE                = 'CM'
                        AND RA.CUSTOMER_TRX_ID              = RPRO.INVOICE_ID
                        AND RT.CUST_TRX_TYPE_ID             = RA.CUST_TRX_TYPE_ID
                        AND RPRO.SALES_ORDER_LINE_BATCH_ID = xpod.SALES_ORDER_LINE_BATCH_ID
                        AND RPRO.sales_order               IS NOT NULL
                        AND RT.NAME                        <>
                          'FTNT SPR Claim CM'
                      )*/
                    ORDER BY so_book_date
                  )
                WHERE rownum <2;
                --order by so_book_date;

              EXCEPTION

              WHEN NO_DATA_FOUND THEN
                lc_return_flag:='E';
                lc_message    :='No Data found for Non Serialized Item';

              WHEN TOO_MANY_ROWS THEN
                lc_return_flag:='E';
                lc_message    :='Too Many Rows Found for Non Serialized Item';

              WHEN OTHERS THEN
                lc_return_flag:='E';
                lc_message    :=
                'Error while getting link for Non Serialized Item '||SQLCODE ||
                ' '||SQLERRM;

              END ;

            END IF;

            IF lc_link_batch_line_seqno IS NULL THEN
              BEGIN

                SELECT invoice_number,
                  seq_number
                INTO lc_link_order_inv ,
                  lc_link_batch_line_seqno
                FROM
                  (
                    SELECT OZF.ATTRIBUTE1 invoice_number ,
                      OZF.ATTRIBUTE15
                      ||'-H' seq_number
                      --   INTO lc_link_order_inv ,
                      --     lc_link_batch_line_seqno
                    FROM OZF_SALES_TRANSACTIONS_ALL OZF,
                      HZ_CUST_ACCOUNTS_ALL HZC
                    WHERE OZF.SOURCE_CODE     = 'MA'
                    AND OZF.transfer_type     = 'IN'
                    AND OZF.reason_code       = 'ENDINVCONV'
                    AND OZF.SOLD_TO_PARTY_ID  = HZC.PARTY_ID
                    AND OZF.INVENTORY_ITEM_ID = lcu_link_line.inventory_item_id
                      --and upper(OZF.ATTRIBUTE5) = upper(
                      -- lcu_link_line.po_number)
                    AND HZC.CUST_ACCOUNT_ID  = ln_cust_account_id
                    AND  NOT EXISTS(  --ITS#543628
                        SELECT /*+ PARALLEL (ozfa, 500) */  NVL(REPLACE(LINE_ATTRIBUTE15,'-H',''),'X')
                        FROM OZF_RESALE_LINES_INT_ALL ozfa
                        WHERE status_code IN ('PROCESSED','CLOSED')
                        AND LINE_ATTRIBUTE15 like '%H'
                        AND NVL(REPLACE(LINE_ATTRIBUTE15,'-H',''),'X') = OZF.ATTRIBUTE15
                        AND quantity > 0
                      )
                   /* AND OZF.ATTRIBUTE15 NOT IN
                      (
                        SELECT NVL(REPLACE(LINE_ATTRIBUTE15,'-H',''),'X')
                        FROM OZF_RESALE_LINES_INT_ALL
                        WHERE status_code IN ('PROCESSED','CLOSED')
                        AND LINE_ATTRIBUTE15 like '%H'
                        AND quantity > 0
                      )*/
                    ORDER BY OZF.TRANSACTION_DATE
                  )
                WHERE rownum <2;

              EXCEPTION

              WHEN NO_DATA_FOUND THEN
                lc_message:='No Data found for Non Serialized Item';

              WHEN TOO_MANY_ROWS THEN
                lc_message:='Too Many Rows Found for Non Serialized Item';

              WHEN OTHERS THEN
                lc_return_flag:='E';
                lc_message    :=
                'error while getting link  for for Non Serialized Item '||
                SQLCODE ||' '||SQLERRM;

              END ;

            END IF;

            IF lc_link_batch_line_seqno IS NULL THEN
              BEGIN

                SELECT SALES_ORDER,
                  SALES_ORDER_LINE_BATCH_ID
                INTO lc_link_order_inv ,
                  lc_link_batch_line_seqno
                FROM
                  (
                    SELECT SALES_ORDER,
                      SALES_ORDER_LINE_BATCH_ID
                      --            INTO lc_link_order_inv,
                      --              lc_link_batch_line_seqno
                    FROM XXFT_RPRO_ORDER_DETAILS xpod
                    WHERE 1               =1
                    AND order_line_status = 'CLOSED'
                    AND PARENT_LINE       = 'Y'
                    AND attribute37 = 'P'--ITS#575976 and 599807
                      --AND upper(po_num)      = upper(lcu_link_line.po_number)
                      --lcu_link_line.order_number --1100175
                    AND item_id                        =lcu_link_line.inventory_item_id --57977
                    AND customer_id                    =ln_cust_account_id
                    AND NOT EXISTS
                     (
                        SELECT /*+ PARALLEL (ozfa, 500) */ NVL(LINE_ATTRIBUTE15,'X')
                        FROM OZF_RESALE_LINES_INT_ALL ozfa
                        WHERE status_code IN ('PROCESSED','CLOSED')
                        AND NVL(LINE_ATTRIBUTE15,'X') = to_char(XPOD.SALES_ORDER_LINE_BATCH_ID)
                        AND quantity > 0
                      )
                  /*  AND to_char(SALES_ORDER_LINE_BATCH_ID) NOT IN
                      (
                        SELECT NVL(LINE_ATTRIBUTE15,'X')
                        FROM OZF_RESALE_LINES_INT_ALL
                        WHERE status_code IN ('PROCESSED','CLOSED')
                        AND quantity > 0
                      ) */
                      AND NOT EXISTS (SELECT RPRO.SALES_ORDER_LINE_BATCH_ID -- Performance Fix
                      FROM XXFT.XXFT_RPRO_INVOICE_DETAILS RPRO,
                        ra_customer_trx_all RA ,
                        FND_LOOKUP_VALUES_VL VL,
                        RA_CUST_TRX_TYPES_ALL RT
                      WHERE RPRO.TRAN_TYPE               = 'CM'
                      AND RA.CUSTOMER_TRX_ID             = RPRO.INVOICE_ID
                      AND RT.CUST_TRX_TYPE_ID            = RA.CUST_TRX_TYPE_ID
                      AND RT.TYPE                        = 'CM'
                      AND RPRO.SALES_ORDER_LINE_BATCH_ID = XPOD.SALES_ORDER_LINE_BATCH_ID
                      AND VL.lookup_type                 = 'CREDIT_MEMO_REASON'
                      AND VL.ATTRIBUTE_CATEGORY          = 'CREDIT_MEMO_REASON'
                      AND VL.VIEW_APPLICATION_ID         =222
                      AND VL.ATTRIBUTE1                  = 'Y'
                      AND RA.REASON_CODE                 = VL.LOOKUP_CODE)
                   -- AND SALES_ORDER_LINE_BATCH_ID NOT IN
                  /* AND NOT EXISTS
                      (
                        SELECT RPRO.SALES_ORDER_LINE_BATCH_ID
                        FROM XXFT.XXFT_RPRO_INVOICE_DETAILS RPRO,
                          ra_customer_trx_all RA ,
                          RA_CUST_TRX_TYPES_ALL RT
                        WHERE RPRO.TRAN_TYPE                = 'CM'
                        AND RA.CUSTOMER_TRX_ID              = RPRO.INVOICE_ID
                        AND RT.CUST_TRX_TYPE_ID             = RA.CUST_TRX_TYPE_ID
                        AND RPRO.SALES_ORDER_LINE_BATCH_ID = xpod.SALES_ORDER_LINE_BATCH_ID
                        AND RPRO.sales_order               IS NOT NULL
                        AND RT.NAME                        <>
                          'FTNT SPR Claim CM'
                      )*/
                    ORDER BY so_book_date
                  )
                WHERE rownum <2;
                --order by so_book_date;

              EXCEPTION

              WHEN NO_DATA_FOUND THEN
                lc_return_flag:='E';
                lc_message    :='No Data found for Non Serialized Item';

              WHEN TOO_MANY_ROWS THEN
                lc_return_flag:='E';
                lc_message    :='Too Many Rows Found for Non Serialized Item';

              WHEN OTHERS THEN
                lc_return_flag:='E';
                lc_message    :=
                'Error while getting link for Non Serialized Item '||SQLCODE ||
                ' '||SQLERRM;

              END ;

            END IF;

          END IF;

        ELSE -- Negative

          IF lc_serailized_item ='Y' THEN
            --get the  valid serial number , sales order , sales order line
            -- batch id from rev pro table
            lc_valid_serial_number:= get_valid_serial_number(
            lcu_link_line.UPC_CODE, ln_cust_account_id,
            lcu_link_line.inventory_item_id);
            PRINT_LOG('serail number ' ||lc_valid_serial_number);
            --IF lc_valid_serial_number IS NULL THEN--IS NOT NULL THEN
            lc_return_flag:='Y';
            BEGIN

              SELECT OZF.ATTRIBUTE1 invoice_number ,
                OZF.ATTRIBUTE15
                ||'-H' seq_number
              INTO lc_link_order_inv ,
                lc_link_batch_line_seqno
              FROM OZF_SALES_TRANSACTIONS_ALL OZF,
                HZ_CUST_ACCOUNTS_ALL HZC
              WHERE OZF.SOURCE_CODE     = 'MA'
              AND OZF.transfer_type     = 'IN'
              AND OZF.reason_code       = 'ENDINVCONV'
              AND OZF.SOLD_TO_PARTY_ID  = HZC.PARTY_ID
              AND OZF.INVENTORY_ITEM_ID = lcu_link_line.inventory_item_id
              AND UPPER(OZF.ATTRIBUTE8) = UPPER(lcu_link_line.UPC_CODE)--
                -- lc_valid_serial_number--
              AND HZC.CUST_ACCOUNT_ID  = ln_cust_account_id
              AND NOT EXISTS
               (
                  SELECT /*+ PARALLEL (ozfa, 500) */  NVL(REPLACE(LINE_ATTRIBUTE15,'-H',''),'X')
                  FROM OZF_RESALE_LINES_INT_ALL ozfa
                  WHERE status_code IN ('PROCESSED','CLOSED')
                  AND LINE_ATTRIBUTE15 like '%H'
                  AND NVL(REPLACE(LINE_ATTRIBUTE15,'-H',''),'X') = OZF.ATTRIBUTE15
                  AND quantity       < 0
                )
             /* AND OZF.ATTRIBUTE15 NOT IN
                (
                  SELECT NVL(REPLACE(LINE_ATTRIBUTE15,'-H',''),'X')
                  FROM OZF_RESALE_LINES_INT_ALL
                  WHERE status_code IN ('PROCESSED','CLOSED')
                  AND LINE_ATTRIBUTE15 like '%H'
                  AND quantity       < 0
                )*/
                ;

            EXCEPTION

            WHEN NO_DATA_FOUND THEN
              PRINT_LOG(' NO Data Found');

            WHEN TOO_MANY_ROWS THEN
              PRINT_LOG(' More Than One Record Found');

            WHEN OTHERS THEN
              PRINT_LOG('Error while geting order_line_batch_id '||SQLCODE ||
              ' '||SQLERRM);
              lc_link_order_inv        :=NULL;
              lc_link_batch_line_seqno :=NULL;

            END;
            --

            IF lc_link_batch_line_seqno IS NULL THEN
              --Check

              IF lcu_link_line.item_number LIKE '%BDL%' THEN
                PRINT_LOG('Inside Bundle ' ||lcu_link_line.item_number);
                PRINT_LOG('Item Id ' ||lcu_link_line.inventory_item_id);
                PRINT_LOG('Cust Id ' ||ln_cust_account_id );
                PRINT_LOG('SN : ' ||lcu_link_line.UPC_CODE );
                BEGIN

                  SELECT SALES_ORDER,
                    SALES_ORDER_LINE_BATCH_ID
                  INTO lc_link_order_inv,
                    lc_link_batch_line_seqno
                  FROM XXFT_RPRO_ORDER_DETAILS XPOD
                  WHERE 1               =1
                  AND order_line_status = 'CLOSED'
                  AND PARENT_LINE       = 'Y'
                  AND attribute37 = 'P'--ITS#575976 and 599807
                  AND item_id           =lcu_link_line.inventory_item_id --
                    -- 57977
                  AND customer_id                    =ln_cust_account_id -- 6042;
                  AND NOT EXISTS( --ITS#543628
                      SELECT /*+ PARALLEL (ozfa, 500) */ NVL(LINE_ATTRIBUTE15,'X')
                      FROM OZF_RESALE_LINES_INT_ALL ozfa
                      WHERE status_code IN ('PROCESSED','CLOSED')
                      AND NVL(LINE_ATTRIBUTE15,'X') = to_char(XPOD.SALES_ORDER_LINE_BATCH_ID)
                      AND quantity       < 0
                    )
                /*  AND to_char(SALES_ORDER_LINE_BATCH_ID) NOT IN
                    (
                      SELECT NVL(LINE_ATTRIBUTE15,'X')
                      FROM OZF_RESALE_LINES_INT_ALL
                      WHERE status_code IN ('PROCESSED','CLOSED')
                      AND quantity       < 0
                    )*/
                  AND SALES_ORDER_LINE_BATCH_ID IN
                    (
                      SELECT SALES_ORDER_LINE_BATCH_ID
                      FROM XXFT_RPRO_ORDER_DETAILS
                      WHERE UPPER(attribute33) = UPPER(lcu_link_line.UPC_CODE)
                      AND attribute37 = 'P'--ITS#575976 and 599807
                      AND order_line_status    = 'CLOSED'
                    );

                EXCEPTION

                WHEN NO_DATA_FOUND THEN
                  Print_log('No Data Found for BDL');

                WHEN TOO_MANY_ROWS THEN
                  Print_log('Too Many Rows Found BDL');

                WHEN OTHERS THEN
                  lc_return_flag:='E';
                  lc_message    :=
                  'Error While geting batch id from RPRO for BDL'||SQLCODE ||
                  ' '||SQLERRM;
                  Print_log(lc_message);

                END;

              ELSE
                --Check
                BEGIN

                  SELECT SALES_ORDER,
                    SALES_ORDER_LINE_BATCH_ID
                  INTO lc_link_order_inv,
                    lc_link_batch_line_seqno
                  FROM XXFT_RPRO_ORDER_DETAILS XPOD
                  WHERE 1               =1
                  AND order_line_status = 'CLOSED'
                  AND PARENT_LINE       = 'Y'
                  AND attribute37 = 'P'--ITS#575976 and 599807
                    --AND po_num      = lcu_link_line.po_number --
                    -- lcu_link_line.order_number --1100175
                  AND UPPER(attribute33) =UPPER(lcu_link_line.UPC_CODE)--
                    -- lc_valid_serial_number
                  AND item_id                        =lcu_link_line.inventory_item_id --57977
                  AND customer_id                    =ln_cust_account_id              -- 6042;
                  AND NOT EXISTS  --ITS#543628
                  (
                      SELECT /*+ PARALLEL (ozfa, 500) */  NVL(LINE_ATTRIBUTE15,'X')
                      FROM OZF_RESALE_LINES_INT_ALL ozfa
                      WHERE status_code IN ('PROCESSED','CLOSED')
                      AND NVL(LINE_ATTRIBUTE15,'X') = to_char(XPOD.SALES_ORDER_LINE_BATCH_ID)
                      AND quantity       < 0
                    );
                 /* AND to_char(SALES_ORDER_LINE_BATCH_ID) NOT IN
                    (
                      SELECT NVL(LINE_ATTRIBUTE15,'X')
                      FROM OZF_RESALE_LINES_INT_ALL
                      WHERE status_code IN ('PROCESSED','CLOSED')
                      AND quantity       < 0
                    )*/

                EXCEPTION

                WHEN NO_DATA_FOUND THEN
                  Print_log('No Data Found');

                WHEN TOO_MANY_ROWS THEN
                  Print_log('Too Many Rows Found');

                WHEN OTHERS THEN
                  lc_return_flag:='E';
                  lc_message    :=
                  'Error While geting invoice num from OZF_SALES_TRANSACTIONS_ALL'
                  ||SQLCODE ||' '||SQLERRM;
                  Print_log(lc_message);

                END;

              END IF;
              --

            END IF;
            --

            IF lc_link_batch_line_seqno IS NULL THEN
              lc_return_flag            :='E';
              lc_message                :='Serialized Item '||
              lcu_link_line.item_number||' does not have valid Link';

            END IF;

          ELSE
            lc_return_flag:='Y';
            -- Get the valid sales order , sales order line batch id from rev
            -- pro table in the combination of PO , SKU and Qty
            BEGIN

              SELECT OZF.ATTRIBUTE1 invoice_number ,
                OZF.ATTRIBUTE15
                ||'-H' seq_number
              INTO lc_link_order_inv ,
                lc_link_batch_line_seqno
              FROM OZF_SALES_TRANSACTIONS_ALL OZF,
                HZ_CUST_ACCOUNTS_ALL HZC
              WHERE OZF.SOURCE_CODE     = 'MA'
              AND OZF.transfer_type     = 'IN'
              AND OZF.reason_code       = 'ENDINVCONV'
              AND OZF.SOLD_TO_PARTY_ID  = HZC.PARTY_ID
              AND OZF.INVENTORY_ITEM_ID = lcu_link_line.inventory_item_id
              AND upper(OZF.ATTRIBUTE5) = upper(lcu_link_line.po_number)
              AND HZC.CUST_ACCOUNT_ID   = ln_cust_account_id
              AND OZF.ATTRIBUTE15 NOT  IN --ITS#543628
                (
                  SELECT /*+ PARALLEL (ozfa, 500) */  NVL(REPLACE(LINE_ATTRIBUTE15,'-H',''),'X')
                  FROM OZF_RESALE_LINES_INT_ALL ozfa
                  WHERE status_code IN ('PROCESSED','CLOSED')
                  AND LINE_ATTRIBUTE15 like '%H'
                  AND quantity       < 0
                )
              AND rownum < 2;
              --order by OZF.TRANSACTION_DATE;

            EXCEPTION

            WHEN NO_DATA_FOUND THEN
              lc_message:='No Data found for Non Serialized Item';

            WHEN TOO_MANY_ROWS THEN
              lc_message:='Too Many Rows Found for Non Serialized Item';

            WHEN OTHERS THEN
              lc_return_flag:='E';
              lc_message    :=
              'error while getting link  for for Non Serialized Item '||SQLCODE
              ||' '||SQLERRM;

            END ;

            IF lc_link_batch_line_seqno IS NULL THEN
              BEGIN

                SELECT SALES_ORDER,
                  SALES_ORDER_LINE_BATCH_ID
                INTO lc_link_order_inv,
                  lc_link_batch_line_seqno
                FROM XXFT_RPRO_ORDER_DETAILS XPOD
                WHERE 1               =1
                AND order_line_status = 'CLOSED'
                AND PARENT_LINE       = 'Y'
                AND attribute37 = 'P'--ITS#575976 and 599807
                AND upper(po_num)     = upper(lcu_link_line.po_number) --
                  -- lcu_link_line.order_number --1100175
                AND item_id                        =lcu_link_line.inventory_item_id --57977
                AND customer_id                    =ln_cust_account_id
                AND NOT EXISTS --ITS#543628
                (
                    SELECT /*+ PARALLEL (ozfa, 500) */  NVL(LINE_ATTRIBUTE15,'X')
                    FROM OZF_RESALE_LINES_INT_ALL ozfa
                    WHERE status_code IN ('PROCESSED','CLOSED')
                    AND  NVL(LINE_ATTRIBUTE15,'X') = to_char(XPOD.SALES_ORDER_LINE_BATCH_ID) 
                    AND quantity       < 0
                  )
               /* AND to_char(SALES_ORDER_LINE_BATCH_ID) NOT IN
                  (
                    SELECT NVL(LINE_ATTRIBUTE15,'X')
                    FROM OZF_RESALE_LINES_INT_ALL
                    WHERE status_code IN ('PROCESSED','CLOSED')
                    AND quantity       < 0
                  )*/
                AND rownum < 2;
                --order by so_book_date;

              EXCEPTION

              WHEN NO_DATA_FOUND THEN
                lc_message:='No Data found for Non Serialized Item';

              WHEN TOO_MANY_ROWS THEN
                lc_message:='Too Many Rows Found for Non Serialized Item';

              WHEN OTHERS THEN
                lc_return_flag:='E';
                lc_message    :=
                'Error while getting link for Non Serialized Item '||SQLCODE ||
                ' '||SQLERRM;

              END ;

            END IF;

            IF lc_link_batch_line_seqno IS NULL THEN
              BEGIN

                SELECT invoice_number,
                  seq_number
                INTO lc_link_order_inv ,
                  lc_link_batch_line_seqno
                FROM
                  (
                    SELECT OZF.ATTRIBUTE1 invoice_number ,
                      OZF.ATTRIBUTE15
                      ||'-H' seq_number
                      --   INTO lc_link_order_inv ,
                      --     lc_link_batch_line_seqno
                    FROM OZF_SALES_TRANSACTIONS_ALL OZF,
                      HZ_CUST_ACCOUNTS_ALL HZC
                    WHERE OZF.SOURCE_CODE     = 'MA'
                    AND OZF.transfer_type     = 'IN'
                    AND OZF.reason_code       = 'ENDINVCONV'
                    AND OZF.SOLD_TO_PARTY_ID  = HZC.PARTY_ID
                    AND OZF.INVENTORY_ITEM_ID = lcu_link_line.inventory_item_id
                      --and upper(OZF.ATTRIBUTE5) = upper(
                      -- lcu_link_line.po_number)
                    AND HZC.CUST_ACCOUNT_ID  = ln_cust_account_id
                    AND NOT EXISTS --ITS#543628
                     (
                        SELECT /*+ PARALLEL (ozfa, 500) */  NVL(REPLACE(LINE_ATTRIBUTE15,'-H',''),'X')
                        FROM OZF_RESALE_LINES_INT_ALL ozfa
                        WHERE status_code IN ('PROCESSED','CLOSED')
                        AND LINE_ATTRIBUTE15 like '%H'
                        AND NVL(REPLACE(LINE_ATTRIBUTE15,'-H',''),'X') = OZF.ATTRIBUTE15
                        AND quantity       < 0
                      )
                   /* AND OZF.ATTRIBUTE15 NOT IN
                      (
                        SELECT NVL(REPLACE(LINE_ATTRIBUTE15,'-H',''),'X')
                        FROM OZF_RESALE_LINES_INT_ALL
                        WHERE status_code IN ('PROCESSED','CLOSED')
                        AND LINE_ATTRIBUTE15 like '%H'
                        AND quantity       < 0
                      )*/
                    ORDER BY OZF.TRANSACTION_DATE
                  )
                WHERE rownum <2;

              EXCEPTION

              WHEN NO_DATA_FOUND THEN
                lc_message:='No Data found for Non Serialized Item';

              WHEN TOO_MANY_ROWS THEN
                lc_message:='Too Many Rows Found for Non Serialized Item';

              WHEN OTHERS THEN
                lc_return_flag:='E';
                lc_message    :=
                'error while getting link  for for Non Serialized Item '||
                SQLCODE ||' '||SQLERRM;

              END ;

            END IF;

            IF lc_link_batch_line_seqno IS NULL THEN
              BEGIN

                SELECT SALES_ORDER,
                  SALES_ORDER_LINE_BATCH_ID
                INTO lc_link_order_inv ,
                  lc_link_batch_line_seqno
                FROM
                  (
                    SELECT SALES_ORDER,
                      SALES_ORDER_LINE_BATCH_ID
                      --            INTO lc_link_order_inv,
                      --              lc_link_batch_line_seqno
                    FROM XXFT_RPRO_ORDER_DETAILS XPOD
                    WHERE 1               =1
                    AND order_line_status = 'CLOSED'
                    AND PARENT_LINE       = 'Y'
                    AND attribute37 = 'P'--ITS#575976 and 599807
                      --AND upper(po_num)      = upper(lcu_link_line.po_number)
                      --lcu_link_line.order_number --1100175
                    AND item_id                        =lcu_link_line.inventory_item_id --57977
                    AND customer_id                    =ln_cust_account_id
                    AND NOT EXISTS --ITS#543628
                     (
                        SELECT /*+ PARALLEL (ozfa, 500) */ NVL(LINE_ATTRIBUTE15,'X')
                        FROM OZF_RESALE_LINES_INT_ALL ozfa
                        WHERE status_code IN ('PROCESSED','CLOSED')
                        AND NVL(LINE_ATTRIBUTE15,'X') = to_char(XPOD.SALES_ORDER_LINE_BATCH_ID) 
                        AND quantity       < 0
                      ) 
                    
                      /*   AND to_char(SALES_ORDER_LINE_BATCH_ID) NOT IN
                      (
                        SELECT NVL(LINE_ATTRIBUTE15,'X')
                        FROM OZF_RESALE_LINES_INT_ALL
                        WHERE status_code IN ('PROCESSED','CLOSED')
                        AND quantity       < 0
                      ) 
                      */
                    ORDER BY so_book_date
                  )
                WHERE rownum <2;
                --order by so_book_date;

              EXCEPTION

              WHEN NO_DATA_FOUND THEN
                lc_return_flag:='E';
                lc_message    :='No Data found for Non Serialized Item';

              WHEN TOO_MANY_ROWS THEN
                lc_return_flag:='E';
                lc_message    :='Too Many Rows Found for Non Serialized Item';

              WHEN OTHERS THEN
                lc_return_flag:='E';
                lc_message    :=
                'Error while getting link for Non Serialized Item '||SQLCODE ||
                ' '||SQLERRM;

              END ;

            END IF;

          END IF;

        END IF;
        --
        --Updating POS Link line

      END IF;
      PRINT_LOG('Link Update Flag :' ||lc_return_flag);

      IF lc_return_flag ='Y' THEN

        UPDATE OZF_RESALE_LINES_INT_ALL ori
        SET ori.Line_Attribute14  = lc_link_order_inv ,
          ori.Line_Attribute15    = lc_link_batch_line_seqno
        WHERE ori.resale_batch_id =p_batch_id
        AND ori.rowid             = lcu_link_line.rowid
        AND ori.LINE_ATTRIBUTE15 IS NULL ;
        COMMIT;

        IF lcu_link_line.quantity < 0 THEN

          UPDATE OZF_RESALE_LINES_INT_ALL ori
          SET ori.Line_Attribute14   = NULL ,
            ori.Line_Attribute15     = NULL
          WHERE ori.Line_Attribute15 = lc_link_batch_line_seqno
          AND quantity               > 0;
          COMMIT;

        END IF;
        --and ori.Line_Attribute14 is null ;
        COMMIT;
        PRINT_LOG('Link Update Flag seq :' ||lc_link_batch_line_seqno);
        ln_pos_line_cnt := ln_pos_line_cnt+1;

      ELSE

        UPDATE OZF_RESALE_LINES_INT_ALL orl
        SET Line_Attribute13      =lc_message
        WHERE orl.resale_batch_id =p_batch_id
        AND orl.rowid             = lcu_link_line.rowid
        AND orl.Line_Attribute15 IS NULL;
        COMMIT;
        ln_pos_reject_cnt:=ln_pos_reject_cnt+1;

      END IF;

    END LOOP;

    IF ln_pos_line_cnt >0 THEN
      o_status        :='S';

    ELSE
      o_status :='E';

    END IF;
    PRINT_LOG(
    '--------------------POS Link Line Process Status-------------------------'
    );
    PRINT_LOG('--------------------Batch id :'||p_batch_id ||
    '--------------------------');
    PRINT_LOG('Total Number of Line  POS Link Successful ' ||ln_pos_line_cnt);
    PRINT_LOG('Total Number of Line  POS Link Rejected   ' ||ln_pos_reject_cnt)
    ;
    PRINT_LOG(
    '--------------------------------------------------------------------------'
    );

  EXCEPTION

  WHEN OTHERS THEN
    Print_log('Error while linking interface lines for Batch id  ' ||P_batch_id
    ||' '||SQLCODE||' '||SQLERRM);

  END pos_link_proc;
-- +===========================================================================
-- =============================================+
-- | Name            : pos_delink_proc
-- |
-- | Type            : PROCEDURE
-- |
-- | Description     : This Procedure used to de-link POS interface
-- |
-- |                                                                     |
-- | Parameter    : errbuff         OUT VARCHAR2                 |
-- |                      ,retcode          OUT NUMBER                 |
-- |      ,p_batch_id    IN NUMBER                 |
-- +===========================================================================
-- =============================================+

  PROCEDURE pos_delink_proc(
      errbuff OUT VARCHAR2 ,
      retcode OUT NUMBER ,
      p_batch_id IN NUMBER )
  IS
    --Cursor declaration

 /*   CURSOR pos_delink_cur
    IS

      SELECT ORL.rowid,
        ORL.*
      FROM OZF_RESALE_LINES_INT_ALL ORL
      WHERE resale_batch_id =p_batch_id
      AND ORL.STATUS_CODE   ='PROCESSED' ;*/
  BEGIN

    --  FOR lcu_delink_line IN pos_delink_cur
    --  LOOP

    UPDATE OZF_RESALE_LINES_INT_ALL orl
    SET Line_Attribute14      = NULL ,
      Line_Attribute15        =NULL ,
      Line_Attribute13        =NULL
    WHERE orl.resale_batch_id =p_batch_id
    AND ORL.STATUS_CODE       ='PROCESSED';
    COMMIT;
    -- END LOOP;
    Print_output('De Link Process complete for batch id '||p_batch_id);

  EXCEPTION

  WHEN OTHERS THEN
    Print_log('Error while De-link for for batch id '||p_batch_id ||SQLCODE||
    ' ' ||SQLERRM);

  END pos_delink_proc;

  PROCEDURE POS_LOAD_TBL(
      p_batch_number IN NUMBER,
      p_lookup_type  IN VARCHAR2 ,
      p_status OUT VARCHAR2)
  IS
    l_sql_stmt VARCHAR2(2000);
    l_sql_inst VARCHAR2(2000) :=
    'INSERT INTO XXFT.XXFT_POS_UPLOAD_TBL ( FILENAME ,BATCH_NUMBER ,RECORD_ID ,STATUS ,'
    ;
    l_sql_sel      VARCHAR2(2000) := 'Select ';
    l_sql_where    VARCHAR2(2000);
    l_batch_number NUMBER;

    CURSOR staging_cur
    IS

      SELECT DISTINCT tag,
        lookup_code
      FROM FND_LOOKUP_VALUES_VL
      WHERE LOOKUP_TYPE=p_lookup_type--'FTNT_CHRM_POS_UPD_ARROW_US'
      AND tag         IS NOT NULL
      ORDER BY to_number(tag);
  BEGIN
    p_status := 'S';
    --Select filename into l_filename from XXFT_PO_UPLOAD_LINKING_STG where
    -- rownum < 2;
    l_batch_number :=p_batch_number;
    --l_sql_sel  := l_sql_sel||''''||l_filename||''''||','||'RECORD_ID ,STATUS
    -- ,';
    l_sql_sel := l_sql_sel||'FILENAME,BATCH_NUMBER, RECORD_ID ,STATUS ,';

    FOR lcu_mapping IN staging_cur --2
    LOOP
      l_sql_inst := l_sql_inst||lcu_mapping.lookup_code||',';
      l_sql_sel  := l_sql_sel||'column'||lcu_mapping.tag||',';

    END LOOP;
    l_sql_inst := SUBSTR (TRIM (l_sql_inst), 1, LENGTH (TRIM (l_sql_inst)) - 1)
    ||')';
    l_sql_sel := SUBSTR (TRIM (l_sql_sel), 1, LENGTH (TRIM (l_sql_sel)) - 1)||
    ' from XXFT_PO_UPLOAD_LINKING_STG where batch_number = :batch_number';
    l_sql_stmt := l_sql_inst|| l_sql_sel;
    --l_sql_stmt := ''''||l_sql_stmt||'''';
    --dbms_output.put_line (l_sql_stmt );
    EXECUTE IMMEDIATE l_sql_stmt USING l_batch_number;
    COMMIT;

  EXCEPTION

  WHEN OTHERS THEN
    Print_log('Error While calling :POS_LOAD_TBL ');
    Print_log(l_sql_stmt);
    p_status := 'E';

  END POS_LOAD_TBL;
--

END XXFT_POS_UPLOADLINK_INT_PKG;

/
