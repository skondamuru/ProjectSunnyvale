create or replace PROCEDURE XXFT_REG_EVENT_REPORT_PRC
                                      (p_retcode       OUT VARCHAR2
                                      ,p_errbuf        OUT VARCHAR2
									  ,p_from_date     VARCHAR2
									  ,p_to_date       VARCHAR2
									  )
AS
/* +===================================================================+
  -- |                         Fortinet, Inc.
  -- |                         Sunnyvale, CA
  -- +===================================================================
  -- |
  -- |Program Name     : XXFT_REG_EVENT_REPORT_PRC
  -- |
  -- |Description      : Fortinet Registration Event Report Procedure
  -- |                   This procedure has been written to provide the
  -- |                   Registration Event Details
  -- |
  -- |Change Record:
  -- |===============
  -- |Version   Date        Author             Remarks
  -- |=======   =========== ==============     ============================
  -- |1.0       28-Aug-2016  ObiReddy K         Initial code version
  -- |1.1       30-Sep-2016  ObiReddy K         Included Warranties
  -- |1.2       28-OCT-2016  ObiReddy K         Modified for ITS#594633
  +========================================================================+*/

  --Declaration of Curosr
  CURSOR cur_regevrep
      IS
	    SELECT service_type
       ,product_category
	   ,customer
	   ,order_number
	   ,contract_number
	   ,start_date
	   ,end_date
	   ,registration_date
	   ,Auto_Start_Date
       ,SERV_START_DATE
       ,SERV_END_DATE
       ,serial_numnber 
	   ,count(*) total_count
FROM(
SELECT 	XXFT_RPRO_DATA_EXTRACT_PKG.get_embedded_service(
			REV.service_reference_type_code,REV.line_id,NVL(
			REV.service_reference_line_id,REV.attribute1), REV.inventory_item_id,
			REV.ship_from_org_id) service_type,
			XXFT_RPRO_DATA_EXTRACT_PKG.get_product_category(REV.inventory_item_id,
			REV.ship_from_org_id) product_category,
			CUSTOMER,
            Item_Number,
            order_number,
            contract_number,
            Start_Date,
            End_date,
            Registration_Date,
            Auto_Start_Date,
            SERV_START_DATE,
            SERV_END_DATE,
            serial_numnber
			FROM (SELECT (SELECT HP.PARTY_NAME
							FROM okc_k_party_roles_b okp,
								hz_parties hp
							WHERE okp.object1_id1    = hp.party_id
							AND okp.rle_code         ='CUSTOMER'
							AND okp.jtot_object1_code='OKX_PARTY'
							AND okp.chr_id = okh.id) CUSTOMER,
						OOL.ORDERED_ITEM Item_Number,
						OOH.ORDER_NUMBER order_number,
						OKH.CONTRACT_NUMBER contract_number,
						OKLB.ATTRIBUTE1 Start_Date,
						OKLB.ATTRIBUTE2 End_Date,
						OKLB.ATTRIBUTE3 Registration_Date,
						OKLB.ATTRIBUTE4 Auto_Start_Date,
						TO_CHAR(OKLB.START_DATE,'DD-MON-YY') SERV_START_DATE,
						TO_CHAR(OKLB.END_DATE,'DD-MON-YY')   SERV_END_DATE,
						CII.SERIAL_NUMBER serial_numnber,
						ool.inventory_item_id,
						ool.ship_from_org_id,
						ool.line_id		,
						OOL.service_reference_type_code,
						OOL.ATTRIBUTE1,
						OOL.service_reference_line_id
					FROM APPS.OE_ORDER_LINES_ALL OOL,
						APPS.OE_ORDER_HEADERS_ALL OOH,
						OKC_K_REL_OBJS REL,
						APPS.OKC_K_HEADERS_ALL_B OKH,
						APPS.OKC_K_LINES_B OKL,
						APPS.OKC_K_ITEMS OKI,
						APPS.OKC_K_LINES_B OKLB,
						APPS.OKC_K_ITEMS OKIB,
						APPS.CSI_ITEM_INSTANCES CII,
						--XXFT_FO_CONTRACT XFC
						(SELECT /*+ PARALLEL(XFC1,1000) */ XFC1.CONTRACT_NUMBER,XFC1.REGISTRATION_DATE 
                             FROM
                            (SELECT CONTRACT_NUMBER,REGISTRATION_DATE FROM XXFT_FO_CONTRACT
							UNION
							SELECT TO_CHAR(WARRANTY_ID) CONTRACT_NUMBER,REGISTRATION_DATE FROM XXFT_FO_WARRANTY
							)XFC1)XFC
					WHERE 1                   =1
					AND OOH.HEADER_ID         = OOL.HEADER_ID
					AND OOH.ORG_ID            = OOL.ORG_ID
					AND REL.jtot_object1_code = 'OKX_ORDERLINE'
					AND REL.OBJECT1_ID1       = TO_CHAR(OOL.LINE_ID)
					AND REL.CHR_ID            = OKH.ID
					AND XFC.CONTRACT_NUMBER   = OKH.CONTRACT_NUMBER
					AND OKH.ID                = OKL.CHR_ID
					AND OKL.ID                = OKI.CLE_ID
					AND OKI.OBJECT1_ID1       = OOL.INVENTORY_ITEM_ID
					AND OKL.ID                = OKLB.CLE_ID
					AND OKLB.ID               = OKIB.CLE_ID
					AND OKIB.OBJECT1_ID1      = CII.INSTANCE_ID
					AND OKLB.ATTRIBUTE3      IS NOT NULL
          AND NOT EXISTS ( SELECT 1 FROM oe_order_sources where order_source_id = ool.order_source_id and name = 'Co-Term with Quote')
					AND FND_CONC_DATE.string_to_date(OKLB.ATTRIBUTE3) = TRUNC(XFC.REGISTRATION_DATE)
					AND TRUNC(XFC.REGISTRATION_DATE) BETWEEN NVL(to_date(P_FROM_DATE, 'DD-MON-YY'),TRUNC(XFC.REGISTRATION_DATE))
												  AND NVL(to_date(P_TO_DATE, 'DD-MON-YY'),TRUNC(XFC.REGISTRATION_DATE))
          AND ool.ordered_item not like 'FCX%'
				)REV
      UNION ALL
	   SELECT 	NULL service_type,
				XXFT_RPRO_DATA_EXTRACT_PKG.get_product_category(REV.inventory_item_id,
				REV.organization_id) product_category,
				CUSTOMER,
				Item_Number,
				order_number,
				contract_number,
				Start_Date,
				End_date,
				Registration_Date,
				Auto_Start_Date,
				SERV_START_DATE,
				SERV_END_DATE,
				serial_numnber
				FROM (SELECT (SELECT HP.PARTY_NAME
								FROM okc_k_party_roles_b okp,
									hz_parties hp
								WHERE okp.object1_id1    = hp.party_id
								AND okp.rle_code         ='CUSTOMER'
								AND okp.jtot_object1_code='OKX_PARTY'
								AND okp.chr_id = okh.id) CUSTOMER,
							MSI.SEGMENT1 Item_Number,
							NULL order_number,
							OKH.CONTRACT_NUMBER contract_number,
							OKLB.ATTRIBUTE1 Start_Date,
							OKLB.ATTRIBUTE2 End_Date,
							OKLB.ATTRIBUTE3 Registration_Date,
							OKLB.ATTRIBUTE4 Auto_Start_Date,
							TO_CHAR(OKLB.START_DATE,'DD-MON-YY') SERV_START_DATE,
							TO_CHAR(OKLB.END_DATE,'DD-MON-YY')   SERV_END_DATE,
							CII.SERIAL_NUMBER serial_numnber,
							msi.inventory_item_id,
							msi.ORGANIZATION_ID
						FROM APPS.MTL_SYSTEM_ITEMS_B MSI,
							APPS.OKC_K_HEADERS_ALL_B OKH,
							APPS.OKC_K_LINES_B OKL,
							APPS.OKC_K_ITEMS OKI,
							APPS.OKC_K_LINES_B OKLB,
							APPS.OKC_K_ITEMS OKIB,
							APPS.CSI_ITEM_INSTANCES CII,
							--XXFT_FO_CONTRACT XFC
							(SELECT /*+ PARALLEL(XFC1,1000) */ XFC1.CONTRACT_NUMBER,XFC1.REGISTRATION_DATE 
                             FROM
                            (SELECT CONTRACT_NUMBER,REGISTRATION_DATE FROM XXFT_FO_CONTRACT
							UNION
							SELECT TO_CHAR(WARRANTY_ID) CONTRACT_NUMBER,REGISTRATION_DATE FROM XXFT_FO_WARRANTY
							)XFC1)XFC
						WHERE 1                   = 1
						AND XFC.CONTRACT_NUMBER   = OKH.CONTRACT_NUMBER
						AND OKH.ID                = OKL.CHR_ID
						AND OKL.ID                = OKI.CLE_ID
						AND OKI.OBJECT1_ID1      = MSI.INVENTORY_ITEM_ID
						AND MSI.ORGANIZATION_ID   = CII.INV_MASTER_ORGANIZATION_ID
						AND OKL.ID                = OKLB.CLE_ID
						AND OKLB.ID               = OKIB.CLE_ID
						AND OKIB.OBJECT1_ID1      = CII.INSTANCE_ID
						AND OKLB.ATTRIBUTE3      IS NOT NULL
						AND FND_CONC_DATE.string_to_date(OKLB.ATTRIBUTE3) = TRUNC(XFC.REGISTRATION_DATE)
						AND TRUNC(XFC.REGISTRATION_DATE) BETWEEN NVL(to_date(P_FROM_DATE, 'DD-MON-YY'),TRUNC(XFC.REGISTRATION_DATE))
													  AND NVL(to_date(P_TO_DATE, 'DD-MON-YY'),TRUNC(XFC.REGISTRATION_DATE))
						AND NOT EXISTS(SELECT 'Y'
										 FROM  OKC_K_REL_OBJS REL
										WHERE 1=1
										AND REL.jtot_object1_code  IN('OKX_ORDERLINE','OKX_ORDERHEAD')
										AND REL.CHR_ID            = OKH.ID)
            AND msi.segment1 not like 'FCX%'
					)REV
					)
					GROUP BY service_type
					   ,product_category
					   ,customer
					   ,order_number
					   ,contract_number
					   ,start_date
					   ,end_date
					   ,registration_date
					   ,Auto_Start_Date
					   ,SERV_START_DATE
					   ,SERV_END_DATE
					   ,serial_numnber ;
	--Declaration of Variables
    lc_file_name       VARCHAR2(200);
    lc_file_dir        VARCHAR2(100);
    lc_file_handler    UTL_FILE.FILE_TYPE;
    lc_file_mode       VARCHAR2(10):='w';
    lc_string          VARCHAR2(3000);
BEGIN
    lc_file_name     :='FTNT_REG_EVENT_REP_'||TO_CHAR(SYSDATE,'MMDDYYYYHHMISS')||'.csv';
	lc_file_dir      :='REVPRO_REPORT_DIR';
	lc_file_handler  := UTL_FILE.FOPEN(lc_file_dir,lc_file_name,lc_file_mode);
	lc_string        :='Sold To Customer'||'~'||
						'Oredr Number'||'~'||
						'Contract#'||'~'||
						--'Item Number'||'~'||
						'Product Category'||'~'||
						'Registration Date'||'~'||
						'Start Date from SC DFF'||'~'||
						'End Date from SC DFF'||'~'||
						'Default Start Date from SC'||'~'||
						'Default End Date from SC'||'~'||
						'Serial Number'||'~'||
						'Service Type'||'~'||
						'Auto Start Date'||'~'||
						'Child Count';
	 UTL_FILE.PUT_LINE(lc_file_handler,lc_string);
	 --Cursor Loop Starts
	 FOR rec_regevrep IN cur_regevrep
	 LOOP
	     lc_string:=rec_regevrep.CUSTOMER||'~'||
					rec_regevrep.ORDER_NUMBER||'~'||
					rec_regevrep.CONTRACT_NUMBER||'~'||
					--rec_regevrep.ITEM_NUMBER||'~'||
					rec_regevrep.PRODUCT_CATEGORY||'~'||
					rec_regevrep.REGISTRATION_DATE||'~'||
					rec_regevrep.START_DATE||'~'||
					rec_regevrep.END_DATE||'~'||
					rec_regevrep.SERV_START_DATE||'~'||
					rec_regevrep.SERV_END_DATE||'~'||
					rec_regevrep.SERIAL_NUMNBER||'~'||
					rec_regevrep.SERVICE_TYPE||'~'||
					rec_regevrep.AUTO_START_DATE||'~'||
					rec_regevrep.total_count;
		SELECT REPLACE(lc_string,CHR(10),NULL)
		  INTO   lc_string
		  FROM   dual;
		--
		UTL_FILE.PUT_LINE(lc_file_handler,lc_string);
	END LOOP;
    UTL_FILE.FCLOSE(lc_file_handler);
    fnd_file.put_line(FND_file.LOG,' Pls check for the output file named '||lc_file_name||' in the folder :=>'||lc_file_dir);
    fnd_file.put_line(FND_file.OUTPUT,' Pls check for the output file named '||lc_file_name||' in the folder :=>'||lc_file_dir);
EXCEPTION

    WHEN UTL_FILE.WRITE_ERROR THEN

      p_retcode   := 'E';
      p_errbuf  := p_errbuf||'Write Error Occured '||'.';

    WHEN UTL_FILE.INTERNAL_ERROR THEN

      p_retcode   := 'E';
      p_errbuf  := p_errbuf||'Internal Error Occured '||'.';

    WHEN UTL_FILE.INVALID_PATH THEN

      p_retcode   := 'E';
      p_errbuf  := p_errbuf||'Invalid Path '||'.';

    WHEN UTL_FILE.INVALID_MODE THEN

      p_retcode   := 'E';
      p_errbuf  := p_errbuf||'Invalid Mode '||'.';

    WHEN UTL_FILE.INVALID_FILEHANDLE THEN

      p_retcode   := 'E';
      p_errbuf  := p_errbuf||'Invalid File Handle '||'.';

    WHEN UTL_FILE.INVALID_OPERATION THEN

      p_retcode   := 'E';
      p_errbuf  := p_errbuf||'Invalid Operation '||'.';

    WHEN OTHERS THEN

      p_retcode   := 'E';
      p_errbuf  := p_errbuf||SUBSTR(SQLERRM,1,200)||'.';
    FND_FILE.PUT_LINE(FND_FILE.LOG,'THE STATUS IS '||p_retcode);
    FND_FILE.PUT_LINE(FND_FILE.LOG,'THE MESSAGE IS '||p_errbuf);
END;
/
show errors;
