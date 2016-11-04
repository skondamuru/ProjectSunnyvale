set serveroutput ON;
create or replace procedure xxft_rpro_get_contracts
IS

l_cnt                 NUMBER;
l_sales_order_line_id NUMBER;
l_qty                 NUMBER;
L_LINE_CNT   NUMBER;

TYPE stg1_tbl_type
  IS
    TABLE OF xxft_rpro_contracts_msmatch%ROWTYPE INDEX BY BINARY_INTEGER;

stg1_tbl stg1_tbl_type;
  ex_dml_errors EXCEPTION;
    PRAGMA EXCEPTION_INIT(ex_dml_errors, -24381);
    

CURSOR cur_check_contracts
is
SELECT distinct --c.sales_order_line_id,
    b.sales_order,
    b.attribute45
FROM xxft_rpro_check_orders_cts a,
  revpro.rpro_arr_transactions b  
WHERE b.sales_order           = a.sales_order
AND b.def_amt                 > 0
AND b.attribute45            IS NOT NULL
AND b.tran_type               ='INV'
AND EXISTS
  (SELECT 1
  FROM revpro.rpro_fn_release_events_v
  WHERE contract_number = b.attribute45
  )
AND EXISTS
  (SELECT 1
  FROM REVPRO.RPRO_ARR_CONTINGENCIES
  WHERE sales_order_line_id = B.SALES_ORDER_LINE_ID
  AND CONTINGENCY_ID        = 10001
  AND processed_flag        ='N'
  )
--  and rownum<200
  ;

cursor cur_contract_so_line(p_sales_order IN VARCHAR2, p_contract_num in VARCHAR2)
is
SELECT sales_order_line_id
from xxft_rpro_order_details 
where sales_order= p_sales_order
and attribute45= p_contract_num
and attribute44= any('E_SUPPORT','E_SUBSCRIPTION','S_SUPPORT','S_SUBSCRIPTION','S_PROFESSIONAL SERVICE','S_SOFTWARE');


cursor cur_contracts_so_line (p_service_line_id IN NUMBER)
IS
	SELECT count(KRO.CHR_ID)
      FROM OKC_K_REL_OBJS kro
         , OKC_K_HEADERS_ALL_B okh
	 WHERE 1                   = 1
	   AND KRO.JTOT_OBJECT1_CODE ='OKX_ORDERLINE'
	   AND kro.object1_id1       = p_service_line_id
	   AND kro.object1_id2       ='#'
     and okh.id = kro.chr_id
     and okh.sts_code=any('ACTIVE','SIGNED');
     
cursor cur_contract_details(p_contract_num VARCHAR2)
is
SELECT OKH.CONTRACT_NUMBER,
  OKL.START_DATE,
  OKL.END_DATE,
  OKLB.ATTRIBUTE3 REG_DATE,
  OKLB.ATTRIBUTE1,
  OKLB.ATTRIBUTE2
FROM OKC_K_HEADERS_ALL_B OKH,
  OKC_K_LINES_B OKL,
  OKC_K_LINES_B OKLB
WHERE contract_number =p_contract_num
AND OKL.CHR_ID        = OKH.ID
AND OKLB.CLE_ID       = OKL.ID
AND OKL.STS_CODE      = ANY('ACTIVE','SIGNED')
AND OKLB.STS_CODE     = ANY('ACTIVE','SIGNED');


cursor cur_sales_order_line(p_line_id in Number)
is
SELECT ordered_quantity
from oe_order_lines_all 
where line_id = p_line_id;


CURSOR cur_get_all_contracts(p_service_line_id IN NUMBER)
	IS
 SELECT --p_line_id,
	       csi.serial_number,
		   okh.contract_number,
           oklb.start_date,
           oklb.end_date,
             OKLB.ATTRIBUTE3 REG_DATE,
  OKLB.ATTRIBUTE1 reg_start_date,
  OKLB.ATTRIBUTE2 reg_end_date,
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
    and oklb1.sts_code= any('ACTIVE','SIGNED')
    AND okh.sts_code = any('ACTIVE','SIGNED') --ITS#573774
    AND kro.cle_id = okl.id
    AND okl.cle_id = oklb1.cle_id
    and oklb.id = oklb1.cle_id
    and oklb1.id = okib.cle_id
    and okh.id = oklb1.dnz_chr_id ;

BEGIN

FOR check_contracts_rec IN cur_check_contracts
LOOP
   OPEN cur_contract_so_line(check_contracts_rec.sales_order,check_contracts_rec.attribute45);
   FETCH cur_contract_so_line INTO l_sales_order_line_id;
   CLOSE cur_contract_so_line;
   
   OPEN cur_contracts_so_line(l_sales_order_line_id);
   FETCH cur_contracts_so_line INTO l_cnt;
   CLOSE cur_contracts_so_line;
   
   OPEN cur_sales_order_line(l_sales_order_line_id);
   FETCH cur_sales_order_line into l_qty;
   CLOSE cur_sales_order_line;
   
   IF l_qty <> l_cnt
   THEN
      FOR get_all_contracts_rec IN cur_get_all_contracts(l_sales_order_line_id)
      LOOP
         IF get_all_contracts_rec.REG_DATE is not null
         THEN
            l_line_cnt := stg1_tbl.COUNT+1;
            stg1_tbl(l_line_cnt).old_contract_number := check_contracts_rec.attribute45;
            stg1_tbl(l_line_cnt).new_contract_number := get_all_contracts_rec.contract_number;
            stg1_tbl(l_line_cnt).line_start_date     := get_all_contracts_rec.start_date;
            stg1_tbl(l_line_cnt).line_end_date     := get_all_contracts_rec.end_date;
            stg1_tbl(l_line_cnt).registration_date     := get_all_contracts_rec.REG_DATE;
            stg1_tbl(l_line_cnt).reg_start_date     := get_all_contracts_rec.reg_start_date;
            stg1_tbl(l_line_cnt).reg_end_date     := get_all_contracts_rec.reg_end_date;
               dbms_output.put_line(' ######get_all_contracts_rec.contract_number #####:=>'||get_all_contracts_rec.contract_number);
         END IF;
      
      END LOOP;
   ELSE
      dbms_output.put_line(' ----Matches the count and hence skipping the launch -----');
      dbms_output.put_line(' check_contracts_rec.attribute45 :=>'||check_contracts_rec.attribute45);
      dbms_output.put_line(' check_contracts_rec.sales_order :=>'||check_contracts_rec.sales_order);
      dbms_output.put_line(' ----Matches the count and hence skipping the launch -----');
      CONTINUE;    
   
   END IF;
   

END LOOP;

BEGIN
				FORALL Q in stg1_tbl.FIRST..stg1_tbl.LAST SAVE EXCEPTIONS
				   INSERT INTO xxft_rpro_contracts_msmatch values stg1_tbl(Q);
				COMMIT;
			 EXCEPTION
				  WHEN ex_dml_errors THEN
				  --    l_error_count := SQL%BULK_EXCEPTIONS.count;
					  fnd_file.put_line(fnd_file.LOG,'Unexpected Exception when inserting records #2: ' || SQL%BULK_EXCEPTIONS.count);
					  FOR p IN 1 .. SQL%BULK_EXCEPTIONS.count LOOP
						fnd_file.put_line(fnd_file.LOG,'Error: ' || p ||
						   ' Array Index: ' || SQL%BULK_EXCEPTIONS(p).error_index ||
							 ' Message: ' || SQLERRM(-SQL%BULK_EXCEPTIONS(p).ERROR_CODE)||
							 ' Sales Order New Line Id :=>'||stg1_tbl(p).new_contract_number);
					  END LOOP;
					  ROLLBACK;
				WHEN OTHERS
				THEN
				  FND_FILE.PUT_LINE(FND_FILE.LOG,'  Unexpected Exception when inserting records #2:=>'||SQLERRM);
				  RAISE;
			 END;

END;