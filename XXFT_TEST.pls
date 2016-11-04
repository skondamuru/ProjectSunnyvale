create or replace procedure xxft_test(errbuff OUT VARCHAR2
                                        ,retcode OUT VARCHAR2
                                        )
IS

l_sql_query       VARCHAR2(4000);
l_contract_num    VARCHAR2(240);
l_line_start_date DATE;
l_line_end_date   DATE;
l_reg_date        DATE;
l_reg_start_date  DATE;
l_reg_end_date    DATE;
l_cnt             NUMBER; 

    ex_dml_errors EXCEPTION;
    PRAGMA EXCEPTION_INIT(ex_dml_errors, -24381);

TYPE stg1_tbl_type
  IS
    TABLE OF xxft_rpro_contracts_regs%ROWTYPE INDEX BY BINARY_INTEGER;

stg1_tbl stg1_tbl_type;

cursor cur_contracts
is
SELECT attribute45
from xxft_rpro_contracts;


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

BEGIN
xxft_rpro_get_contracts;

return; -- just testing the contracts.
for contracts_rec IN cur_contracts
LOOP
   OPEN cur_contract_details(contracts_rec.attribute45);
   FETCH cur_contract_details INTO l_contract_num,l_line_start_date,l_line_end_date,l_reg_date,l_reg_start_date,l_reg_end_date;
   CLOSE cur_contract_details;
   
   l_cnt := stg1_tbl.count+1;
   stg1_tbl(l_cnt).contract_number := l_contract_num;
   stg1_tbl(l_cnt).line_Start_date := l_line_start_date;
   stg1_tbl(l_cnt).line_end_date := l_line_end_date;
   stg1_tbl(l_cnt).registration_date := l_reg_date;
   stg1_tbl(l_cnt).reg_start_dt := l_reg_start_date;
   stg1_tbl(l_cnt).reg_end_dt := l_reg_end_date;  
END LOOP;

 BEGIN
				FORALL Q in stg1_tbl.FIRST..stg1_tbl.LAST SAVE EXCEPTIONS
				   INSERT INTO xxft_rpro_contracts_regs values stg1_tbl(Q);
				COMMIT;
			 EXCEPTION
				  WHEN ex_dml_errors THEN
				  --    l_error_count := SQL%BULK_EXCEPTIONS.count;
					  fnd_file.put_line(fnd_file.LOG,'Unexpected Exception when inserting records #2: ' || SQL%BULK_EXCEPTIONS.count);
					  FOR p IN 1 .. SQL%BULK_EXCEPTIONS.count LOOP
						fnd_file.put_line(fnd_file.LOG,'Error: ' || p ||
						   ' Array Index: ' || SQL%BULK_EXCEPTIONS(p).error_index ||
							 ' Message: ' || SQLERRM(-SQL%BULK_EXCEPTIONS(p).ERROR_CODE)||
							 ' Sales Order New Line Id :=>'||stg1_tbl(p).contract_number);
					  END LOOP;
					  ROLLBACK;
				WHEN OTHERS
				THEN
				  FND_FILE.PUT_LINE(FND_FILE.LOG,'  Unexpected Exception when inserting records #2:=>'||SQLERRM);
				  RAISE;
			 END;
END;