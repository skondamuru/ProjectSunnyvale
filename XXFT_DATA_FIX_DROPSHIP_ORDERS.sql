/****************************************************************************
* File: XXFT_DATA_FIX_DROP_ORDERS.sql
* Description: Update the Order Classification and attribute47 and number3
*****************************************************************************/
UPDATE xxft_rpro_order_details a
SET attribute47    = XXFT_RPRO_DATA_EXTRACT_PKG.get_order_classification(a.sales_order_line_id),
  last_update_date = SYSDATE
WHERE attribute47 IS NULL ;

UPDATE xxft_rpro_order_details
SET attribute37   ='N'
WHERE attribute47 =ANY('DROP SHIP','COTERM','AUTO REGISTERED')
AND processed_flag='N';

UPDATE xxft_rpro_order_details a
SET attribute52    =XXFT_RPRO_DATA_EXTRACT_PKG.get_line_source(a.sales_order_line_id)
WHERE attribute52 IS NULL;

UPDATE xxft_rpro_order_details a
SET number3       = 0
WHERE attribute52 ='AUTO REGISTERED'
AND processed_flag='N';

COMMIT;





