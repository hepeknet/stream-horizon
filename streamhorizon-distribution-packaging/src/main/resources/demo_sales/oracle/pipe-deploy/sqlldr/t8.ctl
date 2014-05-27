options 
(
  rows=200000,
  direct=true,
  bindsize=5000000 
)
load data
  append 
  discardmax 0
  into table sh.sales_fact subpartition (P_20140107_SP_8)
  fields terminated by ","
  (employee_id integer external, 
   customer_id integer external,
   product_id integer external,
   sales_channel_id integer external,
   promotion_id integer external,
   supplier_id integer external,
   booking_date_id constant 20140107,
   sales_date_id integer external,
   delivery_date_id integer external,
   priceBeforeDiscount,
   priceAfterDiscount,
   saleCosts,
   sub constant 8,
   file_id constant 1
  )
