select
  ith.order_no,
  ith.release_no,
  ith.sequence_no,
  ith.date_created,
  (ith.quantity - ith.qty_reversed) as qty_received,
  so.revised_qty_due as qty_due,
  so.need_date
from ifsapp.inventory_transaction_hist ith
join ifsapp.shop_ord so
  on ith.order_no = so.order_no
  and ith.release_no = so.release_no
  and ith.sequence_no = so.sequence_no
join ifsapp.inventory_part ip
  on ith.contract = ip.contract
  and ith.part_no = ip.part_no
where ith.transaction_code = 'OOREC'
  and ith.date_created >= :ts
  and ith.quantity > ith.qty_reversed
  and ip.hazard_code in (
    select
      si.hazard_code
    from ifsapp.safety_instruction si
    where lower(si.description) like '%rolls royce%'
  )
