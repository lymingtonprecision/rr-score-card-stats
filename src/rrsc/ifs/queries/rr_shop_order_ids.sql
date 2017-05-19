select
  so.order_no,
  so.release_no,
  so.sequence_no
from ifsapp.shop_ord so
join ifsapp.inventory_part ip
  on so.contract = ip.contract
  and so.part_no = ip.part_no
where so.objstate <> 'Cancelled'
  and ip.hazard_code in (
    select
      si.hazard_code
    from ifsapp.safety_instruction si
    where lower(si.description) like '%rolls royce%'
  )
