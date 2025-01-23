select
  order_dtl.mitra_id,
  trim(master_prod.prd_name) as nama_produk,
  sum(order_dtl.gmv) as total_gmv
from `mp_mst.mp_mst_order_details` as order_dtl
left join `mp_mst.mp_mst_master_products` as master_prod
  on master_prod.prd_id = order_dtl.trx_prd_id
left join `mp_mst.mp_mst_mitra_location` as mitra_loc
  on mitra_loc.mitra_id = order_dtl.mitra_id
where
  order_dtl.trx_status = "COMPLETED"
  and order_dtl.trx_created_at >= date_sub(current_date(), interval 2 month)
  and mitra_loc.region in ("Jabar", "Jatim")
group by
  order_dtl.mitra_id,
  prd_name