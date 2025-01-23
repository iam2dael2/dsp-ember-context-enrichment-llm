with
  ast_prd as (
    select
      distinct(ast.product_id) as prd_id
    from `mp_bi.mp_bi_fact_product_assortment_territory` as ast
    where
      ast.prc_dt = (select max(prc_dt) from `mp_bi.mp_bi_fact_product_assortment_territory`)
      order by
        prd_id asc
)
, gmv_per_prd as (
  select
    mitra_v2v.cluster,
    master_prod.prd_name,
    sum(ord.gmv) as sum_gmv
  from `mp_mst.mp_mst_order_details` as ord
  left join `mp_mst.mp_mst_mitra_location` as mitra_loc
    on mitra_loc.mitra_id = ord.mitra_id
  left join `mp_mst.mp_mst_master_products` as master_prod
    on master_prod.prd_id = ord.trx_prd_id
  left join `mp_bi.mp_bi_fact_mitra_v2v` as mitra_v2v
    on mitra_v2v.mitra_id = ord.mitra_id
  where
    ord.trx_completed_at > date_sub(current_date("Asia/Jakarta"), interval 3 month)
    and ord.trx_status = "COMPLETED"
    and ord.trx_prd_id in (select ast_prd.prd_id from ast_prd)
    and mitra_loc.region in ("Jabar", "Jatim Timur", "Jatim Barat")
    and mitra_v2v.cluster is not null
  group by
    mitra_v2v.cluster,
    master_prod.prd_name
)
, comp_total_gmv as (
  select sum(gpp.sum_gmv) as total_gmv
  from gmv_per_prd as gpp
)
, comp_pareto_ast as (
  select
    gpp.cluster,
    gpp.prd_name,
    gpp.sum_gmv,
    sum(gpp.sum_gmv) over(order by gpp.sum_gmv desc) as cum_sum_gmv,
    sum(gpp.sum_gmv) over (order by gpp.sum_gmv desc) / ctg.total_gmv as frac_total_gmv
  from gmv_per_prd as gpp, comp_total_gmv as ctg
  order by
    gpp.sum_gmv desc
)
, comp_pareto_rank_temp as (
  select
    cpa.cluster,
    cpa.prd_name,
    cpa.frac_total_gmv,
    row_number() over(partition by cpa.cluster order by cpa.frac_total_gmv asc) as prd_rank
  from comp_pareto_ast as cpa
)
, comp_pareto_rank as (
  select
    cluster,
    prd_name as nama_produk
  from comp_pareto_rank_temp
  where
    prd_rank <= 5
  order by
    cluster asc,
    prd_rank asc
)

select *
from comp_pareto_rank