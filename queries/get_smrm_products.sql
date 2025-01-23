select
  case
    when mitra_loc.region = 'Jabar' then 'Jawa Barat'
    when mitra_loc.region = 'Jatim' then 'Jawa Timur'
    else mitra_loc.region end as region,
  master_prod.prd_name as nama_produk,
  safe_divide(sum(smrm_data.smrm), sum(smrm_data.cogs)) as smrm_rate
from `mp_bi.mp_bi_fact_cogs_trx_tr` as smrm_data
left join `mp_mst.mp_mst_master_products` as master_prod
  on master_prod.prd_id = smrm_data.prd_id
left join `mp_mst.mp_mst_mitra_location` as mitra_loc
  on mitra_loc.mitra_id = smrm_data.mitra_id
where
  smrm_data.trx_created_at between date_sub(current_date(), interval 2 month) and current_date()
  and smrm_data.mitra_id in (
    select distinct mitra_id
    from `mp_bi.mp_bi_fact_context_enrichment_to_sfa`
    where snapshot_dt = current_date()
  )
  and mitra_loc.region in ('Jabar', 'Jatim')
group by
  region,
  master_prod.prd_name
