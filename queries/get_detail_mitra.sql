select
    distinct context_enrichment.mitra_id,
    initcap(mitra_details.store_name) as nama_mitra,
    initcap(mitra_details.full_name) as pemilik_mitra,
    case 
        when mitra_loc.region = 'Jatim' then 'Jawa Timur'
        when mitra_loc.region = 'Jabar' then 'Jawa Barat'
        else mitra_loc.region end as region_mitra,
    mitra_v2v.cluster as cluster_mitra
from `mp_bi.mp_bi_fact_context_enrichment_to_sfa` as context_enrichment
left join `mp_mst.mp_mst_mitra_details` as mitra_details
    on mitra_details.mitra_id = context_enrichment.mitra_id
left join `mp_mst.mp_mst_mitra_location` as mitra_loc
    on mitra_loc.mitra_id = context_enrichment.mitra_id
left join `mp_bi.mp_bi_fact_mitra_v2v` as mitra_v2v
    on mitra_v2v.mitra_id = context_enrichment.mitra_id
where 
    snapshot_dt = current_date()
    and mitra_loc.region in ("Jabar", "Jatim")
order by mitra_id