select *
from `mp_bi.mp_bi_fact_context_enrichment_to_sfa`
where 
    snapshot_dt = current_date()
    and mitra_id in (
        select mitra_id
        from `mp_mst.mp_mst_mitra_location`
        where region in ("Jabar", "Jatim")
    )
order by mitra_id