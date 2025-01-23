with 
latest_pareto_sku as (
    select 
        prd_id, 
        region, 
        prd_name, 
        coalesce(is_bbb, "AgriAku") as client
    from `mp_ref.mp_ref_pareto_sku_ae_ter`
    where 
        input_date = format_date("%m/01/%Y", current_date())
        and region in ('Jabar', 'Jatim')
    group by prd_id, region, prd_name, client
)
, sku_seller_price as (
    select
        seller_loc.region,
        seller_product_units.prd_id,
        master_prod.prd_name,
        cast(round(avg(safe_divide(seller_product_units.price, master_prod_uoms.smallest_uom_unit)), -2) as int) as price
    from `mp_mst.mp_mst_seller_product_units` as seller_product_units
    left join `mp_mst.mp_mst_master_products` as master_prod
        on master_prod.prd_id = seller_product_units.prd_id
    left join `mp_mst.mp_mst_seller_location` as seller_loc
        on seller_loc.seller_id = seller_product_units.seller_id
    left join `mp_mst.mp_mst_master_product_uoms_scd` as master_prod_uoms
        on master_prod_uoms.prd_uom_id = seller_product_units.prd_uom_id
    where 
        seller_loc.region in ('Jabar', 'Jatim')
        and seller_product_units.deleted_at is null
    group by
        seller_loc.region,
        seller_product_units.prd_id,
        master_prod.prd_name
)
, sku_active_ingredients as (
    select
        trim(master_prod.prd_name) as prd_name,
        string_agg(distinct act_ing.name, ", " order by act_ing.name) as active_ingredients
    from `mp_mst.mp_mst_master_products` as master_prod
    inner join `mp_mst.mp_mst_master_product_active_chemical_ingredient_mappings` as act_ing_map
        on act_ing_map.master_prd_id = master_prod.prd_id
    inner join `mp_mst.mp_mst_master_product_active_chemical_ingredients` as act_ing
        on act_ing.active_chemical_ingredient_id = act_ing_map.active_chemical_ingredient_id
    where 
        act_ing_map.deleted_at is null
    group by
        prd_name
)
, initial_sku as (
    select
        case
            when mystique.region = 'Jabar' then  'Jawa Barat'
            when mystique.region = 'Jatim' then 'Jawa Timur'
            else mystique.region end as region,
        mystique.prd_name_base as produk_awal,
        latest_pareto_sku.client as pemasok_produk_awal,
        sku_active_ingredients.active_ingredients as bahan_aktif_produk_awal,
        -- concat("Rp", format("%'d", cast(sku_seller_price.price as int))) as harga_produk_awal,
    from `mp_bi.mp_bi_fact_dsp_mystique_predict` as mystique
    left join latest_pareto_sku
        on latest_pareto_sku.region = mystique.region
        and latest_pareto_sku.prd_id = mystique.base_prd_id
    -- left join sku_seller_price
    --     on sku_seller_price.region = mystique.region
    --     and sku_seller_price.prd_id = mystique.base_prd_id
    left join sku_active_ingredients
        on upper(sku_active_ingredients.prd_name) = upper(trim(mystique.prd_name_base))
    where 
        mystique.snapshot_dt > date_sub(current_date(), interval 7 day)
    group by region, produk_awal, pemasok_produk_awal, bahan_aktif_produk_awal-- , harga_produk_awal
)
, substitute_sku_temp as (
    select
        case
            when mystique.region = 'Jabar' then  'Jawa Barat'
            when mystique.region = 'Jatim' then 'Jawa Timur'
            else mystique.region end as region,
        mystique.prd_name_base as produk_awal,
        mystique.prd_name_compare as produk_substitusi,
        latest_pareto_sku.client as pemasok_produk_substitusi,
        sku_active_ingredients.active_ingredients as bahan_aktif_produk_substitusi,
        concat("Rp", format("%'d", cast(sku_seller_price.price as int))) as harga_produk_substitusi,
        mystique.smrm_rate_compare as smrm_rate_compare,
        mystique.is_better_margin as is_better_margin
    from `mp_bi.mp_bi_fact_dsp_mystique_predict` as mystique
    left join latest_pareto_sku
        on latest_pareto_sku.region = mystique.region
        and latest_pareto_sku.prd_id = mystique.compare_prd_id
    left join sku_seller_price
        on sku_seller_price.region = mystique.region
        and sku_seller_price.prd_id = mystique.compare_prd_id
    left join sku_active_ingredients
        on upper(sku_active_ingredients.prd_name) = upper(trim(mystique.prd_name_compare))
    where 
        mystique.snapshot_dt > date_sub(current_date(), interval 7 day)
        and mystique.recom_rank <= 5
    group by region, produk_awal, produk_substitusi, pemasok_produk_substitusi, harga_produk_substitusi, bahan_aktif_produk_substitusi, smrm_rate_compare, is_better_margin
)
, substitute_sku as (
    select region, produk_awal, produk_substitusi, pemasok_produk_substitusi, harga_produk_substitusi, bahan_aktif_produk_substitusi, is_better_margin
    from (
        select
            *,
            row_number() over(partition by region, produk_awal order by smrm_rate_compare desc) as rank_num
        from substitute_sku_temp
    )
    where rank_num = 1
)

select
    initial_sku.region,
    initial_sku.produk_awal,
    initial_sku.pemasok_produk_awal,
    initial_sku.bahan_aktif_produk_awal,
    -- initial_sku.harga_produk_awal,
    substitute_sku.produk_substitusi,
    substitute_sku.pemasok_produk_substitusi,
    substitute_sku.bahan_aktif_produk_substitusi,
    substitute_sku.harga_produk_substitusi,
    substitute_sku.is_better_margin
from initial_sku
inner join substitute_sku
    on substitute_sku.region = initial_sku.region
    and substitute_sku.produk_awal = initial_sku.produk_awal
order by region, produk_awal