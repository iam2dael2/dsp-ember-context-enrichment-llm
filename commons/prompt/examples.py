from typing import List

# Examples: List of mitra 
detail_mitra_related: List[dict] = []

# Examples: Recommended products
rekomendasi_produk_related: List[dict] = [
    {
        "input": "Buatkan simpulan mengenai rekomendasi produk untuk Sinergi Tani dengan mitra id 32516",
        "query": """
        select produk_substitusi, produk_awal, is_better_margin, harga_produk_substitusi, pemasok_produk_substitusi, pemasok_produk_awal, bahan_aktif_produk_substitusi, bahan_aktif_produk_awal
        from substitusi_produk 
        where region || '_' || produk_awal in (
            select detail_mitra.region_mitra || '_' || rekomendasi_produk.nama_produk
            from detail_mitra
            inner join rekomendasi_produk on rekomendasi_produk.mitra_id = detail_mitra.mitra_id
            where detail_mitra.mitra_id = 32516
        )
        """
    },
    {
        "input": "Berikan rekomendasi produk untuk Dzamar Tani dengan mitra id 49291",
        "query": """
        select produk_substitusi, produk_awal, is_better_margin, harga_produk_substitusi, pemasok_produk_substitusi, pemasok_produk_awal, bahan_aktif_produk_substitusi, bahan_aktif_produk_awal
        from substitusi_produk 
        where region || '_' || produk_awal in (
            select detail_mitra.region_mitra || '_' || rekomendasi_produk.nama_produk
            from detail_mitra
            inner join rekomendasi_produk on rekomendasi_produk.mitra_id = detail_mitra.mitra_id
            where detail_mitra.mitra_id = 49291
        )
        """
    },
    {
        "input": "Rekomendasikan produk untuk Marem Tani dengan mitra id 46465",
        "query": """
        select produk_substitusi, produk_awal, is_better_margin, harga_produk_substitusi, pemasok_produk_substitusi, pemasok_produk_awal, bahan_aktif_produk_substitusi, bahan_aktif_produk_awal
        from substitusi_produk 
        where region || '_' || produk_awal in (
            select detail_mitra.region_mitra || '_' || rekomendasi_produk.nama_produk
            from detail_mitra
            inner join rekomendasi_produk on rekomendasi_produk.mitra_id = detail_mitra.mitra_id
            where detail_mitra.mitra_id = 46465
        )
        """
    }
]

# Examples: Promo Coupon
kupon_promo_related: List[dict] = []

# Examples: Transaction Summarized
ringkasan_transaksi_related: List[dict] = []

examples: List[dict] = detail_mitra_related + rekomendasi_produk_related + kupon_promo_related + ringkasan_transaksi_related