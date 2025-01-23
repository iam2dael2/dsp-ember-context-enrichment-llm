from typing import List

# List of queries that will be run
detail_mitra_queries: List[str] = [
    'ALTER TABLE `detail_mitra` ADD PRIMARY KEY (mitra_id, ae_name);'
]

rekomendasi_produk_queries: List[str] = [
    'ALTER TABLE rekomendasi_produk ADD PRIMARY KEY (mitra_id, ae_name, nama_produk);',
    'ALTER TABLE rekomendasi_produk ADD FOREIGN KEY (mitra_id, ae_name) REFERENCES detail_mitra(mitra_id, ae_name)'
]
    
kupon_promo_queries: List[str] = [
    'ALTER TABLE kupon_promo ADD PRIMARY KEY (mitra_id, ae_name, kode_kupon);',
    'ALTER TABLE kupon_promo ADD FOREIGN KEY (mitra_id, ae_name) REFERENCES detail_mitra(mitra_id, ae_name)'
]

ringkasan_transaksi_queries: List[str] = [
    'ALTER TABLE ringkasan_transaksi_detail ADD PRIMARY KEY (mitra_id, ae_name, detail_transaksi_nama_produk);',
    'ALTER TABLE ringkasan_transaksi_detail ADD FOREIGN KEY (mitra_id, ae_name) REFERENCES detail_mitra(mitra_id, ae_name);',
    'ALTER TABLE ringkasan_transaksi_history ADD PRIMARY KEY (mitra_id, ae_name);',
    'ALTER TABLE ringkasan_transaksi_history ADD FOREIGN KEY (mitra_id, ae_name) REFERENCES detail_mitra(mitra_id, ae_name);'
]

detail_produk_queries: List[str] = [
    'ALTER TABLE detail_produk ADD PRIMARY KEY (mitra_id, nama_produk);'
]

queries: List[str] = detail_mitra_queries + rekomendasi_produk_queries + kupon_promo_queries + ringkasan_transaksi_queries + detail_produk_queries