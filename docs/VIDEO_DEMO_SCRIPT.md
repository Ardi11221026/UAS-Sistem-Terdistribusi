# Script Narasi Video Demo - Pub-Sub Log Aggregator Terdistribusi

**Durasi Total**: 18-22 menit  
**Peralatan**: Screen recorder (OBS Studio), microphone, resolusi 1920x1080, 30 fps

---

## ⚠️ PENTING: PANDUAN CARA MENGGUNAKAN

**JANGAN RUN COMMAND DARI FILE INI!**

1. ✅ **Buka file**: `INSTRUCTIONS.md` di bagian "**Video Demo: Panduan Perekaman Lengkap**"
2. ✅ **Copy-paste command** dari INSTRUCTIONS.md satu per satu ke terminal
3. ✅ **Gunakan script INI** untuk membaca narasi sambil menjalankan command
4. ✅ **Rekam screen** selama menjalankan setiap bagian

---

## Bagian Pendahuluan (1 menit)

*[Tampilkan title slide: "Pub-Sub Log Aggregator Terdistribusi"]*

"Assalamu'alaikum. Pada kesempatan ini, saya akan mendemonstrasikan **Sistem Pub-Sub Log Aggregator Terdistribusi** — suatu sistem agregator log multi-layanan yang dilengkapi dengan mekanisme idempotent consumer, deduplication persisten, serta kontrol transaksi dan konkurensi yang handal.

Sistem ini diimplementasikan menggunakan Python 3.11, framework FastAPI, PostgreSQL 16, serta Docker Compose untuk orkestrasi layanan."

---

## Bagian 1: Arsitektur Sistem (2 menit)

"Sistem ini terdiri atas tiga komponen utama yang saling terintegrasi:

1. **Publisher**: Simulator yang menghasilkan 1000 event dengan tingkat duplikasi terkontrol 30%
2. **Aggregator**: Server REST API yang menerima event dan melakukan deduplication
3. **PostgreSQL**: Basis data persisten dengan volume bernama untuk recovery

Fitur-fitur utama:
- Constraint unik pada (topic, event_id) untuk mencegah duplikat
- Transaksi atomik untuk kontrol konkurensi
- Volume bernama untuk persistensi data
- Health check mekanisme"

---

## Bagian 2-3: Setup & Build (3 menit)

*[Jalankan command dari INSTRUCTIONS.md bagian "Bagian 1-2"]*

"Kami meluncurkan semua service dalam mode detached. Docker Compose akan membuat PostgreSQL container, Aggregator container yang menunggu PostgreSQL siap, dan Publisher container yang mulai menghasilkan events.

Sempurna! Semua service berjalan dengan status 'healthy'."

---

## Bagian 4: Health Check (2 menit)

*[Jalankan command dari INSTRUCTIONS.md bagian "Bagian 3"]*

"Logger menunjukkan aktivitas real-time: database pool initialization, incoming requests, dan duplicate detection. Health check endpoint menunjukkan aggregator dalam kondisi 'healthy'."

---

## Bagian 5: Real-time Statistics (3 menit)

*[Jalankan monitoring statistik dari INSTRUCTIONS.md bagian "Bagian 4"]*

"Mari kita monitor statistik real-time. Anda akan melihat:
- **Events Received**: Total event terima
- **Unique Processed**: Event unik diproses
- **Duplicate Dropped**: Event duplikat terdeteksi
- **Dedup Rate**: Stabil di 30%

Fakta penting: **Dedup rate stabil di 30%** membuktikan deduplication logic sempurna!"

---

## Bagian 6: Demonstrasi Idempotency (2 menit)

*[Jalankan command dari INSTRUCTIONS.md bagian "Bagian 5"]*

"Saya akan mengirimkan event yang sama 3 kali. Hasilnya:
- Attempt 1: Diproses (processed=1)
- Attempt 2 & 3: Duplicate (processed=0)

Ini adalah **idempotency**: publisher bisa retry tanpa takut double-processing. Unique constraint di database adalah kunci mekanisme ini."

---

## Bagian 7: Concurrent Test (2 menit)

*[Jalankan command dari INSTRUCTIONS.md bagian "Bagian 6"]*

"Mengirimkan 5 request sama secara bersamaan:
- Total Processed: 1 (hanya 1 yang menang)
- Total Duplicates: 4

Ini membuktikan **zero double-processing under concurrent load**. Constraint handling + atomicity dari database mencegah race condition. **No explicit locking needed**."

---

## Bagian 8: Query Events (2 menit)

*[Jalankan command dari INSTRUCTIONS.md bagian "Bagian 7"]*

"Endpoint query menunjukkan:
- GET /events: Semua event unik
- GET /events?topic=X: Filtering per topic
- GET /events?limit=N: Pagination

Konsistensi antara internal state dan API response terjaga sempurna."

---

## Bagian 9: Persistence Test (3 menit)

*[Jalankan command dari INSTRUCTIONS.md bagian "Bagian 8"]*

"Ini demonstrasi paling penting: **data bertahan setelah container restart**.

Langkah:
1. Catat statistik saat ini
2. Hentikan semua container (docker compose down)
3. Restart layanan (docker compose up -d)
4. Verifikasi statistik

**HASIL**: Statistik tetap sama! 

PostgreSQL WAL + named volumes memastikan durability. Sistem survive application crash, container deletion, bahkan host reboot."

---

## Bagian 10: Network Security (1 menit)

"Keamanan jaringan:
- **Internal Network**: Semua service dalam Docker bridge network
- **Port Exposure**: Hanya aggregator (8080) diexpose
- **Database**: PostgreSQL internal only, tidak expose
- **No External Dependencies**: Semua lokal

Desain ini memenuhi security best practices."

---

## Bagian 11: Running Tests (1.5 menit)

*[Jalankan command dari INSTRUCTIONS.md bagian "Bagian 10"]*

"**20 integration tests** mencakup:
- Event validation
- Deduplication logic
- Persistence across restart
- Concurrent access
- API consistency
- Error handling

**Hasil: 20 PASSED in 0.12s**

Responsif dan tanpa bottleneck!"

---

## Bagian 12: Cleanup & Summary (1 menit)

*[Jalankan command dari INSTRUCTIONS.md bagian "Bagian 11"]*

"Terima kasih. Sistem ini mendemonstrasikan aplikasi praktis dari teori sistem terdistribusi:

- Loosely coupled architecture
- Publish-subscribe messaging
- Idempotency dan exactly-once semantics
- ACID properties dan concurrency control
- Persistence dan fault tolerance
- Observability

Semua source code dan dokumentasi tersedia di GitHub repository. Terima kasih telah meluangkan waktu!"

---

## Tips Post-Production

1. **Edit**: Potong pause panjang, speed-up waktu tunggu
2. **Subtitle**: Auto-generate, review & perbaiki
3. **Audio**: Normalize level, remove noise
4. **Title**: Tambah opening slide
5. **Upload**: YouTube Unlisted atau Public

**Target durasi**: 20-23 menit

---

**NEXT STEP**: Buka `INSTRUCTIONS.md` bagian "Video Demo: Panduan Perekaman Lengkap" untuk semua command!
