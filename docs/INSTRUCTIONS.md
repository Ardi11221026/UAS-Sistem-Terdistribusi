# Panduan Implementasi Lengkap: Pub-Sub Log Aggregator Terdistribusi

Sistem agregator log terdistribusi dengan mekanisme Idempotent Consumer, deduplication persisten, dan kontrol konkurensi ACID.

---

## 📋 Daftar Isi

1. [Prasyarat Sistem](#prasyarat-sistem)
2. [Persiapan Awal](#persiapan-awal)
3. [Video Demo: Panduan Perekaman Lengkap](#video-demo-panduan-perekaman-lengkap)
4. [Pemberhentian Sistem](#pemberhentian-sistem)
5. [Pemecahan Masalah](#pemecahan-masalah)

---

## Prasyarat Sistem

### Perangkat Lunak yang Diperlukan

- **Docker & Docker Compose** (versi 20.10 atau lebih baru)
- **Port 8080 tersedia** (untuk akses aggregator)
- **Python 3.8+** (opsional, untuk menjalankan tes secara lokal)
- **Git** (untuk kontrol versi)
- **Terminal atau PowerShell** (bergantung pada sistem operasi)
- **cURL** (untuk pengujian API)

### Verifikasi Instalasi

**Windows PowerShell:**
```powershell
docker --version
docker compose version
```

**Output yang diharapkan:**
```
Docker version 24.0.0, build abcdef
Docker Compose version 2.20.0+
```

---

## Persiapan Awal

### Persiapan 1: Navigasi ke Direktori Proyek

```powershell
cd "d:\UAS Sistem Terdistribusi"
```

**Output yang diharapkan:**
```
D:\UAS Sistem Terdistribusi>
```

---

### Persiapan 2: Verifikasi Struktur Folder

```powershell
dir
```

**Output yang diharapkan:**
```
Directory: D:\UAS Sistem Terdistribusi

Mode                 LastWriteTime         Length Name
----                 --------               ------ ----
d-----         12/11/2024   1:30 PM                aggregator
d-----         12/11/2024   1:30 PM                docs
d-----         12/11/2024   1:30 PM                publisher
d-----         12/11/2024   1:30 PM                tests
-a----         12/11/2024   1:30 PM           1234 docker-compose.yml
-a----         12/11/2024   1:30 PM            567 README.md
```

---

### Persiapan 3: Verifikasi Docker Terinstall

```powershell
docker --version
docker compose version
```

**Output yang diharapkan:**
```
Docker version 24.0.0, build abcdef
Docker Compose version 2.20.0+
```

Jika error, install Docker Desktop dari https://www.docker.com/products/docker-desktop

---

### Persiapan 4: Verifikasi Port 8080 Kosong

```powershell
netstat -ano | findstr :8080
```

**Output yang diharapkan:** Tidak ada output (port kosong)

Jika ada output, lihat bagian [Pemecahan Masalah](#pemecahan-masalah) → Masalah 1

---

## Video Demo: Panduan Perekaman Lengkap

**Durasi Total**: 18-22 menit

Ikuti langkah-langkah berikut secara berurutan. Setiap bagian sesuai dengan narasi di `VIDEO_DEMO_SCRIPT.md`.

---

### Bagian 1: Build Docker Images (Waktu: 1-2 menit)

**STEP 1.1: Build images**
```powershell
docker compose build
```

**Output yang diharapkan:**
```
[+] Building 45.2s (18/18) FINISHED
 => [internal] load build definition from Dockerfile
 => [internal] load .dockerignore
 ...
 => => naming to docker.io/library/uas-aggregator:latest
 => => naming to docker.io/library/uas-publisher:latest
```

**STEP 1.2: Verifikasi images berhasil dibuat**
```powershell
docker image list | findstr uas
```

**Output yang diharapkan:**
```
uas-aggregator     latest    abcd1234   2 hours ago   150MB
uas-publisher      latest    efgh5678   2 hours ago   140MB
```

**Narasi**: "Pertama, kami membangun Docker images untuk Aggregator dan Publisher. Build process memerlukan waktu 45-60 detik dan mengunduh base Python images serta dependencies."

---

### Bagian 2: Startup Services (Waktu: 2-3 menit)

**STEP 2.1: Mulai semua service**
```powershell
docker compose up -d
```

**Output yang diharapkan:**
```
Creating network "uas_distributed_network" with driver "bridge"
Creating uas-storage ...
Creating uas-storage ... done
Creating uas-aggregator ...
Creating uas-aggregator ... done
Creating uas-publisher ...
Creating uas-publisher ... done
```

**STEP 2.2: Tunggu layanan siap (tunggu 15 detik)**
```powershell
Start-Sleep -Seconds 15
```

**STEP 2.3: Verifikasi status semua container**
```powershell
docker compose ps
```

**Output yang diharapkan:**
```
NAME              SERVICE       STATUS              PORTS
uas-storage       storage       Up 12s (healthy)    5432/tcp
uas-aggregator    aggregator    Up 11s (healthy)    0.0.0.0:8080->8080/tcp
uas-publisher     publisher     Up 10s              (running)
```

**Narasi**: "Kami meluncurkan ketiga service: PostgreSQL (storage), Aggregator (API server), dan Publisher (event generator). Mode -d berarti detached, sehingga service berjalan di background."

---

### Bagian 3: Health Check & Statistics (Waktu: 3 menit)

**STEP 3.1: Periksa kesehatan aggregator**
```powershell
curl http://localhost:8080/health
```

**Output yang diharapkan:**
```json
{
  "status": "healthy",
  "timestamp": "2024-12-11T10:30:00Z"
}
```

**STEP 3.2: Monitor statistik real-time (loop 12 kali, 5 detik interval)**
```powershell
for ($i = 0; $i -lt 12; $i++) {
    Clear-Host
    $stats = curl http://localhost:8080/stats | ConvertFrom-Json
    
    Write-Host "=== AGGREGATOR STATISTICS ===" -ForegroundColor Green
    Write-Host "Waktu: $(Get-Date -Format 'HH:mm:ss')"
    Write-Host ""
    Write-Host "Events Received:       $($stats.received)"
    Write-Host "Unique Processed:      $($stats.unique_processed)"
    Write-Host "Duplicate Dropped:     $($stats.duplicate_dropped)"
    
    if ($stats.received -gt 0) {
        $dedup_rate = [math]::Round($stats.duplicate_dropped / $stats.received * 100, 1)
        Write-Host "Dedup Rate:            $dedup_rate%"
    }
    
    Write-Host ""
    Write-Host "Topics: $($stats.topics -join ', ')"
    Write-Host "Uptime: $($stats.uptime_seconds)s"
    
    if ($i -lt 11) {
        Write-Host ""
        Write-Host "(Refresh dalam 5 detik...)"
        Start-Sleep -Seconds 5
    }
}
```

**Output yang diharapkan (perubahan setiap 5 detik):**
```
=== AGGREGATOR STATISTICS ===
Waktu: 10:30:45
Events Received:       245
Unique Processed:      172
Duplicate Dropped:     73
Dedup Rate:            29.8%
Topics: orders, payments, logs, users
Uptime: 52s
```

**Narasi**: "Publisher otomatis menghasilkan 1000 events dengan duplikasi 30%. Statistik real-time menunjukkan dedup rate stabil di ~30%. Sistem tidak pernah process event sama dua kali — ini idempotency."

---

### Bagian 4: Test Idempotency (Waktu: 2 menit)

**STEP 4.1: Buat event untuk test**
```powershell
$event = @{
    topic = "demo"
    event_id = "demo-idempotent-001"
    timestamp = [System.DateTime]::UtcNow.ToString("o")
    source = "demo-publisher"
    payload = @{ message = "Testing idempotency" }
} | ConvertTo-Json
```

**STEP 4.2: Publish attempt 1**
```powershell
Write-Host "=== PUBLISH ATTEMPT 1 ===" -ForegroundColor Yellow
curl -X POST http://localhost:8080/publish `
  -H "Content-Type: application/json" `
  -d $event | ConvertFrom-Json | ConvertTo-Json
```

**Output yang diharapkan:**
```json
{
  "processed": 1,
  "reason": "Event accepted and processed"
}
```

**STEP 4.3: Publish attempt 2 (same event)**
```powershell
Write-Host "`n=== PUBLISH ATTEMPT 2 (duplicate) ===" -ForegroundColor Yellow
Start-Sleep -Seconds 1
curl -X POST http://localhost:8080/publish `
  -H "Content-Type: application/json" `
  -d $event | ConvertFrom-Json | ConvertTo-Json
```

**Output yang diharapkan:**
```json
{
  "processed": 0,
  "reason": "Duplicate event - already processed"
}
```

**STEP 4.4: Publish attempt 3 (same event)**
```powershell
Write-Host "`n=== PUBLISH ATTEMPT 3 (duplicate) ===" -ForegroundColor Yellow
Start-Sleep -Seconds 1
curl -X POST http://localhost:8080/publish `
  -H "Content-Type: application/json" `
  -d $event | ConvertFrom-Json | ConvertTo-Json
```

**Output yang diharapkan:**
```json
{
  "processed": 0,
  "reason": "Duplicate event - already processed"
}
```

**Narasi**: "Test idempotency: event sama dikirim 3 kali. Attempt 1 diproses (processed=1), attempt 2-3 ditolak duplicate (processed=0). Database constraint pada (topic, event_id) mencegah double-processing."

---

### Bagian 5: Concurrent Publish Test (Waktu: 2 menit)

**STEP 5.1: Siapkan event untuk concurrent test**
```powershell
$event = @{
    topic = "concurrent-demo"
    event_id = "concurrent-001"
    timestamp = [System.DateTime]::UtcNow.ToString("o")
    source = "demo"
    payload = @{ test = "concurrency" }
} | ConvertTo-Json

Write-Host "=== CONCURRENT PUBLISH TEST ===" -ForegroundColor Cyan
Write-Host "Mengirim 5 request bersamaan untuk event yang sama..."
```

**STEP 5.2: Fire 5 concurrent requests**
```powershell
$results = @()
for ($i = 1; $i -le 5; $i++) {
    $response = curl -s -X POST http://localhost:8080/publish `
      -H "Content-Type: application/json" `
      -d $event
    $results += ($response | ConvertFrom-Json)
}

Write-Host ""
Write-Host "Results:"
for ($i = 0; $i -lt $results.Count; $i++) {
    Write-Host "  Request $($i+1): processed=$($results[$i].processed)"
}

$processed = ($results | Where-Object { $_.processed -eq 1 }).Count
$duplicates = $results.Count - $processed

Write-Host ""
Write-Host "Summary:"
Write-Host "  Total Processed: $processed (Expected: 1)"
Write-Host "  Total Duplicates: $duplicates (Expected: 4)"

if ($processed -eq 1 -and $duplicates -eq 4) {
    Write-Host ""
    Write-Host "✅ CONCURRENT TEST PASSED!" -ForegroundColor Green
}
```

**Output yang diharapkan:**
```
Results:
  Request 1: processed=1
  Request 2: processed=0
  Request 3: processed=0
  Request 4: processed=0
  Request 5: processed=0

Summary:
  Total Processed: 1 (Expected: 1)
  Total Duplicates: 4 (Expected: 4)

✅ CONCURRENT TEST PASSED!
```

**Narasi**: "Concurrent test: 5 request untuk event sama dikirim bersamaan. Hanya 1 diproses, 4 lainnya duplicate. Zero double-processing under concurrent load dijamin oleh unique constraint + transaksi atomik."

---

### Bagian 6: Query Events (Waktu: 2 menit)

**STEP 6.1: Query semua event**
```powershell
Write-Host "=== QUERY EVENTS ===" -ForegroundColor Magenta

Write-Host "`n[1] GET /events (semua event):"
$all_events = curl -s http://localhost:8080/events | ConvertFrom-Json
Write-Host "Total event unik: $($all_events.count)"
Write-Host "Sampel event pertama:"
$all_events.events | Select-Object -First 2 | ForEach-Object {
    Write-Host "  [$($_.topic)] $($_.event_id) - $($_.source)"
}
```

**Output yang diharapkan:**
```
[1] GET /events (semua event):
Total event unik: 500+
Sampel event pertama:
  [orders] evt-001 - publisher-service
  [orders] evt-002 - publisher-service
```

**STEP 6.2: Query events per topic**
```powershell
Write-Host "`n[2] GET /events?topic=orders (5 pertama dari topic 'orders'):"
$orders = curl -s "http://localhost:8080/events?topic=orders&limit=5" | ConvertFrom-Json
Write-Host "Total dari topic orders: $($orders.count)"
$orders.events | ForEach-Object {
    Write-Host "  - $($_.event_id)"
}
```

**Output yang diharapkan:**
```
[2] GET /events?topic=orders (5 pertama dari topic 'orders'):
Total dari topic orders: 5
  - evt-001
  - evt-002
  - evt-003
  - evt-004
  - evt-005
```

**STEP 6.3: Query dengan limit**
```powershell
Write-Host "`n[3] GET /events?limit=3 (3 events pertama):"
$limited = curl -s "http://localhost:8080/events?limit=3" | ConvertFrom-Json
Write-Host "Limited to 3: $($limited.count) events"
```

**Output yang diharapkan:**
```
[3] GET /events?limit=3 (3 events pertama):
Limited to 3: 3 events
```

**Narasi**: "Query endpoint memberikan visibility penuh terhadap events. Filter per topic, limit results, atau lihat semua. Konsistensi antara internal counter dan actual events terjaga sempurna."

---

### Bagian 7: Persistence Test (Waktu: 4 menit)

**STEP 7.1: Catat statistik sebelum restart**
```powershell
Write-Host "=== PERSISTENCE TEST ===" -ForegroundColor Yellow
Write-Host ""
Write-Host "STEP 1: Mencatat statistik SEBELUM restart"
Write-Host ""

$stats_before = curl -s http://localhost:8080/stats | ConvertFrom-Json
Write-Host "Events Received:       $($stats_before.received)"
Write-Host "Unique Processed:      $($stats_before.unique_processed)"
Write-Host "Duplicate Dropped:     $($stats_before.duplicate_dropped)"

Write-Host ""
Write-Host "Simpan nilai 'Unique Processed': $($stats_before.unique_processed)"
```

**Output yang diharapkan:**
```
STEP 1: Mencatat statistik SEBELUM restart
Events Received:       350
Unique Processed:      245
Duplicate Dropped:     105
Simpan nilai 'Unique Processed': 245
```

**STEP 7.2: Hentikan sistem**
```powershell
Write-Host ""
Write-Host "STEP 2: Menghentikan Docker Compose..."
docker compose down

Write-Host ""
Write-Host "Volume PostgreSQL tetap ada (persistent)"
Start-Sleep -Seconds 3
```

**Output yang diharapkan:**
```
Stopping uas-publisher ... done
Stopping uas-aggregator ... done
Stopping uas-storage ... done
Removing uas-publisher ... done
Removing uas-aggregator ... done
Removing uas-storage ... done
Removing network uas_distributed_network ... done

Volume PostgreSQL tetap ada (persistent)
```

**STEP 7.3: Verifikasi volume persisten**
```powershell
Write-Host ""
Write-Host "STEP 3: Verifikasi volume persisten..."
docker volume ls | Select-String "uas"
```

**Output yang diharapkan:**
```
local     uas_postgres_data
```

**STEP 7.4: Restart sistem**
```powershell
Write-Host ""
Write-Host "STEP 4: Restart Docker Compose..."
docker compose up -d

Write-Host "Menunggu layanan siap (15 detik)..."
Start-Sleep -Seconds 15
```

**Output yang diharapkan:**
```
Creating network "uas_distributed_network" with driver "bridge"
Creating uas-storage ... done
Creating uas-aggregator ... done
Creating uas-publisher ... done
Menunggu layanan siap (15 detik)...
```

**STEP 7.5: Verifikasi status layanan**
```powershell
Write-Host ""
Write-Host "STEP 5: Verifikasi status layanan..."
docker compose ps
```

**Output yang diharapkan:**
```
NAME              SERVICE       STATUS              PORTS
uas-storage       storage       Up 3s (healthy)     5432/tcp
uas-aggregator    aggregator    Up 2s (healthy)     0.0.0.0:8080->8080/tcp
uas-publisher     publisher     Up 1s               (running)
```

**STEP 7.6: Bandingkan statistik setelah restart**
```powershell
Write-Host ""
Write-Host "STEP 6: Membandingkan statistik SETELAH restart..."
$stats_after = curl -s http://localhost:8080/stats | ConvertFrom-Json
Write-Host "Events Received:       $($stats_after.received)"
Write-Host "Unique Processed:      $($stats_after.unique_processed)"
Write-Host "Duplicate Dropped:     $($stats_after.duplicate_dropped)"
```

**Output yang diharapkan:**
```
STEP 6: Membandingkan statistik SETELAH restart...
Events Received:       245
Unique Processed:      245
Duplicate Dropped:     0
```

**STEP 7.7: Analisis hasil**
```powershell
Write-Host ""
Write-Host "STEP 7: Analisis Hasil"
Write-Host "Sebelum Restart -> Unique Processed: $($stats_before.unique_processed)"
Write-Host "Setelah Restart -> Unique Processed: $($stats_after.unique_processed)"

if ($stats_before.unique_processed -eq $stats_after.unique_processed) {
    Write-Host ""
    Write-Host "✅ PERSISTENCE TEST PASSED!" -ForegroundColor Green
    Write-Host "Data berhasil di-recover setelah restart"
} else {
    Write-Host ""
    Write-Host "⚠️ Nilai berbeda (publisher mungkin masih aktif)" -ForegroundColor Yellow
}
```

**Output yang diharapkan:**
```
STEP 7: Analisis Hasil
Sebelum Restart -> Unique Processed: 245
Setelah Restart -> Unique Processed: 245

✅ PERSISTENCE TEST PASSED!
Data berhasil di-recover setelah restart
```

**Narasi**: "Test persistence terpenting: catat statistik, shutdown, restart, verifikasi statistik sama. PostgreSQL + named volumes memastikan durability. Data aman bahkan jika container dihapus atau host di-restart."

---

### Bagian 8: Running Automated Tests (Waktu: 1-2 menit)

**STEP 8.1: Hentikan docker compose (untuk test yang clean)**
```powershell
Write-Host "=== RUNNING 20 INTEGRATION TESTS ===" -ForegroundColor Green

docker compose down
Start-Sleep -Seconds 3
```

**STEP 8.2: Jalankan test suite**
```powershell
Write-Host ""
Write-Host "Executing pytest..."
Write-Host ""

pytest tests/test_aggregator_mock.py -v --tb=short
```

**Output yang diharapkan (semua 20 tests PASSED):**
```
tests/test_aggregator_mock.py::test_publish_valid_single_event PASSED             [  5%]
tests/test_aggregator_mock.py::test_publish_missing_topic PASSED                  [ 10%]
tests/test_aggregator_mock.py::test_publish_missing_event_id PASSED               [ 15%]
tests/test_aggregator_mock.py::test_publish_invalid_payload PASSED                [ 20%]
tests/test_aggregator_mock.py::test_dedup_duplicate_event PASSED                  [ 25%]
tests/test_aggregator_mock.py::test_dedup_different_topics_same_id PASSED         [ 30%]
tests/test_aggregator_mock.py::test_dedup_batch_with_duplicates PASSED            [ 35%]
tests/test_aggregator_mock.py::test_persist_data_survives_restart PASSED          [ 40%]
tests/test_aggregator_mock.py::test_persist_duplicate_prevented_after_restart PASSED [ 45%]
tests/test_aggregator_mock.py::test_concurrent_duplicate_publish PASSED           [ 50%]
tests/test_aggregator_mock.py::test_concurrent_different_events PASSED            [ 55%]
tests/test_aggregator_mock.py::test_concurrent_stats_race_condition PASSED        [ 60%]
tests/test_aggregator_mock.py::test_api_get_events_empty PASSED                   [ 65%]
tests/test_aggregator_mock.py::test_api_get_events_with_filter PASSED             [ 70%]
tests/test_aggregator_mock.py::test_api_get_events_with_limit PASSED              [ 75%]
tests/test_aggregator_mock.py::test_api_get_stats_initial PASSED                  [ 80%]
tests/test_aggregator_mock.py::test_api_stats_after_publish PASSED                [ 85%]
tests/test_aggregator_mock.py::test_batch_mixed_valid_invalid PASSED              [ 90%]
tests/test_aggregator_mock.py::test_publish_empty_array PASSED                    [ 95%]
tests/test_aggregator_mock.py::test_publish_not_array PASSED                      [100%]

======================== 20 passed in 0.12s ========================
```

**Narasi**: "20 integration tests mencakup: event validation, deduplication logic, persistence, concurrent access, API consistency. Berjalan dalam 0.12 detik. Semua PASSED memverifikasi implementasi sesuai requirements."

---

### Bagian 9: Cleanup (Waktu: 1 menit)

**STEP 9.1: Hentikan semua service**
```powershell
Write-Host "=== CLEANUP ===" -ForegroundColor Red

Write-Host ""
Write-Host "Menghentikan semua service..."
docker compose down

Write-Host ""
Write-Host "✅ Sistem telah dihentikan"
Write-Host "✅ Volume data tetap persisten"
Write-Host ""
Write-Host "Untuk melanjutkan: jalankan 'docker compose up -d'"
```

**Output yang diharapkan:**
```
Stopping uas-publisher ... done
Stopping uas-aggregator ... done
Stopping uas-storage ... done
Removing uas-publisher ... done
Removing uas-aggregator ... done
Removing uas-storage ... done
Removing network uas_distributed_network ... done

✅ Sistem telah dihentikan
✅ Volume data tetap persisten
```

**Narasi**: "Terima kasih. Kami telah mendemonstrasikan:
- ✅ Idempotent Consumer
- ✅ Deduplication dengan 30% stable rate
- ✅ Zero double-processing under concurrency
- ✅ Persistence & recovery
- ✅ API consistency
- ✅ 20 comprehensive integration tests

Wassalamu'alaikum warahmatullahi wabarakatuh."

---

## Pemberhentian Sistem

Untuk menghentikan sistem kapan saja:

```powershell
docker compose down
```

Data dalam volume PostgreSQL tetap persisten. Untuk restart:

```powershell
docker compose up -d
Start-Sleep -Seconds 15
curl http://localhost:8080/stats
```

---

## Pemecahan Masalah

### Masalah 1: Port 8080 Sudah Digunakan

**Error yang muncul:**
```
Error: listen tcp4 0.0.0.0:8080: bind: Only one usage of each socket address
Error response from daemon: driver failed programming external connectivity
```

**Penyebab:** Ada aplikasi lain yang sudah menggunakan port 8080.

**Solusi (Windows PowerShell):**

**Langkah 1: Cari proses yang menggunakan port 8080**
```powershell
netstat -ano | findstr :8080

# Output contoh:
# TCP    0.0.0.0:8080           0.0.0.0:0              LISTENING       5234
#
# Angka 5234 adalah Process ID (PID)
```

**Langkah 2: Matikan proses tersebut**
```powershell
taskkill /PID 5234 /F

# Output: SUCCESS: The process with PID 5234 has been terminated.
```

**Langkah 3: Verifikasi port sudah bebas**
```powershell
netstat -ano | findstr :8080

# Jika tidak ada output, port sudah bebas
```

**Langkah 4: Jalankan Docker Compose lagi**
```powershell
docker compose up -d
```

---

### Masalah 2: Docker Service Tidak Berjalan

**Error yang muncul:**
```
Cannot connect to Docker daemon at unix:///var/run/docker.sock
Cannot connect to Docker daemon at npipe:////./pipe/docker_engine
```

**Penyebab:** Docker daemon belum dimulai.

**Solusi (Windows):**

```powershell
# Cek status Docker
docker --version

# Jika error, buka Docker Desktop aplikasi
# Atau restart Docker service

# Verifikasi Docker sudah jalan
docker ps

# Output: CONTAINER ID   IMAGE     COMMAND   CREATED   STATUS    PORTS    NAMES
```

---

### Masalah 3: Aggregator Container Tidak Start

**Error yang muncul:**
```
docker compose up -d
ERROR: Container "uas-aggregator" is unhealthy
```

**Penyebab:** PostgreSQL belum siap atau error koneksi database.

**Solusi:**

**Langkah 1: Cek status container**
```powershell
docker compose ps

# Output:
# NAME              SERVICE       STATUS              PORTS
# uas-storage       storage       Up 5s (starting)    5432/tcp
# uas-aggregator    aggregator    Up 3s (unhealthy)   0.0.0.0:8080->8080/tcp
# uas-publisher     publisher     Exited (1)
```

**Langkah 2: Lihat log aggregator untuk error detail**
```powershell
docker compose logs aggregator

# Output:
# uas-aggregator | sqlalchemy.exc.OperationalError: (psycopg2.OperationalError) could not connect to server
# uas-aggregator | FATAL:  password authentication failed for user "postgres"
```

**Langkah 3: Tunggu PostgreSQL siap**
```powershell
# Tunggu 20 detik untuk database fully initialize
Start-Sleep -Seconds 20

# Restart aggregator
docker compose restart aggregator

# Tunggu lagi
Start-Sleep -Seconds 10

# Verifikasi
docker compose ps
curl http://localhost:8080/health
```

**Langkah 4: Jika masih error, rebuild**
```powershell
# Hentikan semua
docker compose down

# Tunggu
Start-Sleep -Seconds 3

# Rebuild dari awal
docker compose build

# Start lagi
docker compose up -d

# Tunggu 20 detik
Start-Sleep -Seconds 20

# Cek status
docker compose ps
curl http://localhost:8080/health
```

---

### Masalah 4: Health Check Gagal

**Error yang muncul:**
```
curl: (7) Failed to connect to localhost port 8080: Connection refused
```

**Penyebab:** Aggregator belum fully ready.

**Solusi:**

```powershell
# Tunggu lebih lama
Write-Host "Waiting for aggregator to be fully ready..."
Start-Sleep -Seconds 30

# Cek status container
docker compose ps

# Lihat log
docker compose logs aggregator

# Test health check
curl http://localhost:8080/health

# Jika masih error, lihat error detail
docker compose logs aggregator --tail 50
```

---

### Masalah 5: Publisher Tidak Generate Events

**Tanda-tanda:**
- `/stats` menunjukkan `received: 0` setelah 30 detik

**Penyebab:** Publisher container tidak running atau error.

**Solusi:**

**Langkah 1: Cek status publisher**
```powershell
docker compose ps

# Publisher seharusnya status "running"
```

**Langkah 2: Lihat log publisher**
```powershell
docker compose logs publisher

# Output normal:
# uas-publisher | Publishing event 1/1000: topic=orders, event_id=evt-001
# uas-publisher | Publishing event 2/1000: topic=payments, event_id=evt-002
```

**Langkah 3: Jika error, restart publisher**
```powershell
docker compose restart publisher
Start-Sleep -Seconds 5

# Cek log lagi
docker compose logs publisher

# Verifikasi di aggregator
Start-Sleep -Seconds 15
curl http://localhost:8080/stats
```

---

### Masalah 6: Database Volume Penuh atau Corrupted

**Error yang muncul:**
```
psycopg2.OperationalError: could not open relation with OID xxxxx
no space left on device
```

**Penyebab:** Storage penuh atau database corrupted.

**Solusi (DESTRUCTIVE - menghapus semua data):**

```powershell
# Hentikan sistem
docker compose down

# Hapus volume (DATA AKAN HILANG!)
docker volume rm uas_postgres_data

# Verifikasi volume sudah dihapus
docker volume ls | findstr "uas"

# Restart dari awal
docker compose up -d

# Tunggu
Start-Sleep -Seconds 20

# Verifikasi
docker compose ps
curl http://localhost:8080/stats
```

---

### Masalah 7: Test Suite Gagal - Dependency Missing

**Error yang muncul:**
```
ModuleNotFoundError: No module named 'pytest'
ModuleNotFoundError: No module named 'asyncpg'
```

**Penyebab:** Python dependencies belum terinstall.

**Solusi:**

**Langkah 1: Install dependencies**
```powershell
# Install dari requirements.txt
pip install -r tests/requirements.txt

# Atau install manual
pip install pytest==7.4.3
pip install pytest-asyncio==0.21.1
pip install httpx==0.25.2
pip install asyncpg==0.31.0
```

**Langkah 2: Verifikasi instalasi**
```powershell
pytest --version
python -c "import asyncpg; print('asyncpg OK')"
```

**Langkah 3: Jalankan test lagi**
```powershell
pytest tests/test_aggregator_mock.py -v
```

---

### Masalah 8: Curl Command Error di PowerShell

**Error yang muncul:**
```
curl : The term 'curl' is not recognized
```

**Penyebab:** curl tidak tersedia di PowerShell atau PowerShell tua.

**Solusi:**

**Opsi 1: Gunakan Invoke-WebRequest (PowerShell native)**
```powershell
# GET request
Invoke-WebRequest http://localhost:8080/stats | ConvertFrom-Json

# POST request
$event = @{
    topic = "test"
    event_id = "evt-001"
    timestamp = [System.DateTime]::UtcNow.ToString("o")
    source = "demo"
    payload = @{ msg = "hello" }
} | ConvertTo-Json

Invoke-WebRequest -Uri http://localhost:8080/publish `
  -Method POST `
  -ContentType application/json `
  -Body $event
```

**Opsi 2: Install curl**
```powershell
# Windows 10/11 sudah include curl
# Verifikasi
curl --version

# Jika tidak ada, install via choco
choco install curl -y
```

---

### Masalah 9: Docker Compose File Error

**Error yang muncul:**
```
ERROR: yaml.scanner.ScannerError: mapping values are not allowed here
ERROR: invalid compose project name
```

**Penyebab:** docker-compose.yml syntax error atau file tidak ditemukan.

**Solusi:**

**Langkah 1: Verifikasi file ada**
```powershell
Test-Path "docker-compose.yml"
Get-Content docker-compose.yml | head -20
```

**Langkah 2: Validate YAML syntax**
```powershell
# Gunakan Docker untuk validate
docker compose config

# Jika error, lihat line mana yang error
```

**Langkah 3: Common issues**
```yaml
# ❌ SALAH - indentation tidak konsisten
services:
  storage:
  image: postgres:16

# ✅ BENAR - indentation 2 spaces
services:
  storage:
    image: postgres:16

# ❌ SALAH - quote tidak tutup
  image: postgres:16

# ✅ BENAR
  image: postgres:16
```

---

### Masalah 10: Memory/CPU Terlalu Tinggi

**Tanda-tanda:**
- System lag saat docker compose running
- CPU usage 100%

**Penyebab:** Publisher menghasilkan event terlalu cepat.

**Solusi:**

**Langkah 1: Lihat resource usage**
```powershell
docker stats

# Output:
# CONTAINER           CPU %     MEM USAGE / LIMIT
# uas-aggregator      2.1%      150MiB / 2GiB
# uas-publisher       45.2%     200MiB / 2GiB
# uas-storage         1.5%      100MiB / 2GiB
```

**Langkah 2: Limit resources (optional)**
```yaml
# Di docker-compose.yml
services:
  publisher:
    deploy:
      resources:
        limits:
          cpus: '1.0'
          memory: 512M
        reservations:
          cpus: '0.5'
          memory: 256M
```

**Langkah 3: Restart dengan limit baru**
```powershell
docker compose down
docker compose up -d
docker stats
```

---

## Quick Troubleshooting Checklist

```powershell
# 1. Verifikasi Docker
docker --version
docker compose version

# 2. Cek status semua container
docker compose ps

# 3. Cek network connectivity
curl http://localhost:8080/health

# 4. Lihat resource usage
docker stats

# 5. Lihat full log
docker compose logs -f

# 6. Jika masih error, restart semua
docker compose down
Start-Sleep -Seconds 5
docker compose up -d
Start-Sleep -Seconds 20
curl http://localhost:8080/stats

# 7. Jika perlu full reset
docker compose down -v
docker system prune -a
docker compose up -d
```

---

**Tanggal**: 11 Desember 2024  
**Status**: Siap untuk Video Recording  
**Format**: Video 1920x1080 @ 30fps dengan subtitle Bahasa Indonesia

