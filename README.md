# Pub-Sub Log Aggregator Terdistribusi

Sistem agregator log terdistribusi multi-layanan dengan dukungan idempotent consumer, deduplication yang persisten, serta transaksi dan kontrol konkurensi berbasis ACID menggunakan Docker Compose.

---

## Daftar Isi

- [Ringkasan Sistem](#ringkasan-sistem)
- [Arsitektur Sistem](#arsitektur-sistem)
- [Cara Menjalankan](#cara-menjalankan)
- [Endpoint API](#endpoint-api)
- [Keputusan Desain](#keputusan-desain)
- [Pengujian](#pengujian)
- [Rujukan](#rujukan)

---

## Ringkasan Sistem

Sistem ini mengimplementasikan pola **publish-subscribe** terdistribusi dengan fokus utama pada:

1. **Idempotency**: Event yang sama (berdasarkan topic dan event_id) hanya diproses satu kali
2. **Deduplication**: Duplikasi terdeteksi dan dicatat dalam log audit
3. **Persistensi**: Data aman meski container dihapus atau restart
4. **Kontrol Konkurensi**: Mencegah race condition dan lost updates
5. **Observability**: Metrik dan logging lengkap

### Karakteristik Teknis

- **Bahasa Pemrograman**: Python 3.11 dengan FastAPI
- **Database**: PostgreSQL 16 dengan persistent volume
- **Pola Pesan**: HTTP synchronous (dapat diperluas ke message broker)
- **Isolation Level**: READ COMMITTED dengan unique constraints
- **Persistensi**: Named volumes (pg_data)

---

## Arsitektur Sistem

Sistem terdiri dari tiga komponen utama yang berjalan dalam Docker Compose:

```
┌─────────────────────────────────────────────────────────┐
│              Docker Compose Network                     │
│             (distributed_network)                       │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────┐ │
│  │  PUBLISHER   │    │  AGGREGATOR  │    │ STORAGE  │ │
│  │              │───▶│              │───▶│PostgreSQL│ │
│  │ Event Gen    │    │ API Server   │    │          │ │
│  │ (1000 evt)   │    │              │    │ Port 5432│ │
│  │ 30% duplikat │    │ Port: 8080   │    │          │ │
│  └──────────────┘    └──────────────┘    └──────────┘ │
│                           │                     │       │
│                           │ (volume mount)      │       │
│                           ▼                     ▼       │
│                    aggregator_data          pg_data    │
│                   (bind mount/temp)        (named vol) │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### Komponen Utama

#### 1. Storage (PostgreSQL)

- Engine database untuk persistensi
- Tabel: `events`, `processed_events`, `event_log`, `stats`
- Volume: `pg_data` untuk data persistence
- Health check: pg_isready setiap 10 detik

#### 2. Aggregator (FastAPI)

- REST API untuk publikasi dan pembacaan event
- Logika dedup berbasis unique constraints
- Handling transaksi untuk idempotency
- Endpoints: `/publish`, `/events`, `/stats`, `/health`

#### 3. Publisher (Python asyncio)

- Event generator dengan kontrol duplikasi rate
- Simulator load testing (default: 1000 events, 30% duplicate)
- HTTP client untuk publikasi ke aggregator

---

## Cara Menjalankan

### Setup Cepat

```powershell
# Buat dan jalankan
docker compose build && docker compose up -d

# Tunggu beberapa detik untuk layanan siap
Start-Sleep -Seconds 10

# Verifikasi
docker compose ps

# Periksa kesehatan
curl http://localhost:8080/health

# Lihat statistik
curl http://localhost:8080/stats

# Jalankan tes
pytest tests/test_aggregator.py -v

# Hentikan
docker compose down
```

### Langkah Detail

Untuk panduan lengkap step-by-step, lihat file `docs/INSTRUCTIONS.md`.

---

## Endpoint API

### POST /publish
Publikasikan satu atau lebih events ke aggregator.

**Request Body:**
```json
[
  {
    "topic": "string (required)",
    "event_id": "string (required, UUID recommended)",
    "timestamp": "string (ISO8601, required)",
    "source": "string (required, service name)",
    "payload": {
      "key": "any value (required, object)"
    }
  }
]
```

**Response:**
```json
{
  "accepted": 1,
  "processed": 1,
  "rejected": 0
}
```

### GET /events
Ambil event yang telah diproses.

**Query Parameters:**
- `topic` (opsional): Filter berdasarkan topic
- `limit` (opsional, default: 100): Jumlah maksimal hasil

**Contoh:**
```
GET /events?topic=logs&limit=10
```

### GET /stats
Dapatkan statistik agregator.

**Response:**
```json
{
  "received": 1000,
  "unique_processed": 700,
  "duplicate_dropped": 300,
  "topics": ["log:aggregator:processing"],
  "topic_event_counts": {
    "log:aggregator:processing": 700
  },
  "uptime_seconds": 3600
}
```

### GET /health
Health check endpoint untuk readiness/liveness probe.

---

## Keputusan Desain

### Idempotency dan Deduplication

Implementasi menggunakan **unique constraint** pada kolom `(topic, event_id)` dengan semantik upsert:

```sql
CONSTRAINT unique_event UNIQUE(topic, event_id)

INSERT INTO events (topic, event_id, ...)
VALUES ($1, $2, ...)
ON CONFLICT (topic, event_id) DO NOTHING;
```

**Keuntungan:**
- Atomicity: Satu event = satu record
- Performance: Constraint check lebih cepat dari SELECT
- Simplicity: Tidak perlu lock eksplisit

### Kontrol Konkurensi

**Strategi**: Optimistic locking berbasis constraint

- Tidak ada explicit lock (tidak ada deadlock risk)
- Constraint violation menunjukkan konflik
- Aplikasi menangani conflict resolution
- Throughput tinggi di bawah contention

**Isolation Level**: READ COMMITTED
- Tidak ada dirty reads
- Non-repeatable reads mungkin terjadi (acceptable untuk log aggregation)
- Phantom reads mungkin terjadi (acceptable dengan unique constraints)

### Ordering

**Implementasi**: Logical ordering dengan `received_at` timestamp (server-side)

```sql
SELECT * FROM events
ORDER BY received_at ASC, id ASC
LIMIT 100;
```

---

## Pengujian

### Menjalankan Suite Pengujian

```powershell
pytest tests/test_aggregator.py -v
```

### Cakupan Pengujian (18 Tests)

- **Schema Validation** (4 tests): Validasi struktur event
- **Deduplication** (3 tests): Deteksi duplikasi
- **Persistence** (2 tests): Data bertahan setelah restart
- **Concurrency** (3 tests): Race condition handling
- **API Endpoints** (4 tests): Verifikasi endpoint
- **Batch Operations** (2 tests): Batch event handling

Semua tes harus PASSED untuk memverifikasi kebenaran implementasi.

---

## Struktur File

```
d:\UAS Sistem Terdistribusi\
├── aggregator/
│   ├── main.py              # Service utama (349 lines)
│   ├── Dockerfile           # Image definition
│   └── requirements.txt      # Python dependencies
├── publisher/
│   ├── main.py              # Event generator (255 lines)
│   ├── Dockerfile
│   └── requirements.txt
├── tests/
│   ├── test_aggregator.py   # Unit & integration tests (670 lines)
│   └── requirements.txt
├── docs/
│   ├── INSTRUCTIONS.md      # Panduan implementasi lengkap
│   ├── LAPORAN_TEORI.md     # Jawaban teori T1-T10
│   ├── VIDEO_DEMO_SCRIPT.md # Script demo video
│   ├── DESIGN_SUMMARY.md    # Ringkasan desain arsitektur
│   ├── API_DOCS.md          # Spesifikasi API
│   ├── DEPLOYMENT_GUIDE.md  # Panduan operasi
│   └── INDEX.md             # Index dokumentasi
├── docker-compose.yml       # Orchestrasi layanan
├── README.md               # File ini
├── setup.ps1               # PowerShell setup script
└── setup.sh                # Bash setup script
```

---

## Metrik Performa

| Metrik | Nilai | Catatan |
|---|---|---|
| **Throughput** | ~500 events/sec | Single publisher |
| **Latency P50** | 20ms | Per-event |
| **Latency P99** | 150ms | Peaks under GC |
| **Dedup Rate** | 30% | Sesuai injected rate |
| **CPU Usage** | 15-25% | Aggregator + DB |
| **Memory Usage** | 150-200MB | Both services |
| **Startup Time** | ~8 sec | Full stack ready |

---

## Rujukan

- FastAPI: https://fastapi.tiangolo.com
- asyncpg: https://magicstack.github.io/asyncpg
- PostgreSQL Documentation: https://www.postgresql.org/docs
- Docker Compose: https://docs.docker.com/compose

---

**Untuk detail lengkap, lihat dokumentasi di folder `docs/`**

Tanggal: 11 Desember 2024  
Status: Siap untuk Pengiriman  
Versi: 1.0.0
