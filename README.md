# Tugas 3 Sistem Paralel dan Terdistribusi: Sinkronisasi Lanjut

Selamat datang di repositori implementasi **Sistem Sinkronisasi Terdistribusi tingkat lanjut**. Proyek ini dikembangkan menggunakan **Python (Asyncio)**, **gRPC**, dan arsitektur *microservices* terisolasi berbasis **Docker**, dirancang khusus untuk memenuhi kriteria dan bonus Tugas 3 Sistem Terdistribusi.

Proyek ini mendemonstrasikan penyelesaian berbagai tantangan kompleks dalam sistem multi-node, menangani konsistensi data, ketersediaan tinggi (*High Availability*), replikasi antar-wilayah, dan algoritma *Machine Learning* untuk *Load Balancing*.

---

## 🌟 Fitur Utama (Core Requirements)

### 1. Distributed Lock Manager (Raft Consensus)
Mencegah terjadinya tabrakan antar node saat mengakses *shared resource* (sumber daya bersama).
* Menggunakan algoritma konsensus **Raft** untuk menjaga *state* kunci (lock) secara terpusat-virtual namun tersebar. Tahan terhadap *node failure*.
* Mendukung tipe lock: **Exclusive** (satu pemegang) dan **Shared** (banyak pembaca).
* **Deteksi Deadlock Aktif:** Mendeteksi *circular dependency* menggunakan DFS Wait-For graph secara berkala. Jika *deadlock* terjadi, transaksi peminta paling *muda (youngest)* akan digagalkan (*abort*).

### 2. Distributed Queue System (Kafka + Consistent Hashing)
Sistem antrean pekerjaan asinkron yang kokoh *(*robust*)* tanpa titik kegagalan tunggal (*single point of failure*).
* Menggunakan **Kafka** sebagai penyimpanan presisten.
* Permintaan dari pengguna didistribusikan ke node memakai model ring **Consistent Hashing** dengan *virtual nodes* untuk memastikan pemerataan partisi.
* Jaminan **At-Least-Once Delivery**: Node menangani *auto-retry*, dan jika gagal lebih dari 3 kali, pesan akan dialihkan ke antrean khusus keruntuhan **Dead-Letter Queue (DLQ)**.

### 3. Cache Coherence Protocol (MOESI)
Menjaga konsistensi data (*memory/cache*) antar node di seluruh jaringan komputer tanpa melakukan beban akses basis data yang membludak.
* Menerapkan 5 *state* MOESI untuk setiap kunci (*key*): **Modified, Owned, Exclusive, Shared, Invalid**.
* Dilengkapi pertukaran via **Intervention Protocol** (Transisi dari `Modified` menjadi `Owned` tanpa harus ke server pusat), memungkinkan node untuk saling menyuapi nilai data kotor (*dirty values*).

---

## 🚀 Fitur Tambahan Terintegrasi (Bonus)

Sebagai penambahan yang komprehensif, repositori ini mengimplementasikan keseluruhan poin Bonus:

#### A. PBFT Consensus (Practical Byzantine Fault Tolerance)
Selain Raft yang aman terhadap *Crash*, PBFT ini tahan terhadap ancaman **Byzantine fault** (Node diretas, memberikan respon palsu / menyesatkan). Konsensus dicapai melalui protokol komunikasi 3-fase: `Pre-Prepare`, `Prepare`, dan `Commit`, di mana suatu keputusan mutlak bila disetujui kuorum `2f+1`.

#### B. Skalabilitas Geo-Distributed
Sistem mampu beroperasi lintas benua dengan memperhitungkan jarak latensi nyata (US, Eropa, Asia). Memanfaatkan **Eventual Consistency** berbasis **Vector Clocks**. Jika dua region melakukan edit bersamaan (concurrent editing), **Last-Writer-Wins (LWW)** digunakan untuk rekonsiliasi bebas sengketa.

#### C. Machine Learning Load Balancer (Adaptive)
Dilengkapi model **RandomForestClassifier** berbasis Python (*scikit-learn*), *Load Balancer* akan mengalihkan lintasan trafik (Routing) dengan membaca metrik internal tiap Node secara *Real-Time* (antrean CPU, latensi RPC). Disertai pemicu **IsolationForest** untuk mendeteksi *anomali* aneh pada metrik Node dan mencetak peringatan. Terdapat skema *Online Retraining* berkala dari data empiris baru.

#### D. Security, Audit Logging, & Failure Detector
Sistem tidak dapat dimasuki dan dieksekusi secara sembarang:
* Manajemen komunikasi gRPC yang dienkripsi secara penuh (**mTLS** automatically generated certificates).
* Model Manajemen Akses berbasis Peran Pengguna (**RBAC**).
* Log aktivitas asinkron (pengubahan kunci, cache, admin) ke **Kafka Audit Log**.
* Menggunakan matematika probabilitas **Phi-Accrual Failure Detector** adaptif untuk mengidentifikasi perangkat yang sedang mati (Node Disconnect / Network Partitions) berdasarkan deviasi standar denyut *heartbeat*.

---

## 🛠 Teknologi Utama

* **Bahasa**: Python 3.11 `asyncio`
* **Persistensi / Log**: MySQL 8.0, Redis, Kafka, Zookeeper
* **Komunikasi Inter-Node**: gRPC (Google Remote Procedure Calls) dengan Protocol Buffers (`.proto`)
* **Arsitektur Gateway**: FastAPI
* **Data Science (Load Balancer)**: Scikit-learn, Numpy, Joblib
* **Pemantauan (Telemetry)**: Prometheus (Metrics Data Scrapper) & Grafana (Dashboard)
* **Wadah (Container)**: Docker & Docker-Compose V2

---

## ⚙️ Petunjuk Menjalankan Instansi (Zero-Config)

Untuk mendemonstrasikan sistem ini tidak membutuhkan setup lokal (kecuali Docker dan Git). Semua infrastruktur dari skema Database, Broker Partisi, hingga sertifikat SSL dibuat otomatis *pada waktu runtime* (*zero-config*).

1. **Clone Repositori**
```bash
git clone https://github.com/RayhanMarcello/sistem-terdistribusi-tugas3-sinkronisasi.git
cd sistem-terdistribusi-tugas3-sinkronisasi
```

2. **Inisialisasi Variabel Lingkungan Lokal**
```bash
cp .env.example .env
```

3. **Jalankan Ensemble Terdistribusi**
```bash
# Menyerahkan orkestrasi penuh ke Docker (Pastikan port 8000, 3306, 9090, 3000 tidak terpakai)
docker-compose up -d --build
```
*Proses ini memakan waktu kurang lebih 2-3 menit pada start awal untuk menunggu Python Wheel dan gRPC menyelesakan resolusi kompilasi.*

4. **Amati Kondisi Lingkungan via Grafana & API**
   - **Swagger Gateway API**: `http://localhost:8000/docs`
   - **Grafana Live Dashboard**: `http://localhost:3000` *(Login secara otomatis dengan `admin`/`admin`)*
   - Cek Status Node Cluster & Latensi (Geo): `docker logs docker-geo-node-eu-1 -f`

---

## 📁 Struktur Direktori Proyek

```text
.
├── benchmarks/         # Skrip pengujian dan skenario beban trafik
├── docker/             # Dockerfiles (Multi-stage gRPC builder) dan Entryoints Bash
├── docs/               # Spesifikasi lengkap, Laporan Metrik Performa, dan Diagram Arsitektur Mermaid!
├── monitoring/         # Provisioning Promotheus dan Grafana (Target Scrapes and Dashboards)
├── scripts/            # Script Helper (Self-Signed mTLS generator, DB Schema Init)
├── src/                # Root Source Code Utama Python Terdistribusi
│   ├── api/            # API Gateway dan Manajemen Transmisi ke Node (FastAPI)
│   ├── communication/  # Algoritma Phi-Accrual dan gRPC Servicers (Network Interface)
│   ├── consensus/      # Logika Protokol Raft & PBFT
│   ├── geo/            # Logika Multi-Region & Vector Clocks
│   ├── ml/             # Random Forest Adapter dan Online Retrainer 
│   ├── nodes/          # Kelas Basis Manager (Lock Manager, Cache MOESI, Queue Hash Ring)
│   ├── proto/          # Spesifikasi Bahasa antarmuka (File .proto)
│   ├── security/       # Sistem Roles & Audit Logger async
│   └── utils/          # Pengumpulan Metrik Prometheus Custom dan Loader Env Config
└── tests/              # Validasi Testing Terintegrasi
```

Dokumentasi detail mengenai *State Machine MOESI*, visualisasi komunikasi *PBFT*, dan Diagram *Sequence* Lock Manager ada di File: `docs/architecture.md`. Laporan Evaluasi Performa Simulasi terdapat di `docs/performance_report.md`.

---
*Kode ditulis untuk pemenuhan Tugas Mata Kuliah Sistem Paralel dan Terdistribusi (Kelas A).*
