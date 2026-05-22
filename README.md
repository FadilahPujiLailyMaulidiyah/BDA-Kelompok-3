# 🏢 Prediksi Retensi Karyawan — Modul Human Capital ERP
> Mata Kuliah: Big Data dan Analitik (CSD60707) — Kelompok 3

## 👥 Anggota Tim
| Nama | NIM | Peran |
|------|-----|-------|
| Raya Abiathar | 235150400111001 | Data Engineer (Ingestion & Medallion Pipeline) |
| Annisa Kayla Jasmine | 235150407111004 | Data Analyst (Spark Processing) |
| Fadilah Puji Laily Maulidiyah | 245150401111008 | ML Engineer (Modeling & Analytics) |
| Dinda Azqa Nur Ramadhani | 235150407111041 | Project Manager & Documentation |

---

## 📌 Deskripsi Proyek
Proyek ini membangun pipeline analitik Big Data end-to-end untuk memprediksi retensi karyawan menggunakan dataset IBM HR Analytics Employee Attrition & Performance (1.470 records, 35 atribut). Pipeline dibangun menggunakan **PySpark + MinIO** dengan pendekatan **Medallion Architecture (Bronze → Silver → Gold)**, dijalankan di lingkungan Jupyter Notebook.

### Tujuan
- **Klasifikasi**: Memprediksi apakah seorang karyawan akan keluar dari perusahaan
- **Segmentasi**: Mengidentifikasi kelompok karyawan berisiko tinggi menggunakan K-Means
- **Rekomendasi**: Menghasilkan insight berbasis data untuk program retensi HR

---

## 🏗️ Arsitektur Pipeline

```
CSV Lokal (/home/jovyan/work/HR-Employee-Attrition.csv)
      │
      ▼  spark.read.csv()
┌──────────────────────────────────────────────────────┐
│               MinIO — Bucket: datalake               │
│                                                      │
│  🥉 Bronze  raw/hr-attrition/                        │
│     └── Parquet (raw, zero transform)                │
│                                                      │
│  🥈 Silver  processed/silver/hr-attrition/           │
│     └── Parquet, partisi per Department              │
│         (cleaning, casting, encoding)                │
│                                                      │
│  🥇 Gold    processed/gold/hr-attrition/             │
│     ├── ml_features/       ← Parquet (Vector)        │
│     └── ml_features_csv/   ← CSV (untuk analitik)   │
└──────────────────────────────────────────────────────┘
      │
      ▼  spark.read.csv() dari MinIO (s3a://)
  Notebook Analitik — EDA, SMOTE, RF, LR, K-Means
```

---

## 🛠️ Tech Stack
| Komponen | Teknologi |
|----------|-----------|
| Bahasa | Python (PySpark) |
| Data Ingestion | PySpark `spark.read.csv()` |
| Data Storage | MinIO (S3-compatible object storage) |
| Data Processing | Apache Spark — PySpark DataFrame API |
| ML Features | Spark MLlib (StringIndexer, VectorAssembler, MinMaxScaler) |
| Machine Learning | Spark MLlib (RandomForestClassifier, LogisticRegression, KMeans) |
| Class Imbalance | SMOTE (`imbalanced-learn`) |
| Notebook & Visualisasi | Jupyter Notebook, Matplotlib |
| Koneksi MinIO | `boto3`, `hadoop-aws 3.3.4`, `aws-java-sdk-bundle 1.12.262` |

> **Catatan:** Tidak menggunakan database relasional (PostgreSQL/MySQL). Seluruh data disimpan dalam format file (Parquet & CSV) di MinIO sebagai Data Lake, sesuai pola Medallion Architecture.

---

## 📊 Dataset
- **Sumber**: [Employee Attrition & Retention Analytics Dataset](https://www.kaggle.com/datasets/ajinkyachintawar/employee-attrition-and-retention-analytics-dataset) (Kaggle)
- **File awal**: `HR-Employee-Attrition.csv` (lokal di Jupyter)
- **Ukuran**: 1.470 baris × 35 kolom
- **Target**: `left_company` (0 = Bertahan, 1 = Keluar)
- **Class Imbalance**: 83.9% Bertahan vs 16.1% Keluar (rasio 5.2:1)

---

## 🔄 Medallion Pipeline (`HR_Attrition_Medallion_Final.ipynb`)

### 🥉 Bronze — Raw Ingestion
- Baca CSV lokal via `spark.read.csv()` di Jupyter
- Tambah metadata kolom: `_ingested_at`, `_source_file`, `_layer`
- Simpan ke MinIO sebagai **Parquet** tanpa transformasi apapun
- Path: `s3a://datalake/raw/hr-attrition/`

### 🥈 Silver — Data Cleaning
| Step | Aksi |
|------|------|
| 1 | Fix BOM character (`\ufeff`) di nama kolom |
| 2 | Rename `Attrition` → `left_company`, encode Yes/No → 1/0 |
| 3 | Drop kolom tidak informatif (`EmployeeCount`, `StandardHours`, `Over18`, `EmployeeNumber`) |
| 4 | Filter baris kotor (Age non-numerik) |
| 5 | Cast tipe data eksplisit ke Integer |
| 6 | Drop duplikat |

- Simpan ke MinIO sebagai **Parquet**, partisi per `Department`
- Path: `s3a://datalake/processed/silver/hr-attrition/`

### 🥇 Gold — ML-Ready Features
| Kolom Output | Keterangan |
|---|---|
| `scaled_features` | Vector 6 fitur numerik dinormalisasi 0–1 (MinMaxScaler) |
| `Department_index` | Label encoded (StringIndexer) |
| `JobRole_index` | Label encoded (StringIndexer) |
| `Gender_index` | Label encoded (StringIndexer) |
| `OverTime_index` | Label encoded (StringIndexer) |
| `left_company` | Target (0=stay, 1=keluar) |

- Proses: StringIndexer → VectorAssembler → MinMaxScaler
- Simpan 2 format ke MinIO:
  - **Parquet**: `s3a://datalake/processed/gold/hr-attrition/ml_features/`
  - **CSV**: `s3a://datalake/processed/gold/hr-attrition/ml_features_csv/` ← dipakai analitik

---

## 🤖 Analitik & ML (`Analitik_Final_MinIO.ipynb`)

### Alur Notebook
1. Baca Gold CSV dari MinIO (`s3a://datalake/.../ml_features_csv/`)
2. Parse `scaled_features` string → DenseVector
3. Expand vector → kolom terpisah + assemble ulang (10 fitur total)
4. EDA (distribusi target, overtime, department)
5. Analisis class imbalance → SMOTE pada training set
6. Split 80:20 → train 2.036 baris (post-SMOTE) / test 254 baris (data asli)
7. Training Random Forest & Logistic Regression
8. Evaluasi + Confusion Matrix + Feature Importance
9. Feature Selection (threshold importance ≥ 0.05 → 10 fitur → 7 fitur)
10. K-Means Clustering (Elbow Method → k=3)

### Random Forest Classifier (Model Utama)
| Metrik | Nilai |
|--------|-------|
| AUC-ROC | **0.7802** ✅ |
| Accuracy | 0.8031 |
| Precision | 0.8253 |
| Recall | 0.8031 |
| F1-Score | 0.8123 |

### Logistic Regression (Baseline)
| Metrik | Nilai |
|--------|-------|
| AUC-ROC | 0.7615 |
| Accuracy | 0.7126 |
| Precision | 0.8117 |
| Recall | 0.7126 |
| F1-Score | 0.7442 |

> **Random Forest dipilih sebagai model utama** karena unggul di semua metrik, terutama AUC-ROC (0.7802 vs 0.7615) yang merupakan metrik paling relevan pada data imbalanced.

### Penanganan Class Imbalance — SMOTE
SMOTE diterapkan **hanya pada data training** (bukan test set) untuk mencegah data leakage. Test set tetap menggunakan distribusi data asli (254 baris).

### Feature Importance & Selection (Random Forest)
Threshold seleksi: importance ≥ 0.05 → **10 fitur → 7 fitur terpilih**

| Rank | Fitur | Score | Status |
|------|-------|-------|--------|
| 1 | OverTime_index | 0.241 | ✅ Dipilih |
| 2 | YearsAtCompany_scaled | 0.191 | ✅ Dipilih |
| 3 | JobSatisfaction_scaled | 0.160 | ✅ Dipilih |
| 4 | Age_scaled | 0.123 | ✅ Dipilih |
| 5 | WorkLifeBalance_scaled | 0.097 | ✅ Dipilih |
| 6 | MonthlyIncome_scaled | 0.085 | ✅ Dipilih |
| 7 | JobRole_index | 0.068 | ✅ Dipilih |
| 8 | Department_index | 0.019 | ❌ Dibuang |
| 9 | Gender_index | 0.011 | ❌ Dibuang |
| 10 | PerformanceRating_scaled | 0.006 | ❌ Dibuang |

---

## 🔵 Segmentasi K-Means (k=3)
| Cluster | Attrition Rate | Profil |
|---------|---------------|--------|
| Cluster 0 | 18.9% | Attrition rate tinggi — prioritas intervensi |
| Cluster 1 | 20.8% | Attrition rate tertinggi — risiko terbesar |
| Cluster 2 | 6.5% | Attrition rate rendah — kelompok paling stabil |

---

## 💡 Rekomendasi HR
1. **Kurangi beban overtime** — OverTime adalah faktor risiko terbesar (importance 0.241)
2. **Program retensi untuk tenure pendek** — YearsAtCompany prediktor terkuat kedua (0.191)
3. **Tingkatkan kepuasan kerja** — JobSatisfaction di urutan 3 (0.160)
4. **Perhatian ekstra untuk Cluster 1** — attrition rate tertinggi (20.8%)
5. **Jadikan Cluster 2 benchmark** — attrition rate terendah (6.5%), pelajari karakteristiknya
6. **Integrasikan RF ke ERP** sebagai early warning system yang dijalankan tiap kuartal
