---
title: Book Job Data — Runbook การติดตั้งและดูแลระบบ
doc_id: BJD-DOC-04
doc_type: runbook
version: 0.1
date: 2026-10-08
status: ร่างเพื่อพิจารณา
owner: DevOps / Operator (Book Dev)
related: 00-overview.md, 01-prd.md, 02-architecture.md, 03-nfr-security.md
---

# Book Job Data — Runbook การติดตั้งและดูแลระบบ

## 1. Environments

| Environment | Lake | API | สถานะ |
|---|---|---|---|
| Local dev | local path (`SOLO_EMPIRE_DATA_LAKE_URI`) | loopback `127.0.0.1:8109` | ใช้งานได้ |
| Local scheduled (Windows) | local path | — | ingest รายชั่วโมง |
| Hosted scheduled (GitHub Actions) | R2 `portfolio-demo/book-job-data-001` | — | capture + ingest ทุก 6 ชม. จาก workflow `scheduled-capture` ใน career-job-scraper (ตั้งแต่ 2026-10-21) |
| CI (GitHub Actions) | temp lake ใน test | — | ใช้งานได้ |
| Hosted (Render Free) | R2 `s3://` prefix | `0.0.0.0`, token บังคับ | parallel service verified 2026-08-09 ที่ `6942dbf`; สถานะปัจจุบัน **[ต้องยืนยัน]** |
| Compatibility mirror (parent) | — | Render service เดิมจากสำเนาใน parent | ยังเป็นตัวหลักจนกว่า cutover **[ต้องยืนยัน]** |

## 2. Build และทดสอบ

```bash
python -m pip install -e ".[lake]" pytest ruff
ruff check .
python -m pytest -q -rs
python -m book_job_data.ingest --input fixtures/job_postings.csv --dry-run
```

E2E contract gate จาก parent (ต้องมี `pyarrow`, `duckdb`):

```bash
python infra/tests/job_data_e2e_test.py
```

## 3. Ingest และ scheduled runs

```bash
# ingest ครั้งเดียว (ใช้ไฟล์ default จาก sibling book-job-scraping/data/ เมื่อไม่ระบุ --input)
python -m book_job_data.ingest --input <path>/job_postings.csv --data-lake-uri <lake path>
# พร้อม CSV projection
python -m book_job_data.ingest --input <path>/job_postings.csv --data-lake-uri <lake path> --project-csv
```

Hosted (ทางหลักตั้งแต่ 2026-10-21): workflow `.github/workflows/scheduled-capture.yml` ใน
career-job-scraper รัน capture ทุก 6 ชม. แล้วติดตั้ง book-job-data ตาม commit ที่ pin ไว้
(`BOOK_JOB_DATA_REF`) และ ingest `job_postings.csv` กับ `matched_jobs.csv` เข้า R2
`portfolio-demo/book-job-data-001` ซึ่งเป็น prefix ที่ Railway `book-job-data-api` อ่าน
ใช้ R2 key แยกที่จำกัดเฉพาะ bucket `solo-empire-portfolio` (Infisical `/job-data`) และส่ง heartbeat
`job-capture` ให้ uptime Worker (แจ้งเตือนเมื่อเงียบเกิน 13 ชม. หรือรันล้มเหลว)
เมื่อแก้ ingest แล้วต้องการให้ hosted ใช้โค้ดใหม่ ให้ bump `BOOK_JOB_DATA_REF` ใน workflow นั้น

Windows Task Scheduler (ทางเลือก local, รายชั่วโมง, idempotent):

```powershell
.\ops\windows\scheduled-ingest.ps1 install
.\ops\windows\scheduled-ingest.ps1 status
.\ops\windows\scheduled-ingest.ps1 remove
```

ต้องสร้าง `.venv` และติดตั้ง `.[lake]` ก่อน `install` ผลแต่ละ tick ต่อท้ายใน `data/ingest.log`

## 4. Backfill / replay

- Backfill: ส่ง capture CSV เก่าผ่าน `--input` (ซ้ำได้หลายไฟล์); ไฟล์ที่ bytes เดิมจะใช้ batch id เดิม
- Replay: landing เก็บ exact bytes จึงสร้าง Bronze ใหม่ได้จาก landing; ใช้ฟังก์ชัน replay ของ `solo-empire-data-lake`
  **[ต้องยืนยัน]** ว่ามีคำสั่ง CLI สำหรับ replay ทั้ง lake หรือไม่
- ไม่ควร backfill เข้า R2 production prefix โดยไม่ได้รับอนุมัติ; `scripts/r2_parity.py` เขียนได้เฉพาะ demo prefix ที่จำกัด

## 5. Deploy (Render)

1. สร้าง Web Service จาก repo นี้ด้วย Blueprint `render.yaml` (Docker, Free, health `/healthz`)
2. ตั้งค่า secret ใน Render Environment (ไม่ใส่ใน YAML)
3. Deploy แล้วรัน hosted verifier จาก solo-data-lake:

```bash
BOOK_JOB_DATA_URL=https://<service>.onrender.com \
BOOK_JOB_DATA_API_TOKEN=<from secret manager> \
PYTHONPATH=<solo-data-lake>/infra/scripts \
  <solo-data-lake>/.venv/bin/python <solo-data-lake>/infra/scripts/data_lake/hosted_job_api_e2e.py
```

ผลที่คาดหวัง: `status=verified`, `storage_model=lake_first_job_bronze_s3_duckdb`, `records_source_kind=bronze_s3_parquet`

การสร้าง service, ตั้ง environment, deploy และ repoint consumer ต้องได้รับอนุมัติจากเจ้าของก่อนทุกครั้ง
ขั้นตอน cutover เต็มอยู่ใน `(solo-empire) operations/evidence/BOOK-JOB-DATA-RENDER-CUTOVER.md`

## 6. Config และ secret (ชื่อตัวแปรเท่านั้น)

| ตัวแปร | ใช้ทำอะไร | Secret |
|---|---|---|
| `SOLO_EMPIRE_DATA_LAKE_URI` (`DATA_LAKE_URI`) | ที่ตั้ง lake (path หรือ `s3://`) | ไม่ (แต่ไม่เก็บค่า production ใน Git) |
| `DATA_LAKE_S3_ENDPOINT` | endpoint R2 | ไม่ |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | credential R2 | ใช่ |
| `BOOK_JOB_DATA_API_TOKEN` (`API_READ_TOKEN`) | Bearer token ของ API | ใช่ |
| `API_HOST`, `PORT` / `API_PORT` | bind address/port (default `127.0.0.1:8109`) | ไม่ |
| `LAKE_READ_MODE`, `LAKE_READ_FALLBACK` | `parquet` / `error` | ไม่ |
| `BOOK_JOB_DATA_CACHE_TTL_SECONDS`, `BOOK_JOB_DATA_RATE_LIMIT_PER_MINUTE` | cache / rate limit | ไม่ |
| `BOOK_JOB_SCRAPING_DATA_DIR` | ที่ตั้ง capture CSV | ไม่ |
| `BOOK_JOB_DATA_MAX_CAPTURE_AGE_HOURS` | threshold stale ของ scheduled ingest (default 12) | ไม่ |
| `DATA_DIR`, `FIXTURES_DIR`, `STALE_AFTER_HOURS`, `REQUEST_TIMEOUT_SECONDS` | ค่าทั่วไป | ไม่ |
| `ALLOW_REFRESH`, `ALLOW_EXTERNAL_WRITES`, `ALLOW_PAID_PROVIDERS`, `FREE_ONLY` | safety flags | ไม่ |
| `DATA_LAKE_CLOUD_WRITE_ENABLED` | อนุญาตการเขียน R2 ใน verifier | ไม่ |

## 7. Health check

```bash
curl -fsS http://127.0.0.1:8109/healthz                 # ไม่อ่าน lake
curl -fsS -H "Authorization: Bearer $BOOK_JOB_DATA_API_TOKEN" http://127.0.0.1:8109/v1/metadata
task data:job:readiness                                 # จาก solo-data-lake: reconcile batch + freshness
.\ops\windows\scheduled-ingest.ps1 status               # LastTaskResult
```

สัญญาณปกติ: `/v1/metadata` มี `dataset_counts` > 0, readiness ok, `LastTaskResult` = 0

## 8. Failure handling / troubleshooting

| อาการ | สาเหตุที่เป็นไปได้ | การแก้ |
|---|---|---|
| API ไม่ start บน Render | ไม่ได้ตั้ง `BOOK_JOB_DATA_API_TOKEN` ขณะ bind `0.0.0.0` | ตั้ง secret แล้ว redeploy |
| 401 | token ไม่ถูกต้อง/ไม่มี | ตรวจ header Bearer |
| 429 | เกิน rate limit | รอ 1 นาที หรือปรับ `BOOK_JOB_DATA_RATE_LIMIT_PER_MINUTE` |
| `data_status=not_loaded` ใน `/healthz` | ยังไม่มีการเรียก endpoint ข้อมูล | ปกติ; เรียก `/v1/records` เพื่อ warm |
| อ่าน R2 ล้ม (fallback `error`) | endpoint/credential/prefix ผิด | ตรวจ env; ห้ามเปิด fallback เพื่อกลบปัญหา |
| ingest exit 2 | non-UTF-8 input หรือ crash | ดู `data/ingest.log`; แก้ encoding ที่ scraper |
| exit 3 + `ALERT stale scraper capture` | scraper หยุดหรือไฟล์หาย/เก่า | ตรวจ task ของ `book-job-scraping` |
| ingest ไม่เขียนอะไร | capture ไม่มีแถวจริง หรือ bytes ซ้ำ batch เดิม | ปกติสำหรับไฟล์ซ้ำ; ตรวจ scraper ถ้าไฟล์ว่าง |
| tick ค้าง | I/O หรือ lock | `faulthandler` dump ใน log หลัง 900 วินาที |
| ผลต่างระหว่าง public กับ parent mirror | drift ของสองสำเนา | ใช้ repo นี้เป็นอ้างอิง; ติดตาม `jd-t3` |

## 9. Rollback

- Render: redeploy commit ก่อนหน้าจาก Render Dashboard; service เดิมของ parent ยังอยู่เป็นทางถอยระหว่าง cutover
- Consumer: ชี้ URL กลับไป service เดิม (การเปลี่ยน URL ของ consumer ต้องได้รับอนุมัติ)
- ข้อมูล: Bronze เป็น append + history; rollback คือ replay จาก landing ไม่ใช่ลบ batch **[ต้องยืนยัน]** ขั้นตอนลบ batch ที่ผิด
- Contract: เปลี่ยนแบบ breaking ต้องออกเวอร์ชันใหม่ ไม่แก้ `job.v1` ในที่

## 10. Backup / restore

- R2 คือแหล่งถาวรของ hosted lane; นโยบาย versioning/backup ของ bucket **[ต้องยืนยัน]**
- Local lake: ไม่มีขั้นตอน backup ใน repository **[ต้องยืนยัน]**
- Restore: ingest capture CSV ใหม่หรือ replay landing bytes; ตรวจด้วย `task data:job:readiness`

## การอนุมัติ

| บทบาท | สถานะ | วันที่ | ลงนาม |
|---|---|---|---|
| DevOps / Operator (Book Dev) | รอพิจารณา | — | ________ |

## แหล่งข้อมูล

- `README.md`
- `render.yaml`
- `Dockerfile`
- `src/book_job_data/config.py`
- `src/book_job_data/ingest.py`
- `src/book_job_data/api.py`
- `scripts/scheduled_ingest.py`
- `scripts/r2_parity.py`
- `ops/windows/scheduled-ingest.ps1`
- `(solo-empire) operations/evidence/BOOK-JOB-DATA-RENDER-CUTOVER.md`
- `(solo-empire) infra/tasks/data.yml` (`data:job:readiness`)
