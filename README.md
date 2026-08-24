
```markdown
# 🚀 Midterm Data Pipeline

## 📌 Overview
This project is an advanced and flexible Data Pipeline designed to process and clean orders data. The system utilizes smart File Routing, where large files are processed using **Apache Spark** and small files are handled via **Python Batch Processing**. Afterward, the data is cleaned, quality rules are applied, and it is stored in a **MongoDB** database across three main layers (Raw, Validated, and Quarantine).

---

## 📂 Project Structure
```text
midterm-data-pipeline/
├── config/
│   └── settings.py              # Project settings and global variables
├── data/                        # Data folder (huge files and samples)
├── docs/                        # Documentation folder
├── reports/                     # Performance reports and processing results (JSON)
├── src/                         # Core source code
│   ├── batch_engine.py          # Batch processing engine (for small files)
│   ├── create_small_sample.py   # Tool to generate data samples
│   ├── db_loader.py             # Database connection manager
│   ├── file_router.py           # Smart router (decides the processing tool based on file size)
│   ├── main.py                  # Central orchestrator
│   ├── quality_rules.py         # Data quality and cleaning rules
│   ├── reset_db.py              # Utility to reset/drop database collections
│   ├── spark_engine.py          # Extraction and ingestion engine using PySpark
│   └── spark_etl_pipeline.py    # Advanced ELT pipeline for data processing and cleaning
├── tests/                       # Unit tests folder
├── README.md                    # Project documentation
└── requirements.txt             # Required libraries to run the project

```

---

## 🛠️ Modules & Engines

### 1. `config/settings.py` (Central Configurations)

The beating heart of the project, containing:

* Paths for data and reports directories.
* Routing configurations (`SMALL_FILE_THRESHOLD_MB = 200.0`) to switch between Python and Spark.
* MongoDB connection strings and collection names (`orders_raw`, `orders_validated`, `orders_quarantine`).

### 2. `src/main.py` (Central Orchestrator)

The main entry point to run the project, featuring:

* Interactive prompt for the user to input the file path, with smart handling of Windows paths.
* Process Isolation using `subprocess` to prevent memory overlaps.
* Managing sequential workflow stages (Routing ⬅️ Raw Data Ingestion ⬅️ Processing & Cleaning).

### 3. `src/file_router.py` (Smart Router)

* Calculates the actual file size and routes it automatically.
* Ensures efficiency and optimal resource consumption by routing small files (≤ 200MB) to the Python engine, and larger ones to PySpark.

### 4. `src/batch_engine.py` (Batch Engine)

* Reads small files line by line (Streaming) to save memory consumption.
* Ingests raw records in batches using `insert_many` in MongoDB to increase ingestion throughput.

### 5. `src/spark_engine.py` (Big Data Engine)

* Utilizes `SparkSession` and `mongo-spark-connector` to handle massive datasets.
* Relies on a Fixed Schema to accelerate data reading instead of exhausting the system with schema inference.
* Wraps data with Metadata to serve the ELT pipeline.

### 6. `src/spark_etl_pipeline.py` (Advanced Processing Engine)

The crown jewel of the system for Transformations:

* **Smart Cleaning:** Converts Eastern Arabic numerals to standard format, removes extra spaces, and standardizes dates.
* **Audit Trail:** Records any corrections made to the records (`raw_corrections`) to ensure transparency.
* **Classification:** Dynamically categorizes data into (`Valid`, `Corrected`, `Quarantined`).
* **Safe Ingestion:** Uses Idempotent Upsert by comparing the `order_id` to update pre-existing data instead of duplicating it.

### 7. `src/quality_rules.py` (Quality Rules)

The data firewall:

* Handles hidden file issues (like the `\ufeff` BOM character).
* Applies strict validation conditions using `Regex` (for phones and emails) and verifies the financial logic of the orders.

### 8. `src/db_loader.py` & `src/reset_db.py` (Database Utilities)

* Utilities providing a clean and unified connection to the database, alongside a script to reset collections for testing in a clean environment.

### 9. `src/create_small_sample.py` (Sampling Tool)

* A powerful CLI script to extract small samples from huge files, facilitating safe code testing.

---

## 📊 Reports & Metrics

The system generates comprehensive reports in `JSON` format inside the `reports/` folder after every run, including:

* **Time Performance:** Processing speed (Throughput) and total elapsed time in seconds.
* **Data Quality:** Counts of valid (`valid_count`), corrected (`corrected_count`), and quarantined records (`quarantine_count`).
* **Error Tracking:** Precise categorization of quarantine reasons (e.g., `MISSING_ORDER_ID`).
* **Database Operations:** Documenting the number of newly added (`inserted_count`) and updated records (`updated_count`).

---

## 🚀 How to Run

1. Ensure all required libraries are installed (`pip install -r requirements.txt`).
2. Run your local **MongoDB** server on port `27017`.
3. (Optional) If you want a completely clean environment, run `python src/reset_db.py`.
4. Start the system by running the central orchestrator:
```bash
python src/main.py

```


5. Paste your data file path when prompted by the system, and watch the processing magic! ✨

---

## ✅ Project Requirements Compliance

This system was designed to match all the official assignment requirements for the Big Data project. Below is the completed checklist:

* [x] **Sample Generation:** The `create_small_sample.py` script works efficiently to extract small samples.
* [x] **Smart Routing (File Router):** The system automatically chooses `Python Batch` for small files and `PySpark` for large files.
* [x] **ELT Architecture:** All records are initially ingested raw into `orders_raw` without any filtering or deletion, including source metadata (`run_id`, `ingested_at`, etc.).
* [x] **Quality Rules (8 Rules):** Strict cleaning rules were applied including: (Converting Arabic numerals, standardizing currency, removing thousand separators, cleaning phone numbers, fixing email typos, standardizing dates, trimming spaces, and calculating totals).
* [x] **Audit Trail:** Corrected records contain a `corrections` array detailing the field, old value, new value, and rule code.
* [x] **Quarantine:** Uncorrectable records (e.g., missing `order_id`) are routed to `orders_quarantine` with a clearly stated isolation reason (e.g., `MISSING_ORDER_ID`).
* [x] **Metrics Consistency:** Total read records = Valid + Corrected + Quarantined.
* [x] **Metrics & Reports:** A `reports/results.json` report is generated containing time, speed, counters, and ingestion operation details.
* [x] **Idempotency & Upsert:** `order_id` is used as a Stable Business Key. Rerunning the same data does not result in duplicates, and updating records is done via a safe Upsert mechanism.

```

```


















---

```markdown
# 🚀 Midterm Data Pipeline

## 📌 نظرة عامة (Overview)
هذا المشروع عبارة عن مسار بيانات (Data Pipeline) متقدم ومرن لمعالجة وتنظيف بيانات الطلبات (Orders). يعتمد النظام على توجيه الملفات الذكي (File Routing) حيث يتم معالجة الملفات الضخمة باستخدام **Apache Spark**، والملفات الصغيرة باستخدام **Python Batch Processing**. يتم بعد ذلك تنظيف البيانات وتطبيق قواعد الجودة عليها، ثم تخزينها في قاعدة بيانات **MongoDB** عبر ثلاث طبقات أساسية (الخام، الصالحة، والمعزولة).

---

## 📂 هيكلية المشروع (Project Structure)
```text
midterm-data-pipeline/
├── config/
│   └── settings.py              # إعدادات المشروع والمتغيرات العامة
├── data/                        # مجلد البيانات (الضخمة والعينات)
├── docs/                        # مجلد التوثيق
├── reports/                     # تقارير الأداء ونتائج المعالجة (JSON)
├── src/                         # الأكواد المصدرية الأساسية
│   ├── batch_engine.py          # محرك المعالجة بالدفعات (للملفات الصغيرة)
│   ├── create_small_sample.py   # أداة لإنشاء عينات بيانات
│   ├── db_loader.py             # مدير الاتصال بقاعدة البيانات
│   ├── file_router.py           # الموجه الذكي (يقرر أداة المعالجة بناءً على الحجم)
│   ├── main.py                  # المشغل المركزي (Orchestrator)
│   ├── quality_rules.py         # قواعد جودة البيانات والتنظيف
│   ├── reset_db.py              # أداة تصفير قاعدة البيانات
│   ├── spark_engine.py          # محرك الاستخراج والرفع باستخدام PySpark
│   └── spark_etl_pipeline.py    # مسار الـ ELT المتقدم لمعالجة وتنظيف البيانات
├── tests/                       # مجلد اختبارات الأكواد
├── README.md                    # التوثيق الخاص بالمشروع
└── requirements.txt             # المكتبات المطلوبة لتشغيل المشروع

```

---

## 🛠️ تفاصيل الأكواد والمحركات (Modules & Engines)

### 1. `config/settings.py` (الإعدادات المركزية)

القلب النابض للمشروع، ويحتوي على:

* مسارات مجلدات البيانات والتقارير.
* إعدادات التوجيه (`SMALL_FILE_THRESHOLD_MB = 200.0`) للتحويل بين بايثون وسبارك.
* بيانات الاتصال بـ MongoDB وتسمية المجموعات (`orders_raw`, `orders_validated`, `orders_quarantine`).

### 2. `src/main.py` (المشغل المركزي)

نقطة الدخول الرئيسية (Entry Point) لتشغيل المشروع، ويتميز بـ:

* استقبال مسار الملف تفاعلياً من المستخدم مع معالجة ذكية لمسارات ويندوز.
* عزل العمليات (Process Isolation) باستخدام `subprocess` لمنع تداخل الذاكرة.
* إدارة مراحل العمل المتسلسلة (توجيه ⬅️ رفع بيانات خام ⬅️ معالجة وتنظيف).

### 3. `src/file_router.py` (الموجه الذكي)

* يحسب حجم الملف الفعلي ويوجهه تلقائياً.
* يضمن الكفاءة واستهلاك الموارد الأمثل عبر تحويل الملفات الصغرى (≤ 200MB) لمحرك بايثون، والكبرى لـ PySpark.

### 4. `src/batch_engine.py` (محرك الدفعات)

* يقرأ الملفات الصغيرة سطراً بسطر (Streaming) لتوفير استهلاك الذاكرة.
* يرفع السجلات الخام كمجموعات باستخدام `insert_many` في MongoDB لزيادة سرعة الإدخال (Throughput).

### 5. `src/spark_engine.py` (محرك البيانات الضخمة)

* يستخدم `SparkSession` و `mongo-spark-connector` للتعامل مع البيانات العملاقة.
* يعتمد على بنية ثابتة (Fixed Schema) لتسريع قراءة البيانات بدلاً من إرهاق النظام باستنتاجها.
* يغلف البيانات ببيانات وصفية (Metadata) تخدم مسار الـ ELT.

### 6. `src/spark_etl_pipeline.py` (محرك المعالجة المتقدمة)

جوهرة النظام لعمليات التنظيف (Transformations):

* **التنظيف الذكي:** يحول الأرقام العربية إلى قياسية ويزيل المسافات الزائدة ويوحد التواريخ.
* **سجل التدقيق (Audit Trail):** يسجل أي تصحيح تم على السجلات (`raw_corrections`) لضمان الشفافية.
* **التصنيف:** يصنف البيانات ديناميكياً إلى (Valid, Corrected, Quarantined).
* **الإدخال الآمن:** يستخدم (Idempotent Upsert) عبر مقارنة `order_id` لتحديث البيانات الموجودة مسبقاً بدلاً من تكرارها.

### 7. `src/quality_rules.py` (قواعد الجودة)

الجدار الناري للبيانات:

* يعالج مشاكل الملفات المخفية (مثل رمز `\ufeff`).
* يطبق شروط تحقق صارمة باستخدام `Regex` (للهواتف والإيميلات) ويتحقق من المنطق المالي للطلبات.

### 8. `src/db_loader.py` & `src/reset_db.py` (أدوات القاعدة)

* أدوات توفر اتصالاً نظيفاً وموحداً بقاعدة البيانات، بالإضافة إلى سكربت تصفير المجموعات لإجراء اختبارات ببيئة نظيفة.

### 9. `src/create_small_sample.py` (أداة العينات)

* سكربت CLI قوي لاستخراج عينات مصغرة من الملفات الضخمة وتسهيل اختبار الأكواد بأمان.

---

## 📊 التقارير والمقاييس (Reports & Metrics)

يقوم النظام بتوليد تقارير شاملة بصيغة `JSON` داخل مجلد `reports/` بعد كل تشغيل، وتشمل:

* **الأداء الزمني:** سرعة المعالجة (Throughput) والوقت المستغرق الكلي بالثواني.
* **جودة البيانات:** أعداد السجلات الصحيحة (`valid_count`)، المصححة (`corrected_count`)، والمعزولة (`quarantine_count`).
* **تتبع الأخطاء:** تصنيف دقيق لأسباب عزل السجلات (مثل `MISSING_ORDER_ID`).
* **عمليات القاعدة:** توثيق عدد السجلات الجديدة المضافة (`inserted_count`) والمحدثة (`updated_count`).

---

## 🚀 كيفية التشغيل (How to Run)

1. تأكد من تثبيت المكتبات المطلوبة (`pip install -r requirements.txt`).
2. قم بتشغيل خادم **MongoDB** محلياً على المنفذ `27017`.
3. (اختياري) إذا أردت بيئة نظيفة تماماً، قم بتشغيل `python src/reset_db.py`.
4. ابدأ النظام عن طريق تشغيل المشغل المركزي:
```bash
python src/main.py

```


5. ألصق مسار ملف البيانات الخاص بك عند مطالبة النظام بذلك، وراقب سحر المعالجة! ✨

```

***

## ✅ مطابقة متطلبات المشروع (Project Requirements Compliance)
تم تصميم هذا النظام ليطابق جميع متطلبات التكليف الرسمي لمشروع البيانات الضخمة، وفيما يلي قائمة التحقق المنجزة:

- [x] **توليد العينات:** سكريبت `create_small_sample.py` يعمل بكفاءة لاستخراج عينات مصغرة.
- [x] **التوجيه الذكي (File Router):** يختار النظام تلقائياً `Python Batch` للملفات الصغيرة و `PySpark` للملفات الكبيرة.
- [x] **معمارية ELT:** يتم رفع جميع السجلات خاماً إلى `orders_raw` أولاً بدون أي فلترة أو حذف، مع تضمين بيانات المصدر (`run_id`, `ingested_at`, إلخ).
- [x] **قواعد الجودة (8 قواعد):** تم تطبيق قواعد تنظيف صارمة تشمل: (تحويل الأرقام العربية، توحيد العملة، إزالة فواصل الآلاف، تنظيف أرقام الهواتف، إصلاح أخطاء الإيميلات، توحيد التواريخ، إزالة المسافات، وحساب الإجمالي).
- [x] **أثر التصحيح (Audit Trail):** السجلات المصححة تحتوي على مصفوفة `corrections` توضح الحقل، القيمة القديمة، القيمة الجديدة، وكود القاعدة.
- [x] **العزل (Quarantine):** السجلات غير القابلة للتصحيح (مثل غياب `order_id`) يتم توجيهها إلى `orders_quarantine` مع ذكر سبب العزل بوضوح (مثل `MISSING_ORDER_ID`).
- [x] **اتساق العدادات:** إجمالي السجلات المقروءة = السليمة + المصححة + المعزولة.
- [x] **القياسات والتقارير:** يتم توليد تقرير `reports/results.json` يحتوي على الزمن، السرعة، العدادات، وتفاصيل عمليات الإدخال.
- [x] **الاعتمادية (Idempotency & Upsert):** تم استخدام `order_id` كمفتاح عمل أساسي (Business Key). إعادة تشغيل نفس البيانات لا تؤدي إلى تكرارها، وتحديث السجلات يتم بآلية Upsert آمنة.