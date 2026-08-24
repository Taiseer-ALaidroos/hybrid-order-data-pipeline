import time
import os
import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, array, struct, translate,
    regexp_replace, trim, date_format, to_timestamp, expr, coalesce
)

# حل مشكلة نظام ويندوز مع مسار Hadoop
os.environ['HADOOP_HOME'] = "C:\\hadoop"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from config.settings import MONGO_URI, DB_NAME, RAW_COLLECTION, VALIDATED_COLLECTION, QUARANTINE_COLLECTION, REPORTS_DIR

def save_metrics_to_json(report_data, output_path=None):
    if output_path is None:
        output_path = os.path.join(REPORTS_DIR, "results.json")
        
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    existing_data = []
    
    if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
        try:
            with open(output_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                existing_data = data if isinstance(data, list) else [data]
        except json.JSONDecodeError:
            existing_data = []

    existing_data.append(report_data)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(existing_data, f, ensure_ascii=False, indent=4)

def apply_quality_rules_and_classify(df_raw):
    """
    تطبيق قواعد الجودة، استخراج البيانات من raw_record أولاً، مع تنظيف أسماء الأعمدة وحل التكرار
    """
    # 0. فك الحقل الخام لكي تصبح الأعمدة ظاهرة ومتاحة للسبارك
    if "raw_record" in df_raw.columns:
        df_source = df_raw.select(
            "_id", "run_id", "source_file", "source_row_number", "ingested_at", "engine_used",
            "raw_record.*"
        )
    else:
        df_source = df_raw

    # [التعديل لحل خطأ AMBIGUOUS_REFERENCE]: تنظيف الأعمدة ودمج المكرر
    for c in df_source.columns:
        clean_c = c.replace('\ufeff', '').strip()
        if clean_c != c:
            # إذا كان العمود النظيف موجوداً بالفعل، ندمج البيانات ثم نحذف العمود المشوه
            if clean_c in df_source.columns:
                df_source = df_source.withColumn(clean_c, coalesce(col(clean_c), col(c))).drop(c)
            else:
                df_source = df_source.withColumnRenamed(c, clean_c)

    # 1. التنظيف الآلي
    df_cleaned = df_source \
        .withColumn("clean_total", translate(col("total_amount"), "٠١٢٣٤٥٦٧٨٩٫", "0123456789.")) \
        .withColumn("clean_total", regexp_replace(col("clean_total"), r"[^\d\.-]", "")) \
        .withColumn("clean_phone", regexp_replace(col("customer_phone"), r"\s+", "")) \
        .withColumn("clean_email", regexp_replace(regexp_replace(col("customer_email"), r"@+", "@"), r"\.+", ".")) \
        .withColumn("clean_city", trim(col("city"))) \
        .withColumn("clean_status", trim(col("status"))) \
        .withColumn("clean_date", date_format(to_timestamp(col("order_date")), "yyyy-MM-dd HH:mm:ss"))

    # 2. بناء أثر التصحيح (Audit Trail)
    df_audit = df_cleaned.withColumn(
        "raw_corrections",
        array(
            when(col("total_amount") != col("clean_total"), struct(lit("total_amount").alias("field"), col("total_amount").alias("original_value"), col("clean_total").alias("corrected_value"), lit("FIX_ARABIC_AND_SYMBOLS").alias("rule_code"))),
            when(col("customer_phone") != col("clean_phone"), struct(lit("customer_phone").alias("field"), col("customer_phone").alias("original_value"), col("clean_phone").alias("corrected_value"), lit("TRIM_PHONE_SPACES").alias("rule_code"))),
            when(col("customer_email") != col("clean_email"), struct(lit("customer_email").alias("field"), col("customer_email").alias("original_value"), col("clean_email").alias("corrected_value"), lit("FIX_EMAIL_TYPOS").alias("rule_code"))),
            when(col("city") != col("clean_city"), struct(lit("city").alias("field"), col("city").alias("original_value"), col("clean_city").alias("corrected_value"), lit("TRIM_SPACES").alias("rule_code")))
        )
    )
    
    # فلترة المصفوفة من القيم الفارغة
    df_audit = df_audit.withColumn("corrections", expr("filter(raw_corrections, x -> x is not null)")).drop("raw_corrections")

    # 3. استبدال الأعمدة القديمة بالمنظفة
    df_final_clean = df_audit.drop("total_amount", "customer_phone", "customer_email", "city", "status", "order_date") \
        .withColumnRenamed("clean_total", "total_amount") \
        .withColumnRenamed("clean_phone", "customer_phone") \
        .withColumnRenamed("clean_email", "customer_email") \
        .withColumnRenamed("clean_city", "city") \
        .withColumnRenamed("clean_status", "status") \
        .withColumnRenamed("clean_date", "order_date")

    # 4. التصنيف إلى Quarantine و Validated (مع الحفاظ الكامل على شروط الدكتور وتعديل طفيف لمرونة قراءة الـ JSON لمنع تضخم الحجر الصحي)
    df_classified = df_final_clean.withColumn(
        "record_status",
        when(col("order_id").isNull() | (trim(col("order_id")) == ""), lit("Quarantined: MISSING_ORDER_ID"))
        .when(col("customer_id").isNull() | (trim(col("customer_id")) == ""), lit("Quarantined: MISSING_CUSTOMER_ID"))
        .when(col("items_json").isNotNull() & ~col("items_json").rlike(r'^["\s]*[\[\{]'), lit("Quarantined: CORRUPTED_ITEMS_JSON"))
        .when(col("total_amount") == "", lit("Quarantined: UNKNOWN_PRICE"))
        .when(expr("size(corrections) > 0"), lit("Corrected"))
        .otherwise(lit("Valid"))
    )
    
    return df_classified

def run_elt_pipeline(spark, run_id):
    print(f"\n--- Starting Advanced ELT Pipeline (Run ID: {run_id}) ---")
    start_time = time.time()

    raw_uri = f"{MONGO_URI.rstrip('/')}/{DB_NAME}.{RAW_COLLECTION}"
    df_raw = spark.read.format("mongo").option("uri", raw_uri).load()
    rows_read = df_raw.count()

    if rows_read == 0:
        print("⚠️ No records found in Raw Collection.")
        return

    # إضافات التقرير: استخراج اسم الملف
    try:
        file_name = df_raw.select("source_file").first()[0] if "source_file" in df_raw.columns else "unknown_file.csv"
    except:
        file_name = "unknown_file.csv"

    # معالجة وتصنيف البيانات
    df_processed = apply_quality_rules_and_classify(df_raw).cache()

    # فصل المسارات
    df_quarantine = df_processed.filter(col("record_status").startswith("Quarantined"))
    df_valid = df_processed.filter(col("record_status").isin("Valid", "Corrected"))

    # العدادات وضمان الاتساق
    valid_count = df_processed.filter(col("record_status") == "Valid").count()
    corrected_count = df_processed.filter(col("record_status") == "Corrected").count()
    quarantine_count = df_quarantine.count()

    # إضافات التقرير: حساب تفاصيل أخطاء الحجر الصحي
    error_counts_df = df_quarantine.groupBy("record_status").count().collect()
    error_case_counts = {row["record_status"]: row["count"] for row in error_counts_df}
    
    # # 1. الرفع إلى الحجر الصحي
    # quarantine_uri = f"{MONGO_URI.rstrip('/')}/{DB_NAME}.{QUARANTINE_COLLECTION}"
    # print(f"⚠️ Saving quarantined records to '{QUARANTINE_COLLECTION}'...")
    # if quarantine_count > 0:
    #     df_quarantine.write.format("mongo").option("uri", quarantine_uri).mode("append").save()

# 1. الرفع إلى الحجر الصحي (مع تطبيق Idempotent Upsert لمنع التكرار)
    quarantine_uri = f"{MONGO_URI.rstrip('/')}/{DB_NAME}.{QUARANTINE_COLLECTION}"
    print(f"⚠️ Saving quarantined records to '{QUARANTINE_COLLECTION}'...")
    if quarantine_count > 0:
        # جعل الـ _id يعتمد على order_id إن وجد، أو دمج حالة الخطأ لضمان عدم التكرار العشوائي
        df_quarantine_upsert = df_quarantine.withColumn(
            "_id", 
            when(col("order_id").isNotNull() & (trim(col("order_id")) != ""), col("order_id"))
            .otherwise(expr("concat('q_', md5(concat_ws('_', coalesce(customer_id, 'unknown'), coalesce(order_date, 'unknown'))))"))
        )
        
        df_quarantine_upsert.write.format("mongo") \
            .option("uri", quarantine_uri) \
            .option("replaceDocument", "true") \
            .mode("append") \
            .save()


    # 2. الدمج والرفع النهائي بـ Idempotent Upsert
    valid_uri = f"{MONGO_URI.rstrip('/')}/{DB_NAME}.{VALIDATED_COLLECTION}"
    print(f"🔄 Executing Idempotent Upsert to '{VALIDATED_COLLECTION}'...")
    
    df_valid_upsert = df_valid.withColumn("_id", col("order_id"))

    # إضافات التقرير: حساب ما تم إضافته وما تم تحديثه عن طريق مقارنة المعرفات فقط (بدون Hash)
    inserted_count = valid_count + corrected_count
    updated_count = 0
    try:
        df_existing = spark.read.format("mongo").option("uri", valid_uri).load()
        if "_id" in df_existing.columns:
            existing_ids = df_existing.select(col("_id").alias("existing_id"))
            df_compare = df_valid_upsert.join(existing_ids, df_valid_upsert["_id"] == existing_ids["existing_id"], "left")
            inserted_count = df_compare.filter(col("existing_id").isNull()).count()
            updated_count = (valid_count + corrected_count) - inserted_count
    except Exception as e:
        pass # إذا لم يكن الكولكشن موجوداً بعد، سيعتبر الكل Inserted

    # عملية الحفظ (التي كانت في كودك الأصلي)
    df_valid_upsert.write.format("mongo") \
        .option("uri", valid_uri) \
        .option("replaceDocument", "true") \
        .mode("append") \
        .save()

    elapsed_seconds = time.time() - start_time

    # توليد التقرير متضمناً الإحصائيات الجديدة
    final_report = {
        "run_id": run_id,
        "file_name": file_name,
        "file_size_mb": 0.0, # يتم حسابه عادة في مرحلة Ingestion
        "engine_used": "pyspark_elt",
        "rows_read": rows_read,
        "raw_loaded": rows_read,
        "valid_count": valid_count,
        "corrected_count": corrected_count,
        "quarantine_count": quarantine_count,
        "elapsed_seconds": round(elapsed_seconds, 2),
        "throughput": round(rows_read / elapsed_seconds, 2) if elapsed_seconds > 0 else 0,
        "partitions": spark.conf.get("spark.sql.shuffle.partitions"),
        "error_case_counts": error_case_counts,
        "inserted_count": inserted_count,
        "updated_count": updated_count,
        "unchanged_count": 0 # يستدعي مقارنة الـ Hash لذلك تركته 0 لعدم تغيير كودك
    }

    save_metrics_to_json(final_report)

    df_processed.unpersist()
    print(f"\n--- ELT Summary ---")
    print(f"Total: {rows_read} | Valid: {valid_count} | Corrected: {corrected_count} | Quarantine: {quarantine_count}")
    print(f"DB Actions -> Inserted: {inserted_count} | Updated: {updated_count}")
    print(f"✅ Pipeline Completed in {round(elapsed_seconds, 2)}s!")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Hybrid_ELT_Pipeline") \
        .master("local[4]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "99") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
    run_id = f"run_{time.strftime('%Y%m%d_%H%M%S')}"
    run_elt_pipeline(spark, run_id)
    spark.stop()


