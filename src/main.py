import os
import sys
import subprocess

# التأكد من المسارات لضمان توافق الاستيراد
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(CURRENT_DIR)

if CURRENT_DIR not in sys.path:
    sys.path.append(CURRENT_DIR)
if PARENT_DIR not in sys.path:
    sys.path.append(PARENT_DIR)

from config import settings
import file_router

def run_script(script_name, target_file):
    """دالة تشغيل الملفات مع تمرير مسار المشروع ومسار الملف كمتغير بيئة"""
    script_path = os.path.join(CURRENT_DIR, script_name)
    
    if not os.path.exists(script_path):
        print(f"❌ خطأ: الملف {script_name} غير موجود في {script_path}")
        return False
        
    print(f"\n▶️ جاري تشغيل: {script_name} ...")
    
    # تجهيز بيئة التشغيل
    env = os.environ.copy()
    env["PYTHONPATH"] = PARENT_DIR + os.pathsep + env.get("PYTHONPATH", "")
    # نمرر مسار الملف كمتغير بيئة لتستفيد منه المحركات لاحقاً
    env["PIPELINE_INPUT_FILE"] = target_file 
    
    result = subprocess.run([sys.executable, script_path], env=env)
    
    if result.returncode == 0:
        print(f"✅ اكتمل تشغيل {script_name} بنجاح.")
        return True
    else:
        print(f"❌ فشل تشغيل {script_name}. (كود الخطأ: {result.returncode})")
        return False


def main():
    print("\n" + "="*60)
    print("🚀 المشغل المركزي لخط سير البيانات (Pipeline Orchestrator)")
    print("="*60)
    
    # طلب المسار مباشرة من التيرمينال
    target_file = input("\nأدخل المسار الكامل لملف البيانات لتبدأ المعالجة:\n> ").strip()
    
    # تنظيف المسار من علامات التنصيص (تحدث عند سحب الملف للتيرمينال في ويندوز)
    target_file = target_file.strip('\'"')
    
    if not os.path.exists(target_file):
        print(f"\n❌ خطأ: لم أتمكن من العثور على الملف في المسار المذكور:\n{target_file}")
        print("تأكد من صحة المسار وحاول مجدداً.")
        return

    # 1. توجيه الملف باستخدام الموجه
    print("\n🔍 [المرحلة 1]: توجيه الملف (File Routing)...")
    try:
        engine_choice, size_mb = file_router.route_file(target_file)
    except Exception as e:
        print(f"❌ حدث خطأ أثناء فحص الملف: {e}")
        return

    # 2. تشغيل محرك الرفع
    print("\n⏳ [المرحلة 2]: رفع البيانات الخام (Raw Load)...")
    if engine_choice == 'python_batch':
        success = run_script("batch_engine.py", target_file)
    elif engine_choice == 'pyspark':
        success = run_script("spark_engine.py", target_file)
    else:
        print("❌ قرار توجيه غير معروف!")
        return

    # 3. تشغيل المعالجة ELT
    if success:
        print("\n⏳ [المرحلة 3]: تطبيق الجودة والمعالجة (ELT Pipeline)...")
        run_script("spark_etl_pipeline.py", target_file)
    else:
        print("\n⚠️ توقف خط السير بسبب فشل مرحلة رفع البيانات.")

    print("\n" + "="*60)
    print("🏆 انتهت جميع العمليات المجدولة في خط السير!")
    print("="*60)


if __name__ == "__main__":
    main()





# import os
# import sys
# import time
# from datetime import datetime
# from pyspark.sql import SparkSession

# # ---------------------------------------------------------
# # 1. حل مشكلة المسارات (لتجنب خطأ ModuleNotFoundError)
# # ---------------------------------------------------------
# # يضمن هذا الجزء أن بايثون سيتعرف على ملفاتك سواء كان main.py 
# # داخل مجلد src أو في المجلد الرئيسي للمشروع.
# CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
# PARENT_DIR = os.path.dirname(CURRENT_DIR)

# if CURRENT_DIR not in sys.path:
#     sys.path.append(CURRENT_DIR)
# if PARENT_DIR not in sys.path:
#     sys.path.append(PARENT_DIR)

# # ---------------------------------------------------------
# # 2. استيراد ملفات المشروع
# # ---------------------------------------------------------
# try:
#     from config import settings
#     from batch_engine import run_batch_processing
#     from spark_engine import run_spark_processing
#     from spark_etl_pipeline import run_elt_pipeline
# except ModuleNotFoundError:
#     # في حال كانت الملفات داخل مجلد src
#     from config import settings
#     from src.batch_engine import run_batch_processing
#     from src.spark_engine import run_spark_processing
#     from src.spark_etl_pipeline import run_elt_pipeline

# # حل مشكلة نظام ويندوز مع مسار Hadoop
# os.environ['HADOOP_USER_NAME'] = 'root'
# os.environ['HADOOP_HOME'] = r"C:\hadoop"

# def main():
#     # 3. تحديد ملف العينة مباشرة لتسهيل التجربة (بدون argparse)
#     input_file = "sample_orders.csv"
    
#     # التحقق من وجود الملف
#     if not os.path.exists(input_file):
#         # البحث في المجلد الأب إذا لم يكن في المجلد الحالي
#         if os.path.exists(os.path.join(PARENT_DIR, input_file)):
#             input_file = os.path.join(PARENT_DIR, input_file)
#         else:
#             print(f"❌ خطأ: لم يتم العثور على ملف {input_file}!")
#             print("تأكد من وضع ملف العينة بجوار هذا السكريبت.")
#             return

#     # 4. توليد run_id
#     run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
#     print("\n" + "="*70)
#     print(f"🚀 بدء التشغيل الشامل للتجربة المصغرة (Test Environment)")
#     print(f"🔑 معرف التشغيل (Run ID): {run_id}")
#     print("="*70)

#     # 5. الموجه التلقائي (File Router)
#     file_size_mb = os.path.getsize(input_file) / (1024 * 1024)
#     print(f"📊 حجم الملف: {file_size_mb:.2f} MB")
    
#     if file_size_mb <= settings.SMALL_FILE_THRESHOLD_MB:
#         engine = "batch"
#         print("⚙️ المحرك المختار: Python Batch (الملف صغير)")
#     else:
#         engine = "spark"
#         print("⚙️ المحرك المختار: Apache Spark")
#     print("-" * 70)

#     # 6. تشغيل مرحلة الـ Raw Load
#     print("📥 [المرحلة الأولى] بدء الرفع إلى طبقة RAW...")
#     if engine == "batch":
#         run_batch_processing(input_file, run_id)
#     else:
#         run_spark_processing(input_file, run_id)
#     print("-" * 70)

#     # 7. تشغيل مرحلة المعالجة والجودة (Quality Pipeline)
#     print("🔄 [المرحلة الثانية] بدء فلترة الجودة والتصنيف (Quality Pipeline)...")
    
#     spark = SparkSession.builder \
#         .appName("Hybrid_ELT_Quality_Phase") \
#         .master("local[*]") \
#         .config("spark.driver.memory", "4g") \
#         .config("spark.executor.memory", "4g") \
#         .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
#         .getOrCreate()
        
#     spark.sparkContext.setLogLevel("ERROR")
    
#     try:
#         # استدعاء دالة الجودة
#         run_elt_pipeline(spark, run_id)
#     except Exception as e:
#         print(f"❌ حدث خطأ أثناء مرحلة الجودة: {e}")
#     finally:
#         spark.stop()
        
#     print("="*70)
#     print("🏆 اكتملت عملية ELT بنجاح على عينة البيانات!")
#     print("="*70)

# if __name__ == "__main__":
#     main()



# هذا الكود 1 الي قسم باتش وسبارك 

# import os
# import sys
# import argparse
# from datetime import datetime

# # إضافة المسار الرئيسي للمشروع لضمان استيراد الملفات بشكل صحيح
# BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# if BASE_DIR not in sys.path:
#     sys.path.append(BASE_DIR)

# # استيراد دالة التوجيه ودوال المعالجة من مجلد src
# from src.file_router import route_file
# from src.batch_engine import run_batch_processing
# from src.spark_engine import run_spark_processing

# def main():
#     # استخدام argparse لتمرير مسار الملف من سطر الأوامر ليكون التشغيل مرناً
#     parser = argparse.ArgumentParser(description="Midterm Data Pipeline - Main Execution")
#     parser.add_argument('--input', type=str, required=True, help="مسار ملف البيانات (CSV)")
#     args = parser.parse_args()
    
#     file_path = args.input
    
#     # إنشاء run_id ديناميكي وفريد لكل عملية تشغيل (أساسي لتتبع البيانات لاحقاً)
#     current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
#     dynamic_run_id = f"run_{current_time}"

#     print("=" * 60)
#     print("🚀 بدء التشغيل الرئيسي للمشروع (Main Entry Point)")
#     print(f"🆔 معرف التشغيل (Run ID): {dynamic_run_id}")
#     print("=" * 60)

#     try:
#         # 1. استدعاء الموجه التلقائي (File Router) لتقييم الملف واتخاذ القرار
#         engine, file_size_mb = route_file(file_path)
        
#         # 2. توجيه المهمة للمحرك المناسب بناءً على النتيجة
#         if engine == 'pyspark':
#             print(f"⚡ جاري تحويل المهمة إلى [Apache Spark] لمعالجة {file_size_mb:.2f} MB...")
#             run_spark_processing(file_path, run_id=dynamic_run_id)
            
#         elif engine == 'python_batch':
#             print(f"⚡ جاري تحويل المهمة إلى [Python Batch] لمعالجة {file_size_mb:.2f} MB...")
#             # تأكد أن دالة الباتش تستقبل المتغير باسم run_id
#             run_batch_processing(file_path, run_id=dynamic_run_id)
            
#     except FileNotFoundError as e:
#         print(f"❌ {e}")
#     except Exception as e:
#         print(f"❌ حدث خطأ أثناء التشغيل: {e}")

# if __name__ == "__main__":
#     main()





# import os
# import sys

# # إضافة المسار الرئيسي للمشروع لضمان استيراد الإعدادات بشكل صحيح
# BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# if BASE_DIR not in sys.path:
#     sys.path.append(BASE_DIR)

# from config import settings
# from src.batch_engine import run_batch_processing
# from src.spark_engine import run_spark_processing  # تم تفعيل استدعاء الاسبارك

# def main():
#     # ----------------------------------------------------
#     # ⬅️ تعديل المسار يدوياً من هنا (ضع مسار ملف الـ CSV الذي تريد تشغيله):
#     # ----------------------------------------------------
#     file_path = r"D:\midterm-data-pipeline\data\orders_huge_mixed_quality.csv"  # يمكنك استبداله بمسار الملف الكبير مباشرة
    
#     # التأكد من أن الملف موجود
#     if not os.path.exists(file_path):
#         print(f"❌ خطأ: ملف البيانات غير موجود في المسار: {file_path}")
#         return

#     # حساب حجم الملف بالميجابايت
#     file_size_bytes = os.path.getsize(file_path)
#     file_size_mb = file_size_bytes / (1024 * 1024)
#     file_size_gb = file_size_mb / 1024

#     print("=" * 60)
#     print("🚦 بدء الموجّه التلقائي (File Router)")
#     print("=" * 60)
#     print(f"📁 مسار الملف: {file_path}")
#     print(f"📊 حجم الملف: {file_size_gb:.2f} GB ({file_size_mb:.2f} MB)")
#     print(f"⚖️ الحد الفاصل المكتوب في الإعدادات: {settings.SMALL_FILE_THRESHOLD_MB} MB")
#     print("-" * 60)

#     # القاعدة التلقائية بناءً على حجم الملف والحد الفاصل
#     if file_size_mb > settings.SMALL_FILE_THRESHOLD_MB:
#         print(f"⚡ القرار: استخدام محرك [Apache Spark]")
#         print(f"السبب: حجم الملف أكبر من الحد الفاصل ({settings.SMALL_FILE_THRESHOLD_MB} MB).")
#         print("-" * 60)
        
#         # استدعاء دالة الاسبارك للملفات الضخمة وتمرير مسار الملف
#         run_spark_processing(file_path, run_id="run_spark_huge_001")
        
#     else:
#         print(f"⚡ القرار: استخدام المحرك العادي [Python Batch]")
#         print(f"السبب: حجم الملف أصغر من أو يساوي الحد الفاصل ({settings.SMALL_FILE_THRESHOLD_MB} MB).")
#         print("-" * 60)
        
#         # استدعاء دالة الباتش للملفات الصغيرة/المتوسطة وتمرير مسار الملف
#         run_batch_processing(file_path, id_run="run_batch_raw_001")

# if __name__ == "__main__":
#     main()










# import os
# import sys
# from datetime import datetime

# # إضافة مسار المشروع الأساسي لضمان استيراد الإعدادات والوحدات بشكل صحيح
# BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# if BASE_DIR not in sys.path:
#     sys.path.append(BASE_DIR)

# from config import settings
# from src.db_loader import get_database
# from src.batch_engine import run_batch_processing
# from src.spark_engine import run_spark_processing
# from src.quality_rules import (
#     convert_arabic_numerals,
#     clean_whitespace,
#     standardize_currency,
#     clean_numeric_field,
#     clean_phone,
#     validate_and_clean_email,
#     standardize_date,
#     validate_items_json
# )

# def execute_transform_and_quality_pipeline():
#     """مرحلة المعالجة والجودة والتحميل النهائي (ELT Transform & Quality)"""
#     print("\n" + "="*60)
#     print("🚀 بدء تنفيذ مرحلة المعالجة والجودة والتحميل النهائي (ELT Transform & Quality)")
#     print("="*60)
    
#     db = get_database()
    
#     raw_count = db[settings.RAW_COLLECTION].count_documents({})
#     print(f"📊 إجمالي السجلات في الخام ({settings.RAW_COLLECTION}): {raw_count:,}")
    
#     if raw_count == 0:
#         print("⚠️ تنبيه: مجموعة البيانات الخام فارغة!")
#         return

#     raw_cursor = db[settings.RAW_COLLECTION].find({})
    
#     valid_batch = []
#     quarantine_batch = []
    
#     processed_count = 0
#     valid_count = 0
#     corrected_count = 0
#     quarantine_count = 0

#     for doc in raw_cursor:
#         processed_count += 1
#         raw_record = doc.get("raw_record", {})
#         corrections = []
#         is_quarantined = False
#         quarantine_reason = None

#         # 1. التحقق من المفتاح الأساسي واستبعاد المعرفات المفقودة (Quarantine)
#         order_id = raw_record.get("order_id")
#         if not order_id or str(order_id).strip() in ["", "NaN", "None"]:
#             is_quarantined = True
#             quarantine_reason = "MISSING_ORDER_ID"
#         else:
#             order_id = clean_whitespace(str(order_id))

#         customer_id = raw_record.get("customer_id")
#         if not customer_id or str(customer_id).strip() in ["", "NaN", "None"]:
#             is_quarantined = True
#             quarantine_reason = "MISSING_CUSTOMER_ID"
#         else:
#             customer_id = clean_whitespace(str(customer_id))

#         if is_quarantined:
#             quarantine_batch.append({
#                 "raw_id": doc.get("_id"),
#                 "raw_record": raw_record,
#                 "error_code": quarantine_reason,
#                 "error_details": "Essential identifier is missing",
#                 "processed_at": datetime.now().isoformat()
#             })
#             quarantine_count += 1
#             continue

#         # 2. تطبيق قواعد التنظيف الآلي مع حفظ أثر التصحيح (Audit Trail)
#         orig_date = raw_record.get("order_date")
#         clean_date = standardize_date(orig_date)
#         if clean_date != orig_date:
#             corrections.append({
#                 "field": "order_date",
#                 "original_value": orig_date,
#                 "corrected_value": clean_date,
#                 "rule_code": "R7_DATE_FORMAT"
#             })

#         status = clean_whitespace(raw_record.get("status"))
#         city = clean_whitespace(raw_record.get("city"))
#         district = clean_whitespace(raw_record.get("district"))
#         delivery_type = clean_whitespace(raw_record.get("delivery_type"))
#         payment_method = clean_whitespace(raw_record.get("payment_method"))
#         payment_status = clean_whitespace(raw_record.get("payment_status"))
#         customer_name = clean_whitespace(raw_record.get("customer_name"))

#         orig_phone = raw_record.get("customer_phone")
#         clean_ph = clean_phone(orig_phone)
#         if clean_ph != orig_phone:
#             corrections.append({
#                 "field": "customer_phone",
#                 "original_value": orig_phone,
#                 "corrected_value": clean_ph,
#                 "rule_code": "R5_PHONE_CLEAN"
#             })

#         orig_email = raw_record.get("customer_email")
#         clean_email, email_valid = validate_and_clean_email(orig_email)
#         if not email_valid:
#             is_quarantined = True
#             quarantine_reason = "INVALID_EMAIL_CORRUPTED"
#         elif clean_email != orig_email:
#             corrections.append({
#                 "field": "customer_email",
#                 "original_value": orig_email,
#                 "corrected_value": clean_email,
#                 "rule_code": "R6_EMAIL_FIX"
#             })

#         if is_quarantined:
#             quarantine_batch.append({
#                 "raw_id": doc.get("_id"),
#                 "raw_record": raw_record,
#                 "error_code": quarantine_reason,
#                 "error_details": "Corrupted email format",
#                 "processed_at": datetime.now().isoformat()
#             })
#             quarantine_count += 1
#             continue

#         delivery_cost = clean_numeric_field(raw_record.get("delivery_cost"))
#         payment_amount = clean_numeric_field(raw_record.get("payment_amount"))
        
#         orig_total = raw_record.get("total_amount")
#         total_amount = clean_numeric_field(orig_total)
#         converted_orig_total = convert_arabic_numerals(str(orig_total)).replace(',', '').strip()
#         if str(total_amount) != converted_orig_total:
#             corrections.append({
#                 "field": "total_amount",
#                 "original_value": orig_total,
#                 "corrected_value": total_amount,
#                 "rule_code": "R1_NUMERIC_CLEAN"
#             })

#         currency = standardize_currency(raw_record.get("currency"))

#         orig_items = raw_record.get("items_json")
#         clean_items, items_valid = validate_items_json(orig_items)
#         if not items_valid or not clean_items:
#             is_quarantined = True
#             quarantine_reason = "CORRUPTED_ITEMS_JSON" if not items_valid else "EMPTY_ITEMS"
        
#         if is_quarantined:
#             quarantine_batch.append({
#                 "raw_id": doc.get("_id"),
#                 "raw_record": raw_record,
#                 "error_code": quarantine_reason,
#                 "error_details": "Invalid or empty items JSON structure",
#                 "processed_at": datetime.now().isoformat()
#             })
#             quarantine_count += 1
#             continue

#         quality_status = "corrected" if len(corrections) > 0 else "valid"
#         if quality_status == "corrected":
#             corrected_count += 1
#         else:
#             valid_count += 1

#         validated_doc = {
#             "order_id": order_id,
#             "order_date": clean_date,
#             "status": status,
#             "customer_id": customer_id,
#             "customer_name": customer_name,
#             "customer_phone": clean_ph,
#             "customer_email": clean_email,
#             "city": city,
#             "district": district,
#             "delivery_type": delivery_type,
#             "delivery_cost": delivery_cost,
#             "payment_method": payment_method,
#             "payment_status": payment_status,
#             "payment_amount": payment_amount,
#             "currency": currency,
#             "total_amount": total_amount,
#             "items": clean_items,
#             "quality_status": quality_status,
#             "corrections": corrections,
#             "row_hash": doc.get("row_hash")
#         }

#         valid_batch.append(validated_doc)

#         if len(valid_batch) >= 2000:
#             _bulk_upsert_validated(db, valid_batch)
#             valid_batch = []
        
#         if len(quarantine_batch) >= 2000:
#             db[settings.QUARANTINE_COLLECTION].insert_many(quarantine_batch)
#             quarantine_batch = []

#     if valid_batch:
#         _bulk_upsert_validated(db, valid_batch)
#     if quarantine_batch:
#         db[settings.QUARANTINE_COLLECTION].insert_many(quarantine_batch)

#     print(f"\n✅ تمت المعالجة بنجاح تام!")
#     print(f"📊 إجمالي السجلات المفحوصة: {processed_count:,}")
#     print(f"✨ السجلات السليمة (Valid): {valid_count:,}")
#     print(f"🔧 السجلات المصححة (Corrected): {corrected_count:,}")
#     print(f"⚠️ السجلات المعزولة (Quarantine): {quarantine_count:,}")
#     print("="*60)

# def _bulk_upsert_validated(db, batch):
#     from pymongo import UpdateOne
#     operations = []
#     for doc in batch:
#         operations.append(
#             UpdateOne(
#                 {"order_id": doc["order_id"]},
#                 {"$set": doc},
#                 upsert=True
#             )
#         )
#     if operations:
#         db[settings.VALIDATED_COLLECTION].bulk_write(operations, ordered=False)

# def run_full_pipeline(file_path: str = None) -> dict:
#     """
#     نقطة التشغيل الموحدة (Unified Entry Point):
#     1. تفحص حجم الملف وتختار المحرك (Python Batch أو PySpark) لتحميل البيانات إلى الخام.
#     2. تستدعي خط الأنابيب (ETL) لمعالجة البيانات وتنظيفها وتطبيق قواعد الجودة.
#     """
#     if not file_path:
#         file_path = r"D:\midterm-data-pipeline\data\orders_huge_mixed_quality.csv"
    
#     if not os.path.exists(file_path):
#         error_msg = f"❌ خطأ: ملف البيانات غير موجود في المسار: {file_path}"
#         print(error_msg)
#         return {"status": "error", "message": error_msg}

#     file_size_bytes = os.path.getsize(file_path)
#     file_size_mb = file_size_bytes / (1024 * 1024)
#     file_size_gb = file_size_mb / 1024

#     print("=" * 60)
#     print("🎯 تشغيل خط الأنابيب الرئيسي والموجه التلقائي (Unified Pipeline)")
#     print("=" * 60)
#     print(f"📁 مسار الملف: {file_path}")
#     print(f"📊 حجم الملف: {file_size_gb:.2f} GB ({file_size_mb:.2f} MB)")
#     print(f"⚖️ الحد الفاصل المكتوب في الإعدادات: {settings.SMALL_FILE_THRESHOLD_MB} MB")
#     print("-" * 60)
    
#     # الخطوة الأولى: التوجيه الآلي وحمل البيانات للخام
#     if file_size_mb > settings.SMALL_FILE_THRESHOLD_MB:
#         print(f"⚡ القرار: استخدام محرك [Apache Spark]")
#         run_spark_processing(file_path, run_id="run_spark_huge_001")
#         engine_used = "Apache Spark"
#     else:
#         print(f"⚡ القرار: استخدام المحرك العادي [Python Batch]")
#         run_batch_processing(file_path, id_run="run_batch_raw_001")
#         engine_used = "Python Batch"
        
#     # الخطوة الثانية: تشغيل مرحلة المعالجة والجودة والتحميل النهائي
#     execute_transform_and_quality_pipeline()
    
#     return {
#         "status": "success",
#         "file_path": file_path,
#         "engine_used": engine_used
#     }

# if __name__ == "__main__":
#     run_full_pipeline()