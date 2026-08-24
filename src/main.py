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


