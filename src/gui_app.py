import streamlit as st
import json
import os
import sys
import subprocess
import pandas as pd
from pymongo import MongoClient

# 1. إعدادات الصفحة
st.set_page_config(page_title="Midterm Data Pipeline", page_icon="🚀", layout="wide")
st.title("🚀 لوحة تحكم مسار البيانات (Data Pipeline Dashboard)")
st.markdown("---")

# ==========================================
# 2. قسم إدخال البيانات (ملف محلي أو رابط) وتشغيل المسار
# ==========================================
st.subheader("▶️ معالجة بيانات جديدة عبر المشغل المركزي (Main)")

input_tab1, input_tab2 = st.tabs(["📁 إدخال مسار ملف محلي", "🔗 إدخال رابط (URL)"])

target_input = ""

with input_tab1:
    local_path = st.text_input("أدخل المسار الكامل لملف البيانات (مثال: D:\\data\\orders.csv):", key="local_file_input")
    if local_path:
        target_input = local_path

with input_tab2:
    url_path = st.text_input("أدخل رابط البيانات (URL):", key="url_input")
    if url_path:
        target_input = url_path

col_space, col_btn = st.columns([3, 1])
with col_btn:
    st.write("")
    run_btn = st.button("🚀 بدء المعالجة الكاملة", use_container_width=True)

if run_btn:
    clean_path = target_input.strip().replace('"', '') 
    if not clean_path:
        st.warning("⚠️ الرجاء إدخال مسار الملف أو الرابط أولاً!")
    elif not clean_path.startswith("http") and not os.path.exists(clean_path):
        st.error("❌ الملف غير موجود! تأكد من صحة المسار واسم الملف.")
    else:
        main_script_path = os.path.join("src", "main.py")
        
        with st.status("🔄 جاري تنفيذ مسار العمل عبر المشغل المركزي...", expanded=True) as status:
            try:
                env = os.environ.copy()
                env["PYTHONIOENCODING"] = "utf-8"

                st.write("🔍 [المرحلة 1]: فحص الملف وتوجيهه للمحرك المناسب...")
                
                process = subprocess.Popen(
                    [sys.executable, main_script_path],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    encoding="utf-8",
                    errors="ignore",
                    env=env
                )
                
                stdout, stderr = process.communicate(input=f"{clean_path}\n")
                
                if stdout:
                    for line in stdout.splitlines():
                        if "المرحلة" in line or "تشغيل" in line or "جاري" in line:
                            st.info(line)
                        elif "✅" in line:
                            st.success(line)
                        elif "❌" in line:
                            st.error(line)
                        else:
                            st.write(line)

                if process.returncode == 0:
                    status.update(label="✅ اكتملت جميع مراحل المعالجة بنجاح!", state="complete", expanded=False)
                    st.success("🎉 تمت معالجة البيانات وتحديث التقارير ولوحة المعلومات بالأسفل بنجاح تام!")
                else:
                    status.update(label="❌ حدث خطأ أثناء تنفيذ المسار!", state="error", expanded=True)
                    st.error("تفاصيل الخطأ:")
                    st.code(stderr or stdout)
                    
            except Exception as e:
                status.update(label="❌ فشل النظام في تشغيل المشغل!", state="error", expanded=True)
                st.error(f"❌ خطأ غير متوقع: {e}")

st.markdown("---")

# ==========================================
# 3. قسم قراءة التقارير وعرض الإحصائيات (محدث لضمان قراءة صحيحة ودقيقة)
# ==========================================
report_path = os.path.join("reports", "results.json")

try:
    if os.path.exists(report_path):
        with open(report_path, 'r', encoding='utf-8') as f:
            results_data = json.load(f)
        
        # التعامل مع ما إذا كان التقرير عبارة عن قائمة سجلات أو كائن مفرد
        if isinstance(results_data, list) and len(results_data) > 0:
            results = results_data[-1]
        elif isinstance(results_data, dict):
            results = results_data
        else:
            results = {}

        st.subheader(f"📊 نتائج آخر معالجة (المحرك المستخدم: {results.get('engine_used', results.get('engine', 'N/A'))})")
        
        # عرض المعرفات والبيانات الأساسية للتشغيل
        run_id_val = results.get('run_id', 'N/A')
        file_name_val = results.get('file_name', results.get('source_file', 'N/A'))
        st.caption(id_info := f"🆔 معرف التشغيل (Run ID): `{run_id_val}` | 📁 الملف المعالج: `{file_name_val}`")

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("إجمالي المقروءة / الخام", results.get("rows_read", results.get("raw_loaded", 0)))
        col2.metric("✅ السجلات السليمة", results.get("valid_count", 0))
        col3.metric("🛠️ السجلات المصححة", results.get("corrected_count", 0))
        col4.metric("🚨 السجلات المعزولة", results.get("quarantine_count", 0))
        
        elapsed_val = results.get("elapsed_seconds", results.get("elapsed_time", 0))
        throughput_val = results.get("throughput", 0)
        st.info(f"⏱️ الوقت المستغرق: {elapsed_val} ثانية | ⚡ سرعة التدفق: {throughput_val} سجل/ثانية")
    else:
        st.warning("⚠️ ملف التقرير غير موجود (`reports/results.json`). قم بتشغيل معالجة جديدة لتوليد التقرير تلقائياً.")

except Exception as e:
    st.error(f"حدث خطأ أثناء قراءة ملف التقرير: {e}")

st.markdown("---")

# ==========================================
# 4. الاتصال بقاعدة البيانات لعرض الأرشيف والبيانات الحية
# ==========================================
st.subheader("🗄️ الأرشيف والبيانات الحية في MongoDB")

@st.cache_resource
def get_db_connection():
    client = MongoClient("localhost", 27017) 
    return client["midterm_db"] 

try:
    db = get_db_connection()
    tab1, tab2, tab3 = st.tabs(["📥 البيانات الخام (Raw)", "✅ السجلات المعتمدة (Validated)", "🚨 السجلات المعزولة (Quarantine)"])
    
    with tab1:
        raw_data = list(db["orders_raw"].find({}, {"_id": 0}).limit(10))
        if raw_data:
            st.dataframe(pd.DataFrame(raw_data), use_container_width=True)
        else:
            st.info("لا توجد بيانات خام حتى الآن.")

    with tab2:
        valid_data = list(db["orders_validated"].find({}, {"_id": 0}).limit(10))
        if valid_data:
            st.dataframe(pd.DataFrame(valid_data), use_container_width=True)
        else:
            st.info("لا توجد بيانات معتمدة حتى الآن.")
            
    with tab3:
        quarantine_data = list(db["orders_quarantine"].find({}, {"_id": 0}).limit(10))
        if quarantine_data:
            st.dataframe(pd.DataFrame(quarantine_data), use_container_width=True)
        else:
            st.info("لا توجد بيانات معزولة حتى الآن.")

except Exception as e:
    st.error(f"❌ حدث خطأ أثناء الاتصال بقاعدة البيانات: {e}")