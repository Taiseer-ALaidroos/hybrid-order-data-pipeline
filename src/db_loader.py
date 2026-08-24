import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config import settings
from pymongo import MongoClient

def get_mongo_client():
    """إنشاء وإرجاع اتصال مع قاعدة بيانات MongoDB باستخدام الرابط من ملف الإعدادات"""
    client = MongoClient(settings.MONGO_URI)
    return client

def get_database():
    """الحصول على كائن قاعدة البيانات مباشرة بناءً على الإعدادات الصحيحة"""
    client = get_mongo_client()
    return client[settings.DB_NAME]






# import sys
# import os
# # إضافة المجلد الرئيسي للمشروع إلى مسارات بايثون
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# from config import settings
# from pymongo import MongoClient

# def get_mongo_client():
#     """إنشاء وإرجاع اتصال مع قاعدة بيانات MongoDB باستخدام الرابط من ملف الإعدادات"""
#     client = MongoClient(settings.MONGO_URI)
#     return client

# def get_database():
#     """الحصول على كائن قاعدة البيانات مباشرة بناءً على الإعدادات الصحيحة"""
#     client = get_mongo_client()
#     # هنا التعديل: نستخدم settings.DB_NAME الذي عرفته في ملف الإعدادات
#     return client[settings.DB_NAME]

# def load_raw_batch(batch_data):
#     """إدخال دفعة من البيانات الخام إلى مجموعة orders_raw"""
#     if not batch_data:
#         return
#     db = get_database()
#     db.orders_raw.insert_many(batch_data)

# def load_processed_batch(valid_data, quarantine_data):
#     """حفظ البيانات بعد التنظيف: السليمة في validated والمرفوضة في quarantine"""
#     db = get_database()
    
#     if valid_data:
#         # استخدام إدخال البيانات السليمة
#         db.orders_validated.insert_many(valid_data)
        
#     if quarantine_data:
#         # استخدام إدخال البيانات المعزولة
#         db.orders_quarantine.insert_many(quarantine_data)

# # يمكنك إضافة أي دوال أخرى تحتاجها هنا...