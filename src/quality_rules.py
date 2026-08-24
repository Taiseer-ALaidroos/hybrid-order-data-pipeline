from pyspark.sql.functions import col, when, lit, array, expr, size, trim

def apply_taysir_quality_rules(df_raw):
    """
    تطبيق قواعد الجودة الأساسية والمرنة مع تنظيف أسماء الأعمدة من الرموز المخفية (BOM)
    """
    if "raw_record" in df_raw.columns:
        df_source = df_raw.select("run_id", "source_file", "source_row_number", "ingested_at", "engine_used", "raw_record.*", "raw_record")
    else:
        df_source = df_raw

    # تنظيف أسماء الأعمدة لمنع مشاكل الحرف المخفي في الـ CSV
    for c in df_source.columns:
        clean_c = c.replace('\ufeff', '').strip()
        if clean_c != c:
            df_source = df_source.withColumnRenamed(c, clean_c)

    cols = df_source.columns

    df_with_all_rules = df_source.withColumn(
        "raw_errors_array",
        array(
            # 1. رقم الطلب (إلزامي وغير فارغ)
            when(col("order_id").isNull() | (trim(col("order_id")) == ""), lit("Order ID is Empty")).otherwise(lit(None)) if "order_id" in cols else lit(None),
            
            # 2. تاريخ الطلب (إلزامي وغير فارغ)
            when(col("order_date").isNull() | (trim(col("order_date")) == ""), lit("Order Date is Empty")).otherwise(lit(None)) if "order_date" in cols else lit(None),
            
            # 3. حالة الطلب (نطاق محدد باللغة العربية مع تنظيف المسافات)
            when(~trim(col("status")).isin("مؤكد", "قيد الانتظار", "مرتجع", "قيد الشحن", "تم التسليم"), lit("Unrecognized Status")).otherwise(lit(None)) if "status" in cols else lit(None),
            
            # 4. معرف العميل (إلزامي وغير فارغ)
            when(col("customer_id").isNull() | (trim(col("customer_id")) == ""), lit("Customer ID is Empty")).otherwise(lit(None)) if "customer_id" in cols else lit(None),
            
            # 5. رقم الهاتف (تحقق مرن يقبل الأرقام الدولية والمحلية من 7 إلى 15 خانة)
            when(col("customer_phone").isNotNull() & ~trim(col("customer_phone")).rlike(r"^\+?[0-9]{7,15}$"), lit("Phone Format Error")).otherwise(lit(None)) if "customer_phone" in cols else lit(None),
            
            # 6. البريد الإلكتروني (تحقق مرن وصحيح)
            when(col("customer_email").isNotNull() & ~trim(col("customer_email")).rlike(r"^[\w\.-]+@[\w\.-]+\.\w+$"), lit("Email Format Error")).otherwise(lit(None)) if "customer_email" in cols else lit(None),
            
            # 7. تكلفة التوصيل (يجب ألا تكون بالسالب)
            when(col("delivery_cost").cast("double") < 0, lit("Delivery Cost Cannot Be Negative")).otherwise(lit(None)) if "delivery_cost" in cols else lit(None),
            
            # 8. مبلغ الدفع أو المبلغ الإجمالي (يجب أن يكون أكبر من الصفر)
            when(col("total_amount").cast("double") <= 0, lit("Payment Must Be Greater Than Zero")).otherwise(lit(None)) if "total_amount" in cols else (
                 when(col("payment_amount").cast("double") <= 0, lit("Payment Must Be Greater Than Zero")).otherwise(lit(None)) if "payment_amount" in cols else lit(None)
            )
        )
    )

    # تنظيف المصفوفة واستخراج السجلات السليمة
    df_validated = df_with_all_rules.withColumn(
        "detected_errors",
        expr("filter(raw_errors_array, x -> x is not null)")
    ).drop("raw_errors_array")

    # السجل يعتبر سليماً (True) فقط إذا كان عدد الأخطاء المكتشفة يساوي صفر
    df_final_results = df_validated.withColumn(
        "is_clean_record",
        when(size(col("detected_errors")) == 0, lit(True)).otherwise(lit(False))
    )

    return df_final_results




# from pyspark.sql.functions import col, when, lit, array, expr, size, trim

# def apply_taysir_quality_rules(df_raw):
#     """
#     تطبيق قواعد الجودة الأساسية والمرنة مع تنظيف أسماء الأعمدة من الرموز المخفية (BOM)
#     """
#     if "raw_record" in df_raw.columns:
#         df_source = df_raw.select("run_id", "source_file", "source_row_number", "ingested_at", "engine_used", "raw_record.*", "raw_record")
#     else:
#         df_source = df_raw

#     # تنظيف أسماء الأعمدة لمنع مشاكل الحرف المخفي في الـ CSV
#     for c in df_source.columns:
#         clean_c = c.replace('\ufeff', '').strip()
#         if clean_c != c:
#             df_source = df_source.withColumnRenamed(c, clean_c)

#     cols = df_source.columns

#     df_with_all_rules = df_source.withColumn(
#         "raw_errors_array",
#         array(
#             # 1. رقم الطلب (إلزامي وغير فارغ)
#             when(col("order_id").isNull() | (trim(col("order_id")) == ""), lit("Order ID is Empty")).otherwise(lit(None)) if "order_id" in cols else lit(None),
            
#             # 2. تاريخ الطلب (إلزامي وغير فارغ)
#             when(col("order_date").isNull() | (trim(col("order_date")) == ""), lit("Order Date is Empty")).otherwise(lit(None)) if "order_date" in cols else lit(None),
            
#             # 3. حالة الطلب (نطاق محدد باللغة العربية مع تنظيف المسافات)
#             when(~trim(col("status")).isin("مؤكد", "قيد الانتظار", "مرتجع", "قيد الشحن", "تم التسليم"), lit("Unrecognized Status")).otherwise(lit(None)) if "status" in cols else lit(None),
            
#             # 4. معرف العميل (إلزامي وغير فارغ)
#             when(col("customer_id").isNull() | (trim(col("customer_id")) == ""), lit("Customer ID is Empty")).otherwise(lit(None)) if "customer_id" in cols else lit(None),
            
#             # 5. رقم الهاتف (تحقق مرن يقبل الأرقام الدولية والمحلية من 7 إلى 15 خانة)
#             when(col("customer_phone").isNotNull() & ~trim(col("customer_phone")).rlike(r"^\+?[0-9]{7,15}$"), lit("Phone Format Error")).otherwise(lit(None)) if "customer_phone" in cols else lit(None),
            
#             # 6. البريد الإلكتروني (تحقق مرن وصحيح)
#             when(col("customer_email").isNotNull() & ~trim(col("customer_email")).rlike(r"^[\w\.-]+@[\w\.-]+\.\w+$"), lit("Email Format Error")).otherwise(lit(None)) if "customer_email" in cols else lit(None),
            
#             # =========================================================================================
#             # 🛑 [تحديد مكان المشكلة الكبرى وتعديلها]:
#             # المشكلة السابقة: استخدام .cast("double") بشكل مباشر على الحقول الرقمية وهي تحتمل أن تكون 
#             # فارغة (Null أو نص فارغ "")، مما يسبب فشل التنفيذ في Spark ويعتبر السجل تالفاً ويوجهه للحجر الصحي.
#             # التعديل: إضافة شرط التحقق من عدم الفراغ (isNotNull و trim != "") قبل إجراء الـ Cast الآمن.
#             # =========================================================================================
            
#             # 7. تكلفة التوصيل (يجب ألا تكون بالسالب)
#             when(col("delivery_cost").isNotNull() & (trim(col("delivery_cost")) != "") & (col("delivery_cost").cast("double") < 0), lit("Delivery Cost Cannot Be Negative")).otherwise(lit(None)) if "delivery_cost" in cols else lit(None),
            
#             # 8. مبلغ الدفع أو المبلغ الإجمالي (يجب أن يكون أكبر من الصفر)
#             when(col("total_amount").isNotNull() & (trim(col("total_amount")) != "") & (col("total_amount").cast("double") <= 0), lit("Payment Must Be Greater Than Zero")).otherwise(lit(None)) if "total_amount" in cols else (
#                  when(col("payment_amount").isNotNull() & (trim(col("payment_amount")) != "") & (col("payment_amount").cast("double") <= 0), lit("Payment Must Be Greater Than Zero")).otherwise(lit(None)) if "payment_amount" in cols else lit(None)
#             )
#         )
#     )

#     # تنظيف المصفوفة واستخراج السجلات السليمة
#     df_validated = df_with_all_rules.withColumn(
#         "detected_errors",
#         expr("filter(raw_errors_array, x -> x is not null)")
#     ).drop("raw_errors_array")

#     # السجل يعتبر سليماً (True) فقط إذا كان عدد الأخطاء المكتشفة يساوي صفر
#     df_final_results = df_validated.withColumn(
#         "is_clean_record",
#         when(size(col("detected_errors")) == 0, lit(True)).otherwise(lit(False))
#     )

#     return df_final_results




# from pyspark.sql.functions import col, when, lit, array, expr, size

# def apply_taysir_quality_rules(df_raw):
#     """
#     تطبيق 8 قواعد جودة على البيانات الخام
#     واستخراج الأخطاء في مصفوفة مستقلة لكل سجل
#     """
#     # 1. تجهيز البيانات واستخراجها من الحقل الرئيسي
#     if "raw_record" in df_raw.columns:
#         df_source = df_raw.select("run_id", "source_file", "source_row_number", "ingested_at", "engine_used", "raw_record.*", "raw_record")
#     else:
#         df_source = df_raw

#     # 2. فحص السجلات وتجميع الأخطاء في مصفوفة (8 شروط فقط)
#     df_with_all_rules = df_source.withColumn(
#         "raw_errors_array",
#         array(
#             # 1. رقم الطلب (إلزامي)
#             when(col("order_id").isNull() | (col("order_id") == ""), lit("Order ID is Empty")).otherwise(lit(None)),
            
#             # 2. تاريخ الطلب (إلزامي)
#             when(col("order_date").isNull() | (col("order_date") == ""), lit("Order Date is Empty")).otherwise(lit(None)),
            
#             # 3. حالة الطلب (نطاق محدد باللغة العربية)
#             when(~col("status").isin("مؤكد", "قيد الانتظار", "مرتجع", "قيد الشحن", "تم التسليم"), lit("Unrecognized Status")).otherwise(lit(None)),
            
#             # 4. معرف العميل (إلزامي)
#             when(col("customer_id").isNull() | (col("customer_id") == ""), lit("Customer ID is Empty")).otherwise(lit(None)),
            
#             # 5. رقم الهاتف (التحقق من الصيغة عبر التعبير النمطي)
#             when(~col("customer_phone").rlike(r"^\+?[0-9]{7,15}$"), lit("Phone Format Error")).otherwise(lit(None)),
            
#             # 6. البريد الإلكتروني (التحقق من الصيغة)
#             when(~col("customer_email").rlike(r"^[\w\.-]+@[\w\.-]+\.\w+$"), lit("Email Format Error")).otherwise(lit(None)),
            
#             # 7. تكلفة التوصيل (يجب ألا تكون بالسالب)
#             when(col("delivery_cost").cast("double") < 0, lit("Delivery Cost Cannot Be Negative")).otherwise(lit(None)),
            
#             # 8. مبلغ الدفع (يجب أن يكون أكبر من الصفر)
#             when(col("payment_amount").cast("double") <= 0, lit("Payment Must Be Greater Than Zero")).otherwise(lit(None))
#         )
#     )

#     # 3. تنظيف المصفوفة من القيم الفارغة للاحتفاظ بالأخطاء الفعلية فقط
#     df_validated = df_with_all_rules.withColumn(
#         "detected_errors",
#         expr("filter(raw_errors_array, x -> x is not null)")
#     ).drop("raw_errors_array")

#     # 4. السجل يعتبر سليماً (True) فقط إذا كان عدد الأخطاء المكتشفة يساوي صفر
#     df_final_results = df_validated.withColumn(
#         "is_clean_record",
#         when(size(col("detected_errors")) == 0, lit(True)).otherwise(lit(False))
#     )

#     return df_final_results