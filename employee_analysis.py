# ============================================================
# PySpark Employee Analysis
# Reconstructed from Anaconda Prompt session
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ----------------------------------------------------------
# 1. Initialize Spark Session
# ----------------------------------------------------------
spark = SparkSession.builder \
    .appName("Employee Analysis") \
    .master("local[*]") \
    .getOrCreate()

# ----------------------------------------------------------
# 2. Load Data
# ----------------------------------------------------------
df = spark.read.csv(
    "C:/Users/Hp/Desktop/bdproject/employees.csv",
    header=True,
    inferSchema=True
)

df.show()
df.printSchema()

# ----------------------------------------------------------
# 3. Explore Columns
# ----------------------------------------------------------
print(df.columns)

# ----------------------------------------------------------
# 4. Rename Columns (original CSV had short names)
#    Name -> Employee_Name, Age -> Employee_Age, etc.
# ----------------------------------------------------------
df = df.withColumnRenamed("Name", "Employee_Name") \
       .withColumnRenamed("Age", "Employee_Age") \
       .withColumnRenamed("City", "Employee_City") \
       .withColumnRenamed("Salary", "Employee_Salary") \
       .withColumnRenamed("Gender", "Employee_Gender")

df.printSchema()

# ----------------------------------------------------------
# 5. Drop Columns
# ----------------------------------------------------------
# Drop a single column
df = df.drop("Employee_Gender")

# NOTE: The pandas-style drop(["col"], axis=1) does NOT work in PySpark.
# Correct PySpark syntax: df.drop("col1", "col2")  or  df.drop(*["col1","col2"])
# Example (not re-run, just for reference):
# df = df.drop("Employee_Gender", "Employee_City")

df.show()
df.printSchema()

# ----------------------------------------------------------
# 6. Rename a column with withColumnRenamed
# ----------------------------------------------------------
# Example shown in session (not persisted):
# df = df.withColumnRenamed("Salary", "Income")

# ----------------------------------------------------------
# 7. Distinct Cities
# ----------------------------------------------------------
df.select("Employee_City").distinct().show()

# ----------------------------------------------------------
# 8. Row Count
# ----------------------------------------------------------
print("Total rows:", df.count())   # 50

# ----------------------------------------------------------
# 9. Aggregations
# ----------------------------------------------------------
# Max salary
df.agg({"Employee_Salary": "max"}).show()

# Min salary
df.agg({"Employee_Salary": "min"}).show()

# Average salary
df.select(F.avg("Employee_Salary")).show()

# Multiple aggregations in one query
df.select(
    F.count("Employee_Salary").alias("Total_Employees"),
    F.max("Employee_Salary").alias("Max_Salary"),
    F.min("Employee_Salary").alias("Min_Salary"),
    F.avg("Employee_Salary").alias("Average_Salary")
).show()

# ----------------------------------------------------------
# 10. Handle Missing Values
# ----------------------------------------------------------
# Drop rows where ALL columns are null
df = df.na.drop(how="all")

# Drop rows where Employee_Salary is null
df = df.na.drop(subset=["Employee_Salary"])

# Fill nulls with 0
df = df.na.fill(0)

# Drop rows with ANY null (default behaviour)
df = df.na.drop()

df.show()

# ----------------------------------------------------------
# 11. Select Specific Columns
# ----------------------------------------------------------
df.select("Employee_Name", "Employee_Salary").show()

# ----------------------------------------------------------
# 12. Filter Rows
# ----------------------------------------------------------
df.filter(df.Employee_Salary > 50000).show()

# ----------------------------------------------------------
# 13. Group By
# ----------------------------------------------------------
# Count employees per department
df.groupBy("Department").count().show()

# Average salary per department
df.groupBy("Department").avg("Employee_Salary").show()

# ----------------------------------------------------------
# 14. Order / Sort
# ----------------------------------------------------------
df.orderBy(df.Employee_Salary.desc()).show()

# ----------------------------------------------------------
# 15. Add a New Column (withColumn)
# ----------------------------------------------------------
df.withColumn("Bonus", df.Employee_Salary + 5000).show()

# ----------------------------------------------------------
# 16. Join Example
#     (Requires a second DataFrame; shown as reference)
# ----------------------------------------------------------
# dept_df = spark.createDataFrame(
#     [("Engineering", "NYC"), ("Marketing", "LA")],
#     ["Department", "HQ_City"]
# )
# joined_df = df.join(dept_df, "Department", "inner")
# joined_df.show()

# NOTE: df1.join(df2, ...) requires both DataFrames to exist.
# The session showed a NameError because df1/df2 were not defined.
# Use the variable names of your actual DataFrames.

# ----------------------------------------------------------
# 17. Stop Spark Session
# ----------------------------------------------------------
spark.stop()
