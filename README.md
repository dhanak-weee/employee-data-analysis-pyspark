# Employee Analysis through Apache Spark

A practical PySpark project for performing employee dataset analysis using Apache Spark DataFrames.

## 📌 Project Overview

This project demonstrates basic to intermediate data analysis operations using PySpark on an employee dataset. It covers loading data, filtering, grouping, aggregation, sorting, handling null values, and creating new columns.

The goal of this project is to help beginners understand how Apache Spark works for big data processing and analytics.

---

# 🛠 Technologies Used

- Python
- Apache Spark (PySpark)
- DataFrames API
- CSV Dataset

---

# 📂 Features Implemented

## 1. Load Dataset
- Read CSV files using PySpark
- Automatically infer schema
- Display rows and schema

### Functions Used
```python
spark.read.csv()
df.show()
df.printSchema()
