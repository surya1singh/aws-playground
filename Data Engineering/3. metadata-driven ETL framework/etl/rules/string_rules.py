from pyspark.sql import functions as F

def lower_trim(col_name: str):
    return F.lower(F.trim(F.col(col_name)))

def upper_trim(col_name: str):
    return F.upper(F.trim(F.col(col_name)))
