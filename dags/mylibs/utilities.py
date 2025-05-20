from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

# Define the UDF
@udf(returnType=DoubleType())
def is_numeric_udf(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return None
