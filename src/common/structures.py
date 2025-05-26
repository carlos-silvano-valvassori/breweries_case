from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)


schema = StructType(
        [
            StructField("id", StringType(), nullable=True),
            StructField("name", StringType(), nullable=True),
            StructField("brewery_type", StringType(), nullable=True),
            StructField("address_1", StringType(), nullable=True),
            StructField("address_2", StringType(), nullable=True),
            StructField("address_3", StringType(), nullable=True),
            StructField("city", StringType(), nullable=True),
            StructField("state_province", StringType(), nullable=True),
            StructField("postal_code", StringType(), nullable=True),
            StructField("country", StringType(), nullable=True),
            StructField("longitude", StringType(), nullable=True),
            StructField("latitude", StringType(), nullable=True),
            StructField("phone", StringType(), nullable=True),
            StructField("website_url", StringType(), nullable=True),
            StructField("state", StringType(), nullable=True),
            StructField("street", StringType(), nullable=True),
        ]
)
