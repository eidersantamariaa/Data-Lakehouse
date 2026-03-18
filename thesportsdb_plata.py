from pyspark.sql.functions import col, coalesce,lit
from limpieza import clean_basic, normalize_text_udf, normalize_date_udf, normalize_height_udf, normalize_weight_udf, normalize_currency_udf

NAMESPACE = "thesportsdb"

SILVER_TRANSFORMS = {
    "leagues": lambda df: clean_basic(df).select(
        col("idLeague"),
        normalize_text_udf(col("strLeague")).alias("strLeague"),
        normalize_text_udf(col("strCountry")).alias("country"),
        col('intFormedYear').cast("int")
    ).fillna(""),
    "teams": lambda df: clean_basic(df).select(
        col("idTeam"),
        normalize_text_udf(col("strTeam")).alias("strTeam"),
        col("intFormedYear").cast("int"),
        normalize_text_udf(col("strStadium")).alias("strStadium"),
        col("intStadiumCapacity").cast("int"),
        normalize_text_udf(col("strLocation")).alias("strLocation"),
        normalize_text_udf(col("strCountry")).alias("strCountry")
    ).fillna(""),
    "players": lambda df: clean_basic(df).select(
        col("idPlayer"),
        normalize_text_udf(col("strPlayer")).alias("strPlayer"),
        normalize_date_udf(col("dateBorn")).alias("dateBorn"),
        normalize_text_udf(col("strStatus")).alias("strStatus"),
        normalize_height_udf(col("strHeight")).alias("intHeight"),
        normalize_text_udf(col("strSide")).alias("strSide"),
        normalize_weight_udf(col("strWeight")).cast("string").alias("strWeight"),
        col("strNumber"),
        normalize_text_udf(col("strBirthLocation")).alias("strBirthLocation"),
        normalize_text_udf(col("strNationality")).alias("strNationality"),
        normalize_text_udf(col("strPosition")).alias("strPosition"),
        normalize_text_udf(normalize_currency_udf(col("strSigning"))).alias("strSigning (€)"),
        normalize_text_udf(normalize_currency_udf(col("strWage"))).alias("strWage (€)")
    ).fillna("")
}