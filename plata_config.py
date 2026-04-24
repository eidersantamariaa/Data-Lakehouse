from pyspark.sql.functions import col, when, trim, regexp_replace, concat_ws, array_join, concat, lit
from limpieza import clean_basic, normalize_text_udf, normalize_date_udf, normalize_height_udf, normalize_weight_udf, normalize_currency_udf

NAMESPACE = "mapping"

SILVER_TRANSFORMS = {
    "players": lambda df: clean_basic(df).select(
        col("id_transfermarkt"),
        col("id_thesportsdb"),
        normalize_text_udf(col("tm_name")).alias("name"),
        normalize_text_udf(col("tm_fullName")).alias("fullName"),
        normalize_date_udf(col("tm_dateOfBirth")).alias("dateOfBirth"),
        normalize_text_udf(col("tm_nameInHomeCountry")).alias("nameInHomeCountry"),
        normalize_height_udf(col("tm_height")).alias("height"),
        normalize_text_udf(col("tm_foot")).alias("foot"),
        col("tm_shirtNumber").alias("shirtNumber"),
        normalize_text_udf(normalize_currency_udf(col("tm_marketValue"))).alias("marketValue (€)"),
        col("tm_isRetired").cast("boolean").alias("isRetired"),
        normalize_text_udf(array_join(col("tm_citizenship"), ", ")).alias("citizenship"),

        normalize_text_udf(array_join(col("tm_placeOfBirth"), ", ")).alias("placeOfBirth"),

        normalize_text_udf(array_join(col("tm_position"), ", ")).alias("position"),

        col("tm_club").getItem("id").alias("clubId"),
        normalize_text_udf(col("tm_club").getItem("name")).alias("clubName"),
        normalize_date_udf(col("tm_club").getItem("joined")).alias("clubJoined"),
        normalize_date_udf(col("tm_club").getItem("contractExpires")).alias("contractExpires"),
    ).fillna("")
}