from pyspark.sql.functions import col, when, trim, regexp_replace, concat_ws, array_join, concat, lit
from limpieza import clean_basic, normalize_text_udf, normalize_date_udf, normalize_height_udf, normalize_weight_udf, normalize_currency_udf

NAMESPACE = "mapping"

SILVER_TRANSFORMS = {
    "players": lambda df: clean_basic(df).select(
        col("id"),
        normalize_text_udf(col("name")).alias("name"),
        normalize_text_udf(col("fullName")).alias("fullName"),
        normalize_date_udf(col("dateOfBirth")).alias("dateOfBirth"),
        normalize_text_udf(col("nameInHomeCountry")).alias("nameInHomeCountry"),
        normalize_height_udf(col("height")).alias("height"),
        normalize_text_udf(col("foot")).alias("foot"),
        col("shirtNumber"),
        normalize_text_udf(normalize_currency_udf(col("marketValue"))).alias("marketValue (€)"),
        col("isRetired").cast("boolean"),
        normalize_text_udf(array_join(col("citizenship"), ", ")).alias("citizenship"),

        normalize_text_udf(when(col("placeOfBirth.city").isNotNull() & col("placeOfBirth.country").isNotNull(),
            concat(col("placeOfBirth.city"), lit(", "), col("placeOfBirth.country"))
        ).when(col("placeOfBirth.country").isNull(), 
            col("placeOfBirth.city")
        ).otherwise(
            col("placeOfBirth.country")
        )).alias("placeOfBirth"),

        normalize_text_udf(trim(regexp_replace(
            concat_ws(", ", col("position.main"), array_join(col("position.other"), ", ")),
            ",\\s*$", ""  # quita coma y espacios al final
        ))).alias("positions"),
        
        col("club.id").alias("clubId"),
        normalize_text_udf(col("club.name")).alias("clubName"),
        normalize_date_udf(col("club.joined")).alias("clubJoined"),
        normalize_date_udf(col("club.contractExpires")).alias("contractExpires"),
    ).fillna("")
}