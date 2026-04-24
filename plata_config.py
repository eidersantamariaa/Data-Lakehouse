from pyspark.sql.functions import col, when, trim, regexp_replace, concat_ws, array_join, concat, lit
from limpieza import clean_basic, normalize_text_udf, normalize_date_udf, normalize_height_udf, normalize_weight_udf, normalize_currency_udf, normalize_position_udf

NAMESPACE = "mapping"

SILVER_TRANSFORMS = {
    "players": lambda df: clean_basic(df).select(
        col("id_transfermarkt"),
        col("id_thesportsdb"),
        when(col("tm_name").isNotNull(), normalize_text_udf(col("tm_name"))).otherwise(normalize_text_udf(col("ts_strPlayer"))).alias("name"),
        when(col("tm_fullName").isNotNull(), normalize_text_udf(col("tm_fullName"))).otherwise(normalize_text_udf(col("ts_strPlayerAlternate"))).alias("fullName"),
        when(col("tm_dateOfBirth").isNotNull(), normalize_text_udf(col("tm_dateOfBirth"))).otherwise(normalize_text_udf(col("ts_dateBorn"))).alias("dateOfBirth"),
        when(col("tm_height").isNotNull(), normalize_height_udf(col("tm_height"))).otherwise(normalize_height_udf(col("ts_strHeight"))).alias("height"),
        when(col("tm_foot").isNotNull(), normalize_text_udf(col("tm_foot"))).otherwise(normalize_text_udf(col("ts_strSide"))).alias("foot"),
        when(col("tm_shirtNumber").isNotNull(), normalize_text_udf(col("tm_shirtNumber"))).otherwise(normalize_text_udf(col("ts_strNumber"))).alias("Number"),
        normalize_text_udf(normalize_currency_udf(col("tm_marketValue"))).alias("marketValue (€)"),
        when(col("tm_isRetired").isNotNull(), normalize_text_udf(col("tm_isRetired"))).otherwise(normalize_text_udf(col("ts_strStatus"))).alias("isRetired"),
        normalize_text_udf(normalize_weight_udf(col("ts_strWeight"))).alias("weight (kg)"),
        when(col("tm_citizenship").isNotNull(), normalize_text_udf(array_join(col("tm_citizenship"), ", "))).otherwise(normalize_text_udf(col("ts_strNationality"))).alias("citizenship"),
        when(col("tm_placeOfBirth").isNotNull(), normalize_text_udf(array_join(col("tm_placeOfBirth"), ", "))).otherwise(normalize_text_udf(col("ts_strBirthLocation"))).alias("placeOfBirth"),
        when(col("tm_position").isNotNull(), normalize_text_udf(normalize_position_udf(array_join(col("tm_position"), ", ")))).otherwise(normalize_text_udf(col("ts_strPosition"))).alias("position"),

        when(col("tm_club").isNotNull(), col("tm_club")[2]).otherwise(col("ts_idTeam")).alias("clubId"),
        when(col("tm_club").isNotNull(), normalize_text_udf(col("tm_club")[4]))
            .otherwise(normalize_text_udf(col("ts_strTeam"))).alias("clubName"),
        when(col("tm_club").isNotNull(), normalize_date_udf(col("tm_club")[3]))
            .otherwise(normalize_date_udf(col("ts_dateSigned"))).alias("clubJoined"),
        when(col("tm_club").isNotNull(), normalize_date_udf(col("tm_club")[0]))
            .otherwise(lit("")).alias("contractExpires"),
        
    ).fillna("")
}