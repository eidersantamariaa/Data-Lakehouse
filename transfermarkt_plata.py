from pyspark.sql.functions import col, when, trim, regexp_replace, concat_ws, array_join, concat, lit
from limpieza import clean_basic, normalize_text_udf, normalize_date_udf, normalize_height_udf, normalize_weight_udf, normalize_currency_udf

NAMESPACE = "transfermarkt"

SILVER_TRANSFORMS = {
    "leagues": lambda df: clean_basic(df).select(
        col("id"),
        normalize_text_udf(col("name")).alias("name"),
        normalize_text_udf(col("country")).alias("country"),
        normalize_text_udf(col("continent")).alias("continent"),
        col("clubs").cast("int"),
        col("players").cast("int"),
        normalize_text_udf(normalize_currency_udf(col("totalMarketValue"))).alias("totalMarketValue (€)"),
        normalize_text_udf(normalize_currency_udf(col("meanMarketValue"))).alias("meanMarketValue (€)"),
    ).fillna(""),
    "teams": lambda df: clean_basic(df).select(
        col("id"),
        normalize_text_udf(col("name")).alias("name"),
        normalize_text_udf(col("officialName")).alias("officialName"),
        normalize_date_udf(col("foundedOn")).alias("foundedOn"),
        normalize_text_udf(col("stadiumName")).alias("stadiumName"),
        col("stadiumSeats").cast("int"),
        normalize_text_udf(normalize_currency_udf(col("currentMarketValue"))).alias("currentMarketValue (€)"),
        normalize_text_udf(normalize_currency_udf(col("currentTransferRecord"))).alias("currentTransferRecord (€)"),
        array_join(col("colors"), ", ").alias("colors"),
        col("league")["id"].alias("leagueId"),
        normalize_text_udf(col("league")["name"]).alias("leagueName"),
        normalize_text_udf(col("league")["countryName"]).alias("leagueCountry"),
        normalize_text_udf(col("league")["tier"]).alias("leagueTier"),
        col("squad")["size"].alias("squadSize").cast("int"),
        col("squad")["nationalTeamPlayers"].alias("squadNationalPlayers").cast("int"),
        normalize_text_udf(col("addressLine1")).alias("addressLine1"),
        normalize_text_udf(col("addressLine2")).alias("addressLine2"),
        normalize_text_udf(col("addressLine3")).alias("addressLine3"),
    ).fillna(""),
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