import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, when}

object QuestionThree{
  def analyzeTravelHistory(flightData: DataFrame, passengers: DataFrame): DataFrame = {
    // Join the flightData and passengers DataFrames on "passengerId" column
    val joinedData = flightData.join(passengers, Seq("passengerId"))
    // Add a lagged column to check the previous country for each passenger
    val windowSpec = Window.partitionBy("passengerId").orderBy("date")
    val dataWithPrevCountry = joinedData.withColumn("prevCountry", lag(col("to"), 1).over(windowSpec))
    // Count the number of non-UK countries for each passenger
    val nonUKCountries = dataWithPrevCountry.withColumn("isUK", when(col("to") === "UK", 0).otherwise(1))
      .withColumn("nonUKCount", functions.sum("isUK").over(windowSpec.rangeBetween(Window.unboundedPreceding, 0)))
    // Find the longest run of non-UK countries for each passenger
    val longestRun = nonUKCountries.groupBy("passengerId")
      .agg(functions.max("nonUKCount").alias("Longest Run"))
    longestRun
  }
}
