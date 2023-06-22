import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, count, expr}
object QuestionFour{
  def analyzeFlyTogether(flightData: DataFrame): DataFrame = {
    // Self-join the flightData DataFrame to find pairs of passengers who have been on flights together
    val flightsTogether = flightData.alias("f1")
      .join(flightData.alias("f2"), expr("f1.flightId = f2.flightId and f1.passengerId < f2.passengerId"))
      .selectExpr("f1.passengerId as passenger1", "f2.passengerId as passenger2")
    // Group the data by the pair of passengers and count the number of flights together
    val flightsCount = flightsTogether.groupBy("passenger1", "passenger2")
      .agg(count("*").alias("Number of flights together"))
    // Filter the result to include only pairs with more than 3 flights together
    val result = flightsCount.filter(col("Number of flights together") > 3)
    result
  }
}
