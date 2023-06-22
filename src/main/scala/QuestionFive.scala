import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, count, expr}

import java.sql.Date
object QuestionFive{
  def flownTogether(flightData: DataFrame, passengers: DataFrame, atLeastNTimes: Int, from: Date, to: Date): DataFrame = {
    // Filter the flights within the specified date range
    val filteredFlights = flightData.filter(col("date").between(from, to))
    // Self-join the filteredFlights DataFrame to find pairs of passengers who have been on flights together
    val flightsTogether = filteredFlights.alias("f1")
      .join(filteredFlights.alias("f2"), expr("f1.flightId = f2.flightId and f1.passengerId < f2.passengerId"))
      .selectExpr("f1.passengerId as passenger1", "f2.passengerId as passenger2")
    // Group the data by the pair of passengers and count the number of flights together
    val flightsCount = flightsTogether.groupBy("passenger1", "passenger2")
      .agg(count("*").alias("Number of flights together"))
    // Filter the result to include only pairs with more than N flights together
    val result = flightsCount.filter(col("Number of flights together") > atLeastNTimes)
    val resultWithPassengerDetails = result.join(passengers.as("p1"), col("passenger1") === col("p1.passengerId"))
      .join(passengers.as("p2"), col("passenger2") === col("p2.passengerId"))
      .select(
        col("passenger1").alias("Passenger 1 ID"),
        col("passenger2").alias("Passenger 2 ID"),
        col("Number of flights together"),
        col("p1.firstName").alias("Passenger 1 First Name"),
        col("p1.lastName").alias("Passenger 1 Last Name"),
        col("p2.firstName").alias("Passenger 2 First Name"),
        col("p2.lastName").alias("Passenger 2 Last Name")
      )
    resultWithPassengerDetails
  }
}
