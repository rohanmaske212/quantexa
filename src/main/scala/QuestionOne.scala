import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count}
import org.apache.spark.sql.functions._

object QuestionOne {
  def analyzeFlights(flightData: DataFrame, passengers: DataFrame): DataFrame = {
    // Join the flightData and passengers DataFrames on "passengerId" column
    val joinedData = flightData.join(passengers, Seq("passengerId"))
    // Convert the "date" column to DateType
    val dataWithDate = joinedData.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
    // Extract the month from the date column and create a new column "month"
    val dataWithMonth = dataWithDate.withColumn("month", month(col("date")))
    // Group the data by "month" and count the number of flights
    val flightsPerMonth = dataWithMonth.groupBy("month").agg(count("*").alias("Number of Flights"))
    // Sort the result by "month" column in ascending order
    val sortedResult = flightsPerMonth.sort("month")
    sortedResult
  }
}





