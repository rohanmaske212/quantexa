import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions._

object QuestionOne extends App {
  // Create a SparkSession
  val spark = SparkSession.builder()
    .appName("FlightDataAnalysis")
    .master("local[*]") // Set the master URL
    .getOrCreate()

  // Read the flightData.csv file into a DataFrame
  val flightData = spark.read
    .option("header", "true")
    .csv("src/main/resources/flightData.csv")

  // Read the passengers.csv file into a DataFrame
  val passengers = spark.read
    .option("header", "true")
    .csv("src/main/resources/passengers.csv")

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

  // Display the result
  sortedResult.show()
}





