import QuestionFive.flownTogether
import QuestionFour.analyzeFlyTogether
import QuestionOne.analyzeFlights
import QuestionThree.analyzeTravelHistory
import QuestionTwo.analyzeFrequentFlyers
import org.apache.spark.sql.SparkSession

import java.sql.Date

object Main extends App {
  // Create a SparkSession
  val spark = SparkSession.builder()
    .appName("QuantexaAssesment")
    .master("local[*]")
    .getOrCreate()

  // Read the flightData.csv file into a DataFrame
  val flightData = spark.read
    .option("header", "true")
    .csv("src/main/resources/flightData.csv")

  // Read the passengers.csv file into a DataFrame
  val passengers = spark.read
    .option("header", "true")
    .csv("src/main/resources/passengers.csv")

  // Perform flight analysis
  val flightsResult = analyzeFlights(flightData, passengers)
  flightsResult.show()

  // Perform frequent flyers analysis
  val frequentFlyersResult = analyzeFrequentFlyers(flightData, passengers)
  frequentFlyersResult.show()

  // Perform travel history analysis
  val travelHistoryResult = analyzeTravelHistory(flightData, passengers)
  travelHistoryResult.show()

  // Perform Flying together for more than 3 times analysis
  val flyingTogether = analyzeFlyTogether(flightData)
  flyingTogether.show()

  // Perform Flying together from -> to analysis
  val from = Date.valueOf("2017-01-01")
  val to = Date.valueOf("2017-12-31")
  val flownTogFromTo = flownTogether(flightData, passengers, 5, from, to)
  flownTogFromTo.show()

  // Save the results as CSV files
  flightsResult.write
    .mode("overwrite")
    .option("header", "true")
    .csv("output/flights_per_month.csv")

  frequentFlyersResult.write
    .mode("overwrite")
    .option("header", "true")
    .csv("output/topFrequentFlyers.csv")

  travelHistoryResult.write
    .mode("overwrite")
    .option("header", "true")
    .csv("output/longestRun.csv")

  flyingTogether.write
    .mode("overwrite")
    .option("header", "true")
    .csv("output/FlightsTogether.csv")

  // Save the result as a CSV file
  flownTogFromTo.write
    .mode("overwrite")
    .option("header", "true")
    .csv("output/resultWIthPassengerDetails.csv")

}

