import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Date

class QuantexaTest extends AnyFunSuite {

  // Set the JVM options before running the tests
  sys.props += "java.add-opens" -> "java.base/sun.nio.ch=ALL-UNNAMED"

  // Create a SparkSession for testing
  val spark: SparkSession = SparkSession.builder()
    .appName("QuestionOneTest")
    .master("local[*]")
    .getOrCreate()

  // Import implicits to enable DataFrame transformations
  import spark.implicits._

  test("analyzeFlights should correctly analyze flight and passenger data") {
    // Create a sample flightData DataFrame
    val flightData = Seq(
      (1, "2022-01-01"),
      (2, "2022-01-02"),
      (3, "2022-01-03"),
      (4, "2022-01-04"),
      (5, "2022-02-01"),
      (6, "2022-02-02")
    ).toDF("passengerId", "date")

    // Create a sample passengers DataFrame
    val passengers = Seq(
      (1, "John"),
      (2, "Jane"),
      (3, "Alice"),
      (4, "Bob"),
      (5, "Eve"),
      (6, "Mark")
    ).toDF("passengerId", "name")

    // Call the analyzeFlights method to analyze the data
    val result = QuestionOne.analyzeFlights(flightData, passengers)

    // Define the expected result DataFrame
    val expected = Seq(
      (1, 4),
      (2, 2)
    ).toDF("month", "Number of Flights")

    // Assert that the result matches the expected DataFrame
    assert(result.collectAsList() == expected.collectAsList())
  }





  test("analyzeTravelHistory should correctly analyze travel history") {
    // Create a sample flightData DataFrame
    val flightData = Seq(
      (1, "2022-01-01", "UK"),
      (1, "2022-01-02", "USA"),
      (1, "2022-01-03", "USA"),
      (2, "2022-02-01", "UK"),
      (2, "2022-02-02", "UK"),
      (2, "2022-02-03", "UK"),
      (2, "2022-02-04", "USA")
    ).toDF("passengerId", "date", "to")

    // Create a sample passengers DataFrame
    val passengers = Seq(
      (1, "John", "Doe"),
      (2, "Jane", "Smith")
    ).toDF("passengerId", "firstName", "lastName")

    // Call the analyzeTravelHistory method to analyze the data
    val result = QuestionThree.analyzeTravelHistory(flightData, passengers)

    // Define the expected result DataFrame
    val expected = Seq(
      (1, 2),
      (2, 1)
    ).toDF("passengerId", "Longest Run")

    // Assert that the result matches the expected DataFrame
    assert(result.collectAsList() == expected.collectAsList())
  }

  test("analyzeFlyTogether should correctly analyze flight data to find passengers who have been on flights together") {
    // Create a sample flightData DataFrame
    val flightData = Seq(
      (1, 1),
      (1, 2),
      (1, 3),
      (2, 1),
      (2, 3),
      (3, 2),
      (3, 3),
      (4, 1),
      (4, 3),
      (5, 2),
      (5, 3)
    ).toDF("flightId", "passengerId")

    // Define the expected result DataFrame
    val expected = Seq(
      (1, 2, 3),
      (1, 3, 3),
      (2, 3, 2)
    ).toDF("passenger1", "passenger2", "Number of flights together")

    // Call the analyzeFlyTogether method to analyze the flight data
    val result = QuestionFour.analyzeFlyTogether(flightData)

    // Assert that the result matches the expected DataFrame
    assert(result.collect().toSet == expected.collect().toSet)
  }

  test("flownTogether should correctly analyze flight and passenger data within a date range") {
    // Create a sample flightData DataFrame
    val flightData = Seq(
      (1, "2022-01-01", "US"),
      (2, "2022-01-02", "UK"),
      (3, "2022-01-03", "US"),
      (4, "2022-01-04", "US"),
      (5, "2022-02-01", "UK"),
      (6, "2022-02-02", "US"),
      (1, "2022-01-05", "US"),
      (2, "2022-01-06", "US"),
      (1, "2022-01-07", "UK")
    ).toDF("passengerId", "date", "to")

    // Create a sample passengers DataFrame
    val passengers = Seq(
      (1, "John", "Doe"),
      (2, "Jane", "Smith"),
      (3, "Alice", "Johnson"),
      (4, "Bob", "Brown"),
      (5, "Eve", "Davis"),
      (6, "Mark", "Wilson")
    ).toDF("passengerId", "firstName", "lastName")

    // Define the expected result DataFrame
    val expected = Seq(
      (1, 2, 3, "John", "Doe", "Jane", "Smith"),
      (1, 2, 3, "John", "Doe", "John", "Doe")
    ).toDF(
      "Passenger 1 ID", "Passenger 2 ID", "Number of flights together",
      "Passenger 1 First Name", "Passenger 1 Last Name",
      "Passenger 2 First Name", "Passenger 2 Last Name"
    )

    // Define the date range for filtering
    val from = Date.valueOf("2022-01-01")
    val to = Date.valueOf("2022-01-31")

    // Call the flownTogether method to analyze the data
    val result = QuestionFive.flownTogether(flightData, passengers, 1, from, to)

    // Assert that the result matches the expected DataFrame
    assert(result.collect().toSet == expected.collect().toSet)
  }


  test("analyzeFrequentFlyers should correctly analyze flight and passenger data") {
    // Create a sample flightData DataFrame
    val flightData = Seq(
      (1, "2022-01-01"),
      (2, "2022-01-02"),
      (3, "2022-01-03"),
      (4, "2022-01-04"),
      (5, "2022-02-01"),
      (6, "2022-02-02"),
      (1, "2022-01-05"),
      (2, "2022-01-06"),
      (1, "2022-01-07")
    ).toDF("passengerId", "date")

    // Create a sample passengers DataFrame
    val passengers = Seq(
      (1, "John", "Doe"),
      (2, "Jane", "Smith"),
      (3, "Alice", "Johnson"),
      (4, "Bob", "Brown"),
      (5, "Eve", "Davis"),
      (6, "Mark", "Wilson")
    ).toDF("passengerId", "firstName", "lastName")

    // Call the analyzeFrequentFlyers method to analyze the data
    val result = QuestionTwo.analyzeFrequentFlyers(flightData, passengers)

    // Define the expected result DataFrame
    val expected = Seq(
      (1, "John", "Doe", 3),
      (2, "Jane", "Smith", 2)
    ).toDF("passengerId", "firstName", "lastName", "Number of Flights")

    // Assert that the result matches the expected DataFrame
    assert(result.collect().toSet == expected.collect().toSet)
  }
}
