import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{count, desc}

object QuestionTwo {
  def analyzeFrequentFlyers(flightData: DataFrame, passengers: DataFrame): DataFrame = {
    // Join the flightData and passengers DataFrames on "passengerId" column
    val joinedData = flightData.join(passengers, Seq("passengerId"))
    // Group the data by passenger and count the number of flights for each passenger
    val flightsPerPassenger = joinedData.groupBy("passengerId", "firstName", "lastName")
      .agg(count("*").alias("Number of Flights"))
    // Sort the result in descending order of the number of flights and select the top 100 records
    val top100FrequentFlyers = flightsPerPassenger
      .sort(desc("Number of Flights"))
      .limit(100)
    top100FrequentFlyers
  }
}
