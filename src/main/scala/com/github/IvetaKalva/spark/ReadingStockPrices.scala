package com.github.IvetaKalva.spark

import org.apache.spark.sql.functions.{avg, col, desc}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.language.postfixOps
import org.apache.spark.sql.functions._

object ReadingStockPrices extends App {

  val spark = SparkSession
    .builder
    .appName("ReadingStockPrices")
    .master("local")
    .getOrCreate()


  //TODO load up stock_prices.csv as a DataFrame

  val pathToCSV = "./src/resources/csv/stock_prices.csv"

  val dfStockPrices = spark
    .read.format("csv")
    .option("inferSchema", "true") // when setting to true it automatically infers column types based on the data.
    .option("header", "true")
    .option("mode", "FAILFAST") // exit if any errors
    .option("nullValue", "") // replace any null data with quotes
    .load(pathToCSV)


  dfStockPrices.createOrReplaceTempView("stock_prices")  // Create a temporary view
  dfStockPrices.show(10,truncate = false) //1246
  //  dfStockPrices.printSchema()
  //  dfStockPrices.describe().show(false)

  //TODO compute the average daily return of every stock for every date. Print the results to screen

  val averageDailyReturn: DataFrame = dfStockPrices
    .withColumn("daily_return",(col("close") - col("open")) / col("open")*100)
    averageDailyReturn.show(10,truncate = false)

  //TODO date average_return - your output should have the columns - yyyy-MM-dd return of all stocks on that date

  averageDailyReturn
    .groupBy("date")
    .agg(avg("daily_return"))
    .orderBy(desc("avg(daily_return)"))
    .show(10,truncate = false)

  // Aggregation methods are types of calculations used to group attribute values into a metric for each dimension value.


  //TODO - Save the results to the file as Parquet(CSV and SQL are optional)
  averageDailyReturn
    .write.format("parquet")
    .mode("overwrite")
    .save("./src/resources/parquet/stock_prices")

  averageDailyReturn
    .write
    .format("csv")
    .mode("overwrite")
    .save("./src/resources/csv/stock_average_daily_return.csv")

//  averageDailyReturn
//  .write
//  .mode("overwrite")
//  .saveAsTable("stock_average_daily_return_tbl") //???

  //TODO Which stock was traded most frequently - as measured by closing price * volume - on average?
  //??? "closing price * volume" ???
  averageDailyReturn
    .groupBy("ticker")
    .agg(avg("volume"))
    .orderBy(avg("volume").desc)
    .show(false)
  // The stock volume shows the action that has taken place in a particular stock.
  // All the activity, be it selling or buying, gets recorded in the volume metric.
  // High volumes indicate the number of times shares have changed hands.


  //TODO Bonus Question
  // Which stock was the most volatile as measured by annualized standard deviation of daily returns?
  //STDDEV, STDDEV_SAMP, STDDEV_POP Functions - An aggregate function that returns the standard deviation of a set of numbers.
  //This function works with any numeric data type.
  //The STDDEV_POP() and STDDEV_SAMP() functions compute the population standard deviation and sample standard deviation,
  // respectively, of the input values. Both functions evaluate all input rows matched by the query.
  // The difference is that STDDEV_SAMP() is scaled by 1/(N-1) while STDDEV_POP() is scaled by 1/N.
  // STDDEV() and STDDEV_SAMP() return the same result, while STDDEV_POP() uses a slightly different calculation to
  // reflect that the input data is considered part of a larger "population". https://docs.cloudera.com/documentation/enterprise/6/6.3/topics/impala_stddev.html
  val standardDeviation = averageDailyReturn
    .groupBy("ticker")
    .agg (stddev(col("daily_return").alias("stdev")))
  standardDeviation.show(5,truncate = false)


  //  The primary measure of volatility used by traders and analysts is the standard deviation.

  // What Is Volatility?
  // Volatility is a statistical measure of the dispersion of returns for a given security or market index.
  // In most cases, the higher the volatility, the riskier the security.
  // Volatility is often measured as either the standard deviation or variance between returns from that same security or market index.

  //  This metric reflects the average amount a stock's price has differed from the mean over a period of time.
  //  It is calculated by determining the mean price for the established period and then subtracting this figure from each price point.
  //  The differences are then squared, summed, and averaged to produce the variance.


  //TODO Big Bonus: Build a model either trying to predict the next day's price(regression) or simple UP/DOWN/UNCHANGED?
  // :(

// Get this - WARN  ProcfsMetricsGetter:69 - Exception when trying to compute pagesize, as a result reporting of ProcessTree metrics is stopped
//  - looking for solutions and find out - this:
//  https://issues.apache.org/jira/browse/SPARK-33001
  //wypoon
  //Wing Yew Poon added a comment - 15/Nov/20 02:53
  //... The warning happened because the command "getconf PAGESIZE" was run and it is not a valid command on Windows so
  // an exception was caught.
  //ProcfsMetricsGetter is actually only used when spark.executor.processTreeMetrics.enabled=true. However, the class
  // is instantiated and the warning occurs then, even though after that the class is not used.
  //Ideally, you should not see this warning. Ideally, isProcfsAvailable should be checked before computePageSize() is
  // called (the latter should not be called if procfs is not available, and it is not on Windows).
  // So it is a minor bug that you see this warning. But it can be safely ignored.

}
