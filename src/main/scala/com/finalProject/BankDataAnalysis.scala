package com.finalProject

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, lead}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util._
import org.jfree.chart
import org.jfree.chart._
import org.jfree.chart.plot.PlotOrientation
import org.jfree.data.category.{CategoryDataset, DefaultCategoryDataset}
import org.jfree.data.xy._

// Define case classes to represent the data
case class Transaction(customer_id: Int, transaction_date: String, transaction_type: String, amount: Double)

case class CustomerTransactionFrequency(customer_id: Int, transaction_frequency: Long)

case class TransactionPattern(customer_id: Int, transaction_date: String, transaction_type: String, amount: Double)

object BankDataAnalysis extends App {

  // Function to load the bank dataset from the CSV file into an RDD
  def loadDataFromCSV(sc: SparkContext, filename: String): RDD[Transaction] = {
    val dataRDD = sc.textFile(filename)
    return dataRDD.map(line => {
      val fields = line.split(",")
      Transaction(fields(0).toInt, fields(1), fields(2), fields(3).toDouble)
    })

  }

  def exploreDataset(df: RDD[Transaction]) = {
    df.takeSample(withReplacement = false, num = 5, seed = 0)
  }

  // Function to handle missing or erroneous data and return a cleaned RDD
  def handleMissingData(data: RDD[Transaction]): RDD[Transaction] = {
    // Implement your logic here to handle missing or erroneous data
    // For example, you can use RDD transformations like filter or map to clean the data
    // Return the cleaned RDD
    return data.filter(transaction => transaction.amount > 0)
  }

  // Function to calculate and display basic statistics
  def calculateBasicStatistics(data: RDD[Transaction]): Unit = {
    val totalDeposits = data.filter(_.transaction_type == "deposit").map(_.amount).sum()
    val totalWithdrawals = data.filter(_.transaction_type == "withdrawal").map(_.amount).sum()
    val averageTransactionAmount = data.map(_.amount).mean()

    println(s"Total Deposits: $totalDeposits")
    println(s"Total Withdrawals: $totalWithdrawals")
    println(s"Average Transaction Amount: $averageTransactionAmount")
  }

  // Function to determine the number of unique customers and the frequency of transactions per customer
  def customerTransactionFrequency(data: RDD[Transaction]): RDD[CustomerTransactionFrequency] = {
    val transactionCounts = data.map(transaction => (transaction.customer_id, 1))
      .reduceByKey(_ + _)

    return transactionCounts.map { case (customer_id, frequency) =>
      CustomerTransactionFrequency(customer_id, frequency)
    }
  }

  // Function to group transactions based on the transaction date
  def groupByTransactionDate(data: RDD[Transaction], timeUnit: String): DataFrame = {
    // Implement your logic here to group transactions based on the transaction date (daily, monthly, etc.)
    // For example, you can use RDD transformations like map and reduceByKey to aggregate transactions by date
    // Return an RDD with the aggregated data, where the key is the date and the value is the total transaction amount
    def getKey(transaction: Transaction): String = {
      timeUnit.toLowerCase match {
        case "daily" => transaction.transaction_date
        case "monthly" => transaction.transaction_date.substring(0, 7)
        case "yearly" => transaction.transaction_date.substring(0, 4)
        case _ => throw new IllegalArgumentException(s"Unsupported time unit: $timeUnit")
      }
    }

    val rdd_by_date = data.map(transaction => (getKey(transaction), transaction.amount))
      .reduceByKey(_ + _)

    return spark.createDataFrame(rdd_by_date)
  }

  // Function to analyze the trends of deposits and withdrawals over time using line charts or bar plots
  def plotTransactionTrends(data: RDD[(String, Double)], timeUnit: String): Unit = {
    // Implement your logic here to create line charts or bar plots to visualize the trends of deposits and withdrawals over time
    // You can use external plotting libraries like Matplotlib or JFreeChart in combination with Scala to create visualizations
    val xy = new XYSeries("")
    data.collect().foreach { case (y: String, x: Double) => xy.add(x, x) }
    val dataset = new XYSeriesCollection(xy)
    val chart = ChartFactory.createXYLineChart(
      "MyChart", // chart title
      "x", // x axis label
      "y", // y axis label
      dataset, // data
      PlotOrientation.VERTICAL,
      false, // include legend
      true, // tooltips
      false // urls
    )
  }

  // Function to segment customers based on the ir transaction behavior
  def customerSegmentation(data: RDD[Transaction]): RDD[(Int, String)] = {
    // Implement your logic here to segment customers based on transaction behavior (e.g., high-value customers, frequent transactors, inactive customers)
    // You can use RDD transformations like map to label customers with segments
    // Return an RDD with customer ID and corresponding segment label
    return customerTransactionFrequency(cleaned_data).map(customerTransaction => if (customerTransaction.transaction_frequency < 3) {
      (customerTransaction.customer_id, "inactive customer")
    } else {
      (customerTransaction.customer_id, "frequent transactor")
    })
  }
  // Example: Assign all customers as "high-value" for simplicity

  // Function to calculate the average transaction amount for each customer segment
  def calculateAvgTransactionAmount(data: RDD[Transaction], segments: RDD[(Int, String)]): RDD[(String, Double)] = {
    // Implement your logic here to calculate the average transaction amount for each customer segment
    // You can use RDD transformations like join and aggregateByKey to calculate the average transaction amount
    // Return an RDD with segment labels and corresponding average transaction amount
    val customerAmounts = data.map(transaction => (transaction.customer_id, transaction.amount))
    val joinedData = customerAmounts.join(segments)
    val avgTransactionAmounts = joinedData.map { case (_, (amount, segment)) => (segment, amount) }
      .aggregateByKey((0.0, 0L))(
        (acc, value) => (acc._1 + value, acc._2 + 1),
        (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
      )
      .mapValues { case (totalAmount, count) => totalAmount / count.toDouble }

    avgTransactionAmounts
  }

  // Function to identify patterns in customer transactions
  def identifyTransactionPatterns(data: RDD[Transaction]): DataFrame = {
    // Implement your logic here to identify patterns in customer transactions (e.g., large deposits followed by large withdrawals)
    // You can use RDD transformations like window functions or groupBy to analyze the transaction patterns
    // Return an RDD with relevant information (e.g., customer ID, transaction date, transaction type, amount) for identified patterns
    val windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")
    val transactionPatterns = spark.createDataFrame(data).withColumn("previous_amount", lag("amount", 1).over(windowSpec))
      .withColumn("next_amount", lead("amount", 1).over(windowSpec))
    val identifiedPatterns = transactionPatterns.filter(
      (col("transaction_type") === "deposit" && col("amount") >= 500) &&
        (col("next_amount").isNotNull && col("next_amount") > 500) &&
        (col("next_amount") > col("amount"))
    )
    val finalResult = identifiedPatterns.select("customer_id", "transaction_date", "transaction_type", "amount")
    finalResult
  }

  // Function to visualize identified transaction patterns
  def visualizeTransactionPatterns(data:DataFrame, finalResult: DataFrame): Unit = {
    // Implement your logic here to visualize the identified transaction patterns using appropriate charts and graphs
    // You can use external plotting libraries like Matplotlib or JFreeChart in combination with Scala to create visualizations
    val customerIDs = finalResult.select("customer_id").collect().map(_.getInt(0))
    val transactionDates = finalResult.select("transaction_date").collect().map(_.getString(0))
    val transactionAmounts = finalResult.select("amount").collect().map(_.getDouble(0))

    val dataset = new DefaultCategoryDataset()
    for (i <- customerIDs.indices) {
      dataset.addValue(transactionAmounts(i), s"Customer ${customerIDs(i)}", transactionDates(i))
    }

    val chart = ChartFactory.createBarChart(
      "Transaction Amounts over Time for Identified Patterns",
      "Transaction Date",
      "Transaction Amount",
      dataset,
      PlotOrientation.VERTICAL,
      true,
      true,
      false
    )

    val chartPanel = new ChartPanel(chart)
    chartPanel.setPreferredSize(new java.awt.Dimension(800, 600))

    val frame = new javax.swing.JFrame("Transaction Patterns")
    frame.setContentPane(chartPanel)
    frame.pack()
    frame.setVisible(true)
  }

  // Optional: Function to detect fraudulent transactions based on unusual patterns
  def detectFraudulentTransactions(data: RDD[Transaction]): RDD[Transaction] = {
    // Implement your logic here to detect fraudulent transactions based on unusual patterns
    // You can use RDD transformations to define rules or criteria for fraud detection
    // Return an RDD with flagged transactions or any additional information as needed
    data // For simplicity, this example function returns the original data as RDD
  }

  // Optional: Function to optimize performance using Spark configurations and caching strategies
  def optimizePerformance(data: RDD[Transaction]): RDD[Transaction] = {
    // Implement your logic here to optimize performance using Spark configurations and caching strategies
    // You can experiment with different Spark configurations and caching methods to improve processing speed and memory usage
    // Return the optimized RDD for further analysis
    data.persist() // For simplicity, this example function returns the original data as RDD
  }

  // Optional: Function to create visualizations for presentation
  def createVisualization(data: DataFrame): Unit = {
    // Implement your logic here to create visualizations for presenting the findings and insights
    // You can use external plotting libraries like Matplotlib or JFreeChart in combination with Scala to create visualizations
    val customerIDs = data.select("customer_id").collect().map(_.getInt(0))
    val transactionDates = data.select("transaction_date").collect().map(_.getString(0))
    val transactionAmounts = data.select("amount").collect().map(_.getDouble(0))

    val dataset = new DefaultCategoryDataset()
    for (i <- customerIDs.indices) {
      dataset.addValue(transactionAmounts(i), s"Customer ${customerIDs(i)}", transactionDates(i))
    }
    val chart = ChartFactory.createBarChart(
      "Transaction Amounts over Time for Identified Patterns",
      "Transaction Date",
      "Transaction Amount",
      dataset,
      PlotOrientation.VERTICAL,
      true,
      true,
      false
    )

    val chartPanel = new ChartPanel(chart)
    chartPanel.setPreferredSize(new java.awt.Dimension(800, 600))

    val frame = new javax.swing.JFrame("Transaction Patterns")
    frame.setContentPane(chartPanel)
    frame.pack()
    frame.setVisible(true)
  }

  // Optional: Function to create a presentation to showcase the analysis and results
  def createPresentation(data: RDD[Transaction]): Unit = {
    // Implement your logic here to create a concise and visually appealing presentation showcasing the analysis and results
    // You can use presentation tools like PowerPoint or Jupyter Notebooks (with Scala support) to create the presentation
  }

  val spark: SparkSession = SparkSession.builder().master("local").getOrCreate
  val sc = spark.sparkContext
  var data = loadDataFromCSV(sc = sc, "C:/Users/WIM-ESP025/IdeaProjects/FinalProjectSpark/src/main/scala/com/finalProject/data.csv")
  exploreDataset(data)
  val cleaned_data = handleMissingData(data)
  calculateBasicStatistics(cleaned_data)
  customerTransactionFrequency(cleaned_data)
  groupByTransactionDate(cleaned_data, "yearly")
  customerSegmentation(cleaned_data)
  calculateAvgTransactionAmount(cleaned_data, customerSegmentation(cleaned_data))
  val transactionPatterns: DataFrame = identifyTransactionPatterns(cleaned_data)
  transactionPatterns.show()
  visualizeTransactionPatterns(spark.createDataFrame(cleaned_data), transactionPatterns)
}
