package org.fiware.cosmos.orion.spark.connector.prediction

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}
import org.apache.spark.ml.regression._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object TrainingJob {
  def main(args: Array[String]): Unit = {
    train().write.overwrite().save("./prediction-job/model")

  }
  def train(): RandomForestRegressionModel = {
    val schema = StructType(
      Array(StructField("id", IntegerType),
            StructField("date", StringType),
            StructField("time", IntegerType),
            StructField("items", IntegerType),
            StructField("day", IntegerType),
            StructField("month", IntegerType),
            StructField("year", IntegerType),
            StructField("weekDay", IntegerType)
      ))

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    // Load and parse the data file, converting it to a DataFrame.
    val data = spark.read.format("csv")
      .schema(schema)
      .option("header", "true")
      .option("delimiter", ",")
      .load("./prediction-job/carrefour_data.csv")
      .drop("id")
      .withColumnRenamed("items","label")

    val assembler = new VectorAssembler()
        .setInputCols(Array("year", "month", "day", "weekDay","time" ))
        .setOutputCol("features")

    // Automatically identify categorical features, and index them.
    var transformedDf = assembler.transform(data)

    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = transformedDf.randomSplit(Array(0.8, 0.2))

    // Train a RandomForest model.
    val rf = new RandomForestRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxDepth(30)
      .setMaxBins(32)
      .setNumTrees(100)

    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .fit(transformedDf)

    // Chain indexer and forest in a Pipeline.
    val pipeline = new Pipeline()
    .setStages(Array(featureIndexer, rf))
    // Train model. This also runs the indexer.
    val model = pipeline.fit(trainingData)
    // Make predictions.
    val predictions = model.transform(testData)

    // Select (prediction, true label) and compute test error.
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)

    val rfModel = model.stages(1).asInstanceOf[RandomForestRegressionModel]
    rfModel
  }
}