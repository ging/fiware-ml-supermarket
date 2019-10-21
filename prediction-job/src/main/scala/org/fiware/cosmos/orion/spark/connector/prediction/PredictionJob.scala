package org.fiware.cosmos.orion.spark.connector.prediction

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.fiware.cosmos.orion.spark.connector.{ContentType, HTTPMethod, OrionReceiver, OrionSink, OrionSinkObject}
import org.apache.spark.ml.feature.{VectorAssembler}
import org.apache.spark.ml.regression.{RandomForestRegressionModel}
import org.apache.spark.sql.SparkSession


/**
  * Prediction Job
  * @author @sonsoleslp
  */

case class PredictionResponse(socketId: String, predictionId: String, predictionValue: Int, year: Int, month: Int, day: Int, time: Int) {
  override def toString :String = s"""{
  "socketId": { "value": "${socketId}", "type": "String"},
  "predictionId": { "value":"${predictionId}", "type": "String"},
  "predictionValue": { "value":${predictionValue}, "type": "Integer"},
  "year": { "value":${year}, "type": "Integer"},
  "month": { "value":${month}, "type": "Integer"},
  "day": { "value":${day}, "type": "Integer"},
  "time": { "value": ${time}, "type": "Integer"}
  }""".trim()
}
case class PredictionRequest(year: Int, month: Int, day: Int, weekDay: Int, time: Int, socketId: String, predictionId: String)

object PredictionJob {
  final val URL_CB = "http://orion:1026/v2/entities/ResTicketPrediction1/attrs"
  final val CONTENT_TYPE = ContentType.JSON
  final val METHOD = HTTPMethod.PATCH
  final val BASE_PATH = "./prediction-job"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("PredictionJob")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))
    // ssc.checkpoint("./output")

    // Load the numeric vector assembler
    //    val vectorAssemblerPath = "%s/models/numeric_vector_assembler.bin".format(BASE_PATH)
    //    val vectorAssembler = VectorAssembler.load(vectorAssemblerPath)
    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("year", "month", "day", "weekDay", "time"))
      .setOutputCol("features")

    // Load model
    // val randomForestModelPath = "%s/models/spark_random_forest_classifier.flight_delays.5.0.bin".format(BASE_PATH)
    val model = RandomForestRegressionModel.load(BASE_PATH+"/model")

    // Create Orion Source. Receive notifications on port 9001
    val eventStream = ssc.receiverStream(new OrionReceiver(9001))

    // Process event stream to get updated entities
    val processedDataStream = eventStream
      .flatMap(event => event.entities)
      .map(ent => {
        val year = ent.attrs("year").value.toString.toInt
        val month = ent.attrs("month").value.toString.toInt
        val day = ent.attrs("day").value.toString.toInt
        val time = ent.attrs("time").value.toString.toInt
        val weekDay = ent.attrs("weekDay").value.toString.toInt
        val socketId = ent.attrs("socketId").value.toString
        val predictionId = ent.attrs("predictionId").value.toString
        PredictionRequest(year, month, day, weekDay, time, socketId, predictionId)
      })

    // Feed each entity into the prediction model
    val predictionDataStream = processedDataStream
      .transform(rdd => {
        val df = rdd.toDF
        val vectorizedFeatures  = vectorAssembler
          .setHandleInvalid("keep")
          .transform(df)
        val predictions = model
          .transform(vectorizedFeatures)
          .select("socketId","predictionId", "prediction", "year", "month", "day", "time")

        predictions.toJavaRDD
    })
      .map(pred=> PredictionResponse(pred.get(0).toString,
        pred.get(1).toString,
        pred.get(2).toString.toFloat.round,
        pred.get(3).toString.toInt,
        pred.get(4).toString.toInt,
        pred.get(5).toString.toInt,
        pred.get(6).toString.toInt)
    )

    // Convert the output to an OrionSinkObject and send to Context Broker
    val sinkDataStream = predictionDataStream
      .map(res => OrionSinkObject(res.toString, URL_CB, CONTENT_TYPE, METHOD))

    // Add Orion Sink
    OrionSink.addSink(sinkDataStream)
    sinkDataStream.print()
    predictionDataStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
