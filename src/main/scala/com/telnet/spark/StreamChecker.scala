package com.telnet.spark

import org.apache.spark.sql.kafka010
import com.google.gson.{Gson, JsonObject, JsonParser}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}

import Utilities._

object StreamChecker {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Kafka format as data input tests")
      .master("local[*]")
      .config("spark.io.compression.codec", "snappy")
      //    .config("spark.sql.shuffle.partitions", "20")
      .getOrCreate()

    Logger.getLogger("org").setLevel(Level.ERROR)

    import spark.implicits._

    val structExternal = new StructType()
      .add("event_type", StringType)
      .add("event_id", StringType)
      .add("source_id", StringType)
      .add("timestamp", StringType)
      .add("payload", StringType)

    val externalTopic = spark
      .readStream
      .format("kafka")
      .option("failOnDataLoss", "false")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "mooogle")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[(String)]

    val externalRaw = externalTopic.select(from_json($"value", structExternal).as("streamerData")).select("streamerData.*").toJSON

    val streamedData = externalRaw.map(x => {
      val streamerJson = new Gson().fromJson(x, classOf[JsonObject])
      val jsonStringAsObject = new JsonParser().parse(streamerJson.toString()).getAsJsonObject
      val source_id = jsonStringAsObject.get("source_id").getAsString
      val rawDate = jsonStringAsObject.get("timestamp").getAsString()
      val streamed_at = transformDateTime(rawDate)
      (source_id, streamed_at)
    }).toDF("sourceId", "streamedAt")

    streamedData.createOrReplaceTempView("streaming_data")

    val resultData = streamedData.dropDuplicates("sourceId", "streamedAt")

    resultData
      .writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode("append")
      .start()

    System.in.read()
    spark.stop()
  }
}
