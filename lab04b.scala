import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

object agg {
  def connect_kafka_stream(spark: SparkSession): DataFrame = {
    val kafkaParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1:6667",
      "subscribe" -> "arsen_kishmiryan",
    )
    spark

      .readStream
      .format("kafka")
      .options(kafkaParams)
      .load
  }

  def process_kafka_data(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    val schema = MapType(StringType, StringType)
    df.select('value.cast("string"), 'timestamp).withColumn("value", from_json(col("value"), schema))
      .select(
        'timestamp,
        'value.getItem("event_type").as("event_type"),
        'value.getItem("category").as("category"),
        'value.getItem("item_id").as("item_id"),
        'value.getItem("item_price").as("item_price"),
        'value.getItem("uid").as("uid"),
        'value.getItem("timestamp").as("event_timestamp")
      ).withColumn("event_timestamp", from_unixtime('event_timestamp / 1000).cast(TimestampType))
  }

  def aggregate_with_window(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    val window_df =
      df
        .withWatermark("event_timestamp", "1 hours")
        .groupBy(window($"event_timestamp", "1 hours")).agg(
          sum(when(col("event_type") === "buy", 'item_price).otherwise(0)).as("revenue"),
          count("uid").as("visitors"),
          sum(when(col("event_type") === "buy", 1).otherwise(0)).as("purchases")
        ).withColumn("aov", 'revenue / 'purchases)

    window_df
      .withColumn("start_ts",  unix_timestamp('window.getItem("start")))
      .withColumn("end_ts",  unix_timestamp('window.getItem("end")))
      .select("start_ts", "end_ts", "revenue", "visitors", "purchases", "aov")
  }

  def write_to_kafka(df: DataFrame) = {
    val kafkaParams = Map("kafka.bootstrap.servers" -> "spark-master-1:6667")
    val data = df.toJSON.withColumn("topic", lit("arsen_kishmiryan_lab04b_out"))
    data
      .writeStream
      .outputMode("update")
      .format("kafka")
      .options(kafkaParams)
      .option("checkpointLocation", s"/tmp/arsen_kishmiryan_lab04b_checkpoint/")
      .trigger(Trigger.ProcessingTime("5 seconds"))
  }


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("My Spark App")
      .master("local[*]")
      .getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val df = connect_kafka_stream(spark)
    val processed_df = process_kafka_data(spark, df)
    val aggregated_df = aggregate_with_window(spark, processed_df)
    val kafka_sink = write_to_kafka(aggregated_df)
    kafka_sink.start
  }
}
