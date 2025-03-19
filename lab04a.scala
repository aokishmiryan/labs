import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._



object filter {
  def connect_kafka(spark: SparkSession, topic_name: String, offset: String): DataFrame = {
    import spark.implicits._
    val raw_df = spark.read.format("kafka")
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("subscribe", topic_name)
      .option("startingOffsets",
        if(offset.contains("earliest"))
          offset
        else {
          "{\"" + topic_name + "\":{\"0\":" + offset + "}}"
        }
      )
      .load()
    val schema = MapType(StringType, StringType)
    val casted_df = raw_df.selectExpr("cast(value as string) value").withColumn("value", from_json(col("value"), schema))
    casted_df.select(
      'value.getItem("event_type").as("event_type"),
      'value.getItem("category").as("category"),
      'value.getItem("item_id").as("item_id"),
      'value.getItem("item_price").as("item_price"),
      'value.getItem("uid").as("uid"),
      'value.getItem("timestamp").as("timestamp")
    )
  }

  def convert_timestamp(df: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._
    df.withColumn(
      "date", date_format(from_unixtime('timestamp / 1000), "yyyyMMdd")
    ).withColumn("p_date", 'date)
  }

  def filter_df(df: DataFrame, event_type: String, spark: SparkSession): DataFrame = {
    import spark.implicits._
    df.filter('event_type === lit(event_type))
  }

  def save_df(df: DataFrame, path: String): Unit = {
    df.write.partitionBy("p_date").mode("Overwrite").json(path)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("My Spark App")
      .master("local[*]")
      .getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    val topic_name: String = spark.sparkContext.getConf.get("spark.filter.topic_name")
    val offset: String = spark.sparkContext.getConf.get("spark.filter.offset")
    val output_dir_prefix: String = spark.sparkContext.getConf.get("spark.filter.output_dir_prefix")
    val full_path = output_dir_prefix match {
      case path if (output_dir_prefix.startsWith("/")) => path
      case path if output_dir_prefix.startsWith("file:/") => path
      case _ =>"/user/arsen.kishmiryan/" + output_dir_prefix
    }
    val df = connect_kafka(spark, topic_name, offset)
    val converted_df = convert_timestamp(df, spark)
    val df_buy = filter_df(converted_df, "buy", spark)
    val df_view = filter_df(converted_df, "view", spark)
    save_df(df_buy, full_path + "/buy")
    save_df(df_view, full_path + "/view")

  }
}
