import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object features {
  def get_logs(spark: SparkSession, path_to_logs: String): DataFrame = {
    spark.read.json(path_to_logs)
  }

  def get_user_items(spark: SparkSession, path_to_user_items: String): DataFrame = {
    spark.read.parquet(path_to_user_items)
  }

  def prepare_domains(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    df.withColumn("host", lower(call_udf("parse_url", $"url", lit("HOST"))))
      .withColumn("domain", regexp_replace($"host", "www.", ""))
  }

  def convert_timestamp(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    df.withColumn(
      "date", date_format(from_unixtime('timestamp / 1000), "yyyy-MM-dd HH:mm:ss")
    ).withColumn(
      "weekday", date_format(col("date"), "E")
    ).withColumn(
      "hour", date_format(col("date"), "HH")
    )
  }

  def prepare_logs(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    val exploded_df = df.withColumn("visits", explode(col("visits")))
    val edf = exploded_df.select($"uid", $"visits".getField("timestamp").as("timestamp"), $"visits".getField("url").as("url"))
    convert_timestamp(spark, prepare_domains(spark, edf))
  }

  def get_domain_feature(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    val top1000_domains = df.filter(col("domain").isNotNull).groupBy("domain").count.orderBy($"count".desc).limit(1000)
    val domains = top1000_domains.select("domain").as[String].collect().toSeq.sorted
    val count_domains = udf(
      (listOfDomains: Seq[String], top100Domains: Seq[String]) => {
        top100Domains.map(domain => listOfDomains.count(_ == domain))
      }
    )
    df.groupBy("uid").agg(collect_list("domain").as("domain")).withColumn(
      "domain_features", count_domains($"domain", typedLit(domains))
    )
  }

  def create_date_feature(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    val hours = 0 to 23
    val df_with_hours = hours.foldLeft(df) { (df, h) =>
      df.withColumn(s"web_hour_$h", when($"hour" === h, 1).otherwise(0))
    }
    val weekdays = Seq("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
    val df_with_weekdays = weekdays.foldLeft(df_with_hours) { (df, w) =>
      df.withColumn(s"web_day_$w".toLowerCase(), when($"weekday" === w, 1).otherwise(0))
    }
    val df_with_all_columns = df_with_weekdays.withColumn(
      "web_fraction_work_hours", when(col("hour") >= 9 && col("hour") < 18, 1).otherwise(0)
    ).withColumn(
      "web_fraction_evening_hours", when(col("hour") >= 18 && col("hour") < 24, 1).otherwise(0)
    ).drop("timestamp", "url", "date", "weekday", "hour", "host", "domain")

    val allColumns = df_with_all_columns.columns
    val aggExprs = allColumns.tail.map(colName => sum(colName).as(s"$colName")) :+ count("uid").as("uid_count")
    df_with_all_columns.groupBy("uid").agg(aggExprs.head, aggExprs.tail: _*).withColumn(
      "web_fraction_work_hours", col("web_fraction_work_hours") / col("uid_count")
    ).withColumn(
      "web_fraction_evening_hours", col("web_fraction_evening_hours") / col("uid_count")
    ).drop("uid_count")
  }

  def join_date_features_and_domain_features(domain_features: DataFrame, date_features: DataFrame): DataFrame = {
    date_features.join(domain_features.select("uid", "domain_features"), Seq("uid"), "left")
  }

  def join_with_users_items(user_items: DataFrame, logs: DataFrame): DataFrame = {
    user_items.join(logs, Seq("uid"), "inner")
  }

  def save_data(df: DataFrame, path: String): Unit = {
    df.write.mode("Overwrite").parquet(path)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("My Spark App")
      .master("local[*]")
      .getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val logs = get_logs(spark, "hdfs:///labs/laba03/weblogs.json")
    val user_items = get_user_items(spark, "/user/arsen.kishmiryan/users-items/20200429")

    val proc_logs = prepare_logs(spark, logs)
    val domain_features = get_domain_feature(spark, proc_logs)
    val date_features = create_date_feature(spark, proc_logs)

    val logs_features = join_date_features_and_domain_features(domain_features, date_features)
    val feature_df = join_with_users_items(user_items, logs_features)

    save_data(feature_df, "/user/arsen.kishmiryan/features")
  }
}
