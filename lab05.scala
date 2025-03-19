import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.nio.file.{Files, Path, Paths}

object users_items {
  def get_data_from_hdfs(spark: SparkSession, path: String): DataFrame = {
    spark.read.json(path)
  }

  def get_user_item_df_folders(spark: SparkSession, input_path: String): DataFrame = {
    if (input_path.startsWith("file://")) {
      val prefix = "file://"
      val rest_path = input_path.stripPrefix("file://")
      val path: Path = Paths.get(rest_path)
      val folders = Files.list(path)
        .filter(Files.isDirectory(_))
        .toArray
        .map(_.asInstanceOf[Path])
      val last_folder = folders.last.toString
      println(s"last_folder: $last_folder")
      spark.read.parquet(prefix + last_folder)
    } else if (input_path.startsWith("hdfs://")) {
      val conf = new Configuration()
      val file_system = fs.FileSystem.get(conf)
      val path = new fs.Path(input_path)
      val fileStatuses = file_system.listStatus(path)
      val folders = fileStatuses.filter(_.isDirectory).map(_.getPath.getName)
      val last_folder = folders.last
      spark.read.parquet(input_path + "/" + last_folder)
    } else {
      val path: Path = Paths.get(input_path)
      val folders = Files.list(path)
        .filter(Files.isDirectory(_))
        .toArray
        .map(_.asInstanceOf[Path])
      val last_folder = folders.last.toString
      println(s"last_folder: $last_folder")
      spark.read.parquet(last_folder)
    }
  }

  def process_items(df: DataFrame, prefix: String): DataFrame = {
    def to_lowercase(cat: String): String = {
      cat.toLowerCase().replace(' ', '_').replace('-', '_')
    }
    val process_item_id = udf(to_lowercase(_))
    df.filter(col("uid").isNotNull).withColumn("item_id", concat(lit(prefix + "_"), process_item_id(col("item_id")))).select("uid", "item_id")
  }

  def get_last_date(df: DataFrame): Any = {
    df.select(max("p_date")).collect()(0)(0)
  }

  def get_user_item_df(df_buy: DataFrame, df_view: DataFrame): DataFrame = {
    val pivot_df_buy = df_buy.groupBy("uid").pivot("item_id").count
    val pivot_df_view = df_view.groupBy("uid").pivot("item_id").count
    pivot_df_buy.join(pivot_df_view, Seq("uid"), "outer").na.fill(0)
  }

  def update_data(df1: DataFrame, df2: DataFrame): DataFrame = {
    val allColumns = (df1.columns ++ df2.columns).distinct

    // Add missing columns to df1 and fill with null
    val df1Aligned = allColumns.foldLeft(df1) { (df, colName) =>
      if (df.columns.contains(colName)) df
      else df.withColumn(colName, lit(0))
    }

    // Add missing columns to df2 and fill with null
    val df2Aligned = allColumns.foldLeft(df2) { (df, colName) =>
      if (df.columns.contains(colName)) df
      else df.withColumn(colName, lit(0))
    }

    // Union the aligned DataFrames
    val unionDF = df1Aligned.union(df2Aligned.select(allColumns.map(col): _*))
    val aggExprs = allColumns.tail.map(colName => sum(colName).as(s"$colName"))

    unionDF.groupBy("uid").agg(aggExprs.head, aggExprs.tail: _*)
  }

  def save_data(df: DataFrame, path: String, date: String): Unit = {
    val full_path = path + "/" + date
    df.write.mode("Overwrite").parquet(full_path)
  }


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("My Spark App")
      .master("local[*]")
      .getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val update: String = spark.sparkContext.getConf.get("spark.users_items.update")
    val output_dir: String = spark.sparkContext.getConf.get("spark.users_items.output_dir")
    val input_dir: String = spark.sparkContext.getConf.get("spark.users_items.input_dir")
    println(s"update value: $update")
    println(s"output_dir value: $output_dir")
    println(s"input_dir value: $input_dir")

    val full_output_path = output_dir match {
      case path if (output_dir.startsWith("/")) => path
      case path if output_dir.startsWith("file:/") => path
      case _ => "/user/arsen.kishmiryan/" + output_dir
    }

    val full_input_path = input_dir match {
      case path if (input_dir.startsWith("/")) => path
      case path if input_dir.startsWith("file:/") => path
      case _ => "/user/arsen.kishmiryan/" + input_dir
    }

    println(s"full_output_path value: $full_output_path")
    println(s"full_input_path value: $full_input_path")

    if (update == "0") {
      val df_buy: DataFrame =  get_data_from_hdfs(spark, full_input_path + "/buy")
      val df_view: DataFrame =  get_data_from_hdfs(spark, full_input_path + "/view")

      val df_buy_pr: DataFrame = process_items(df_buy, "buy")
      val df_view_pr: DataFrame = process_items(df_view, "view")

      val user_item_df: DataFrame = get_user_item_df(df_buy_pr, df_view_pr)
      val date: String = get_last_date(df_view).toString
      println(s"date value: $date")
      save_data(user_item_df, full_output_path, date)
    } else {
      val user_items: DataFrame = get_user_item_df_folders(spark, full_output_path)

      val df_buy: DataFrame =  get_data_from_hdfs(spark, full_input_path + "/buy")
      val df_view: DataFrame =  get_data_from_hdfs(spark, full_input_path + "/view")

      val df_buy_pr: DataFrame = process_items(df_buy, "buy")
      val df_view_pr: DataFrame = process_items(df_view, "view")

      val additional_user_item_df: DataFrame = get_user_item_df(df_buy_pr, df_view_pr)
      val updated_user_item_df: DataFrame = update_data(user_items, additional_user_item_df)

      val date: String = get_last_date(df_view).toString
      println(s"date value: $date")
      save_data(updated_user_item_df, full_output_path, date)
    }
  }
}
