import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import java.net.URL

import org.postgresql.Driver
import java.sql.{Connection, DriverManager, Statement}



class Connect(spark: SparkSession) {
  def clients: DataFrame = {
    val spark: SparkSession = SparkSession.builder()
      .config("spark.cassandra.connection.host", "*****")
      .config("spark.cassandra.connection.port", "****")
      .getOrCreate()

    spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("****" -> "****", "****" -> "****"))
      .load()
  }

  def visits: DataFrame = {
    spark.read
      .format("org.elasticsearch.spark.sql")
      .options(Map("****" -> "****",
        "es.nodes.wan.only" -> "****",
        "es.port" -> "****",
        "es.nodes" -> "****",
        "es.net.ssl" -> "****"))
      .load("visits")
  }

  def logs: DataFrame = {
    val spark: SparkSession = SparkSession.builder().getOrCreate
    spark.read.json("hdfs:///****/****/****.****")
  }

  def cats: DataFrame = {
    val spark: SparkSession = SparkSession.builder().getOrCreate
    spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://****:****/****")
      .option("dbtable", "****")
      .option("user", "****")
      .option("password", "****")
      .option("driver", "org.postgresql.Driver")
      .load()
  }
}

class Processer(spark: SparkSession) {
  private def prepare_logs(df: DataFrame): DataFrame = {
    import spark.implicits._
    val exploded_df = df.withColumn("visits", explode(col("visits")))
    val edf = exploded_df.select($"uid", $"visits".getField("timestamp").as("timestamp"), $"visits".getField("url").as("url"))
    def remove_www(url: String) = {
      try {
        val host = new URL(url).getHost
        if (host.startsWith("www.")) host.stripPrefix("www.") else host
      } catch {
        case _: Exception => url // Возвращаем исходный URL в случае ошибки
      }
    }
    val remove_udf = udf(remove_www(_))
    edf.withColumn("url", remove_udf(col("url"))).select("uid", "url")
  }

  private def add_category_to_age(df: DataFrame): DataFrame = {
    def age_category(age: Int): String = age match {
      case age if age >= 18 & age <= 24 => "18-24"
      case age if age >= 25 & age <= 34 => "25-34"
      case age if age >= 35 & age <= 44 => "35-44"
      case age if age >= 45 & age <= 54 => "45-54"
      case age if age >= 55 => ">=55"
      case _ => "<18"
    }
    val age_cat = udf(age_category(_))
    df.withColumn("age_cat", age_cat(col("age"))).drop("age")
  }

  def prepare_visits(df: DataFrame): DataFrame = {
    def to_lowercase(cat: String): String = {
      cat.toLowerCase().replace(' ', '_').replace('-', '_')
    }
    val process_item_cat = udf(to_lowercase(_))
    val logs_visits = df.withColumn("category", process_item_cat(col("category"))).select("category", "uid").withColumnRenamed("category", "item_category")
    val flogs_visits = logs_visits.withColumn("item_category", concat(lit("shop_"), col("item_category")))
    flogs_visits.groupBy("uid").pivot("item_category").count
  }

  def join_cat(logs: DataFrame, cats: DataFrame): DataFrame = {
    import spark.implicits._
    val logs_cat = logs.join(cats, $"url" === $"domain", "inner").select("uid", "category")
    val formatted_logs_cat = logs_cat.withColumn("category", concat(lit("web_"), col("category")))
    formatted_logs_cat.groupBy("uid").pivot("category").count
  }

  def concat_info(logs_cat: DataFrame, logs_visits: DataFrame): DataFrame = {
    logs_cat.join(logs_visits, Seq("uid"), "outer").na.fill(0)
  }
  def join_with_socdem(socdem: DataFrame, logs: DataFrame): DataFrame = {
    socdem.join(logs, Seq("uid"), "left")
  }

  def execute(logs: DataFrame, visits: DataFrame, cats: DataFrame, clients: DataFrame): DataFrame = {
    val clients_cats = add_category_to_age(clients)
    val splitted_logs = prepare_logs(logs)
    val processed_visits = prepare_visits(visits)
    val join_category = join_cat(splitted_logs, cats)
    val concated_df = concat_info(processed_visits, join_category)
    join_with_socdem(clients_cats, concated_df)
  }
}


class PostgresSave(spark: SparkSession) {
  def save_dataframe(result: DataFrame): Unit = {
    result.write
      .format("jdbc")
      .option("url", "jdbc:postgresql://****:****/****")
      .option("dbtable", "****")
      .option("user", "****")
      .option("password", "****")
      .option("driver", "org.postgresql.Driver")
      .option("truncate", value = true)
      .mode("overwrite")
      .save()
  }

  def grantTable(): Unit = {
    val driverClass: Class[Driver] = classOf[org.postgresql.Driver]
    val driver: Any = Class.forName("org.postgresql.Driver").newInstance()
    val url = "jdbc:postgresql://****:****/****?user=****&password=****"
    val connection: Connection = DriverManager.getConnection(url)
    val statement: Statement = connection.createStatement()
    val bool: Boolean = statement.execute("****")
    connection.close()
  }
}

object data_mart {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("My Spark App")
      .master("local[*]")
      .getOrCreate()

    val connector = new Connect(spark)
    val clients = connector.clients
    val visits = connector.visits
    val logs = connector.logs
    val cats = connector.cats

    val processor = new Processer(spark)
    val result = processor.execute(logs, visits, cats, clients)

    val persister = new PostgresSave(spark)
    persister.save_dataframe(result)
    persister.grantTable()
  }
}
