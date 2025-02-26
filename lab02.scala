import org.apache.spark.sql.types._
// import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import java.net.URLDecoder
import java.net.URL
import scala.util.{Failure, Success, Try}

class Auto {
    // val spark = SparkSession.builder()
    //     .appName("lab02")
    //     .getOrCreate()
    def get_autousers: DataFrame = {
        val autousersPath = "/labs/laba02/autousers.json"
        val usersSchema =
            StructType(
              List(
                StructField("autousers", ArrayType(StringType, containsNull = false))
              )
            )

        spark.read.schema(usersSchema).json(autousersPath)
        .withColumn("autousers", explode(col("autousers")))
        .withColumn("auto-flag", lit(1))
    }

        def filter_logs(df: DataFrame): DataFrame = {
        val df_filtered = df.filter(col("uid").isNotNull && col("url").isNotNull)
        def decode_utf8(url: String) = {
            Try(URLDecoder.decode(url, "UTF-8")) match {
                case Success(url: String) => url //если успех - возвращаем url
                case Failure(_) => "" //неуспех - возвращаем пустую строку вместо url
            }
        }
        val decode_udf = udf(decode_utf8(_))
        val decoded_df = df_filtered.withColumn("url", decode_udf(col("url")))
        decoded_df.filter(col("url").startsWith("https://") || col("url").startsWith("http://"))
    }

    def strip_url(df: DataFrame): DataFrame = {
        def remove_www(url: String) = {
            val host = new URL(url).getHost
            host match {
                case host if host.startsWith("www.") => host.stripPrefix("www.")
                case _ => host
            }
        }
        val remove_udf = udf(remove_www(_))
        df.withColumn("url", remove_udf(col("url")))
    }

    def preprocess_logs(df: DataFrame): DataFrame = {
        val filtered_df = filter_logs(df)
        val stripped_df = strip_url(filtered_df)
        stripped_df
    }

    def get_logs: DataFrame = {
        val logs_path = "/labs/laba02/logs"
        val logsSchema =
            StructType(
              List(
                StructField("uid", StringType),
                StructField("ts", StringType),
                StructField("url", StringType)
              )
            )
        val logs = spark.read.schema(logsSchema).option("delimiter", "\t").csv(logs_path)
        val processed_logs = preprocess_logs(logs)
        processed_logs
    }
        
    def calculate_relevance(users: DataFrame, logs: DataFrame): DataFrame = {
        val joined_df = logs.join(
            users, $"uid" === $"autousers", "left"
        ).select("url", "auto-flag").na.fill(Map("auto-flag" -> 0))
        val user_prob = joined_df.agg(sum(col("auto-flag"))).first.get(0)
        val agg_df = joined_df.groupBy("url").agg(sum(col("auto-flag")).alias("sum"), count(col("auto-flag")).alias("count"))
        val rel_df = agg_df.withColumn(
            "rel", col("sum") * col("sum") / (col("count") * lit(user_prob))
        ).orderBy(col("rel").desc, col("url").asc).select("url", "rel").withColumn(
            "rel", format_number(col("rel"), 15)
        )
        rel_df
    }

    def save_results(relevance_df: DataFrame, limit: Int, path: String) = {
        relevance_df.limit(limit).coalesce(1).write.mode("overwrite").option("delimiter", "\t").csv(path)
    }

    def main(save: Boolean): DataFrame = {
        val users = get_autousers
        val logs = get_logs
        users.show(1)
        logs.show(1)
        val relevance = calculate_relevance(users, logs)
        if (save == true) {
            save_results(relevance, 100, "lab02_result")
        }
        relevance
    }
}
