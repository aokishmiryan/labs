class Ratings {
    val source: BufferedSource = fromFile("/data/home/arsen.kishmiryan/ml-100k/u.data")

    val lines: Seq[Array[String]] = source.getLines.toList.map(string => string.split("\t"))
    source.close()

    def missing_scores(rating: Map[String, Int]) = {
        for  { 
            i <- 1 to 5
            if (!rating.contains(i.toString))
        } yield {i.toString -> 0}
    }

    def add_missing_scores(rating: Map[String, Int], missing_scores: Map[String, Int]): Map[String, Int] = {
        rating ++ missing_scores
    }

    def sort_ratings(rating_map: Map[String, Int]): List[Int] = {
        rating_map.toList.sortBy(_._1).map(x => x._2)
    }

    def get_ratings(film_id: Option[String]): List[Int] = {
        val rating = film_id match {
            case Some(id) => lines.filter(x => x(1) == id).groupBy(x => x(2)).mapValues(_.size)
            case _ => lines.groupBy(x => x(2)).mapValues(_.size)
        }
        val missing_values = missing_scores(rating)
        val all_scores = add_missing_scores(rating, missing_values.toMap)
        sort_ratings(all_scores)
    }
}
