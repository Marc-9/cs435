import org.apache.spark.sql.SparkSession

object PageRank {

	def main(args: Array[String]): Unit = {
		val sc = SparkSession.builder().master("spark://olympia:30154").getOrCreate().sparkContext

		val lines = sc.textFile(args(0))
		val links = lines.map(s=>(s.split(": ")(0), s.split(": ")(1))).cache()
		val count = links.count()

		var ranks = links.mapValues(v => 1.0 / count).cache()


		for(i <- 0 until 25) {
			val tempRank = links.join(ranks).values.flatMap {
				case (urls, rank) => 
					val outgoingLinks = urls.split(" ")
					outgoingLinks.map(url => (url, rank / outgoingLinks.length))
			}
			ranks = tempRank.reduceByKey(_+_)
		}



		val titles = sc.textFile("/titles").zipWithIndex()
		
		val indexTitle = titles.map{
				case (title,index) => (s"${index + 1}", title)
			}

		var sortedWithTitles = ranks.join(indexTitle)
		

		val sorted = sortedWithTitles.sortBy(_._2, ascending=false, numPartitions=1)

		sorted.saveAsTextFile(args(1))

	}
}