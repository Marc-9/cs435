import org.apache.spark.sql.SparkSession

object PageRankWeighted {
    def main(args: Array[String]): Unit = {
		val sc = SparkSession.builder().master("spark://olympia:30154").getOrCreate().sparkContext

		val lines = sc.textFile(args(0))
		val links = lines.map(s=>(s.split(": ")(0), s.split(": ")(1))).cache()
		val count = links.count()

		var weightedRanks = links.mapValues(v => 1.0 / count).cache()
		val beta = 0.85


		for(i <- 0 until 25) {
			val tempRankWeight = links.join(weightedRanks).values.flatMap {
				case (urls, rank) => 
					val outgoingLinks = urls.split(" ")
					outgoingLinks.map(url => (url, rank / outgoingLinks.length))
			}

			weightedRanks = tempRankWeight.reduceByKey(_+_).mapValues(v => v*beta + (1.0-beta)*(1.0/count))
		}



		val titles = sc.textFile("/titles").zipWithIndex()
		
		val indexTitle = titles.map{
				case (title,index) => (s"${index + 1}", title)
			}

		var rankedSortedWithTitles = weightedRanks.join(indexTitle)

		val rankedSorted = rankedSortedWithTitles.sortBy(_._2, ascending=false, numPartitions=1)

		rankedSorted.saveAsTextFile(s"${args(1)}_taxation")

	}
}