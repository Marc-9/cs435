import org.apache.spark.sql.SparkSession

object WikiBomb {

	def main(args: Array[String]): Unit = {
		val sc = SparkSession.builder().master("spark://nashville:30220").getOrCreate().sparkContext

		val lines = sc.textFile(args(0))
		val links = lines.map(s=>(s.split(": ")(0), s.split(": ")(1)))
		val wikiBomb = links.mapValues(v => v.concat(" 4290745")).cache()


		val count = links.count()
		var ranks = wikiBomb.mapValues(v => 1.0 / count).cache()

		for(i <- 0 until 25) {
			val tempRank = wikiBomb.join(ranks).values.flatMap {
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
		val sortedWithTitles = ranks.join(indexTitle)
		val surfing = sortedWithTitles.filter(record => record._2._2.toLowerCase.contains("surfing") || record._2._2.equals("Rocky_Mountain_National_Park"))
		

		val sorted = surfing.sortBy(_._2, ascending=false, numPartitions=1)

		sorted.saveAsTextFile(args(1))

	}
}