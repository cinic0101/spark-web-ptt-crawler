import java.time.LocalDate

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.jsoup.Jsoup

import scala.collection.JavaConverters._

object DateIndexScanner {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .appName("PTT Page Date Scanner")
      .getOrCreate()
    val sc = sparkSession.sparkContext

    val board = args(0)
    val url = s"https://www.ptt.cc/bbs/$board/index.html"
    val doc = Jsoup.connect(url).cookie("over18", "1").get()
    val lastPageUrl = doc.select("div.btn-group-paging > a:contains(上頁)").first().attr("href")

    val indexRegex = """index(?<index>[0-9]+)\.html""".r
    val lastIndex = indexRegex.findFirstMatchIn(lastPageUrl) match {
      case Some(m) => m.group(1)
      case None => "1"
    }

    val begin = 20000 // It should be 1
    val last = lastIndex.toInt
    val indexes = sc.parallelize(begin to last)

    val dates = indexes.flatMap(i => {
      val url = s"https://www.ptt.cc/bbs/$board/index$i.html"
      val doc = Jsoup.connect(url).cookie("over18", "1").get()
      doc.select("div.r-ent > div.meta > div.date").iterator().asScala.map(_.text).toList.distinct.map((i, _))
    })
    
    val mappings = sparkSession.createDataFrame(dates).toDF("index", "date")
    val groupMappings = mappings.groupBy(mappings("date")).agg(min(mappings("index")).as("index"))

    val now = LocalDate.now
    val path = s"src/main/resources/$board-index-$now"
    groupMappings.orderBy(groupMappings("index")).coalesce(1).write.mode(SaveMode.Overwrite).csv(path)
  }
}
