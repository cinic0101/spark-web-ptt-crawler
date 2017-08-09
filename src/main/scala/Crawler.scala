import org.apache.spark.sql.SparkSession
import org.jsoup.Jsoup

import scala.collection.JavaConverters._

object Crawler {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .appName("PTTCrawler")
      .getOrCreate()
    val sc = sparkSession.sparkContext

    val domain = sc.broadcast("https://www.ptt.cc")
    val titleRegex = sc.broadcast("""(?<reply>Re:)?\s*\[(?<category>.+)\]\s*(?<title>.+)""".r)
    val linkRegex = sc.broadcast("""/bbs\/\S+\/(?<id>\S+)\.html""".r)

    val urls = sc.textFile(args(0))
    urls.foreach(url => {
      val doc = Jsoup.connect(url).cookie("over18", "1").get()
      //val lastPageUrl = doc.select("div.btn-group-paging > a:contains(上頁)").first().attr("href")

      val links = doc.select("div.r-ent").iterator().asScala.map(x => {
        val titleLink = x.select("div.title > a")
        val titleText = titleLink.text
        val link = titleLink.attr("href")


        val title = titleRegex.value.findFirstMatchIn(titleText) match {
          case Some(m) => (if(m.group(1) != null) 1 else 0, m.group(2), m.group(3))
          case None => (0, "", titleText)
        }
        val id = linkRegex.value.findFirstMatchIn(link) match {
          case Some(m) => m.group(1)
          case None => ""
        }

        val date = x.select("div.meta > div.date").text
        val author = x.select("div.meta > div.author").text

        (title._1, date, id, author, title._2, title._3, if(link.isEmpty) "" else domain.value + link)
      })


      links.foreach(println)
    })
  }
}
