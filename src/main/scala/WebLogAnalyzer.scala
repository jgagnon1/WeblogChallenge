import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.Duration
import org.joda.time.format.ISODateTimeFormat

import scala.util.Try

object WebLogAnalyzer {

  val DateParser = ISODateTimeFormat.dateTimeParser()

  val RequestPattern = "\"(\\w+)\\s+(.*)\\s+HTTP\\/[0-9.]*\"".r

  val SessionDuration = Duration.standardMinutes(15)

  def main(args: Array[String]) {
    // Spark Configuration + Context creation
    val logFile = "data/2015_07_22_mktplace_shop_web_log_sample.log.gz"
    val conf = new SparkConf().setAppName("WebLog Analyzer").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // Read source Log file (from local path)
    val logData = sc.textFile(logFile, 4)

    // Extract Hits information from logs and emit (Visitor -> Hit) tuples
    val visitorHits = logData
      .flatMap(parseHitEntry)
      .map(hit => hit.visitorId -> hit)

    // 1. Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a fixed time window.
    val sessionPerVisitor = visitorHits
      .groupByKey()
      .map { case (visitorId, hits) =>
        val timeSortedHits = hits.toSeq.sortBy(_.timestamp.getMillis)

        // Fold through the list of hits and create sub-lists (sessions) based on SessionDuration
        val sessions = timeSortedHits.foldLeft(List.empty[List[Hit]]) { (sessions, entry) =>
          sessions.headOption.getOrElse(List.empty[Hit]) match {
            case current @ start :: _ if start.timestamp.plus(SessionDuration).getMillis >= entry.timestamp.getMillis =>
              // Same session; append the entry to the current list
              (current :+ entry) :: sessions.tail
            case _ =>
              // New session; star a new hits list
              List(entry) :: sessions
          }
        }

        // Instanciate sub-lists to (Visitor -> Sessions) tuples
        visitorId -> sessions.map(Session(visitorId, _))
      }
      // Cache the RDD at this point since this will be the base for all of the other steps
      .cache()

    // 2. Determine the average session time
    val sessionDurations = sessionPerVisitor
      .flatMap { case (_, sessions) => sessions.map(_.durationMs) }

    val averageSessionTimeMin = sessionDurations.mean() / (1000*60)
    val maximumSessionTimeMin = sessionDurations.max() / (1000*60)

    println(s"2. Average session time (minutes) : $averageSessionTimeMin\nMax session time (minutes) : $maximumSessionTimeMin")

    // 3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
    val uniqueVisitCountSession = sessionPerVisitor
        .flatMap { case (_, sessions) => sessions.map(_.uniqueVisitedUrls.size) }

    // For test-display purpose let's calculate the mean and the max of unique visits
    val averageUniqueVisitSession = uniqueVisitCountSession.mean()
    val maximumUniqueVisitSession = uniqueVisitCountSession.max()

    println(s"3. Average unique visit per session : $averageUniqueVisitSession\nMax unique visit per session : $maximumUniqueVisitSession")

    // 4. Find the most engaged users, ie the IPs with the longest session times
    val mostEngaged = sessionPerVisitor
      .map { case (visitorId, sessions) => visitorId -> sessions.foldLeft(0L)( _ + _.durationMs ) / (60.0*1000) }
      .takeOrdered(10)(Ordering.by { case (_, value) => -value })

    println(s"4. Top 10 most engaged users (IP, Sum of session duration (minutes)) :\n${mostEngaged.mkString("\n")}")
  }

  /***
    * Parse needed information from AWS Elastic load balancer log line into an Hit object
    *
    * @param logEntry - String representation of a log entry (line)
    * @return Hit instance with the information extracted from the log entry
    */
  private def parseHitEntry(logEntry: String): Option[Hit] = {
    // Log Format : timestamp elb client:port backend:port request_processing_time backend_processing_time response_processing_time elb_status_code backend_status_code received_bytes sent_bytes "request" "user_agent" ssl_cipher ssl_protocol
    val Array(timestamp, elbName, clientIP, backendIP, rest) = logEntry.split(" ", 5)

    val parsedRequest = RequestPattern.findFirstMatchIn(rest).map { matched =>
      Request(matched.group(1), normalizeURL(matched.group(2)))
    }

    for {
      Array(clientAddr, clientPort) <- Try(clientIP.split(":")).toOption
      request <- parsedRequest
    } yield Hit(DateParser.parseDateTime(timestamp).toInstant, elbName, Host(clientAddr, clientPort.toInt), request)
  }

  // Naive function that normalize URL according to simple heuristics
  private def normalizeURL(url: String): String = {
    url.split("\\?").head
      .replaceAll("https", "http")
  }

}
