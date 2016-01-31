import org.joda.time.Instant

case class Session(visitorId: String, hits: Seq[Hit]) {

  val durationMs: Long = hits.last.timestamp.getMillis - hits.head.timestamp.getMillis

  // Build a set of visited URL (unique per set definition)
  // This will be more efficient over time assuming the unique views are often used
  val uniqueVisitedUrls = hits.map(_.request.url).toSet

}

case class Host(address: String, port: Int)

case class Hit(timestamp: Instant, elb: String, clientHost: Host, request: Request) {

  // Use client IP as visitor id, however adding information like user-agent specific could add more uniqueness to it.
  val visitorId = clientHost.address

}

case class Request(method: String, url: String)
