import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.Locale
import java.util.regex.{Matcher, Pattern}

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Exception._

object SimpleApp {

  val ddd = "\\d{1,3}"
  val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"
  val client = "(\\S+)"
  val user = "(\\S+)"
  val dateTime = "(\\[.+?\\])"
  val request = "\"(.*?)\""
  val status = "(\\d{3})"
  val bytes = "(\\S+)"
  val referer = "\"(.*?)\""
  val agent = "\"(.*?)\""
  val regex = s"$ip $client $user $dateTime $request $status $bytes $referer $agent"
  val p = Pattern.compile(regex)

//  var graph: Map[String, Map[String, Int]] = Map()
//  var users: Map[String, String] = Map()

//  def parseDate(dateField: String): Option[java.util.Date] = {
//    val dateRegex = "\\[(.*?) .+]"
//    val datePattern = Pattern.compile(dateRegex)
//    val dateMatcher = datePattern.matcher(dateField)
//    if (dateMatcher.find) {
//      val dateString = dateMatcher.group(1)
//      val dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
//      allCatch.opt(dateFormat.parse(dateString)) // return Option[Date]
//    } else {
//      None
//    }
//  }

  def getStatusCode(line: Option[AccessLogRecord]) = {
    line match {
      case Some(l) => l.httpStatusCode
      case None => "0"
    }
  }

  def getIp(rawAccessLogString: String) = {
    val accessLogRecordOption = parseLine(rawAccessLogString)
    accessLogRecordOption match {
      case Some(l) => l.clientIpAddress
      case None => "0"
    }
  }

  def getRequestParameters(rawAccessLogString: String): Option[String] = {
    val accessLogRecordOption = parseLine(rawAccessLogString)
    accessLogRecordOption match {
      //      case Some(rec) => Some( Map( ( extractUriFromRequest(rec.request), parseDate(rec.dateTime).get.toString() ) ) )
      case Some(rec) => Some(extractUriFromRequest(rec.request))
      case None => None
    }
  }



  def parseLine(record: String): Option[AccessLogRecord] = {
    val matcher = p.matcher(record)
    if (matcher.find) {
      Some(buildRecord(matcher))
    } else {
      println("Invalid request parameters.!")
      None
    }
  }

  def buildRecord(matcher: Matcher) = {
    AccessLogRecord(
      matcher.group(1),
      matcher.group(2),
      matcher.group(3),
      matcher.group(4),
      matcher.group(5),
      matcher.group(6),
      matcher.group(7),
      matcher.group(8),
      matcher.group(9)
    )
  }

  def extractUriFromRequest(requestField: String) = requestField.split(" ")(1).split("\\?")(0)

  def main(args: Array[String]) {

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost/log_parser"
    val username = "root"
    val password = "9414063897"
    var connection: Connection = null
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)
    val statement = connection.createStatement()
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val logFile = sc.textFile("zemoso_access_logs").cache()

    val validRequests = logFile.filter(line => getStatusCode(parseLine(line)) == "200")


//    val ips = validRequests.map(line => getIp(line)).cache().collect().distinct
//
//    for (ip <- ips) {
//      val userUrls = validRequests.filter(line => getIp(line) == ip)
//        .map(getRequestParameters(_).get).filter(line => !line.contains("."))
//        .collect.distinct
//      var userPath = ""
//      for (userUrl <- userUrls) {
//        userPath += userUrl + ", "
//      }
//      val resultSet = statement.executeQuery(s"SELECT * FROM user_paths WHERE path='$userPath'")
//      if (resultSet.next()) {
//        statement.execute(s"Update user_paths set user_count = user_count + 1 WHERE path='$userPath'")
//      }
//      else {
//        val prep = connection.prepareStatement("INSERT INTO user_paths VALUES (?, ?) ")
//        prep.setString(1, userPath)
//        prep.setString(2, 1.toString)
//        prep.executeUpdate
//      }
//    }
//    connection.close()
  }
}
