/*
 * This scala script contains all the exercises from the workshop.
 */

// First the necessary utilities to speed things along
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.collection.JavaConverters._
import com.uebercomputing.mailparser.enronfiles.AvroMessageProcessor
import com.uebercomputing.mailrecord._
import com.uebercomputing.mailrecord.Implicits.mailRecordToMailRecordOps
import org.apache.avro.mapred.AvroKey
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import scala.math._
import org.joda.time._

// load data
val args = Array("--avroMailInput", "enron.avro")
val config = CommandLineOptionsParser.getConfigOpt(args).get
val recordsRdd = MailRecordAnalytic.getMailRecordsRdd(sc, config)
val numDocs = recordsRdd.count


// First question: What do the folders look like?  Find a histogram of the number of folders per user.
def pairLift[A,B](tup: (Option[A], Option[B])): Option[(A,B)] = {
    if (tup._1.isDefined && tup._2.isDefined) Some((tup._1.get, tup._2.get))
    else None
}
val userfolders = recordsRdd.map(rec => (rec.getMailFields.asScala.toMap.get("UserName"), rec.getMailFields.asScala.toMap.get("FolderName"))).flatMap(tup => pairLift(tup))
userfolders.cache
val foldercountbyuser = userfolders.groupByKey.mapValues(_.toSet.size)
foldercountbyuser.map(_._2.toDouble).stats
// (count: 150, mean: 22.033333, stdev: 26.773474, max: 193.000000, min: 2.000000)
foldercountbyuser.map(_._2.toDouble).histogram(100)._2.toList.foreach(n => println("*"*n.toInt))
// Who has 193?
foldercountbyuser.map { case (user, numfolders) => (numfolders, user) }.lookup(193)
// What folders does he have?
userfolders.groupByKey.mapValues(_.toSet).lookup("kean-s")(0).toList.sortBy(identity).foreach(println)
// lol@kean-s he even organized his deleted items
// what if we ignore him?
foldercountbyuser.filter(_._2 < 22 + 2*27).map(_._2.toDouble).stats

// ------------------

// What hour of the week do people send the most email?
val byDayHour = recordsRdd.map(rec => {
  val dt = new DateTime(rec.getDateUtcEpoch)
  (dt.dayOfWeek.getAsText, dt.hourOfDay.getAsText)
})
byDayHour.cache
val dhcounts = byDayHour.map((_,1)).reduceByKey(_+_).collect
dhcounts.sortBy(_._2).reverse.foreach{ case ((dow,how),n) => println(s"$dow at $how : $n") }

// What do mondays look like?
val byWeeks = recordsRdd.map(_.getDateUtcEpoch).sortBy(identity).map(instant => {
    val dt = new DateTime(instant)
    (dt.dayOfWeek.getAsText, dt.weekOfWeekyear.getAsText,dt.year.getAsText)})
byWeeks.cache
val mondays = byWeeks.filter { case (dow, woy, y) => dow=="Monday"}
mondays.cache
val mondayWeekHist = mondays.map((_,1)).reduceByKey(_+_)
val mondayWeekTimeSeries = mondayWeekHist.collect.sortBy {
  case ((dow,woy,y), num) => {
    val dt = (new DateTime()).withWeekOfWeekyear(woy.toInt).withYear(y.toInt)
    dt.toInstant.getMillis
  }
}
val maximumNum = mondayWeekHist.map(_._2).max
mondayWeekTimeSeries.map(_._2/maximumNum.toDouble*100).foreach(d => println("*"*d.toInt))

// Challenge Exercise: Find emails discussing the FBI Investigation.

val TFIDF = sc.objectFile[(String, String, Double)]("TFIDF")
val docTFIDF = TFIDF.map { case (uuid, term, tfidf) => (uuid, (term,tfidf))}.groupByKey
val summaries = docTFIDF.map { case (uuid, terms) => (uuid, terms.toList.sortBy(_._2).reverse.map(_._1).take(10))}
summaries.cache
val investigationemails = summaries.filter(_._2.contains("investigation"))
investigationemails.count
investigationemails.collect.foreach(println)

// Now can we get back the actual messages?

val byUuid = recordsRdd.map(rec => (rec.getUuid, rec))
byUuid.lookup("e688b416-2ac3-4ff8-9ada-2a5bc3a148fc")

val FBIinvestigationemails = summaries.filter { case (uuid, summary) => summary.contains("investigation") && summary.contains("fbi")}
val thejoin = FBIinvestigationemails.join(byUuid)
thejoin.cache
thejoin.count

val firsttime = thejoin.map { case (uuid, (summary, record)) => record.getDateUtcEpoch}.min
val lasttime = thejoin.map { case (uuid, (summary, record)) => record.getDateUtcEpoch}.max

val duringInvestigation = byUuid.filter { case (uuid, record) => {
  val t = record.getDateUtcEpoch
  t > firsttime && t < lasttime}}

def sameDayAs(t1: Long, t2: Long): Boolean = {
  val dt1 = new DateTime(t1)
  val dt2 = new DateTime(t2)
  dt1.getYear == dt2.getYear && dt1.getDayOfYear == dt2.getDayOfYear }

val firstFBIday = duringInvestigation.filter { case (uuid, record) => sameDayAs(firsttime,record.getDateUtcEpoch) }

firstFBIday.join(summaries).collect.sortBy { case (uuid, (record, summary)) => record.getDateUtcEpoch }.foreach { case (uuid, (record, summary)) => println(summary) }


// Time for new stuff

def ratioLetters(s: String): Double = {
  val (numNotWhiteSpace, numIsLetter) = "\\s+".r.replaceAllIn(s,"").foldLeft((0.0, 0.0)) {
    (acc, c) => {
      (acc._1 + 1, acc._2 + (if (c.isLetter) 1 else 0))
    }
  }
  numIsLetter/numNotWhiteSpace
}

def stringDistance(s1: String, s2: String): Int = {
  def sd(s1: List[Char], s2: List[Char], costs: List[Int]): Int = s2 match {
    case Nil => costs.last
    case c2 :: tail => sd( s1, tail,
        (List(costs.head+1) /: costs.zip(costs.tail).zip(s1))((a,b) => b match {
          case ((rep,ins), chr) => Math.min( Math.min( ins+1, a.head+1 ), rep + (if (chr==c2) 0 else 1) ) :: a
        }).reverse
      )
  }
  sd(s1.toList, s2.toList, (0 to s1.length).toList)
}

def sample(dist: Iterable[(String, Int)]): String = {
  val p = scala.util.Random.nextDouble*dist.map(_._2).sum
  val iter = dist.iterator
  var accum = 0.0
  while (iter.hasNext) {
    val (item, itemProb) = iter.next
    accum += itemProb
    if (accum >= p)
      return item  // return so that we don't have to search through the whole distribution
  }
  sys.error("something totally crazy happened.")
}

val sentenceSplitter = """(?<=[.\!\?])\s+(?=[A-Z])"""

