/*
 * This scala script contains all the exercises from the workshop.
 */

// First the necessary utilities to speed things along
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import com.uebercomputing.mailparser.enronfiles.AvroMessageProcessor
import com.uebercomputing.mailrecord._
import com.uebercomputing.mailrecord.Implicits.mailRecordToMailRecordOps
import org.apache.avro.mapred.AvroKey
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import scala.math._
import org.joda.time._

// load data 
val data = sc.newAPIHadoopFile("enron.avro",classOf[MailRecordInputFormat], classOf[AvroKey[MailRecord]], classOf[FileSplit],sc.hadoopConfiguration)
val records = data.map(_._1.datum())

// or easier
val args = Array("--avroMailInput", "enron.avro")
val config = CommandLineOptionsParser.getConfigOpt(args).get
val recordsRdd = MailRecordAnalytic.getMailRecordsRdd(sc, config)
val numDocs = recordsRdd.count

val tokenizedBodies = recordsRdd.map(_.getBody).map(p => p.toLowerCase.replaceAll("[^a-z\\s]", " ").split("\\s+")).map(_.filter(l => l.size > 0 && l.size < 77))
tokenizedBodies.cache
val numDocs = tokenizedBodies.count
// find the number of times each word appears across the corpus
val termhist = tokenizedBodies.flatMap(_.distinct.map(w => (w,1))).reduceByKey((a,b) => a + b)
termhist.cache
//investigate: how many words appear in only one document across the corpus?
termhist.collect.sortBy(_._2).toList.prefixLength(_._2==1)
// 187,949
val termMap = sc.broadcast(termhist.collect.toMap)


def lengthLongestRun(s: String): Int = s.toList.tails.map(l => l.prefixLength(_ == l(0))).toList.max
val stopwords = Array("the", "and", "enron", "for", "com", "you", "ect", "that", "this", "with", "from", "will", "have", "are", "hou")
def termFilter(term: String): Boolean = term.size > 2 &&
                                        term.size < 77 &&
                                        !stopwords.contains(term) &&
                                        lengthLongestRun(term) < 5 &&
                                        termMap.value(term) > 1

def bootlegTokenize(body: String): List[String] = {
  body.toLowerCase.replaceAll("[^a-z\\s]", " ").split("\\s+").toList.filter(termFilter(_))
}
// TODO: these should be broadcast
val uuids = sc.broadcast(recordsRdd.map(_.getUuid).collect)
val terms = sc.broadcast(termhist.map(_._1).filter(termFilter).collect.sortBy(identity))


val wordDocRDD: RDD[(String, String)] = recordsRdd.flatMap(rec => bootlegTokenize(rec.getBody).map((_,rec.getUuid)))
wordDocRDD.cache
val wordCorpusHist: RDD[(String, Int)] = wordDocRDD.groupByKey.mapValues(_.toSet.size)
wordCorpusHist.cache
val wordDocHist: RDD[(String, String), Int] = wordDocRDD.map((_,1)).reduceByKey(_+_)
val wordDocReshaped: RDD[(String, (String, Int))] = wordDocHist.map(t => (t._1._1, (t._1._2,t._2)))
wordDocReshaped.cache
val docFreqTermFreqRDD: RDD[(String, Int, String, Int)] = wordCorpusHist.join(wordDocReshaped).map {case (word, (rdf, (uuid, rtf))) => (word, rdf, uuid, rtf)}
docFreqTermFreqRDD.cache
val reshape = docFreqTermFreqRDD.map(t => (t._3, (t._4,t._1,t._2)))
val maxFreqByDoc = docFreqTermFreqRDD.map(t => (t._3, (t._4,t._1,t._2))).map(t => (t._1,t._2._1)).reduceByKey(max)
val finalRawTermFreqDocFreqMaxFreqRDD = reshape.join(maxFreqByDoc).map {case (uuid,((rtf,word,rdf), maxtf)) => (uuid, rtf, word, rdf, maxtf) }
finalRawTermFreqDocFreqMaxFreqRDD.cache

def tfidf(rtf: Int, rdf: Int, mtf: Int): Double = {
    val tf = 0.5 + 0.5*rtf.toDouble/mtf.toDouble
    val idf = log(numDocs.toDouble/rdf.toDouble)
    tf*idf
}
val TFIDF = finalRawTermFreqDocFreqMaxFreqRDD.map { case (uuid, rtf, word, rdf, maxtf) => (uuid,word,tfidf(rtf,rdf,maxtf))}
// have access to uuids, terms, termMap
recordsRdd.flatMap(rec => bootlegTokenize(rec.getBody).map(t => (uuids.indexOf(rec.getUuid), terms.indexOf(t))))
val TFIDF = recordsRdd.flatMap(rec => {
    val body = rec.getBody
    val uuid = rec.getUuid
    val tokens = bootlegTokenize(body)
    val inDocTermHist = tokens.groupBy(identity).mapValues(_.size)
    //val maxFreqOfAnyTermInDoc = inDocTermHist.map(_._2).max.toDouble
    def tf(term: String): Double = 0.5 + 0.5*inDocTermHist(term)
    def idf(term: String): Double = log(numDocs.toDouble/termMap.value(term).toDouble)
    tokens.distinct.map(term => (uuids.value.indexOf(uuid), terms.value.indexOf(term), tf(term)*idf(term)))
})
// Try again on RDD


// folder count play
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

// date play
val byTimeBoundary = recordsRdd.map(rec => {
    val dt = new DateTime(rec.getDateUtcEpoch)
    val dow = dt.dayOfWeek.getAsText
    val hod = dt.hourOfDay.getAsText
    (dow,hod)
})
byTimeBoundary.cache
val dhcounts = byTimeBoundary.map((_,1)).reduceByKey(_+_).collect
dhcounts.sortBy(_._2).reverse.foreach{ case ((dow,how),n) => println(s"$dow at $how : $n") }

val mondays = byTimeBoundary.filter(_._1=="Monday")
val mondayWeekHist = mondays.filter(_._3=="2001").map(t=> (t._2,1)).reduceByKey(_+_).collect.sortBy(_._1.toInt)
val maximumNum = mondayWeekHist.map(_._2).max
mondayWeekHist.map(_._2/maximumNum.toDouble*100).foreach(d => println("*"*d.toInt))
