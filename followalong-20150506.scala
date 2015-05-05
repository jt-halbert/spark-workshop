/*
 * This is an update to the scala script from the first workshop.
 */

import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import com.uebercomputing.mailparser.enronfiles.AvroMessageProcessor
import com.uebercomputing.mailrecord._
import com.uebercomputing.mailrecord.Implicits.mailRecordToMailRecordOps

val args = Array("--avroMailInput", "../../data/filemail.avro", "--hadoopConfPath", "hadoop-local.xml")
val config = CommandLineOptionsParser.getConfigOpt(args).get
val recordsRdd = MailRecordAnalytic.getMailRecordsRdd(sc, config)


def ratioLetters(s: String): Double = {
  val (numNotWhiteSpace, numIsLetter) = s.replaceAll("""\s+""","").foldLeft((0.0, 0.0)) {
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


val fromVince = recordsRdd.filter(_.getFrom=="vince.kaminski@enron.com")
val vinceBodies = fromVince.map(_.getBody)
val ratioStats = vinceBodies.map(ratioLetters).stats
val mostlyLetters = vinceBodies.filter(b => ratioLetters(b) > ratioStats.mean + ratioStats.stdev)
val uptoSig = ".*?\\s+Vince".r
val vinceCorpus = mostlyLetters.flatMap(uptoSig.findFirstIn)
vinceCorpus.cache
val sentenceSplitter = """(?<=[.\!\?])\s+(?=[A-Z])"""
val sentences = vinceCorpus.flatMap(_.split(sentenceSplitter)).filter(_!="Vince")
vinceCorpus.map(_.split(sentenceSplitter)).map(_.size.toDouble).stats
//note that the mean ~ stdev ... maybe it is a Gamma distribution?
// though it is discrete... so Negative Binomial.  I like to think so.
sentences.cache
val trigrams = sentences.map(List("","") ::: _.split("\\s+").toList ::: List("","")).flatMap(_.sliding(3))

val model = trigrams.map((_,1)).reduceByKey(_+_).map {case (triple, count) =>
              (triple.init, (triple.last, count))}.groupByKey.collect.toMap

//law of large numbers
Vector.fill(100)(sample(model(List("","")))).groupBy(identity).mapValues(_.size).toList.sortBy(_._2).reverse

case class State[S,+A](next: S => (A,S))
val randomWord = State[List[String], String](state => {
  val next = sample(model(state))
  (next, List(state.last, next))})

def randomSentence(): String = {
  def recur(acc: List[String], state: (String, List[String])): List[String] = state match {
    case ("", _) => acc
    case (w, ws) => recur(w::acc, randomWord.next(ws))
  }
  recur(List(), randomWord.next(List("",""))).reverse.mkString(" ")
}

def probabilityOfSentence(sentence: String): Double = {
   val words = List("",""):::sentence.split("\\s+").toList:::List("","")
   val triples = words.sliding(3).toList.map(l=>(l.init,l.last))
   triples.map { case (state, transitionTo) =>
    val possibleTransitions = model(state)
    val total = possibleTransitions.map(_._2).sum.toDouble
    val actualCount = possibleTransitions.find(_._1==transitionTo).getOrElse(("",0))._2.toDouble
    actualCount/total
   }.reduce(_*_)
}

//ideas: find most likely sentence starting with I
