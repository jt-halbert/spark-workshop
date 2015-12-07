// read in data
val enronDF = sqlContext.read.json("data/enron_small.json.gz")

// who sent the most email?
val fromCounts = enronDF.groupBy("headers.From").count.sort(desc("count"))

val fromVinceDF = enronDF.where($"headers.From".startsWith("vince.ka"))

// Find all the email bodies from Vince that contain mostly letters

def ratioLetters(s: String): Double = {
  val (tot, n) = s.replaceAll("""\s+""","")
                  .foldLeft((0.0, 0.0)) {
    (acc, c) => {
      (acc._1 + 1, acc._2 + (if (c.isLetter) 1 else 0))
    }
  }
  n/tot
}

// register and use in SQL statements
sqlContext.udf.register("ratioLetters",(s:String) => ratioLetters(s))
fromVinceDF.registerTempTable("vinceEmails")
sqlContext.sql("SELECT payload, ratioLetters(payload) as ratioLetters FROM vinceEmails")
val vinceTextDF = sqlContext.sql("SELECT payload FROM vinceEmails WHERE ratioLetters(payload) > 0.8")

// Can we find the text that belongs to Vince?

vinceTextDF.rdd.takeSample(false,10).foreach(println)
val upToForward = "---------------------- Forwarded by".r
val upToSig = ".*?\\s+(?=Vince)".r
val vinceText = vinceTextDF.rdd.map(_.getString(0)).map(_.replaceAll("\n"," "))
val corpus = vinceText.flatMap(s=>upToSig.findFirstIn(upToForward.split(s)(0)))
val sentenceSplitter = """(?<=[.\!\?])\s+(?=[A-Z])"""
val sentences = corpus.flatMap(_.split(sentenceSplitter))
sentences.cache

import scala.annotation.tailrec

def sampleMultinomial[T](dist: List[(T, Int)]): T = {
  val p = scala.util.Random.nextDouble*dist.map(_._2).sum
  require(dist != Nil, "Distributions must be nonempty.")
  @tailrec
  def recur(acc: Int, d: List[(T, Int)]): T = d match {
    case (t,n) :: Nil => t
    case (t,n) :: ds  => if (acc + n >= p) t else {
      recur(acc+n,ds)
    }
  }
  recur(0,dist)
}

val trigrams = sentences.map(List("","") ::: _.split("\\s+").toList ::: List("","")).flatMap(_.sliding(3))

val model = trigrams.map((_,1)).reduceByKey(_+_).map {case (List(w1,w2,w3), count) =>
              ((w1,w2), (w3, count))}.groupByKey.collect.toMap

// What are the most popular starting words in sentences?
model(("","")).toList.sortBy(_._2).reverse.take(10).foreach(println)


//law of large numbers
Vector.fill(100)(sampleMultinomial(model(("","")).toList)).groupBy(identity).mapValues(_.size).toList.sortBy(_._2).reverse

//case class State[S,+A](next: S => (A,S))
// The case class approach is neater but doesn't work in function arguments like the sentence Generator.. why?
class State[S,+A](trans: S => (A,S)) {
  def next = trans
}

val randomWord = new State[(String,String), String](state => {
  val next = sampleMultinomial(model(state).toList)
  (next, (state._2,next))})

val mostLikelyWord = new State[(String,String), String](state => {
   val next = model(state).toList.sortBy(_._2).reverse.head._1
   (next, (state._2, next))
})

def randomSentence(): String = {
  def recur(acc: List[String], trans: (String, (String, String))): List[String] = trans match {
    case ("", _) => acc //end of sentence
    case (n, state) => recur(n::acc, randomWord.next(state)) //N.B. prepending to list is best
  }
  recur(List(), randomWord.next(("",""))).reverse.mkString(" ")
}

def sentenceGenerator(stateTransition: State[(String, String), String]): String = {
  def recur(acc: List[String], trans: (String, (String, String))): List[String] = trans match {
    case ("", _) => acc //end of sentence
    case (n, state) => recur(n::acc, stateTransition.next(state)) //N.B. prepending to list is best
  }
  recur(List(), stateTransition.next(("",""))).reverse.mkString(" ")
}

// extras
//
// different implementations of sampler:

def sample(dist: List[(String, Int)]): String = {
  val p = scala.util.Random.nextDouble*dist.map(_._2).sum
  val iter = dist.iterator
  var accum = 0.0
  while (iter.hasNext) {
    val (item, itemProb) = iter.next
    accum += itemProb
    if (accum >= p)
      return item  // return so that we don't have to search through the whole distribution
  }
  sys.error("This will never be reached.")
}

// test timing differences
//
def time[R](block: => R): R = {
   val t0 = System.currentTimeMillis
   val result = block
   println("Elapsed time: " + (System.currentTimeMillis - t0)+"ms")
   result
}

val test = (1 to 100).map(_.toString).zip(Vector.fill(100)(5)).toList
time(Vector.fill(100000)(sample(test)))
time(Vector.fill(100000)(sampleMultinomial(test)))


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



def greedySentence(startState: List[String]): String = {
  def recur(acc: List[String], state: (String, List[String])): List[String] = state match {
    case ("", _) => acc
    case (w, ws) => recur(w::acc, mostLikelyWord.next(ws))
  }
  (recur(List(), mostLikelyWord.next(startState)):::List(startState.last)).reverse.mkString(" ")
}

// only works on sentences drawn from original corpus... pretty busted really.
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


// Did you actually read this far?  Email me at jt at tetraconcepts.com if you are interested in doing cool work.
