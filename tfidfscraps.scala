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
val uuids = sc.broadcast(recordsRdd.map(_.getUuid).collect)
val terms = sc.broadcast(termhist.map(_._1).filter(termFilter).collect.sortBy(identity))


val wordDocRDD: RDD[(String, String)] = recordsRdd.flatMap(rec => bootlegTokenize(rec.getBody).map((_,rec.getUuid)))
wordDocRDD.cache
val wordCorpusHist: RDD[(String, Int)] = wordDocRDD.groupByKey.mapValues(_.toSet.size)
wordCorpusHist.cache
val wordDocHist: RDD[((String, String), Int)] = wordDocRDD.map((_,1)).reduceByKey(_+_)
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

