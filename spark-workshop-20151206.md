% Apache Spark Workshop
% Tetra Concepts  || Jailbreak Brewing Company.


![](./images/datacake.png)

# Workshop goals

* Work a data modeling problem with Spark (Dataframes and the basics of the RDD api).
* Learn some of Scala's cooler features (Collections api, regexes, a simple state machine).
* Leave feeling you are interested in learning more.

# Workshop non-goals

* Deploy and monitor Spark Applications
* Graphs (well, sort of, if you consider a Markov Process to be a graph... which it is, I guess...)
* MLlib (well... maybe if we have time)

# Overview of resources

* This presentation and "answers" to the "exercises" are available at
  [https://github.com/jt-halbert/spark-workshop/](https://github.com/jt-halbert/spark-workshop/).
* Thanks Markus!!
  [https://github.com/medale/spark-mail/](https://github.com/medale/spark-mail/)
* The data we will need is already in the docker image you are using.  It is a subset of the famous ENRON dataset.
    * [https://www.cs.cmu.edu/~./enron/](https://www.cs.cmu.edu/~./enron/)

# Who are we?

* I am Tetra's Chief Data Scientist and Spark is one of the tools I use to solve problems.
* Tetra Concepts is a group of talented engineers and scientists that are lucky enough to get to work on some of the most challenging national security problems.  We dare to dream small.

# Why Apache Spark?

* Excellent question

# We work with data

* NIH's BD2K: Major Challenges captures it pretty well.  We all need to
    * Locate the data
    * Get access to the data
    * Standardize the data
    * Extend policies and practices for sharing the data
    * Organize, manage, and process the data
    * _Develop new methods for analyzing the data_
    * Train people to use data more effectively


# How do we find new methods?

1. Find a Problem.
2. Find a Solution.
3. Automate?

Be very careful about the order.

# How do we find a solution?

* Use models.
    * Models are sets of equations involving random variables and their associated distributional assumptions.
    * These models are devised in the context of some question (problem) about some body of data concerning some phenomenon.
    * _Tentative answers_ can be derived along with some _measure of uncertainty._
* Doing things carefully means you _make smaller claims_ than if you just shoved your data into whatever Machine Learning approach was handy and then regurgitated the result.

# Why Apache Spark?

* Spark gives you a way to explore small, medium, large, (very large ?) data in
  a convenient way.
    * You can actually explore distributed datasets: lazy evaluation and a rich
      collections api.
    * You can scale your exploratory code up to a full job relatively quickly:
      REPL driven development.
* It wraps an increasing amount of the Hadoop Ecosystem and plays naturally.

# Let's get started

* The first step is to learn enough Scala to be dangerous.

# Combinator functions on Scala collections

* Examples: map, flatMap, filter, reduce, fold, aggregate
* Background - Combinatory logic, higher-order functions...

# Functional Programming

* An approach/style of programming that deals with expressions and values rather than statements.
* Functions are treated (to the extent possible) as Mathematical Functions (no side effects, deterministic)

# map

* Applies a given function to every element of a collection
* Returns collection of outputs of that function
* input argument - same type as collection type
* return type - can be any type

#

![](./images/map.png)

# map - Scala
```scala
def computeLength(w: String): Int = w.length

val words = List("when", "shall", "we", "three",
  "meet", "again")
val lengths = words.map(computeLength)

> lengths  : List[Int] = List(4, 5, 2, 5, 4, 5)
```

# map - Scala syntactic sugar
```scala
//anonymous function (specifying input arg type)
val list2 = words.map((w: String) => w.length)


//let compiler infer arguments type
val list3 = words.map(w => w.length)


//use positionally matched argument
val list4 = words.map(_.length)
```

# map - ScalaDoc

See [immutable List ScalaDoc](http://www.scala-lang.org/api/2.10.4/index.html#scala.collection.immutable.List)
```scala
List[+A]
...
final def map[B](f: (A) => B): List[B]
```
* Builds a new collection by applying a function to all elements of this list.
* B - the element type of the returned collection.
* f - the function to apply to each element.
* returns - a new list resulting from applying the given function f to each
          element of this list and collecting the results.

#

![](./images/whatif.png)

#

![](./images/flatMap.png)

# flatMap

* ScalaDoc:
```scala
List[+A]
...
def flatMap[B](f: (A) =>
           GenTraversableOnce[B]): List[B]
```

* [GenTraversableOnce](http://www.scala-lang.org/api/2.10.4/index.html#scala.collection.GenTraversableOnce) - List, Array, Option...

  * can be empty collection or None

* flatMap takes each element in the GenTraversableOnce and puts it in
order to output List[B]

  * removes inner nesting - flattens
  * output list can be smaller or empty (if intermediates were empty)


# flatMap Example
```scala
val charsNested = words.map(_.toCharArray)
// List(Array(w, h, e, n),
//      Array(s, h, a, l, l), ... )

val chars = words.flatMap(_.toCharArray)
// List(w, h, e, n, s, h, a, l, l,... )
```
# reduce
```scala
List[+A]
...
def reduce[A1 >: A](op: (A1, A1) => A1): A1
```
* Creates one cumulative value using the specified associative binary operator.
* A1 - A type parameter for the binary operator, a supertype (super or same) of A.
(List is covariant +A)
* op - A binary operator that must be associative.
* returns - The result of applying op between all the elements if the list is nonempty.
Result is same type as (or supertype of) list type.
* UnsupportedOperationException if this list is empty.

#

![](./images/reduce.png)

# reduce Example
```scala
//beware of overflow if using default Int!
val numberOfAttachments: List[Long] =
  List(0, 3, 4, 1, 5)
val totalAttachments =
  numberOfAttachments.reduce((x, y) => x + y)
//Order unspecified/non-deterministic, but one
//execution could be:
//0 + 3 = 3, 3 + 4 = 7,
//7 + 1 = 8, 8 + 5 = 13
```

# So what does this have to do with Apache Spark?

* Resilient Distributed Dataset ([RDD](https://spark.apache.org/docs/1.2.0/api/scala/#org.apache.spark.rdd.RDD))
* From API docs: "immutable, partitioned collection of elements that can be operated on in parallel"
* map, flatMap, filter, reduce, fold, aggregate...

# Spark - RDD API
* [RDD API](http://spark.apache.org/docs/1.2.0/api/scala/index.html#org.apache.spark.rdd.RDD)
* Transforms - map, flatMap, filter, reduce, fold, aggregate...

    * Lazy evaluation (not evaluated until action!)

* Actions - count, collect, first, take, saveAsTextFile...

# Spark - From RDD to Dataframes

* A _DataFrame_ is a distributed collection of data organized into named (and typed) columns.
* Think table in a relational database, or dataframe in R or Python Pandas.
* Critical pieces for us
    * sqlContext.read.json
    * show
    * printSchema
    * select
    * groupBy
    * count
    * AND STARRING! sql

# OK, time for puzzles

* Goal: build a Text Generator using a simple Markov Model.
    * Generate a corpus to work with (Dataframes)
    * Construct Tri-grams from that corpus (RDD API)
    * Use those to construct a Markov Model (RDD API)
    * Use the model to generate random sentences. (Cool Scala Tricks)

# Generate a Corpus (Dataframes)

* First, load the data.  Hints:

```bash
./data/enron_small.json.gz
```

* Next: who sent the most email? Hints:

```scala
   .groupBy("headers.From")
                        
    .count()
    
    .sort(desc("count"))
```


# Generate a Corpus (Dataframes)

* Find all the email bodies from Vince that contain mostly letters.
    * This is somewhat arbitrary, but it limits to emails without spreadsheets pasted into them.
* Hint: Use this (if you want)

```scala
def ratioLetters(s: String): Double = {
  val (tot, n) = s.replaceAll("""\s+""","")
                  .foldLeft((0.0, 0.0)) {
    (acc, c) => {
      (acc._1 + 1, acc._2 + (if (c.isLetter) 1 else 0))
    }
  }
  n/tot
}
```

# Generate a Corpus (Dataframes: User Defined Functions)

* Register the function and use it in SQL statements.  Hints:
```scala
sqlContext.udf.register
registerTempTable
sqlContext.sql
```
* Get only those emails that have a high ratio of Letters. Hint:

```scala
sqlContext.sql("SELECT ... FROM ... WHERE ...")
```

# Extra: Concerns and issues with UDFs
* Note: this function doesn't work outside of SQL

```scala
fromVinceDF.select($"payload",ratioLetters($"payload"))
// FAIL!
```
* Need to

```scala
import org.apache.spark.sql.functions.udf
val ratioLettersUDF = udf(ratioLetters(_:String))
fromVinceDF.select($"payload",ratioLettersUDF($"payload"))
```

# Interactive investigation (RDD takeSample)

* Can we find just the text that belongs to Vince?
```scala
vinceTextDF.rdd.takeSample(false,10).foreach(println)
```
* Seems there are two natural breaks: "-------- Forwarded By" and his closer "Vince"

```scala
val upToForward = "---------------------- Forwarded by".r
val upToSig = ".*?\\s+(?=Vince)".r
val vinceText = vinceTextDF.rdd
                    .map(_.getString(0))
                    .map(_.replaceAll("\n"," "))
val corpus = vinceText.flatMap{
                s=>upToSig
                    .findFirstIn(upToForward.split(s)(0))}
val sentenceSplitter = """(?<=[.\!\?])\s+(?=[A-Z])"""
val sentences = corpus.flatMap(_.split(sentenceSplitter))
sentences.cache
```

# Build an RDD of trigrams

* e.g. "This is a simple sentence." becomes 
```scala
("","","This")
("","This","is")
("This","is","a")
("is","a","simple")
("a","simple","sentence.")
("simple","sentence.","")
("sentence.","","")
```
* Hint: 
```scala
 _.split("\\s+")
```
and
```scala
.sliding(3)
```

# Build a model (Challenge)

* Reduce to ```RDD[((String,String),(String, Int))]``` of Antecedent Pair of Word followed by Consequent Word and count.

# Build a Vince Sentence Generator (Challenge)

* Hint:  Multinomial Sampler

```scala
def sampleMultinomial[T](dist: List[(T, Int)]): T = {
  val p = scala.util.Random.nextDouble*dist.map(_._2).sum
  def recur(acc: Int, d: List[(T, Int)]): T = d match {
    case (t,n) :: Nil => t
    case (t,n) :: ds  => if (acc + n >= p) t else {
      recur(acc+n,ds)
    }
  }
  recur(0,dist)
}
```
#

![](./images/multinomial.png)
