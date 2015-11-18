Benford Analysis for Apache Spark
-----------

Analysis and detection methods based on Benford's Law for Apache Spark.

Goal
====

[Benford's Law](https://en.wikipedia.org/wiki/Benford's_law) is a powerful tool with a wide range of applications.

### Accounting

It has been used for detecting fraud in accounting records for several years. Accounting records are supposed to follow Benford's Law and large deviations from the expected probabilities for the first two significant digits may suggest the numbers have been tampered with.

Traditional statistical tests, though, yield a large number of false positives when they are applied to large data sets. That is commonly known as the "excessive power" of the tests.

Recently, two techniques based on bootstrap procedures were developed to tackle this problem yielding higher accuracy and sensitivity.

The bootstrap procedure is very computationally-expensive yet highly parallelizable, so one of the goals of the ***Benford Analysis for Spark*** package is to enable the application of these new techniques in large data sets while delivering results fast.

For further details on the procedures and the decision criteria, please refer to:

[SUH, I.; HEADRICK, T.C. A comparative analysis of the bootstrap versus traditional statistical procedures applied to digital analysis based on Benford’s Law. Journal of Forensic and Investigative Accounting, Vol. 2, n. 2, p. 144-175, 2010.](http://epublications.marquette.edu/cgi/viewcontent.cgi?article=1031&context=account_fac)

[SUH, I., HEADRICK, T.C.; MINABURO, S. An Effective and Efficient Analytic Technique: A Bootstrap Regression Procedure and Benford's Law. Journal of Forensic & Investigative Accounting, Vol. 3, n. 3, p.25-44, 2011.](http://epublications.marquette.edu/cgi/viewcontent.cgi?article=1045&context=account_fac)

### Signal and Image Processing

Benford's Law can also be used to detect weak peaks in signals and edges in images.

For further details, please refer to:

[BHOLE, G.; SHUKLA, A.; MAHESH, T.S. Benford Analysis: A useful paradigm for spectroscopic analysis](http://arxiv.org/abs/1408.5735)

### Suspicious Behavior in Social Networks

Another use of Benford's Law is the detection of suspiciously behaving nodes in social networks.

For further details, please refer to:

[GOLDBECK, J. Benford's Law Applies to Online Social Networks](http://journals.plos.org/plosone/article?id=10.1371/journal.pone.0135169)

Installation
============

To include this package in your Spark Application:

1- Download this repository.

2- Build an uberjar containing all dependencies with `sbt assembly`.

3- Include the uberjar `spark-benford-analysis-assembly-0.0.1-SNAPSHOT.jar` both in the `--jars` and `--driver-class-path` parameters of `spark-submit`.

### spark-shell, pyspark, or spark-submit

```
> $SPARK_HOME/bin/spark-shell --packages dvgodoy:spark-benford-analysis:0.0.1-SNAPSHOT
```

### sbt

If you use the [sbt-spark-package plugin](https://github.com/databricks/sbt-spark-package),
in your sbt build file, add:

```scala
spDependencies += "dvgodoy/spark-benford-analysis:0.0.1-SNAPSHOT"
```

Otherwise,

```scala
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

libraryDependencies += "dvgodoy" % "spark-benford-analysis" % "0.0.1-SNAPSHOT"
```

Examples
========

In order to perform analyis of accounting records and fraud detection, you need to import the following objects and case classes:

```scala
scala> import com.dvgodoy.spark.benford.distributions.{Benford, Bootstrap}
scala> import com.dvgodoy.spark.benford.util.{JobId, DataByLevel, ResultsByLevel, StatsCIByLevel}
```

All results are returned as JsValues, so you also need:

```scala
scala> import play.api.libs.json._
```

You should start by creating both Bootstrap and Benford objects which will perform bootstrap estimations based on frequencies of your data and an exact Benford distribution. Then you need to set the number of bootstrap samples to be used (usually 25,000). In this example we set it to 1,000 to speed up calculations.

```scala
scala> val boot = Bootstrap()
scala> val benf = Benford()
scala> val numSamples = 1000
```

Next you should name your Spark Jobs by creating an implicit parameter of the JobId class. In this example we named our job "test".

```scala
scala> implicit val jobId = JobId("test")
```

Then you should point to the data you want to analyze. It must be a CSV file containing the values in the first column and the hierarchical structure associated with those values in the columns that follow, from top-level in the left-most column to bottom-level in the right-most column. Please note that the hierarchical structure is NOT expected to be numeric.

Your file should look like this:

```
10.35,TOPLEVEL,SUBLEVEL 1,SUBSUBLEVEL 1.1
128.17,TOPLEVEL,SUBLEVEL 1,SUBSUBLEVEL 1.2
8712.33,TOPLEVEL,SUBLEVEL 2,SUBSUBLEVEL 2.1
...
```

A test file is provided with the package and we point to it in this example:
```
scala> val filePath = "./src/test/resources/datalevels.csv"
```

Now you can load your file using:

```scala
scala> val data =  boot.loadData(sc, filePath)
data: com.dvgodoy.spark.benford.util.DataByLevel = DataByLevel(Map(0 -> (L.1,0), 5 -> (L.1.B.3,2), 1 -> (L.1.A,1), 6 -> (L.1.B.4,2), 2 -> (L.1.A.1,2), 3 -> (L.1.A.2,2), 4 -> (L.1.B,1)),Map(0 -> [J@181098bf, 5 -> [J@632b5c79, 1 -> [J@6a552721, 6 -> [J@3815a7d1, 2 -> [J@24dc150c, 3 -> [J@1d2d4d7a, 4 -> [J@5e020dd1),[Lcom.dvgodoy.spark.benford.util.package$FreqByLevel;@4bbc02ef,MapPartitionsRDD[27] at map at Bootstrap.scala:141)
```

It returns a `DataByLevel` class with contains information regarding:

1- all the unique combinations (groups) of levels in the data (map where keys are generated unique IDs associated with each combination, values contain both generated name by concatenation of level names and the depth of the level):

```scala
scala> data.levels
res0: Map[Long,(String, Int)] = Map(0 -> (L.1,0), 5 -> (L.1.B.3,2), 1 -> (L.1.A,1), 6 -> (L.1.B.4,2), 2 -> (L.1.A.1,2), 3 -> (L.1.A.2,2), 4 -> (L.1.B,1))
```

2- hierachical structure of all groups (map from each group to its children):

```scala
scala> data.hierarchy
res1: Map[Long,Array[Long]] = Map(0 -> Array(4, 1), 5 -> Array(-1), 1 -> Array(2, 3), 6 -> Array(-1), 2 -> Array(-1), 3 -> Array(-1), 4 -> Array(6, 5))
```

You can also get information regarding frequencies of both first and second digits in your data:

```scala
scala> val groupFreq = boot.getFrequenciesByGroupId(data, 0)
groupFreq: play.api.libs.json.JsValue = [{"count":1000,"d1d2":[0.038,0.038,0.03,0.041,0.023,0.035,0.026,0.025,0.023,0.017,0.025,0.021,0.02,0.018,0.014,0.024,0.022,0.017,0.015,0.01,0.014,0.015,0.017,0.006,0.013,0.011,0.013,0.016,0.011,0.016,0.008,0.006,0.007,0.009,0.012,0.006,0.01,0.006,0.012,0.006,0.012,0.014,0.009,0.01,0.007,0.009,0.003,0.011,0.006,0.011,0.005,0.009,0.003,0.007,0.004,0.006,0.006,0.01,0.004,0.007,0.003,0.002,0.006,0.005,0.004,0.008,0.006,0.003,0.005,0.004,0.005,0.008,0.003,0.006,0.006,0.003,0.004,0.002,0.011,0.006,0.004,0.005,0.003,0.004,0.004,0.006,0.008,0.007,0.008,0.002],"d1":[0.296,0.186,0.132,0.082,0.092,0.061,0.046,0.054,0.051],"d2":[0.114,0.118,0.098,0.106,0.087,0.108,0.098,0.097,0.095,0.079]}]
scala> val levelFreq = boot.getFrequenciesByLevel(data, 1)
levelFreq: play.api.libs.json.JsValue = [{"count":400,"d1d2":[0.04,0.055,0.03,0.0425,0.0275,0.03,0.03,0.035,0.01,0.015,0.03,0.01,0.0075,0.0125,0.0125,0.0225,0.02,0.0175,0.01,0.01,0.02,0.015,0.025,0.0075,0.01,0.005,0.025,0.015,0.01,0.0125,0.0075,0.0025,0.0025,0.01,0.0075,0.0075,0.0075,0.005,0.01,0.01,0.0175,0.02,0.0125,0.0075,0.0025,0.0125,0,0.01,0.0075,0.015,0.005,0.005,0,0.01,0.0075,0,0.0075,0.0225,0.0025,0,0.005,0.0025,0.0075,0.0025,0.005,0.0075,0.005,0,0.0075,0.0025,0,0.01,0,0.005,0.0075,0.0025,0.005,0,0.0125,0.0075,0.005,0.005,0,0.005,0.0075,0.0025,0.0125,0.0075,0.01,0.0025],"d1":[0.315,0.1525,0.145,0.07,0.105,0.06,0.045,0.05,0.0575],"d2":[0.13,0.125,0.085,0.1025,0.0875,0.09,0.1125,0.1125,0.08,0.075]},{"count":600,"d1d2":[0.0366666667,0.0266666667,0.03,0.04,0.02,0.0383333333,0.02333...
```

The next step is to set the necessary (and lazy) Spark operations to calculate bootstrap estimates based on both your data and the exact Benford distribition. These estimates will be used to calculate the results, that is, the acceptance or rejection of the null hypothesis that your data IS a sample drawn from an actual Benford distribution.

```scala
scala> val sampleRDD = boot.calcSampleCIs(sc, data, numSamples)
scala> val benfordRDD = benf.calcBenfordCIs(sc, data, numSamples)
scala> val resultsRDD = boot.calcResults(sampleRDD, benfordRDD)
```

Everything is set now! It is time to actually get the results!

1- Confidence intervals by group ID:

The structure of the JSON response is as follows:
```
{
"id": groupID,
"level": level depth,
// confidence intervals...
"CIs": {
 // ... for the first two significant digits
 "d1d2": {
  "n": number of valid lines/elements in the data,
  // estimated parameters (mean, variance, skewness, kurtosis)
  // with alpha values of 0.975 and 0.99
  // corresponding to significance levels of 2.5% and 1.0%
  "mean": [{
   "alpha": (1 - significance level / 2),
   "li": corresponding element index of lowerbound,
   "ui": corresponding element index of upperbound,
   "lower": CI lowerbound,
   "upper": CI upperbound,
   "t0": statistic computed on the original data
   },{
    ...
   }],
  "variance": [ ... ],
  "skewness": [ ... ],
  "kurtosis": [ ... ]
  },
 // ... for the first significant digit only
 "d1": ...,
 // ... for the second significant digit only
 "d2": ...,
 // ... for pearson and bootstrap regression (alpha0, alpha1, beta0 and beta1) coefficients
 "r": {
  "n": number of valid lines/elements in the data,
  "pearson": [{
   "alpha":
   ...
   "t0":
   },{
    ...
   }],
   "alpha0": [ ... ],
   "alpha1": [ ... ],
   "beta0": [ ... ],
   "beta1": [ ... ],
  }
 }
}
```

In the example, you can get CIs for group 0 as follows:
```scala
scala> val group = 0
group: Int = 0
scala> val groupCI = boot.getCIsByGroupId(sampleRDD, group)
groupCI: play.api.libs.json.JsValue = [{"id":0,"level":0,"CIs":{"d1d2":{"n":1000,"mean":[{"alpha":0.975,"li":14.4132480208,"ui":990.2867752674,"lower":36.969261423,"upper":40.4787839229,"t0":38.568},{"alpha":0.99,"li":6.2925373461,"ui":997.1244903632,"lower":36.6742176029,"upper":40.7362248519,"t0":38.568}],"variance":[{"alpha":0.975,"li":20.9411036941,"ui":994.5710088844,"lower":583.5460349472,"upper":708.7116935144,"t0":632.625376},{"alpha":0.99,"li":10.3454310009,"ui":999.1168446586,"lower":575.1564526836,"upper":717.8026922609,"t0":632.625376}],"skewness":[{"alpha":0.975,"li":19.6723637771,"ui":993.4595448131,"lower":0.7079945559,"upper":0.9463749095,"t0":0.8168660437},{"alpha":0.99,"li":8.6863874261,"ui":998.3113392204,"lower":0.6961341567,"upper":0.9615797289,"t0":0.8168660437}],"...
scala> Json.prettyPrint(groupCI)
res0: String =
[ {
  "id" : 0,
  "level" : 0,
  "CIs" : {
    "d1d2" : {
      "n" : 1000,
      "mean" : [ {
        "alpha" : 0.975,
        "li" : 14.4132480208,
        "ui" : 990.2867752674,
        "lower" : 36.969261423,
        "upper" : 40.4787839229,
        "t0" : 38.568
      }, {
        "alpha" : 0.99,
        "li" : 6.2925373461,
        "ui" : 997.1244903632,
        "lower" : 36.6742176029,
        "upper" : 40.7362248519,
        "t0" : 38.568
      } ],
      "variance" : [ {
        "alpha" : 0.975,
        "li" : 20.9411036941,
        "ui" : 994.5710088844,
        "lower" : 583.5460349472,
        "upper" : 708.7116935144,
        "t0" : 632.625376
      }, {
        "alpha" : 0.99,
        "li" : 10.3454310009,
        "ui" : 999.1168446586,
        "lower" : 57...
```


2- Confidence intervals by level (returns an array of CIs, one for each group in a given level)
```scala
scala> val level = 1
level: Int = 1
scala> val levelCIs = boot.getCIsByLevel(sampleRDD, 1)
levelCIs: play.api.libs.json.JsValue = [{"id":4,"level":1,"CIs":{"d1d2":{"n":400,"mean":[{"alpha":0.975,"li":15.6484978447,"ui":991.259801465,"lower":36.1330954577,"upper":41.8062554336,"t0":38.705},{"alpha":0.99,"li":6.9413770428,"ui":997.5537988299,"lower":35.9066747157,"upper":42.0099908581,"t0":38.705}],"variance":[{"alpha":0.975,"li":14.4202841466,"ui":990.1442415833,"lower":567.7315608856,"upper":752.8274641622,"t0":657.567975},{"alpha":0.99,"li":5.7577901905,"ui":996.6459593852,"lower":548.2048914919,"upper":763.1995041253,"t0":657.567975}],"skewness":[{"alpha":0.975,"li":16.5738822517,"ui":991.7914287015,"lower":0.613728507,"upper":1.0085163781,"t0":0.795359968},{"alpha":0.99,"li":7.171749893,"ui":997.6311316075,"lower":0.5913386751,"upper":1.0401580743,"t0":0.795359968}],"kurto...
```

3- Results by group Id:

The structure of the JSON response is as follows:
```
{
"id": groupID,
"level": level depth,
"results": {
  "n": number of valid lines/elements in the data,
  // for a diagnosis based on the estimated parameters (mean, variance...)...
  "statsDiag": FALSE if you CANNOT infer that your data is a sample drawn from a sample distribution => (possible fraud), TRUE otherwise,
  // for a diagnosis based on the estimated coefficientes (alpha0, alpha1...)...
  "regsDiag": FALSE if you CANNOT infer that your data is a sample drawn from a sample distribution => (possible fraud), TRUE otherwise,
  // Overlapping /Containing results...
  // ... for the first two significant digits
  "d1d2": {
    "mean": {
      "overlaps": TRUE if CIs estimated based on your data's and Benford's distributions overlap,
      "contains": TRUE if CI estimated based on your data's distribution contains actual Benford parameter,
     },
    "variance": { ... },
    "skewness": { ... },
    "kurtosis": { ... }
   },{
  // ... for the first significant digit only
  "d1": { ... },
  // ... for the second significant digit only
  "d2": { ... },
  // ... for pearson and bootstrap regression (alpha0, alpha1, beta0 and beta1) coefficients
  "reg": {
    "pearson": {
      "overlaps":
      "contains":
     },
    "alpha0": { ... },
    "alpha1": { ... },
    "beta0": { ... },
    "beta1": { ... }
   }
 }
}
```

In the example, you can get results for group 0 as follows:

```scala
scala> val groupResults = boot.getResultsByGroupId(resultsRDD, group)
groupResults: play.api.libs.json.JsValue = [{"id":0,"level":0,"results":{"n":1000,"statsDiag":true,"regsDiag":true,"d1d2":{"mean":{"overlaps":true,"contains":true},"variance":{"overlaps":true,"contains":true},"skewness":{"overlaps":true,"contains":true},"kurtosis":{"overlaps":true,"contains":true}},"d1":{"mean":{"overlaps":true,"contains":true},"variance":{"overlaps":true,"contains":true},"skewness":{"overlaps":true,"contains":true},"kurtosis":{"overlaps":true,"contains":true}},"d2":{"mean":{"overlaps":true,"contains":true},"variance":{"overlaps":true,"contains":true},"skewness":{"overlaps":true,"contains":true},"kurtosis":{"overlaps":true,"contains":true}},"reg":{"pearson":{"overlaps":true,"contains":true},"alpha0":{"overlaps":true,"contains":true},"alpha1":{"overlaps":true,"contains":...
scala> Json.prettyPrint(groupResults)
res0: String =
[ {
  "id" : 0,
  "level" : 0,
  "results" : {
    "n" : 1000,
    "statsDiag" : true,
    "regsDiag" : true,
    "d1d2" : {
      "mean" : {
        "overlaps" : true,
        "contains" : true
      },
      "variance" : {
        "overlaps" : true,
        "contains" : true
      },
      "skewness" : {
        "overlaps" : true,
        "contains" : true
      },
      "kurtosis" : {
        "overlaps" : true,
        "contains" : true
      }
    },
    "d1" : {
      "mean" : {
        "overlaps" : true,
        "contains" : true
      },
      "variance" : {
        "overlaps" : true,
        "contains" : true
      },
      "skewness" : {
        "overlaps" : true,
        "contains" : true
      },
      "kurtosis" : {
        "overlaps" : true,
        "contain...
```

Results by level:
```scala
scala> val levelResults = boot.getResultsByLevel(resultsRDD, level)
levelResults: play.api.libs.json.JsValue = [{"id":4,"level":1,"results":{"n":400,"statsDiag":true,"regsDiag":false,"d1d2":{"mean":{"overlaps":true,"contains":true},"variance":{"overlaps":true,"contains":true},"skewness":{"overlaps":true,"contains":true},"kurtosis":{"overlaps":true,"contains":true}},"d1":{"mean":{"overlaps":true,"contains":true},"variance":{"overlaps":true,"contains":true},"skewness":{"overlaps":true,"contains":true},"kurtosis":{"overlaps":true,"contains":true}},"d2":{"mean":{"overlaps":true,"contains":true},"variance":{"overlaps":true,"contains":true},"skewness":{"overlaps":true,"contains":true},"kurtosis":{"overlaps":true,"contains":true}},"reg":{"pearson":{"overlaps":true,"contains":true},"alpha0":{"overlaps":false,"contains":false},"alpha1":{"overlaps":false,"contain...
```

What's next
==============

1- Detection of weak peaks in signal processing.

2- Edge detection in image processing.

3- Detection of suspiciously behaving nodes in social networks.


Contributing
============

If you run across any bugs, please file issues!