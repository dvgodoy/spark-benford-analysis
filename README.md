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

### Image Processing

A novel use of Benford's Law is edge detection. Sets of pixels depicting edges show higher adherence to the expected distribution of the leading digit.

So, researchers have developed a metric called Benford Goodness of Parameter (BGP) which is equivalent to an RMSE of the observed distribution that can be applied to subsets of pixels as defined by a sliding window over the original image. This procedure was named 2D-Scanning Benford Analysis (2D-SBA).

Similarly to a convolutional kernel, the use of a transformation based on the BGP of a given set of pixels allows the detection of faint features in images.

It is easy to see that even moderate-size images will span thousands of windows to be transformed. The goal of the ***Benford Analysis for Spark*** package is to quickly deliver results when applying the SBA procedure to images.

For further details on edge detection using Benford's Law, please refer to:

[BHOLE, G.; SHUKLA, A.; MAHESH, T.S. Benford Analysis: A useful paradigm for spectroscopic analysis](http://arxiv.org/abs/1408.5735)

Installation
============

To include this package in your Spark Application:

### spark-shell, pyspark, or spark-submit

If you want to build it yourself and include the uberjar in your application:

1- Clone this repository with `git clone https://github.com/dvgodoy/spark-benford-analysis.git`.

2- Build an uberjar containing all dependencies with `sbt assembly`.

3- Include the uberjar `spark-benford-analysis-assembly-0.0.1-SNAPSHOT.jar` both in the `--jars` and `--driver-class-path` parameters of `spark-shell`.

```
> $SPARK_HOME/bin/spark-shell --jars spark-benford-analysis-assembly-0.0.1-SNAPSHOT.jar --driver-class-path spark-benford-analysis-assembly-0.0.1-SNAPSHOT.jar
```

### sbt

In your sbt build file, add:

```scala
resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies += "com.github.dvgodoy" % "spark-benford-analysis" % "v0.0.1"
```

Examples
========

### Image Processing

In order to perform edge detection on images, you need to import the following:

```scala
scala> import com.dvgodoy.spark.benford.image.SBA
scala> import com.dvgodoy.spark.benford.util.JobId
```

You should start by importing the image you want to process:

```scala
scala> val image = loadImage("./src/test/resources/chess.png")
```

Next you should name your Spark Jobs by creating an implicit parameter of the JobId class. In this example we named our job "test".

```scala
scala> implicit val jobid = JobId("test")
```
The next step is to perform the 2D-Scanning Benford Analysis (SBA) on the loaded image. You also should provide the window size, that is, the size of the sliding window from where pixels will be analyzed. Size 15 is the default, but the results may vary depending on the features of your image. For images with sharp and well-defined edges, lower values (as low as 3) will yield better results.

```scala
scala> val sba = performSBA(sc, image, 15)
```

Then you can get the resulting image using ```getSBAImage```. You should also choose the percentage of pixels you want to discard: 80% (0.8) is the default value. Once again, results may vary depending on the features of the image: some images keep their features even when 95% of the pixels are discarded. The last parameter refers to background color: white (true) or black (false).

```scala
scala> val newImage = getSBAImage(sba, 0.8, true).get
newImage: String = iVBORw0KGgoAAAANSUhEUgAAAkoAAAJKCAAAAAD2SDVFAACAAElEQVR4
```

The result is returned in a Base 64 encoded string.

### Accounting

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

Then you should point to the data you want to analyze. It must be a CSV file containing the values in the first column.

If your values have an associated hierarchical structure (for instance, departments and sub-departments in a company), you can take advantage of the ***Benford Analysis for Spark drill-down capability***, that is, you can test your accounting data for different levels of granularity.

To include the structure of your data, insert it in the columns that follow the values, from top-level in the left-most column to bottom-level in the right-most column. Please note that the hierarchical structure is NOT expected to be numeric.

Then your file should look like this:

```
10.35,COMPANY NAME,DEPARTMENT A,SUB-DEPARTMENT A.1,...
128.17,COMPANY NAME,DEPARTMENT A,SUB-DEPARTMENT A.2,...
8712.33,COMPANY NAME,DEPARTMENT B,SUB-DEPARTMENT B.1,...
...
```

A test file is provided with the package and we point to it in this example:
```scala
scala> val filePath = "./src/test/resources/datalevels.csv"
```

Now you can load your file using:

```scala
scala> val data =  boot.loadData(sc, filePath)
data: com.dvgodoy.spark.benford.util.DataByLevelMsg = Good(DataByLevel(Map(0 -> (L.1,0), 5 -> (L.1.B.3,2), 1 -> (L.1.A,1), 6 -> (L.1.B.4,2), 2 -> (L.1.A.1,2), 3 -> (L.1.A.2,2), 4 -> (L.1.B,1)),Map(0 -> [J@eee26bd, 5 -> [J@3d1c8f35, 1 -> [J@498f5728, 6 -> [J@39451d92, 2 -> [J@46610fc9, 3 -> [J@1fdd5517, 4 -> [J@2cd877d3),[Lcom.dvgodoy.spark.benford.util.package$FreqByLevel;@3d605657,MapPartitionsRDD[44] at map at Bootstrap.scala:165))
```

The unique group IDs generated for each different combination of levels and its associated names and children can be found with:

```scala
scala> Json.prettyPrint(boot.getGroups(data))
res0: String =
[ {
  "id" : 0,
  "level" : 0,
  "name" : "1",
  "children" : [ 1, 4 ]
}, {
  "id" : 1,
  "level" : 1,
  "name" : "1.A",
  "children" : [ 2, 3 ]
}, {
  "id" : 2,
  "level" : 2,
  "name" : "1.A.1",
  "children" : [ -1 ]
}, {
  "id" : 3,
  "level" : 2,
  "name" : "1.A.2",
  "children" : [ -1 ]
}, {
  "id" : 4,
  "level" : 1,
  "name" : "1.B",
  "children" : [ 5, 6 ]
}, {
  "id" : 5,
  "level" : 2,
  "name" : "1.B.3",
  "children" : [ -1 ]
}, {
  "id" : 6,
  "level" : 2,
  "name" : "1.B.4",
  "children" : [ -1 ]
} ]
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
scala> val basicBoot = boot.calcBasicBoot(sc, data, 1000)
scala> val sampleRDD = boot.calcSampleCIs(sc, data, numSamples)
scala> val benfordRDD = benf.calcBenfordCIs(sc, data, numSamples)
scala> val resultsRDD = boot.calcResults(sampleRDD, benfordRDD)
```

Everything is set now! It is time to actually get the results!

You can either get results for each individual group or for all groups in a given level.

For an individual group, use:

```scala
scala> val group = 0
group: Int = 0
scala> val dataStatsRDD = boot.calcDataStats(data, group)
scala> val sampleRDD =  boot.calcSampleCIs(basicBoot, dataStatsRDD, data, group)
scala> val benfordRDD = benf.calcBenfordCIs(basicBoot, dataStatsRDD, data, group)
scala> val resultsRDD = boot.calcResults(sampleRDD, benfordRDD)

scala> val groupResults = boot.getResults(resultsRDD)
groupResults: play.api.libs.json.JsValue = [{"id":0,"level":0,"results":{"n":1000,"statsDiag":true,"regsDiag":true,"d1d2":{"mean":{"overlaps":true,"contains":true},"variance":{"overlaps":true,"contains":true},"skewness":{"overlaps":true,"contains":true},"kurtosis":{"overlaps":true,"contains":true}},"d1":{"mean":{"overlaps":true,"contains":true},"variance":{"overlaps":true,"contains":true},"skewness":{"overlaps":true,"contains":true},"kurtosis":{"overlaps":true,"contains":true}},"d2":{"mean":{"overlaps":true,"contains":true},"variance":{"overlaps":true,"contains":true},"skewness":{"overlaps":true,"contains":true},"kurtosis":{"overlaps":true,"contains":true}},"reg":{"pearson":{"overlaps":true,"contains":true},"alpha0":{"overlaps":true,"contains":true},"alpha1":{"overlaps":true,"contains":...
```

A very detailed explanation of all JSON responses can be found [here](https://github.com/dvgodoy/spark-benford-analysis/blob/master/Responses.md).

For a straightforward answer regarding the data being suspicious or not, you can call `getSuspiciousGroups` with any of the previous results:

```scala
scala> boot.getSuspiciousGroups(groupResults)
res0: play.api.libs.json.JsValue = {"stats":[],"regs":[]}
```

All suspicious groups are listed under the corresponding criteria under which they were classified as suspicious. In the example, our data complies with Benford's Law and therefore is not classified as suspicious.

The "stats" criteria follows Suh and Headrick (2010) and is based on the overlapping of confidence intervals for mean, variance, skewness, kurtosis and Pearson correlation.

The "regs" criteria follows Suh, Headrick and Minaburo (2011) and is based on the overlapping of confidence intervals for coefficients of regressions over first and second digits.

What's next
==============

### Signal Processing

Benford's Law can also be used to detect weak peaks in signals.

For further details, please refer to:

[BHOLE, G.; SHUKLA, A.; MAHESH, T.S. Benford Analysis: A useful paradigm for spectroscopic analysis](http://arxiv.org/abs/1408.5735)

### Suspicious Behavior in Social Networks

Another use of Benford's Law is the detection of suspiciously behaving nodes in social networks.

For further details, please refer to:

[GOLDBECK, J. Benford's Law Applies to Online Social Networks](http://journals.plos.org/plosone/article?id=10.1371/journal.pone.0135169)

Contributing
============

If you run across any bugs, please file issues!
