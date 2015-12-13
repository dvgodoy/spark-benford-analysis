Methods and Responses
=====================

### 1- Confidence intervals by group ID:

The structure of the JSON response is as follows:
```scala
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
scala> val groupCI = boot.getCIs(sampleRDD)
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

### 2- Results by group ID:

The structure of the JSON response is as follows:
```scala
{
"id": groupID,
"level": level depth,
"results": {
  "n": number of valid lines/elements in the data,
  // for a diagnosis based on the estimated parameters (mean, variance...)...
  "statsDiag": -1 if you CANNOT infer that your data is a sample drawn from a sample distribution => (possible fraud), 1 otherwise,
  // for a diagnosis based on the estimated coefficientes (alpha0, alpha1...)...
  "regsDiag": -1 if you CANNOT infer that your data is a sample drawn from a sample distribution => (possible fraud), 0 if it is undefined and 1 otherwise,
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
scala> val groupResults = boot.getResults(resultsRDD)
groupResults: play.api.libs.json.JsValue = [{"id":0,"level":0,"results":{"n":1000,"statsDiag":true,"regsDiag":true,"d1d2":{"mean":{"overlaps":true,"contains":true},"variance":{"overlaps":true,"contains":true},"skewness":{"overlaps":true,"contains":true},"kurtosis":{"overlaps":true,"contains":true}},"d1":{"mean":{"overlaps":true,"contains":true},"variance":{"overlaps":true,"contains":true},"skewness":{"overlaps":true,"contains":true},"kurtosis":{"overlaps":true,"contains":true}},"d2":{"mean":{"overlaps":true,"contains":true},"variance":{"overlaps":true,"contains":true},"skewness":{"overlaps":true,"contains":true},"kurtosis":{"overlaps":true,"contains":true}},"reg":{"pearson":{"overlaps":true,"contains":true},"alpha0":{"overlaps":true,"contains":true},"alpha1":{"overlaps":true,"contains":...
scala> Json.prettyPrint(groupResults)
res0: String =
[ {
  "id" : 0,
  "level" : 0,
  "results" : {
    "n" : 1000,
    "statsDiag" : 1,
    "regsDiag" : 1,
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
