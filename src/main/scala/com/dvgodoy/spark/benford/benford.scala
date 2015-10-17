import breeze.linalg._
import breeze.stats.distributions._
import com.dvgodoy.spark.benford.distributions._
import BigInt._

object SimpleApp {

  class MomentsSums(sample: DenseVector[BigInt],
                     pSum: BigInt = 0,
                     pSumP2: BigInt = 0,
                     pSumP3: BigInt = 0,
                     pSumP4: BigInt = 0,
                     n: BigInt = 0) {
    private val sampleP2 = sample :* sample
    val dSum: BigInt = pSum + sum(sample)
    val dSumP2: BigInt = pSumP2 + sum(sampleP2)
    val dSumP3: BigInt = pSumP3 + sum(sample :* sampleP2)
    val dSumP4: BigInt = pSumP4 + sum(sampleP2 :* sampleP2)
    val nSum = n + sample.length

    def +(sample: DenseVector[BigInt]): MomentsSums = {
      new MomentsSums(sample, dSum, dSumP2, dSumP3, dSumP4, nSum)
    }

    def apply() = List(dSum, dSumP2, dSumP3, dSumP4, nSum)
  }

  object MomentsSums {
    def apply(sample: DenseVector[BigInt]): MomentsSums = new MomentsSums(sample)
  }

  def boot(//mult: Multinomial[DenseVector[Double],Int],
          mult: Benford,
          size:Int,
          chunk: Int,
          d1d2Sums: MomentsSums = MomentsSums(DenseVector()),
          d1Sums: MomentsSums = MomentsSums(DenseVector()),
          d2Sums: MomentsSums = MomentsSums(DenseVector()),
          d1d2ProdSum: BigInt = 0):
  (MomentsSums, MomentsSums, MomentsSums, BigInt) = {
    if (size <= 0) {
      (d1d2Sums, d1Sums, d2Sums, d1d2ProdSum)
    } else {
      val d1d2 = DenseVector(mult.sample(if (size <= chunk) size else chunk).toArray)
      val d1 = d1d2 / BigInt(10)
      val d2 = d1d2 - (d1 :*= BigInt(10))
      boot(mult,
        size - chunk,
        chunk,
        d1d2Sums + d1d2,
        d1Sums + d1,
        d2Sums + d2,
        d1d2ProdSum + sum(d1 :* d2)
      )
    }
  }

  def time[A](a: => A, n:Int) = {
    var times = List[Long]()
    for (_ <- 1 to n) {
      val now = System.nanoTime
      val res = a
      times :::= List(System.nanoTime - now)
    }
    val result = times.sum / n
    println("%d microseconds".format(result / 1000))
    result
  }

  def main(args: Array[String]) {
    //val benf = DenseVector.range(10, 100).map(x => math.log10(1.0 + 1.0/x))
    //val mult = new Multinomial(DenseVector.vertcat(DenseVector.zeros[Double](10),benf))
    val mult = Benford(true)

    val logFile = "src/data/sample.txt"
    //val sc = new SparkContext("local", "Simple App", "/usr/local/share/spark", List("target/scala-2.11/simple-project_2.11-1.0.jar"))
    //val logData = sc.textFile(logFile, 2).cache()
    //val numTHEs = logData.filter(line => line.contains("the")).count()
    //val benf = ((1 to 10).map(x => math.log10(1.0 + 1.0/x)))
    //val numTHEs = mult.sample(1000).sum
    //time(mult.sample(10000).sum, 100)
    //time(boot(10000,sumTotal,sumTotalSquare), 100)
    //time(boot(mult,1000000,1000), 100)

    val (d1d2, d1, d2, d1d2prod) = boot(mult,1000,1000)
    println(d1d2())
    println(d1())
    println(d2())
    println(d1d2prod)
    println(d1.dSum.toDouble/d1.nSum.toDouble)
    //println("Lines with the: %s".format(numTHEs))
  }
}
