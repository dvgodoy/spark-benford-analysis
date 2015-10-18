import breeze.linalg._
import breeze.stats.distributions._
import com.dvgodoy.spark.benford.distributions._
import BigInt._

object SimpleApp {

  class MomentsSums (val pSum: BigInt = 0, val pSumP2: BigInt = 0, val pSumP3: BigInt = 0, val pSumP4: BigInt = 0, val n: BigInt = 0) {
    def + (that: MomentsSums): MomentsSums = new MomentsSums(pSum + that.pSum, pSumP2 + that.pSumP2, pSumP3 + that.pSumP3, pSumP4 + that.pSumP4, n + that.n)
    def apply() = List(pSum, pSumP2, pSumP3, pSumP4, n)
  }

  object MomentsSums {
    def apply(sample: DenseVector[BigInt]): MomentsSums = {
      val sampleP2 = sample :* sample
      new MomentsSums(sum(sample), sum(sampleP2), sum(sampleP2 :* sample), sum(sampleP2 :* sampleP2), sample.length)
    }
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
        d1d2Sums + MomentsSums(d1d2),
        d1Sums + MomentsSums(d1),
        d2Sums + MomentsSums(d2),
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
    println(d1.pSum.toDouble/d1.n.toDouble)
    //println("Lines with the: %s".format(numTHEs))
  }
}
