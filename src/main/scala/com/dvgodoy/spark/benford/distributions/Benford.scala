package com.dvgodoy.spark.benford.distributions

import breeze.stats.distributions._
import breeze.linalg.{sum, accumulate, DenseVector}
import org.apache.commons.math3.random.MersenneTwister
import scala.collection.mutable
import scala.collection.immutable.HashMap
import BigInt._

/**
 * Created by dvgodoy on 17/10/15.
 */

abstract class distBenford {
  val rand: RandBasis = new RandBasis(new MersenneTwister())
  val prob: DenseVector[Double] = DenseVector[Double](0)
  lazy val aliasTable: BenfordAliasTable = buildBenfordAliasTable()

  val draw: IndexedSeq[(Int,Double)] = null

  //def mult = new Multinomial(prob)
  //def sample1(n: Int): IndexedSeq[BigInt] = mult.sample(n).map(BigInt(_))

  def buildBenfordAliasTable(): BenfordAliasTable = {
    val nOutcomes = prob.length
    val aliases = DenseVector.zeros[Int](nOutcomes)
    val sum = breeze.linalg.sum(prob)

    val probs = DenseVector(prob.map { param => param / sum * nOutcomes }.toArray)
    val (iSmaller, iLarger) = (0 until nOutcomes).partition(probs(_) < 1d)
    val smaller = mutable.Stack(iSmaller:_*)
    val larger = mutable.Stack(iLarger:_*)

    while (smaller.nonEmpty && larger.nonEmpty) {
      val small = smaller.pop()
      val large = larger.pop()
      aliases(small) = large
      probs(large) -= (1d - probs(small))
      if (probs(large) < 1)
        smaller.push(large)
      else
        larger.push(large)
    }

    new BenfordAliasTable(probs, aliases, nOutcomes, rand)
  }

  //def findOutcome(roll: Int, toss: Double): Int = if (toss < aliasTable.probs(roll)) roll + 1 else aliasTable.aliases(roll) + 1
  def findOutcome(roll: Int, toss: Double): Int = if (toss < aliasTable.probsMap(roll)) roll + 1 else aliasTable.aliasesMap(roll) + 1

  def rollToss(n: Int): IndexedSeq[(Int,Double)] = {
    (1 to n).map(x => (rand.randInt(aliasTable.nOutcomes).get(),rand.uniform.get()))
  }

  def sample: IndexedSeq[BigInt] = {
    draw.map(x => BigInt(findOutcome(x._1, x._2)))
    //draw.map(x => BigInt(if (x._2 < aliasTable.probsMap(x._1)) x._1 + 1 else aliasTable.aliasesMap(x._1) + 1))
  }
}

class BenfordAliasTable(val probs: DenseVector[Double],
                            val aliases: DenseVector[Int],
                            val nOutcomes: Int,
                            val rand: RandBasis) {
  val probsMap: HashMap[Int,Double] = HashMap(probs.iterator.toArray: _*)
  val aliasesMap: HashMap[Int,Int] = HashMap(aliases.iterator.toArray: _*)
}

private object distBenfordD1 extends distBenford {
  override val prob = DenseVector.range(1, 10).map(x => math.log10(1.0 + 1.0 / x))
}

private object distBenfordD2 extends distBenford {
  override val prob = DenseVector((10 to 19).map(x => (List.range(x, 100, 10).map(x => math.log10(1.0 + 1.0 / x))).sum).toArray)
}

private object distBenfordD1D2 extends distBenford {
  override val prob = DenseVector.vertcat(DenseVector.zeros[Double](10), DenseVector.range(10, 100).map(x => math.log10(1.0 + 1.0 / x)))
}

class Benford(FSD: Boolean, SSD: Boolean, n: Int) extends distBenford {
  override val prob = (if (FSD && SSD) distBenfordD1D2 else if (FSD && !SSD) distBenfordD1 else distBenfordD2).prob
  override val draw = rollToss(n)
}

object Benford {
  def apply(n: Int): Benford = new Benford(true, false, n)
  def apply(FSD: Boolean, n: Int) = new Benford(FSD, true, n)
}