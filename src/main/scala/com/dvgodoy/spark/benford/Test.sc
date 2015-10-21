import breeze.linalg.{sum, accumulate, DenseVector}
import breeze.stats.distributions.RandBasis
import org.apache.commons.math3.random.MersenneTwister
import scala.collection.mutable

val dist = DenseVector.range(1, 10).map(x => math.log10(1.0 + 1.0 / x)).toArray
//dist.map(x => l.filter(y => y > x).length)
val params = dist
val nOutcomes = params.iterator.length
val aliases = DenseVector.zeros[Int](nOutcomes)
val sum = breeze.linalg.sum(params)
val probs = params.iterator.map { param => param / sum * nOutcomes }.toArray
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
val outcomes = params.toIndexedSeq
val rand = new RandBasis(new MersenneTwister())
def draw():Double = {
    val roll = rand.randInt(outcomes.length).get()
    val toss = rand.uniform.get()
    if (toss < probs(roll))
        roll + 1
    else
        aliases(roll) +1
}

val rollToss = (1 to 10).map(x => (rand.randInt(nOutcomes).get(),rand.uniform.get()))
def findOutcome(roll: Int, toss: Double): Int = if (toss < probs(roll)) roll + 1 else aliases(roll) + 1
val result = rollToss.map(x => BigInt(findOutcome(x._1, x._2)))

val roll = rand.randInt(nOutcomes).get()
val toss = rand.uniform.get()
if (toss < probs(roll))
    roll + 1
else
    aliases(roll) + 1


rollToss.getClass

scala.collection.immutable.HashMap(DenseVector(dist).iterator.map(x => (x._1+1,x._2)).toArray: _*)