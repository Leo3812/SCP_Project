import Types._
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.math.Ordering.Implicits._

class Approach2 extends Approach {
  override def Name: String = "TwoPass"

  override def run(dateCoord: DateCoordRDD, numPartitions: Int): ((Coord, Coord), Seq[Date]) = {

    // Aggregate coordinates into a set per date
    val coordsByDate: RDD[(Date, mutable.HashSet[Coord])] = dateCoord
      .aggregateByKey(mutable.HashSet.empty[Coord], numPartitions)(
        (s, c) => s += c,
        (s1, s2) => s1 ++= s2
      )
      .filter { case (_, coords) => coords.size > 1 }
      .persist(StorageLevel.MEMORY_AND_DISK)

    // Count how many distinct dates contain each unordered coordinate pair
    val pairCounts: RDD[((Coord, Coord), Int)] = coordsByDate
      .flatMap { case (_, coords) =>
        val list = coords.toIndexedSeq
        for {
          i <- list.indices.iterator
          j <- ((i + 1) until list.size).iterator
        } yield {
          val a = list(i); val b = list(j)
          ((if (a < b) a else b, if (a < b) b else a), 1)
        }
      }
      .combineByKey(
        (v: Int) => v,
        (c: Int, v: Int) => c + v,
        (c1: Int, c2: Int) => c1 + c2,
        new HashPartitioner(numPartitions),
        mapSideCombine = false
      )

    // Find a pair with the maximum number of shared dates
    val (bestPair, _) = pairCounts.reduce { (e1, e2) =>
      if (e1._2 > e2._2) e1 else e2
    }

    val (bestA, bestB) = bestPair

      // Scan the cached dates  to retrieve the ones containing both coordinates of the winning pair
      val sharedDates = coordsByDate
      .filter { case (_, c) => c.contains(bestA) && c.contains(bestB)  }
      .keys
      .collect()
      .sorted

    coordsByDate.unpersist()
    (bestPair, sharedDates)
  }
}