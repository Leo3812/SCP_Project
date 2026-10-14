import Types._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

class Approach3 extends Approach {
  override def Name: String = "LowerBoundPruning"

  // Prune coordinates that cannot belong to an optimal pair, then
  // run the first approach algorithm on the remaining records.
  override def run(dateCoord: DateCoordRDD, numPartitions : Int): ((Coord, Coord), Seq[Date]) = {
    val pruned = filter(dateCoord).persist(StorageLevel.MEMORY_AND_DISK)
    val result = findBestPair(pruned, numPartitions)
    pruned.unpersist()
    result
  }
  // Count the number of input records associated with each coordinate
  private def computeCoordDateCount(dateCoord: DateCoordRDD): RDD[(Coord, Int)] = {
    dateCoord
      .map { case (date, coord) => (coord, 1) }
      .reduceByKey(_ + _)
  }

  // Return the two coordinates with the largest record counts
  private def topKCoords(coordDateCount: RDD[(Coord, Int)], k: Int): Array[Coord] = {
    coordDateCount
      .takeOrdered(k)(Ordering[Int].reverse.on({case (_, count) => count}))
      .map({case (coord, _) => coord})
  }


  private def computeSharedDays(dateCoord: DateCoordRDD, coords: Set[Coord]): Int = {
    dateCoord
      .filter { case (_, coord) => coords.contains(coord) }
      .groupByKey()
      .filter {
        case (_, presentCoords) =>
          val s = presentCoords.toSet
          coords.forall(s.contains)
      }
      .count()
      .toInt
  }

  def filter(dateCoord: DateCoordRDD): DateCoordRDD = {
    val coordDateCount = computeCoordDateCount(dateCoord).persist(StorageLevel.MEMORY_AND_DISK)
    val top2 = topKCoords(coordDateCount, 2)
    val lowerBound = computeSharedDays(dateCoord, top2.toSet)
    val frequentCoords : Set[Coord] =
      coordDateCount
        .filter {case (_, count) => count >= lowerBound }
        .keys
        .collect()
        .toSet

    val result = dateCoord.filter { case (_, coord) => frequentCoords.contains(coord) }

    coordDateCount.unpersist()
    result
  }
}