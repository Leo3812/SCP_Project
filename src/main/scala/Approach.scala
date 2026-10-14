import Types._
import org.apache.spark.rdd.RDD
import scala.math.Ordering.Implicits._
import org.apache.spark.HashPartitioner

trait Approach {
  def Name: String
  def run(dateCoord: DateCoordRDD, numPartitions : Int): ((Coord, Coord), Seq[Date])

  protected def findBestPair(dateCoord: DateCoordRDD, numPartitions : Int): ((Coord, Coord), Seq[Date]) = {

    val partitioner = new HashPartitioner(numPartitions)

    // Remove duplicate records
    val deduped: DateCoordRDD =
      dateCoord.distinct(numPartitions)

    // Group coordinates by date using hash partitioning
    val coordsByDate: RDD[(Date, Iterable[Coord])] =
      deduped
        .groupByKey(partitioner)
        .filter { case (_, coords) => coords.size > 1 }

    // Generate all unordered coordinate pairs for each date
    val pairDate: RDD[((Coord, Coord), Date)] = coordsByDate.flatMap { case (date, coords) =>
      val coordList = coords.toSeq
      for {
        i <- coordList.indices
        j <- (i + 1) until coordList.size
      } yield {
        val a = coordList(i)
        val b = coordList(j)
        val orderedPair = if (a < b) (a, b) else (b, a)
        (orderedPair, date)
      }
    }

    // Group dates by coordinate pair using hash partitioning
    val pairToDates: RDD[((Coord, Coord), Seq[Date])] =
      pairDate
        .groupByKey(partitioner)
        .mapValues(_.toSeq.sorted)

    // Find the pair with the most dates
    pairToDates.reduce { (e1, e2) =>
      if (e1._2.size > e2._2.size) e1 else e2
    }
  }
}