import Types._

class Approach1 extends Approach {
  val Name = "Baseline"
  def run(dateCoord: DateCoordRDD, numPartitions : Int): ((Coord, Coord), Seq[Date]) = findBestPair(dateCoord, numPartitions)
}