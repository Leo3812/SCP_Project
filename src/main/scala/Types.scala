import org.apache.spark.rdd.RDD

object Types {
  type Date = String
  case class Coord(lon: Double, lat: Double)

  object Coord {
    implicit val ord: Ordering[Coord] = Ordering.by(c => (c.lon, c.lat))
  }
  type DateCoordRDD = RDD[(Date, Coord)]
}
