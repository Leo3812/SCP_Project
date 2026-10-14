
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types._

import org.apache.hadoop.fs.Path

import Types._

object Main {

  def main(args: Array[String]): Unit = {

    //Spark Configuration
    val sparkConfig = new SparkConf()

    sparkConfig.setAppName(s"Earthquake Application")
    if (!sparkConfig.contains(s"spark.master"))
      sparkConfig.setMaster(s"local[*]")

    val spark = SparkSession.builder()
      .config(sparkConfig)
      .getOrCreate()

    //Remove unnecessary warnings
    spark.sparkContext.setLogLevel("WARN")


    //CLI Arguments (InputFile, OutputFile, [Partitions], [ApproachNum])
    val filename: String =
      if (args.length > 0) args(0)
      else "Datasets/dataset-earthquakes-full.csv"

    val outputPath: String =
      if (args.length > 1) args(1)
      else "Results/output.json"

    val numPartitions: Int =
      if (args.length > 2) args(2).toInt
      else spark.sparkContext.defaultParallelism * 2

    val approachNum: Int =
      if (args.length > 3) args(3).toInt
      else 1

    //Schema for reading the data
    val schema = StructType(Array(
      StructField("longitude", DoubleType),
      StructField("latitude", DoubleType),
      StructField("date", StringType)
    ))

    //Read the data
    val raw = spark.read
      .option("header", value = true)
      .schema(schema)
      .csv(filename)
      .rdd

    // Round coordinates to 1 decimal place and truncate timestamp to day
    val dateCoord: DateCoordRDD = raw.map { row =>
      val lon = math.round(row.getAs[Double]("longitude") * 10.0) / 10.0
      val lat = math.round(row.getAs[Double]("latitude") * 10.0) / 10.0
      val date = row.getAs[String]("date").take(10)
      (date, Coord(lon, lat))
    }

    val approach: Approach = approachNum match {
      case 1 => new Approach1()
      case 2 => new Approach2()
      case 3 => new Approach3()
      case _ => sys.error(s"Invalid approach: $approachNum")
    }

    //In order to not count input/output
    dateCoord.persist(StorageLevel.MEMORY_AND_DISK).count()

    val startTime = System.nanoTime()
    val ((coord1, coord2), dates) = approach.run(dateCoord, numPartitions)
    val elapsedTime = (System.nanoTime() - startTime) / 1e9

    //Save to a json file
    def coordJson(c: Coord): String = c match {
      case Coord(lon, lat) => s"""{"longitude": $lon, "latitude": $lat}"""
    }

    val json =
      s"""{"coord1": ${coordJson(coord1)}, "coord2": ${coordJson(coord2)}, "elapsedTime": $elapsedTime, "dates": [${dates.map(d => s""""$d"""").mkString(", ")}]}"""

    val path = new Path(outputPath)
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)

    val out = fs.create(path, true)
    out.write(json.getBytes("UTF-8"))
    out.close()

    // Spec-format output
    try {
      def coordTxt(c: Coord): String = c match {
        case Coord(lon, lat) => s"($lat, $lon)"   // spec prints (lat, lon)
      }
      val txt = (s"(${coordTxt(coord1)}, ${coordTxt(coord2)})" +: dates).mkString("\n") + "\n"

      val txtPath = new Path(outputPath.stripSuffix(".json") + ".txt")
      val txtOut = fs.create(txtPath, true)
      txtOut.write(txt.getBytes("UTF-8"))
      txtOut.close()
    } catch {
      case e: Exception => println(s"WARNING: could not write spec-format output: ${e.getMessage}")
    }

    dateCoord.unpersist()
    spark.stop()
  }

}
