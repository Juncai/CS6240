
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object HallOfFame {
    def main(args: Array[String]) {
       val conf = new SparkConf().
            setAppName("Hall of Fame").
            setMaster("local")
        val sc = new SparkContext(conf)
       
        // Input
        val players = sc.textFile("baseball/Master.csv").
            map { _.split(",") }.
            filter { _(0) != "playerID" }.
            keyBy { _(0) }

        val hall = sc.textFile("baseball/HallOfFame.csv").
            map { _.split(",") }.
            filter { _(0) != "playerID" }.
            keyBy { _(0) }

        // Join
        val data = hall.join(players)

        // Output
        val text = data.map(x => { 
            val (k, v) = x;
            val (p, h) = v;
            p.mkString(",") + " :: " + h.mkString(",")
        })
        text.saveAsTextFile("out")

        // Shut down Spark, avoid errors.
        sc.stop()
    }
}

// vim: set ts=4 sw=4 et:
