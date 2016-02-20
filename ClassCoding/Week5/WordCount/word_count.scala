
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object WordCount {
    def main(args: Array[String]) {
       val conf = new SparkConf().
            setAppName("Word Count").
            setMaster("local")
        val sc = new SparkContext(conf)
       
        // Input
        val alice = sc.textFile("alice.txt")

        // Map
        val words = alice.flatMap(line => {
            "\\W+".r.split(line).map(word => (word, 1))
        })

        // Reduce
        val counts = words.reduceByKey((a, b) => a + b)

        // Output
        val text = counts.map(p => {
            val (k, v) = p;
            "" + k + "\t" + v;
        })
        text.saveAsTextFile("out")

        // Shut down Spark, avoid errors.
        sc.stop()
    }
}

// vim: set ts=4 sw=4 et:
