
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.immutable.ListMap

import java.text.SimpleDateFormat
import java.util.Date

/*
 * Authors: Neha Patwardhan, Joy Machado
 * */ 

object MissedConnections {

    def convertToMinutes( time:String, date:String) : String = {
      var hr:String = "00"
      var min:String = "00"

      if(time.length() == 4)
      {
        hr = time.substring(0,2);
        min = time.substring(2);
      }
      else if(time.length() == 3)
      {
        hr = "0"+time.substring(0,1)
        min = time.substring(1)
      }
      else if(time.length() == 2)
      {
            hr = "00"
            min = time
       }
       else
        {
            hr = "00"
            min = "0"+time
        }
      return (date+" "+hr+":"+min).mkString("")
   }
    

    // Gets the diffrence between two times in minutes
    def timeDifferenceInMinutes( time1:String, time2:String) : Long = {

        var  simpleDateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm");
        var  date1:Date = simpleDateFormat.parse(time1);
        var  date2:Date = simpleDateFormat.parse(time2);
        val  timeDifference = (date2.getTime() - date1.getTime())/(1000*60)
        return timeDifference
   }
    

    def main(args: Array[String]) {
       val conf = new SparkConf().
            setAppName("Airlines").
            setMaster("local")
        val sc = new SparkContext(conf)
       
        // reading the input for origin as key
        val origin = sc.textFile("all/*").
            map { _.replace("\"","").replace(", ",":").split(",") }.
            filter { _(0) != "YEAR"}.
            filter ( x => {SanityChecker.sanityTest(x)}).
            keyBy ( x => {x(0) +" "+x(8)+" "+x(14)}) 

            // reading the input for destination as key
            val destination = sc.textFile("all/*").
            map { _.replace("\"","").replace(", ",":").split(",") }.
            filter { _(0) != "YEAR"}.
            filter ( x => {SanityChecker.sanityTest(x)}).
            keyBy ( x => {x(0)+" "+x(8)+" "+x(23)}) 
        
            
            // reading the input for destination as key
            val data = origin.join(destination)

            // Find connecting flights
            val text = data.map(x => { 
                         val (k, v) = x;
            val (p, h) = v;
	        var connection = 0
	        var missedConnection = 0
	        
	        val dateOrigin = p(0).mkString("") + "/" + p(2).mkString("") + "/" + p(3).mkString("")
            val crsDeptTimeOrigin = convertToMinutes(p(29).mkString(""), dateOrigin)
            val deptTimeOrigin = convertToMinutes(p(30).mkString(""), dateOrigin)
            val crsArrTimeOrigin = convertToMinutes(p(40).mkString(""), dateOrigin)
            val arrTimeOrigin = convertToMinutes(p(41).mkString(""), dateOrigin)
            
	        val dateDestination = h(0).mkString("") + "/" + h(2).mkString("") + "/" + h(3).mkString("")
            val crsDeptTimeDestination = convertToMinutes(h(29).mkString(""), dateDestination)
            val deptTimeDestination = convertToMinutes(h(30).mkString(""), dateDestination)
            val crsArrTimeDestination = convertToMinutes(h(40).mkString(""), dateDestination)
            val arrTimeDestination = convertToMinutes(h(41).mkString(""), dateDestination)
            
	         var crsDiff:Long = timeDifferenceInMinutes(crsArrTimeOrigin,crsDeptTimeDestination)     
                            
            if(crsDiff >= 30 && crsDiff <= 360)
            {
            	connection = connection + 1
                var timeDifference = timeDifferenceInMinutes(arrTimeOrigin,deptTimeDestination)
                                
                if(timeDifference <= 30)
                {
                	missedConnection +=1
                }
		         else
	            {
			        missedConnection +=0
		        }
            }
	       else
	        {
		        connection +=0
	        } 

        // Create key with the number of connections and missed connections 
	    val count = connection + ":" + missedConnection
	    val key = p(0)+" "+p(8)
	    
    	key -> count
      })

       // Stores the result for each carrier and year 
       val result = text.groupByKey()
       val final_result= result.map(x => { 
           var count = 0.0
           var missedCount = 0.0
           var percentage = 0.0
           x._2.foreach(record => {
                var value1 = record.split(":")(0)
                var value2 = record.split(":")(1)
                //println(value)
                count = count + value1.toFloat
                missedCount = missedCount + value2.toFloat
            })
            if(count !=0)
            {
                percentage = (missedCount / count) * 100
            }
            val resultString = missedCount + " " + percentage
            x._1 -> resultString
        })
       

        final_result.saveAsTextFile("out")
        
        sc.stop()
    }
}

// vim: set ts=4 sw=4 et:
