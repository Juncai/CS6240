import scala.collection.mutable
var ll: List[Double] = List(2, 3, 7, 1, 6)
val len = ll.length
println(len / 2)
ll = ll.sorted
println(ll(len / 2))
/* var l: Double = 0 */
/* var aMap:mutable.Map[String, List[Double]] = mutable.Map() */
/* aMap("key") = ll */
/* /1* ll::l *1/ */
/* /1* aMap("key") = aMap("key"):+l *1/ */
/* aMap("key"):+l */
/* if (aMap contains "key") { */
/* 	println(aMap("key")(0)) */
/* } */
