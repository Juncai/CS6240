/*
Author for 'iterate' function: Jun Cai
The shortest path from Apple to Ivory
Apple
Yellow
Gray
Elephant
Ivory
 */
import scala.annotation.tailrec
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object ShortestPath {
  // curValue, Neibs, preNode, dist
  type Node = Tuple4[String, Array[String], String, Double]
  type Link = Tuple2[String, Double]

  var src = ""
  var dst = ""

  def main(args: Array[String]) {
    if (args.length < 3) {
      println("Usage:")
      println("   shortest_path Graph From To")
      return
    }

    val input = args(0)
    src = args(1)
    dst = args(2)

    val conf = new SparkConf().
      setAppName("Shortest Path").
      setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile(input)

    val nodes0 = lines.map(makeNode)
    val nodes1 = iterate(nodes0, sc)

    val path = findPath(nodes1, dst, sc)

    sc.stop()

    path.foreach(n => println(n))
  }

  def findPath(nodes: RDD[Node], name: String, sc: SparkContext): Array[String] = {
    if (name == src) {
      return Array(name)
    }

    val (_, _, prev, _) = nodes.filter(_._1 == name).first
    return findPath(nodes, prev, sc) :+ name
  }

  @tailrec
  def iterate(nodes: RDD[Node], sc: SparkContext): RDD[Node] = {
    var cnodes = nodes
    var newLinks = cnodes.flatMap(nextLinks)

    cnodes = cnodes.keyBy(_._1).cogroup(newLinks).map(nextNode)

    // check if we have found a path to dest with 'filter'
    val tnode = cnodes.filter(cn => {
      cn._1 == dst
    }).first();
    if (tnode._3 != "") {
      return cnodes
    } else {
      return iterate(cnodes, sc)
    }
  }

  def makeNode(line: String): Node = {
    val words = line.split("\\s+")

    val name = words(0)
    val neibs = words.slice(1, words.length)
    val dist = if (words(0) == src) 0 else Double.PositiveInfinity

    return new Node(name, neibs, "", dist)
  }

  def nextLinks(node: Node): Array[Tuple2[String, Link]] = {
    val (name, neibs, _, dist) = node

    if (dist == Double.PositiveInfinity) {
      return Array()
    }

    return neibs.map(neib =>
      (neib, (name, dist + 1))
    )
  }

  def nextNode(step: Tuple2[String, Tuple2[Iterable[Node], Iterable[Link]]]): Node = {
    val (name, (nodes, links)) = step
    val (from, best) = if (links.size == 0) ("", Double.PositiveInfinity) else links.minBy(_._2)

    if (nodes.size == 0) {
      return (name, Array(), from, best)
    }

    val (_, neibs, prev, dist) = nodes.head

    if (best < dist) {
      return (name, neibs, from, best)
    }
    else {
      return nodes.head
    }
  }
}

// vim: set ts=4 sw=4 et:
