import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import java.io._

import scala.collection.mutable.ListBuffer
object RulesEngine {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "C:\\winutils")

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val inputf = sc.textFile("ontology_outputs\\Triplets_org.txt", 4)

    inverseof(inputf)
    symmetry(inputf)
    transtive(inputf)
    propertyAxim(inputf)
    irreflexive(inputf)

  }
  def inverseof(inputf : RDD[String]): Unit =
  {

    val trip1 = inputf.map(line => line.split(",")).collect()
    val trip2 = trip1
    val pw = new PrintWriter(new File("outputs/a_inverse_of.txt" ))

    trip1.foreach( t1 =>
    {
      trip2.foreach( t2 => {
        if (t1(0).equals(t2(2))  &&  t1(2).equals(t2(0)) && !t1(1).equals(t2(1)) )
        println(t1(0) + " " + t1(1) + " " + t1(2) + " inverse to " + t2(0) + " " + t2(1) + " " + t2(2))
          pw.write(t1(0) + "," + t1(1) + "," + t1(2) + " INVERSE TO " + t2(0) + "," + t2(1) + "," + t2(2) + "\n")
      })
    })
    pw.close()
  }
  def symmetry(inputf : RDD[String]): Unit =
  {

    val trip1 = inputf.map(line => line.split(",")).collect()
    val trip2 = trip1
    val pw = new PrintWriter(new File("outputs/b_symmetric_property.txt" ))

    trip1.foreach( t1 =>
    {
      trip2.foreach( t2 => {
        if (t1(0).equals(t2(2))  &&  t1(2).equals(t2(0)) && t1(1).equals(t2(1)) )
          pw.println(t1(0) + "," + t1(1) + "," + t1(2) + " SYMMETRIC TO " + t2(0) + "," + t2(1) + "," + t2(2) + "\n")
      })
    })
    pw.close()
  }
  def transtive(inputf : RDD[String]): Unit =
  {

    val trip1 = inputf.map(line => line.split(",")).collect()
    val trip2 = trip1
    val pw = new PrintWriter(new File("outputs/c_transitive_property.txt" ))

    trip1.foreach( t1 =>
    {
      trip2.foreach( t2 => {
        if (t1(2).equals(t2(0))   && t1(1).equals(t2(1)) &&  !t1(2).equals(t2(2))) {
          println(t1(0) + "," + t1(1) + "," + t1(2) )
          println(t2(0) + "," + t2(1) + "," + t2(2) )
          println("Transtive :" )
          println(t1(0) + "," + t1(1) + "," + t2(2) )
        }
      })
    })
    pw.close()
  }
  def propertyAxim(inputf : RDD[String]): Unit =
  {

    val trip1 = inputf.map(line => line.split(",")).collect()
    val trip2 = trip1
    val trip3 = trip1
    val pw = new PrintWriter(new File("outputs/d_axiom_property.txt" ))

    trip1.foreach( t1 =>
    {
      trip2.foreach( t2 => {
        if (t1(2).equals(t2(0))   && t1(1).equals(t2(1)) &&  !t1(2).equals(t2(2))) {
          trip3.foreach(t3 => {
            if (t1(0).equals(t3(0)) && t2(2).equals(t3(2)) && !t1(1).equals(t3(1)) ) {
              println(t1(0) + "," + t1(1) + "," + t1(2) )
              println(t2(0) + "," + t2(1) + "," + t2(2) )
              println("property axiom :" )
              println(t3(0) + "," + t3(1) + "," + t3(2) )
            }
          }
          )
        }
      })
    })

    pw.close()
  }
  def irreflexive(inputf : RDD[String]): Unit =
  {

    val trip1 = inputf.map(line => line.split(",")).collect()
    val trip2 = trip1
    val pw = new PrintWriter(new File("outputs/f_irreflexive_property.txt" ))

    trip1.foreach( t1 =>
    {
      trip2.foreach( t2 => {
        if (t2(2).equals(t2(0)) && t1(0).equals(t2(0)) && !t1(2).equals(t2(2))  && t1(1).equals(t2(1)) ) {
          println(t1(0) + "," + t1(1) + "," + t1(2) )
          println(t2(0) + "," + t2(1) + "," + t2(2) )
        }
      })
    })

    pw.close()
  }
}