import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.{HashingTF, IDF}

import scala.collection.immutable.HashMap

/**
  * Created by Mayanka on 19-06-2017.
  */
object TF_IDF {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\winutils")

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    //Reading the Text File
//    val documents = sc.textFile("data/Article.txt")
    //val documents = sc.textFile("D:\\umkc\\2018Fall\\Knowledge_discovery\\Tutorial-4-Intro-to-Sparking-Programming\\Spark WordCount\\inputFolder\\abs_1.txt")
//    val input_folder = sc.wholeTextFiles("D:\\umkc\\2018Fall\\Knowledge_discovery\\Tutorial-4-Intro-to-Sparking-Programming\\Spark WordCount\\inputFolder")
    val input_folder = sc.wholeTextFiles("D:\\umkc\\2018Fall\\Knowledge_discovery\\Lab-2\\data")
    val documents = input_folder.map(abs=>{
      abs._2
    })

    //Getting the Lemmatised form of the words in TextFile
    val documentseq = documents.map(f => {
      val lemmatised = CoreNLP.returnLemma(f)
      val splitString = f.split(" ")
      splitString.toSeq
    })
//    val documentseq = documents.map(f=>{
//  val splitString = f.split("")
//  splitString.toSeq
//})

    //Creating an object of HashingTF Class
    val hashingTF = new HashingTF()

    //Creating Term Frequency of the document
    val tf = hashingTF.transform(documentseq)
    tf.cache()


    val idf = new IDF().fit(tf)

    //Creating Inverse Document Frequency
    val tfidf = idf.transform(tf)

    val tfidfvalues = tfidf.flatMap(f => {
      val ff: Array[String] = f.toString.replace(",[", ";").split(";")
      val values = ff(2).replace("]", "").replace(")", "").split(",")
      values
    })

    val tfidfindex = tfidf.flatMap(f => {
      val ff: Array[String] = f.toString.replace(",[", ";").split(";")
      val indices = ff(1).replace("]", "").replace(")", "").split(",")
      indices
    })

    tfidf.foreach(f => println(f))

    val tfidfData = tfidfindex.zip(tfidfvalues)

    var hm = new HashMap[String, Double]

    tfidfData.collect().foreach(f => {
      hm += f._1 -> f._2.toDouble
    })

    val mapp = sc.broadcast(hm)

    val documentData = documentseq.flatMap(_.toList)
    val dd = documentData.map(f => {
      val i = hashingTF.indexOf(f)
      val h = mapp.value
      (f, h(i.toString))
    })

    val dd1 = dd.distinct().sortBy(_._2, false)
    dd1.take(20).foreach(f => {
      println(f)
    })

  }

}
