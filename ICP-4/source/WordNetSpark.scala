package wordnet

import org.apache.spark.{SparkConf, SparkContext}
import rita.RiWordNet

/**
  * Created by Mayanka on 26-06-2017.
  */
object WordNetSpark {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\winutils")
    val conf = new SparkConf().setAppName("WordNetSpark").setMaster("local[*]").set("spark.driver.memory", "4g").set("spark.executor.memory", "4g")
    val sc = new SparkContext(conf)


    //val data=sc.textFile("data/sample")
//    val data=sc.textFile("data/my_inputs.txt") // Input

    val inputf = sc.wholeTextFiles("D:\\umkc\\2018Fall\\Knowledge_discovery\\Tutorial-4-Intro-to-Sparking-Programming\\Spark WordCount\\inputFolder", 10)

    // Example on how to refer whthin wholeTextFiles
    val data = inputf.map(abs=>{
      //abs._1 // File Path
      abs._2 // File Content
    })

  val dd=data.map(line=>{
  val wordnet = new RiWordNet("D:\\umkc\\2018Fall\\Knowledge_discovery\\Tutorial 3 Source Code\\WordNet-3.0")
  val wordSet=line.split(" ")
  val synarr=wordSet.map(word=>{
    if(wordnet.exists(word))
      (word,getSynoymns(wordnet,word))
    else
      (word,null)
  })
  synarr
})
    dd.collect().foreach(linesyn=>{
      linesyn.foreach(wordssyn=>{
        if(wordssyn._2 != null)
          println(wordssyn._1+":"+wordssyn._2.mkString(","))
      })
    })
  }
  def getSynoymns(wordnet:RiWordNet,word:String): Array[String] ={
    println(word)
    val pos=wordnet.getPos(word)
    println(pos.mkString(" "))
    val syn=wordnet.getAllSynonyms(word, pos(0), 10)
    syn
  }

}
