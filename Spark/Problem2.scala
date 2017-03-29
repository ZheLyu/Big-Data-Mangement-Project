import org.apache.spark.SparkContext  
import org.apache.spark.SparkContext._  
import org.apache.spark.SparkConf 

import scala.collection.mutable.ListBuffer

  
object Problem2 {  
  def main(args: Array[String]): Unit =  {  
    val conf = new SparkConf().setAppName("haha").setMaster("local[2]")  
    val sc = new SparkContext(conf)  
    val input = sc.textFile("points.csv")
    val points = input.map {line =>
      val point = line.split(",")
      (point(0).toFloat.toInt)/20 + 500* (500-point(1).toFloat.toInt/20-1) + 1 }
    val stepB=points.map((_, 1)).reduceByKey(_+_).sortByKey(true, 10)
    val stepC=input.map{line=>
      val point = line.split(",")
      val x=(point(0).toFloat).toInt/20
      val y=500-(point(1).toDouble).toInt/20-1
      val cell=500*y+x+1
      val list = ListBuffer[Int](cell-1,cell+1,cell-499,cell-500,cell-501,cell+501,cell+499,cell+500)
      if(x==0){
        list-=(cell-1,cell-501,cell+499)        
      }
      if(x==499){
          list-=(cell+1,cell-499,cell+501)        
      }
      if(y==0){
        list-=(cell-499,cell-500,cell-501)        
      }
      if(y==499){
        list-=(cell+499,cell+500,cell+501)        
      }
      list.toList.toArray  
      }
    val stepD=stepC.flatMap {line =>line}.map(word=>(word, 1)).reduceByKey(_+_).sortByKey(true, 10) 
   //println(stepD.collect().mkString("\n"))
    val stepE=stepD.map{line =>   
      var n=8
      if(line._1==1||line._1==500||line._1==249501||line._1==250000){
        n=3        }
      else if((line._1<500&&line._1>1)||(line._1<250000&&line._1>249501)||line._1%500==1||line._1%500==0){
        n=5 }
      (line._1,line._2.toFloat/n)    
    }
   // println(stepE.collect().mkString("\n"))
    val stepF=stepB.join(stepE,2).map(line=>(line._1,line._2._1/line._2._2))
    //val stepG=stepB.join(stepE,2).map(line=>(line._1,line._2._1/line._2._2)).sortBy(_._2,false).top(50).foreach(println)
 //   val stepJ=stepF.sortBy(_._2)
   // println(stepJ.top(50).mkString("\n"))
   val B=stepF.collect().sortBy(_._1)
    //val stepI=stepH.filter(line=>(line._1==stepJ.apply(1))).foreach(println)
   val A_ =stepF.collect().sortBy(_._2).reverse.take(50)
   println("\n Step2 report\n") 
   println(A_.mkString("\n"))
   val r = 0;
    println("\n Step3 report\n")  
  for(r<-0 to 49){
      if(A_(r)._1==1){
      println("The neibor of"+B(A_(r)._1-1)+"is\n"+B(A_(r)._1)+"\n"+B(A_(r)._1+499)+"\n"+B(A_(r)._1+500))
      println("\n")
      }
      else if(A_(r)._1==500){
      println("The neibor of"+B(A_(r)._1-1)+"is\n"+B(A_(r)._1-2)+"\n"+B(A_(r)._1+498)+"\n"+B(A_(r)._1+499))
      println("\n")
      }
      else if(A_(r)._1==249501){
      println("The neibor of"+B(A_(r)._1-1)+"is\n"+B(A_(r)._1)+"\n"+B(A_(r)._1-501)+"\n"+B(A_(r)._1-500))
      println("\n")
      }
      else if(A_(r)._1==250000){
      println("The neibor of"+B(A_(r)._1-1)+"is\n"+B(A_(r)._1-2)+"\n"+B(A_(r)._1-501)+"\n"+B(A_(r)._1-502))
      println("\n")
      }
      else if(A_(r)._1<250000&&A_(r)._1>249501){
      println("The neibor of"+B(A_(r)._1-1)+"is\n"+B(A_(r)._1-2)+"\n"+B(A_(r)._1)+"\n"+B(A_(r)._1-500)+"\n"
          +B(A_(r)._1-501)+"\n"+B(A_(r)._1-502))
      println("\n")
          
      }
      else if(A_(r)._1<500&&A_(r)._1>0){
      println("The neibor of"+B(A_(r)._1-1)+"is\n"+B(A_(r)._1-2)+"\n"+B(A_(r)._1)+"\n"+B(A_(r)._1+498)+"\n"
          +B(A_(r)._1+499)+"\n"+B(A_(r)._1+500))
      println("\n")    
      }
      else if(A_(r)._1%500==0){
      println("The neibor of"+B(A_(r)._1-1)+"is\n"+B(A_(r)._1-501)+"\n"+B(A_(r)._1)+"\n"+B(A_(r)._1+500)+"\n"
          +B(A_(r)._1+499)+"\n"+B(A_(r)._1-500))
      println("\n")    
      }
      else if(A_(r)._1%500==499){
      println("The neibor of"+B(A_(r)._1-1)+"is\n"+B(A_(r)._1-501)+"\n"+B(A_(r)._1-2)+"\n"+B(A_(r)._1-502)+"\n"
          +B(A_(r)._1+498)+"\n"+B(A_(r)._1+499))
      println("\n")    
      }
      else{
         println("The neibor of"+B(A_(r)._1-1)+"is\n"+B(A_(r)._1-502)+"\n"+B(A_(r)._1-501)+"\n"+B(A_(r)._1-500)+"\n"
          +B(A_(r)._1-2)+"\n"+B(A_(r)._1)+"\n"+B(A_(r)._1+498)+"\n"+B(A_(r)._1+499)+"\n"+B(A_(r)._1+500)+"\n")
         println("\n") 
      }
        
      }  
  }
  
 }