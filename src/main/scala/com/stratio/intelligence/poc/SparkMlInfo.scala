package com.stratio.intelligence.poc

import java.io.File

import org.clapper.classutil.{ClassFinder, ClassInfo}
import org.apache.spark.ml.param.{Param, ParamMap, Params}


object SparkMlInfo extends App{

  val cl = ClassLoader.getSystemClassLoader
  val classpath = cl.asInstanceOf[java.net.URLClassLoader].getURLs.map(x=>new File(x.getFile))

  val spark_classpath = classpath.filter(_.toString.contains("spark"))

  val finder = ClassFinder(spark_classpath)
  val classes = finder.getClasses
  //classes.foreach(println)

  val estimatorsClasses: Seq[ClassInfo] = ClassFinder.concreteSubclasses(
      "org.apache.spark.ml.Estimator", classes
    ).toList.filter(_.toString()!="org.apache.spark.ml.Pipeline")


  //plugins.foreach(println)


  val estimatorsInstances: Seq[Any] = estimatorsClasses.map(c => Class.forName(c.toString()).newInstance)

  // => Explain params
//  estimatorsInstances.foreach( e =>
//    println(s"\n${"*"*200}\n=> ${e.getClass.getName}\n${"*"*200}\n${e.asInstanceOf[Params].explainParams()}")
//  )

  val estimatorsParamMap: Seq[ParamMap] = estimatorsInstances.map(e =>
    e.asInstanceOf[Params].extractParamMap()
  )

  val estimatorsParams: Seq[(String, Array[Param[_]])] = estimatorsInstances.map(e =>
    (e.getClass.getSimpleName, e.asInstanceOf[Params].params)
  )

  val estimatorsParamsProcessed = estimatorsParams.map( e =>
    (e._1, e._2.map( x => (x.name, (x.doc, x.getClass.getSimpleName))).toMap )
  )


  print("a")
}


