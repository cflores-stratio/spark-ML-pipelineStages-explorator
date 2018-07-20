package com.stratio.intelligence.poc

import java.beans.Statement
import java.io.File

import org.apache.spark.ml.param.Param
import org.apache.spark.ml.{Pipeline, PipelineStage}

import scala.util.parsing.json._

object SparkMlBuilder extends App {

  val cl = ClassLoader.getSystemClassLoader
  val classpath = cl.asInstanceOf[java.net.URLClassLoader].getURLs.map(x=>new File(x.getFile))
  val spark_classpath = classpath.filter(_.toString.contains("spark"))



  val classStage = "org.apache.spark.ml.feature.Tokenizer"
  val parameters = Map("inputCol" -> "input", "outputCol" -> "output")


  val jsonPipelineStr = """{
                          |  "pipelineName": "myCustomPipeline",
                          |  "stages": [
                          |    {
                          |      "className": "org.apache.spark.ml.feature.Tokenizer",
                          |      "parameters": {
                          |        "inputCol": "input",
                          |        "outputCol": "output_tokenizer"
                          |      }
                          |    },
                          |    {
                          |      "className": "org.apache.spark.ml.feature.HashingTF",
                          |      "parameters": {
                          |        "numFeatures": 10,
                          |        "inputCol": "output_tokenizer",
                          |        "outputCol": "features"
                          |      }
                          |    },
                          |    {
                          |      "className": "org.apache.spark.ml.classification.LogisticRegression",
                          |      "parameters": {
                          |        "maxIter": 20,
                          |        "regParam": "output_tokenizer"
                          |      }
                          |    }
                          |  ]
                          |}""".stripMargin


  /*
    val jsonPipelineStr =  """{
                             |  "pipelineName": "myCustomPipeline",
                             |  "stages": [
                             |    {
                             |      "className": "org.apache.spark.ml.feature.Tokenizer",
                             |      "parameters": {
                             |        "inputCol": "input",
                             |        "outputCol": "output_tokenizer"
                             |      }
                             |    }
                             |  ]
                             |}""".stripMargin
                             */
  val jsonPipelineObj = JSON.parseFull(jsonPipelineStr)

  val pipelineMap = jsonPipelineObj.get.asInstanceOf[Map[String, Object]]

  val stagesMaps: List[Map[String, Object]] = pipelineMap.get("stages").get.asInstanceOf[List[Map[String, Object]]]
  val stageNum = stagesMaps.size

  var finalStages = Array()
  for (stage <- stagesMaps) {
    val stageClass = stage.get("className").get
    val stageParemeters = stage.get("parameters").get.asInstanceOf[Map[String, Object]]
    val instancedClass = Class.forName(stageClass.toString).newInstance
    println("------------------------------")
    println(stageClass)
    println(instancedClass)
    println(" ---> params...")
    for (param <- stageParemeters) {
      println(param._1)
      println(param._2)
      val methodPrefix = "set"
      val methodSufix = param._1.charAt(0).toUpper + param._1.substring(1)
      val methodName = methodPrefix + methodSufix
      val statement = new Statement(instancedClass, methodName, Array {param._2})
      statement.execute()
      // instancedClass.getClass.getMethod(methodName, Class {}).invoke()
      // (methodName)
      val statement2 = new Statement(instancedClass, s"get${methodSufix}", Array())
      println(statement2.execute())
    }
  }

  // val pipeline = Pipeline

  //

  // Class.forName(c.toString()).newInstance

}
