package com.base

import java.util
import java.util.regex.Pattern

import scala.collection.mutable
import scala.io.Source


case class Template(start: Int, end: Int, length: Int, fieldName: String, format: String)

object HandleFixedWidth {

  val strLen: Int = 4607

  def readCSV(): scala.List[Template] = {
    val attributesFilePath= "~/Attribute File Layout Mockup.csv"
    val inputStream = getClass.getResourceAsStream(attributesFilePath)
    Source.fromFile(attributesFilePath, "utf-8").getLines().filterNot(x => x.contains("BEGIN,END,LENGTH,FIELD NAME,FORMAT")).map(row => {
//      println(row)
      val values: Array[String] = row.split(",")
      Template(values(0).toInt, values(1).toInt, values(2).toInt, values(3), values(4))
    }).toList

  }

  def main(args: Array[String]): Unit = {

    //    regExp
    val metadata: scala.List[Template] = readCSV
    val filename = "~/filename"
    for (line <- Source.fromFile(filename).getLines()) {
      println(line)

      val result: Map[String, Any] = parseRow(metadata, line)
//        val result: Map[String, Any] = carlsCode(metadata, line).toMap
//
//      result.foreach { x => println(s"${x._1}, ${x._2}") }
//      println("----------")
//      val exp = result.filterKeys(x => (x.equals("CKID"))).toList(0)._2
//      println(exp)
//        println("----------")
//      val tmp = result.filterKeys(x => !(x.equals("CKID") || x.equals("MEMBERSHIPDATE")))
//      tmp.foreach { x => println(s"${x._1}, ${x._2}") }

      println("----------")
    }
  }

  def carlsCode(metadata: scala.List[Template], line: String): mutable.Map[String, Any] = {
    val result: (mutable.Map[String, Any], String) = metadata.foldLeft((mutable.Map.empty[String, Any], line)) { (tuple, row) => {
      val length = row.length
//      println(tuple._2)
//      println(length)
      val value: Any = row.format match {
        case "NUMERIC" => tuple._2.take(length).toDouble
        case _ => tuple._2.take(length).toString
      }

      tuple._1(row.fieldName) = value

      (tuple._1, line.drop(length))
    }

    }
//    print(result._2)
    result._1
  }

  def parseRow(metadata: scala.List[Template], line: String): Map[String, Any] = {
    var lower: Int = 0
    val charLine: Array[Char] = line.padTo(strLen, " ").mkString.toCharArray
    metadata.map { details => {
      val b = StringBuilder.newBuilder
      while (lower < details.end) {
        b.append(charLine(lower))
        lower += 1
      }
//      println(s"$b, $details")
      val finalStr = b.toString.trim
      val value = details.format match {
        case "NUMERIC" => if (finalStr.isEmpty) 0 else finalStr.toDouble
        case _ => finalStr
      }
      details.fieldName -> value
    }
    }.toMap
  }


  def regExp = {
    val input = "ab12cefg123hig"
//    val output = new util.ArrayList[String]
//    val mapping = ("qaz" -> "String", "wsx" -> "Numeric", "edc" -> "String", "rfv" -> "String")
//
//    val pattern_2 = Pattern.compile("^(.{2})(\\d{2})(.{3})(.{2}).*")
//    val matcher = pattern_2.matcher(input)
//
//    if (matcher.matches) {
//      var i = 1
//      while ( {
//        i <= 4
//      }) {
//        output.add(matcher.group(i))
//
//        {
//          i += 1;
//          i - 1
//        }
//      }
//    }
//
//    System.out.println(output)

    println(input.take(3))
    println(input.drop(3))
    println(input.take(3))
  }


//  private def mapAttributeToValue(rawString: String): Map[String, CreditReportAttributeValue] = {
//    println("**********" + rawString)
//    val result: (mutable.Map[String, CreditReportAttributeValue], String) = attributeDescriptions.foldLeft((mutable.Map.empty[String, CreditReportAttributeValue], rawString)) { (tuple, row) =>
//    {
//      val length = row.length
//      val value: CreditReportAttributeValue = row.format match {
//        case "NUMERIC"   => CreditReportAttributeValue.DoubleVal(tuple._2.take(length).toString.trim.toDouble)
//        case "CHARACTER" => CreditReportAttributeValue.StringVal(tuple._2.take(length).toString.trim)
//      }
//      tuple._1(row.fieldName) = value
//      (tuple._1, rawString.drop(length))
//    }
//    }
//    result._1
//    }.toMap
}
