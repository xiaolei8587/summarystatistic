/**
 *
 */
package com.mucfc.common

import java.io.File
import java.io.InputStreamReader
import java.util.Properties
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import scala.collection.mutable.HashMap
import scala.io.Source
import java.io.FileInputStream
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.MD5Hash

/**
 * @author Eapha Yim
 * @time 2015年6月29日
 */
object Utils {

  def getFileName(filename: String): String = {
    val str = filename.substring(0, filename.indexOf("."))
    str
  }
 
  /**
   * 读取配置文件
   */
  def readTextFile(file: File) = {
    val cols = ArrayBuffer[String]()
    if (file.isFile() && file.exists()) {
      for (line <- Source.fromFile(file).getLines()) {
        if(null != line && !"".equals(line.trim)) cols += line.trim
      }
    }
    cols
  }
  /**
   * 字段名转换为大写
   * 读取配置文件
   */
  def readTextFile(filepath: String) = {
    val file = new File(filepath)
       val cols = ArrayBuffer[String]()
                 if (file.isFile() && file.exists()) {
                      for (line <- Source.fromFile(file).getLines()) {
                           if(null != line && !"".equals(line.trim)) cols += line.trim.toUpperCase
                      }
                 }
       cols
  }
  /**
   * 字段名转换为大写
   * 读取配置文件
   */
  def readTextFile(filepath: String, isUpperCase:Boolean) = {
	  val file = new File(filepath)
	  val cols = ArrayBuffer[String]()
	  if (file.isFile() && file.exists()) {
		  for (line <- Source.fromFile(file).getLines()) {
			  if(null != line && !"".equals(line.trim)) cols += line.trim.toUpperCase
		  }
	  }
	  cols
  }
  /**
   * 读取表分隔符配置文件
   */
  def readPropertiesFile(filepath: String):scala.collection.mutable.Map[String,String] = {
	  val file = new File(filepath)
	  if (file.isFile() && file.exists()) {
      val p = new Properties();
      p.load(new InputStreamReader(new FileInputStream(file),"UTF-8"))
      val prop = scala.collection.JavaConversions.propertiesAsScalaMap(p)
      return prop
	  }
    null
  }

  /**
   * 表名转换为大写
   * 读取表字段配置文件
   */
  def readTableDefineFile(filepath: String): HashMap[String, ArrayBuffer[String]] = {
    val tables = new HashMap[String, ArrayBuffer[String]]()
    val file = new File(filepath)
    if (file.isDirectory()) {
      for (f <- file.listFiles.toIterator) {
        val filename = getFileName(f.getName).toUpperCase
        val cols = readTextFile(f)
        tables += (filename -> cols)
      }
    } else {
      val filename = getFileName(file.getName)
      val cols = readTextFile(file)
      tables += (filename -> cols)
    }
    tables
  }
  
  def parseArr2Str(arr:Buffer[String]) = {
    val sb = new StringBuffer()
    for(x <- arr) {
      sb.append(x).append(",")
    }
    sb.setLength(sb.length() -1)
    sb.toString()
  }
  
  /**
   * 取key值的MD5前8位再拼接key值
   */
  def getRowKey(key:String, isMD5:Boolean): Array[Byte] = {
    val keyBytes =  Bytes.toBytes(key)
    if (isMD5) {
      return Bytes.add(MD5Hash.getMD5AsHex(keyBytes).substring(0, 8).getBytes(), keyBytes)
    }
    keyBytes
  }
  
  def main(args: Array[String]): Unit = {
    val rowkey = getRowKey("20150325001004493", true)
    print(new String(rowkey))
  }
}