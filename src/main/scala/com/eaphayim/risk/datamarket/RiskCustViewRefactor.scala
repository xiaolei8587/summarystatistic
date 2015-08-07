/**
 *
 */
package com.eaphayim.risk.datamarket

import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * @author Eapha Yim
 * @time 2015年6月5日
 */
object RiskCustViewRefactor extends Logging {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("RiskCustViewRefactor")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val userTable = "r_cif_per_user"
    val custTable = "r_cif_per_individual"
    val zkUrl = "A-HDS111,A-HDS112,A-HDS113,A-HDS114,A-HDS115"
    val userDf = sqlContext.load("org.apache.phoenix.spark", Map("table" -> userTable,
      "zkUrl" -> zkUrl))
    print("r_cif_per_user size:"+userDf.count())
    val custDf = sqlContext.load("org.apache.phoenix.spark", Map("table" -> custTable,
      "zkUrl" -> zkUrl))
    print("r_cif_per_individual size:"+custDf.count())
    val result = userDf.join(custDf, userDf("cust_id") === custDf("cust_id"), "left_outer").count()
    print(result)
  }
}