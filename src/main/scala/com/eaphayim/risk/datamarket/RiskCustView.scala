/**
 *
 */
package com.eaphayim.risk.datamarket

import org.apache.spark.Logging
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.spark.SparkConf
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.HBaseConfiguration
import scala.collection.mutable.ListBuffer
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

/**
 * @author Eapha Yim
 * @time 2015年6月5日
 */
object RiskCustView extends Logging {
  def getString(result: Result, famliy: String, column: String): String = {
    val ret = result.getValue(Bytes.toBytes(famliy), Bytes.toBytes(column))
    if (null == ret)
      null
    else
      new String(ret)
  }
  def getKey(result: Result): String = {
    new String(result.getRow)
  }
  case class UserInfo(user_id: String, cust_id: String, user_nam: String, gdr_cod: String, user_sta: String, reg_dat: String, act_dat: String, clz_dat: String, clz_rsn: String, cha_typ: String, src_sys: String, lst_log_ip: String, lst_log_tim: String)
  case class CustInfo(cust_id: String, cust_nam: String, apl_cod: String, cust_sta: String, ath_sta: String, ath_typ: String, bth_dat: String, cnr_cod: String, liv_cty_cod: String, rgs_cty_cod: String, nat_cty_cod: String, lng_cod: String, emp_pos_cod: String, cust_lst_nam: String, cust_fst_nam: String, cust_nam_eng: String, cust_nam_for: String)

  val jars = new ListBuffer[String]()
  val sparkConf = new SparkConf().setAppName("RiskCustView")

  val sc = new SparkContext(sparkConf)

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext._

  val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum", "A-HDS111,A-HDS112,A-HDS113,A-HDS114,A-HDS115");
  conf.set("hbase.zookeeper.property.clientPort", "2181");
  conf.set("hbase.master", "A-HDS113");
  conf.set("hbase.master.port", "60000");
  conf.set(TableInputFormat.INPUT_TABLE, "r_cif_per_user")
  // Initialize hBase table if necessary
  val admin = new HBaseAdmin(conf)
  if (!admin.isTableAvailable("r_cif_per_user")) {
    val tableDesc = new HTableDescriptor("r_cif_per_user")
    admin.createTable(tableDesc)
  }
  val userHbaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[org.apache.hadoop.hbase.client.Result])

  //
  //    val confcust = HBaseConfiguration.create()
  //    confcust.set("hbase.zookeeper.quorum", "A-HDS111,A-HDS112,A-HDS113,A-HDS114,A-HDS115");
  //    confcust.set("hbase.zookeeper.property.clientPort", "2181");
  //    confcust.set("hbase.master", "A-HDS113");
  //    confcust.set("hbase.master.port", "60000");
  conf.set(TableInputFormat.INPUT_TABLE, "r_cif_per_individual")
  val admincust = new HBaseAdmin(conf)
  if (!admincust.isTableAvailable("r_cif_per_individual")) {
    val tableDesc = new HTableDescriptor("r_cif_per_individual")
    admincust.createTable(tableDesc)
  }
  import sqlContext.implicits._
  val custHbaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[org.apache.hadoop.hbase.client.Result])
  val userRdd = userHbaseRDD.map(x => UserInfo(getKey(x _2), getString(x _2, "per_user", "cust_id"), getString(x _2, "per_user", "user_nam"), getString(x _2, "per_user", "gdr_cod"), getString(x _2, "per_user", "user_sta"), getString(x _2, "per_user", "reg_dat"), getString(x _2, "per_user", "act_dat"), getString(x _2, "per_user", "clz_dat"), getString(x _2, "per_user", "clz_rsn"), getString(x _2, "per_user", "cha_typ"), getString(x _2, "per_user", "src_sys"), getString(x _2, "per_user", "lst_log_ip"), getString(x _2, "per_user", "lst_log_tim")))
  //    userRdd.registerTempTable("usertable")
  var userDF = userRdd.toDF
  val custRdd = custHbaseRDD.map(x => CustInfo(getKey(x _2), getString(x _2, "per_individual", "cust_nam"), getString(x _2, "per_individual", "apl_cod"), getString(x _2, "per_individual", "cust_sta"), getString(x _2, "per_individual", "ath_sta"), getString(x _2, "per_individual", "ath_typ"), getString(x _2, "per_individual", "bth_dat"), getString(x _2, "per_individual", "cnr_cod"), getString(x _2, "per_individual", "liv_cty_cod"), getString(x _2, "per_individual", "rgs_cty_cod"), getString(x _2, "per_individual", "nat_cty_cod"), getString(x _2, "per_individual", "lng_cod"), getString(x _2, "per_individual", "emp_pos_cod"), getString(x _2, "per_individual", "cust_lst_nam"), getString(x _2, "per_individual", "cust_fst_nam"), getString(x _2, "per_individual", "cust_nam_eng"), getString(x _2, "per_individual", "cust_nam_for")))
  var custDF = custRdd.toDF()
  val result=userDF.join(custDF, userDF("cust_id") === custDF("custDF"), "left_outer").count()
}