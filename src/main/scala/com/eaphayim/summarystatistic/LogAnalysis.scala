/**
 *
 */
package com.eaphayim.summarystatistic

import java.io.Serializable
import java.text.SimpleDateFormat
import java.util.Date
import java.util.UUID

/**
 * @author 641158
 * appname,date,time,methodname,costs,success,total
 */
class LogAnalysis(_appname: String, _date: String, _time: String, _methodname: String, _costs: Int, _success: Int, _total: Int) extends AnyRef with Serializable {
  val id: String = createUniqueId
  var date: String = _date
  var time: String = _time
  var appname: String = _appname
  var methodname: String = _methodname
  var costs: Int = _costs
  var success: Int = _success
  var total: Int = _total

  def count(that: LogAnalysis): LogAnalysis = {
    if (that != null && appname.equals(that.appname) && date.equals(that.date) && methodname.equals(that.methodname)) {
      costs = costs + that.costs
      success = success + that.success
      total = total + that.total
    }
    this
  }

  def getUniqueId(): String = {
    appname + date + methodname
  }
  
  def getLog():(String, String, String, String, String, Int, Int, Int) = {
    (id, appname, date, time, methodname, costs, success, total)
  }

  def sum(that: LogAnalysis): LogAnalysis = {
    if (that != null) {
      costs = costs + that.costs
      success = success + that.success
      total = total + that.total
    }
    this
  }
  
  private def createUniqueId(): String = {
    val df = new SimpleDateFormat("yyyyMMddHHmmss")
    df.format(new Date) + UUID.randomUUID().toString
  }
}