/**
 *
 */
package com.eaphayim.summarystatistic

import java.io.Serializable

/**
 * @author 641158
 * appname,date,methodname,costs,success,total
 */
class LogAnalysisSummary(_appname: String, _date: String, _methodname: String, _costs: Int, _success: Int, _total: Int) extends AnyRef with Serializable {
  var date: String = _date
  var appname: String = _appname
  var methodname: String = _methodname
  var costs: Int = _costs
  var success: Int = _success
  var total: Int = _total

  def count(that: LogAnalysisSummary): LogAnalysisSummary = {
    if (that != null && appname.equals(that.appname) && date.equals(that.date) && methodname.equals(that.methodname)) {
      costs = costs + that.costs
      success = success + that.success
      total = total + that.total
    }
    this
  }
  
  def minus(that: LogAnalysisSummary): LogAnalysisSummary = {
    if (that != null && appname.equals(that.appname) && date.equals(that.date) && methodname.equals(that.methodname)) {
      costs = costs - that.costs
      success = success - that.success
      total = total - that.total
    }
    this
  }

  def getUniqueId(): String = {
    appname + date + methodname
  }

  def sum(that: LogAnalysisSummary): LogAnalysisSummary = {
    if (that != null) {
      costs = costs + that.costs
      success = success + that.success
      total = total + that.total
    }
    this
  }
  
  override def toString(): String = {
    "LogAnalysisSummary[appname="+appname+",date="+date+",methodname="+methodname+",costs="+costs+",success="+success+",total="+total+"]"
  }
}