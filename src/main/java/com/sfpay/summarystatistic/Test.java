/**
 * 
 */
package com.sfpay.summarystatistic;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author Eapha Yim
 * @time 2015年5月29日
 */
public class Test {

	/**
	 * @param args
	 * @throws UnknownHostException 
	 */
	public static void main(String[] args) throws UnknownHostException {
		// TODO Auto-generated method stub
		String host = InetAddress.getLocalHost().getHostName();
		String ip = InetAddress.getLocalHost().getHostAddress();
		System.out.println(host+" "+ip);
	}

}
