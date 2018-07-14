package com.db.common.utils;

import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;

public class TimerArrayList<T> extends ArrayList<T> implements Runnable {

	
	private static final long serialVersionUID = -650703703373568548L;
	
	private static Logger log = LogManager.getLogger(TimerArrayList.class);
	
	
	/**
	 * Default timer value set to 30 Minute. Specified value is in milliseconds.
	 */
	private static long timer=1800000;
	
	public TimerArrayList(){
		this(timer);
	} 
	
	public TimerArrayList(long l) {
		timer=l;
		Thread t=new Thread(this);
		t.setDaemon(true);
		t.start();
	}
	
	

	
	public static void main(String[] args) throws Exception{
		TimerArrayList<String> list=new TimerArrayList<String>(5000);
		list.add("hb4");
		list.add("hb1");
		list.add("hb7");
		list.add("hb8");
		list.add("hb6");
		
		System.out.println(list);
		Thread.sleep(10000);
		System.out.println(list);
	}
	
	@Override
	public void run() {
		try {
			Thread.sleep(timer);
			this.clear();
			Thread.currentThread().interrupt();
			return;
		} catch (InterruptedException e) {
			log.error("Error occured in sleep of TimerArrayList",e);
		}
	}

}
