package com.db.common.utils;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;

import com.db.common.exception.DBAnalyticsException;


/**
 * This class represents the Configuration Manager common to the project.
 * It is a singleton class causing only one instance of the configuration
 * to be created per VM. It further creates objects of the different 
 * configurations used.
 *
 */
public class DBConfig{	
	
	private static Logger log = LogManager.getLogger(DBConfig.class);
	private SlaveConfig system_cfg = null;
	
	public static final long REFRESH_INTERVAL = 1000 * 60 * 10;
	
	private static Thread configDaemon = null;
	 
	private DBConfig(){
		this.system_cfg = new SysConfig();
		configDaemon = new Thread() {
			@Override
			public void run() {
				while (true) {
					try {
						Thread.sleep(REFRESH_INTERVAL);
						system_cfg = new SysConfig();
						log.info("Reloading project configuration...");
					} catch (InterruptedException ie) {
						log.info(ie);
						return;
					}
				}
			}
		};
		configDaemon.setDaemon(true);
		configDaemon.start();
	}
	
	private static DBConfig instance = null;	
	public static DBConfig getInstance(){
		if(instance==null){
			synchronized (DBConfig.class) {
				DBConfig.instance = new DBConfig();
			}
		}
		return instance;
	}
	
	/**
	 * This interface defines the structure for the various types of configuration
	 * classes. 
	 *
	 */
	
	private interface SlaveConfig{
		public String getProperty(String key);
	}
	
	private class SysConfig implements SlaveConfig{
		private Properties confOverrides = null;		
		public SysConfig(){			
			try {
			File f = new File("C:\\Users\\rakesh\\git\\db-analytics\\src\\main\\resources\\config.properties");
				//File f = new File("/opt/config.properties");
					if(f.exists() && f.canRead()){
						confOverrides = new Properties();
						confOverrides.load(new FileInputStream(f));
					}else{
						throw new DBAnalyticsException(new FileNotFoundException(f.getAbsolutePath())); //$NON-NLS-1$
					}				
			} catch (Exception e) {
				throw new DBAnalyticsException(e); 
			}						
		}
	
		public String getProperty(String key){
			if(this.confOverrides!=null && this.confOverrides.containsKey(key)){
				return this.confOverrides.getProperty(key);							
			}else{
//				return DBConfig.getConf(key);
				return null;
			}
		}
	}
	
	/**
	 * This function gets the String value of the key.
	 * @param key
	 * @return string value
	 */
	public String getString(String key){	
		return this.getProperty(key);
	}
	
	/**
	 * This function gets the integer value of the key.
	 * @param key
	 * @return integer value
	 */
	public Integer getInteger(String key){
		String s = this.getProperty(key);
		if(s!=null){			
			try {
				return new Integer(s);
			} catch (NumberFormatException e) {
				log.info("" + e);
			}
		}
		return null;
	}
	

	/**
	 * This function returns the configuration value according to the key.
	 * @param key
	 * @return
	 */
	public String getProperty(String key){		
			return this.system_cfg.getProperty(key);
	}
	
	/**
	 * Static method to shutdown daemon thread that runs to reload config every ten
	 * minutes
	 */
	public static void shutdown() {
		if(configDaemon!=null) {
			log.info("Shutting Down DB Configuration daemon.");
			configDaemon.interrupt();
		}
	}
	
	public static void main(String[] args){
		System.out.println(DBConfig.getInstance().getProperty("index.retries"));
	
	}
}
