package com.db.common.services;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;

import com.db.common.utils.CricketGlobalVariableUtils;
import com.db.common.utils.DBConfig;
import com.db.cricket.controller.CricketIngestionController;
import com.db.cricket.services.CricketIngestionService;
import com.db.kafka.producer.KafkaByteArrayProducerService;

@WebListener
public class WebappContextListner implements ServletContextListener {

	private static Logger log = LogManager.getLogger(WebappContextListner.class);

	@Override
	public void contextDestroyed(ServletContextEvent arg0) {

		log.info("Webapp context is about to be destroyed.");
		// Shutdown kafka consumers
		KafkaIngestionGenericService.close();

		// Shutdown kafka producers
		KafkaByteArrayProducerService.stop();

		CricketIngestionController.shutdown();
		CricketIngestionService.shutdown();
		
		DBConfig.shutdown();

		// Wait for some time before closing ES connection so that all
		// consumers stop gracefully. This is required to prevent
		// consumers from sending data on a closed ES connection,
		// thereby preventing data loss.
		try {
			Thread.sleep(20000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Shutdown ES client.
		ElasticSearchIndexService.getInstance().close();

	}

	@Override
	public void contextInitialized(ServletContextEvent arg0) {
		// DO nothing
		CricketGlobalVariableUtils.load();
	}

}
