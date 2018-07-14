package com.db.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;

import com.db.common.utils.DBConfig;

/**
 * This class implements the IProducerService and produce data in Message Broker
 * in Async mode.
 * 
 * {@link Ref} https://github.com/CameronGregory/kafka/blob/master/TestProducer.java
 * 
 * 
 */
public class KafkaProducerService {

//	public Producer<String, Map<String, String>> producer;
	private String topic;
	private Properties properties;
	public static final String TOPIC = "topic";
	public static final String DUMMY = "dummy";
	
	
	private KafkaProducer<String, String> kafkaProducer;
	
	private static Logger log = LogManager.getLogger(KafkaProducerService.class);

//	public Producer<String, Map<String, String>> getProducer() {
//		return producer;
//	}
//
//	public void setProducer(Producer<String, Map<String, String>> producer) {
//		this.producer = producer;
//	}

	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	public KafkaProducerService(String topic) {
		log.info("Instantiate the producer service : ");
		// get topic from propertyMap
		this.topic = topic;
		this.setProperties(getConfiguartionProperties());
//		this.setProducer(new Producer<String, Map<String, String>>(
//				new ProducerConfig(this.getProperties())));
		kafkaProducer=new KafkaProducer<String, String>(this.getProperties());

	}

	/**
	 * get the producer configurations
	 * 
	 * @return
	 */
	private Properties getConfiguartionProperties() {
		// get configurations
		Properties props = new Properties();
	
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,DBConfig.getInstance().getProperty("kafka.metadata.broker.list"));
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		
//		props.put("metadata.broker.list",
//				BDGConfig.getInstance().getProperty("kafka.metadata.broker.list"));
//		props.put("serializer.class",
//				BDGConfig.getInstance().getProperty("kafka.serializer.class"));
//
//		props.put("producer.type", BDGConfig.getInstance().getProperty("kafka/producer/type"));
		// props.put("queue.time", config.getString("kafka/queue/time"));
		// props.put("queue.size", config.getString("kafka/queue/size"));
		// props.put("batch.size", config.getString("kafka/batch/size"));
		// props.put("zk.sessiontimeout.ms",
		// config.getString("kafka/zk/sessiontimeout/ms"));
//		props.put("request.required.acks", "1");
		return props;
	}

	/**
	 * Produce data in message broker in Async mode.
	 */
	// @Override
	// public void execute(String input) {
	// Map<String, String> typecontent = new HashMap<String, String>();
	// typecontent.put(Constants.CONTENT_TYPE, content_type);
	// typecontent.put(Constants.ACTUAL_CONTENT, input);
	//
	// ProducerData<String, Map<String, String>> data = new ProducerData<String,
	// Map<String, String>>(
	// topic, typecontent);
	// producer.send(data);
	// }
	//
	// public void execute(String topic, byte[] input) {
	// this.topic = topic;
	// this.execute(input);
	// }

	// public void execute(byte[] input) {
	// KeyedMessage<String, byte[]> data = new KeyedMessage<String, byte[]>(
	// topic, "dummy", input);
	// this.producer.send(data);
	// }

	public void execute(String input) {
//		KeyedMessage<String, Map<String, String>> data = new KeyedMessage<String, Map<String, String>>(
//				topic, "dummy", input);
//		this.producer.send(data);
		ProducerRecord<String,String> producerRecord = new ProducerRecord<String,String>(topic, input);
		this.kafkaProducer.send(producerRecord);
	}
	
	public static void main(String[] args) {
		KafkaProducerService kafkaProducerService=new KafkaProducerService("test17");
//		Map<String,String> map=new HashMap<String, String>();
//		map.put("name", "hanish");
//		map.put("age", "21");
		
		kafkaProducerService.execute("Test 123...............");
		
	}

}
