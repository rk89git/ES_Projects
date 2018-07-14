package com.db.kafka.producer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;

import com.db.common.utils.DBConfig;

/**
 * The class <code>KafkaByteArrayProducerService</code> gets initialized based
 * on the configuration specified in the project documentation. This class
 * produces records as byte-array in Kafka message broker.
 *
 */
public class KafkaByteArrayProducerService{

	private String topic = "default";

	/**
	 * Variable of type Producer that produces byte array as message in Kafka.
	 */

	private static Logger log = LogManager.getLogger(KafkaByteArrayProducerService.class);

	private static List<Producer<String, byte[]>> runningKafkaProducers = new ArrayList<>();
	private Producer<String, byte[]> producer;

	public KafkaByteArrayProducerService(String topic) {
		this.topic = topic;
		producer = new KafkaProducer<>(getConfigurableProperties());
		runningKafkaProducers.add(producer);
	}

	public KafkaByteArrayProducerService() {
		this.producer = new KafkaProducer<>(getConfigurableProperties());
		runningKafkaProducers.add(producer);
	}

	private Properties getConfigurableProperties() {
		Properties props = new Properties();
		DBConfig config = DBConfig.getInstance();

		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.metadata.broker.list"));
		props.put(ProducerConfig.ACKS_CONFIG, config.getString("kafka.producer.request.required.acks"));
		props.put(ProducerConfig.RETRIES_CONFIG, config.getString("kafka.producer.message.send.max.retries"));
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, config.getString("kafka.producer.batch.size"));
		props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, config.getString("kafka.producer.retry.backoff.ms"));
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.ByteArraySerializer");
		return props;
	}

	/**
	 * Produces byte array data in message broker in specified topic. Topic name
	 * can be passed on the fly to use same producer to produce the data in
	 * multiple topics.
	 * 
	 * @param topic
	 *            name of Kafka topic
	 * @param input
	 *            byte array string
	 */
	public void execute(String topic, byte[] input) {
		ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(topic, input);
		producer.send(record);
	}

	/**
	 * Produces byte array data in message broker.
	 *
	 * @param input
	 *            byte array string
	 */
	public void execute(byte[] input) {
		execute(this.topic, input);
	}

	/**
	 * closes the producer.
	 */
	public void close() {
		this.producer.close();
	}

	public static void main(String[] args) throws IOException {
		KafkaByteArrayProducerService kafkaByteArrayProducerService = new KafkaByteArrayProducerService("hb");
		Map<String, String> map = new HashMap<String, String>();
		map.put("name", "hanish");
		map.put("age", "21");
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(bos);
		oos.writeObject(map);
		oos.flush();
		oos.close();
		bos.close();

		kafkaByteArrayProducerService.execute(bos.toByteArray());

	}

	public static void stop() {
		log.info("Stopping kafka producers as servlet context about to be destroyed.");
		for (Producer<String, byte[]> producer : runningKafkaProducers) {
			producer.close();
		}
	}
}
