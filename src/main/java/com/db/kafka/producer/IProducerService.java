package com.db.kafka.producer;
/**
 * Producer interface.
 *
 * @param <T>
 */
public interface IProducerService<T> {

	void execute(T input);
	
}
