package com.db.kafka.codecs;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * Round robin partitioner using a simple thread safe AotmicInteger
 */
public class RoundRobinPartitioner implements Partitioner {
    private static final Logger log = LogManager.getLogger(RoundRobinPartitioner.class);

    final AtomicInteger counter = new AtomicInteger(0);

    public RoundRobinPartitioner(VerifiableProperties props) {
        log.trace("Instatiated the Round Robin Partitioner class");
    }

    public int partition(Object key, int partitions) {
        int i = counter.getAndIncrement();
        if (i == Integer.MAX_VALUE) {
            counter.set(0);
            return 0;
        } else {
            return i % partitions;
        }
    }
}