package com.db.kafka.codecs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;

import com.db.common.exception.DBAnalyticsException;

import kafka.utils.VerifiableProperties;

/**
 * A Kafka message encoder that implements <code>Encoder</code> interface from
 * Kafka API and contains the logic to encode input List into kafka byte array
 * message.
 */
public class KafkaListEncoder implements
        kafka.serializer.Encoder<List<Map<String, Object>>> {

    public KafkaListEncoder(VerifiableProperties verifiableProperties) {

    }

    /**
     * Encodes a java list into kafka byte array message
     */
    public byte[] toBytes(List<Map<String, Object>> inputList) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(inputList);
            oos.flush();
            oos.close();
            bos.close();
            return bos.toByteArray();
        } catch (IOException ex) {
            throw new DBAnalyticsException(ex);
        }
    }

}