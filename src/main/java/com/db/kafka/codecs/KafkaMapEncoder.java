package com.db.kafka.codecs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;

import com.db.common.exception.DBAnalyticsException;

import kafka.utils.VerifiableProperties;

/**
 * A Kafka message encoder that implements <code>Encoder</code> interface from
 * Kafka API and contains the logic to encode input Map into kafka byte array
 * message.
 */
public class KafkaMapEncoder implements kafka.serializer.Encoder<Map<String, String>> {

    public KafkaMapEncoder(VerifiableProperties verifiableProperties) {

    }

    /**
     * Encodes input map into kafka byte array message.
     */
    public byte[] toBytes(Map<String, String> inputMap) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(inputMap);
            oos.flush();
            oos.close();
            bos.close();
            return bos.toByteArray();
        } catch (IOException ex) {
            throw new DBAnalyticsException(ex);
        }
    }
}
