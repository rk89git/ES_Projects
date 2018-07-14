package com.db.kafka.codecs;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.Map;

import com.db.common.exception.DBAnalyticsException;

/**
 * @y.exclude Utility class that contains logic to decode byte array into plain
 * java collections or String.
 */
@SuppressWarnings("unchecked")
public class KafkaMessageDecoder {

    /**
     * This method convert kafka message (JSON String/XML String) into Map.
     *
     * @param message the message
     * @return the map
     */
    public Map<String, Object> decode(byte[] message) {

        try {
            // Parse byte array to Map
            ByteArrayInputStream bis = new ByteArrayInputStream(message);
            ObjectInputStream ois;
            ois = new ObjectInputStream(bis);
            Map<String, Object> inputMap = (Map<String, Object>) ois.readObject();
            ois.close();
            bis.close();
            return inputMap;
        } catch (Exception e) {
            throw new DBAnalyticsException( "Unable to map kafka message to json/xml map.", e);
        }
    }
}