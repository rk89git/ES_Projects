package com.db.kafka.codecs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;

import com.db.common.exception.DBAnalyticsException;

public class KafkaUtils {
	
	  public byte[] toBytes(Map<String, Object> inputMap) {
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
