package com.db.kafka.codecs;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

/**
 * Kafka encoder class for ByteArray.
 *
 */
public class ByteArrayEncoder implements Encoder<byte[]> {

    public ByteArrayEncoder(VerifiableProperties props) {

    }

    public byte[] toBytes(byte[] arg0) {
        return arg0;
    }

}