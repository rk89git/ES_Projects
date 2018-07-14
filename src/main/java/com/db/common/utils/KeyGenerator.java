package com.db.common.utils;

import java.util.UUID;

import org.apache.commons.codec.digest.DigestUtils;

public class KeyGenerator {

	public static String getKey(){
		return DigestUtils.md5Hex(UUID.randomUUID().toString());
	}
	
	public static String getKey(String parameter){
		return DigestUtils.md5Hex(parameter);
	}
}
