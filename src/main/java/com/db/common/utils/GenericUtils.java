package com.db.common.utils;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import com.db.common.constants.Constants;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.http.HttpStatus;

import com.db.common.model.ResponseMessage;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class GenericUtils {

	private Gson gson = new GsonBuilder().setPrettyPrinting().create();
	
	private static Logger log = LogManager.getLogger(GenericUtils.class);
	
	public static Map<Integer, Integer> getImageResizeDimensions(String dimension,
			String dimension2) {
		Map<Integer, Integer> result = new HashMap<Integer, Integer>();
		
		if (StringUtils.isBlank(dimension)) {
			return result;
		}
		
		if (dimension == "0x0") {
			return new HashMap<Integer, Integer>();
		}
		String imagesizeArr1[] = dimension.split("x");
		String imagesizeArr2[] = dimension2.split("x");
		int width = Integer.valueOf(imagesizeArr1[0]);
		int height = Integer.valueOf(imagesizeArr1[1]);

		int constraintWidth = Integer.valueOf(imagesizeArr2[0]);
		int constraintHeight = Integer.valueOf(imagesizeArr2[1]);

		if (dimension2 == "321x278" || dimension2 == "251x216"
				|| dimension2 == "655x588") {
			constraintHeight = height;

		} else if (dimension2 == "199x173" && width > 199 && height > 173) {
			constraintHeight = height;
		} else if (dimension2 == "655x588" && width > 655 && height > 588) {
			constraintHeight = height;
		} else if (dimension2 == "636x303") {
			return new HashMap<Integer, Integer>();
		}
		constraintWidth = (constraintWidth == 0) ? width : constraintWidth;
		constraintHeight = (constraintHeight == 0) ? height : constraintHeight;

		if ((width > constraintWidth) || (height > constraintHeight)) {
			while ((constraintWidth < width) || (constraintHeight < height)) {
				if (constraintWidth < width) {
					height = Math.round(((constraintWidth * height) / width));
					width = constraintWidth;
				}

				if (constraintHeight < height) {
					width = Math.round(((constraintHeight * width) / height));
					height = constraintHeight;
				}
			}
		}
		// Some super short || skinny images will return 0 for these. Make sure
		// that doesn't happen!
		if (width < 1) {
			width = 1;
		}
		if (height < 1) {
			height = 1;
		}

		if (width > 0 && height > 0) {
			result.put(width, height);
			return result;
		}
		else {
			return new HashMap<Integer, Integer>();
		}
	}
	
	public static ResponseMessage getResponseMessage(String message, HttpStatus status){
		ResponseMessage responseMessage = new ResponseMessage();
		responseMessage.setStatus(status.value());
		responseMessage.setMessage(message);
		return responseMessage;
	}
	
	public static void main(String[] args) {
		System.out.println(getPaddedVersionString("1.22.3",3)
	);


	}


	static public String getPaddedVersionString(String versionString, int padding) {
		if (versionString == null)
			return null;

		String[] strings = versionString.split("\\.");
		StringBuilder output = new StringBuilder();

		for (String string : strings) {
			output.append(StringUtils.leftPad(string, padding, '0'));
		}

		return output.toString();


	}


	public static void addPaddedFields(Map<String, Object> record) {
		if (record.get(Constants.APP_VERSION) != null) {
			record.put(Constants.APP_VERSION_PADDED, getPaddedVersionString(record.get(Constants.APP_VERSION).toString(), 3));
		}

		if (record.get(Constants.DEVICE_OS_VERSION) != null) {
			record.put(Constants.DEVICE_OS_VERSION_PADDED, getPaddedVersionString(record.get(Constants.DEVICE_OS_VERSION).toString(), 3));
		}

	}
	
	public static JSONObject jsonFileReader(String path){
		JSONObject jsonObject = new JSONObject();

		synchronized(jsonObject){
			if(Files.exists(Paths.get(path))){
				try {
					String fileStr = new String(Files.readAllBytes(Paths.get(path)));
					jsonObject = (JSONObject) new JSONParser().parse(fileStr);

				}catch (Exception e) {
					log.error("Error while reading File at path: "+path);
				}
			}else{
				log.info("No File found at path: "+path);
			}
			return jsonObject;
		}
	}
}
