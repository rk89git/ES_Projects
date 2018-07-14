package com.db.common.model;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by DB on 08-06-2017.
 */
public class HTTPModel {
	Map<String, String> headers = new HashMap<>();
	String url = new String();
	Object body = new Object();
	Map<String,String> parameters=new HashMap<>();

	public Map<String, String> getHeaders() {
		return headers;
	}

	public void setHeaders(Map<String, String> headers) {
		this.headers = headers;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public Object getBody() {
		return body;
	}

	public void setBody(Object body) {
		this.body = body;
	}

	public Map<String, String> getParameters() {
		return parameters;
	}

	public void setParameters(Map<String, String> parameters) {
		this.parameters = parameters;
	}

	@Override
	public String toString() {
		return "HTTPModel{" +
				"headers=" + headers +
				", url='" + url + '\'' +
				", body=" + body +
				", parameters=" + parameters +
				'}';
	}
}
