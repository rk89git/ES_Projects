package com.db.elastic.model;

public class Response {

	private int took;
	private Boolean timeout;

	public int getTook() {
		return took;
	}

	public void setTook(int took) {
		this.took = took;
	}

	public Boolean getTimeout() {
		return timeout;
	}

	public void setTimeout(Boolean timeout) {
		this.timeout = timeout;
	}

}
