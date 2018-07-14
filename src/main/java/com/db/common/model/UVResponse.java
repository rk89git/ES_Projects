package com.db.common.model;

public class UVResponse {

	private String datetime;
	
	private long uvs;

	public String getDatetime() {
		return datetime;
	}

	public void setDatetime(String datetime) {
		this.datetime = datetime;
	}

	public long getUvs() {
		return uvs;
	}

	public void setUvs(long uvs) {
		this.uvs = uvs;
	}

	@Override
	public String toString() {
		return "PVResponse [datetime=" + datetime + ", uvs=" + uvs + "]";
	}
	
	
	
}
