package com.db.common.model;

public class PVResponse {

	private String datetime;
	
	private long pvs;

	public String getDatetime() {
		return datetime;
	}

	public void setDatetime(String datetime) {
		this.datetime = datetime;
	}

	public long getPvs() {
		return pvs;
	}

	public void setPvs(long pvs) {
		this.pvs = pvs;
	}

	@Override
	public String toString() {
		return "PVResponse [datetime=" + datetime + ", pvs=" + pvs + "]";
	}
	
	
	
}
