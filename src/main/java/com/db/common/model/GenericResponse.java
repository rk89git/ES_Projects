package com.db.common.model;

public class GenericResponse {

	private String date;
	
	private long count;

	public String getDate() {
		return date;
	}

	public void setDate(String datetime) {
		this.date = datetime;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	@Override
	public String toString() {
		return "GenericResponse [date=" + date + ", count=" + count + "]";
	}

	
	
	
	
}
