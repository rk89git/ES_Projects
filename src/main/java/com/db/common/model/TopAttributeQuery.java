package com.db.common.model;

public class TopAttributeQuery {
	
	private String session_id;
	private String startDate;
	private String endDate;
	private String attribute;
	public String getStartDate() {
		return startDate;
	}
	public void setStartDate(String stateDate) {
		this.startDate = stateDate;
	}
	public String getEndDate() {
		return endDate;
	}
	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}
	public String getSession_id() {
		return session_id;
	}
	public void setSession_id(String session_id) {
		this.session_id = session_id;
	}
	public String getAttribute() {
		return attribute;
	}
	public void setAttribute(String attribute) {
		this.attribute = attribute;
	}
	
	

}
