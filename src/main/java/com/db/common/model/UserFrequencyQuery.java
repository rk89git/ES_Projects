package com.db.common.model;

public class UserFrequencyQuery {
	private String session_id;
	private int dayCount;
	private String operator;
	private int aggsCount;
	
	public String getSession_id() {
		return session_id;
	}
	public void setSession_id(String session_id) {
		this.session_id = session_id;
	}
	public int getDayCount() {
		return dayCount;
	}
	public void setDayCount(int dayCount) {
		this.dayCount = dayCount;
	}
	public String getOperator() {
		return operator;
	}
	public void setOperator(String operator) {
		this.operator = operator;
	}
	public int getAggsCount() {
		return aggsCount;
	}
	public void setAggsCount(int aggsCount) {
		this.aggsCount = aggsCount;
	}
	
	
}
