package com.db.wisdom.model;

public class VideoReport {
	private Long total_users;
	private Long total_sessions;	
	private Long daily_active_users;
	private Long monthly_active_users;
	
	public Long getTotal_users() {
		return total_users;
	}
	public void setTotal_users(Long total_users) {
		this.total_users = total_users;
	}
	public Long getTotal_sessions() {
		return total_sessions;
	}
	public void setTotal_sessions(Long total_sessions) {
		this.total_sessions = total_sessions;
	}
	public Long getDaily_active_users() {
		return daily_active_users;
	}
	public void setDaily_active_users(Long daily_active_users) {
		this.daily_active_users = daily_active_users;
	}
	public Long getMonthly_active_users() {
		return monthly_active_users;
	}
	public void setMonthly_active_users(Long monthly_active_users) {
		this.monthly_active_users = monthly_active_users;
	}	
	
}
