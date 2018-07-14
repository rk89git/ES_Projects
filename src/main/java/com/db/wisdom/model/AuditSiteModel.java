package com.db.wisdom.model;

public class AuditSiteModel {
	
	private Long users;	
	private Double frequency;
	private Long sessions;
	private Long pageviews;	
	private Long uniquePageviews;	
	private String interval;
	private Double PD_Session;
	private Double UPD_Session;
	private String avg_session_duration;
	private String avg_page_load_time;
	private String bounce_rate_percentage;
	private String new_sessions;
	private String other;
	private String direct;
	private String display;
	private String organic_search;
	private String paid_search;
	private String referral;
	private String social;
	private String photo_impressions;
	private String video_impressions;
	public Long getUsers() {
		return users;
	}
	public void setUsers(Long users) {
		this.users = users;
	}
	public Double getFrequency() {
		return frequency;
	}
	public void setFrequency(Double frequency) {
		this.frequency = frequency;
	}
	public Long getSessions() {
		return sessions;
	}
	public void setSessions(Long sessions) {
		this.sessions = sessions;
	}
	public Long getPageviews() {
		return pageviews;
	}
	public void setPageviews(Long pageviews) {
		this.pageviews = pageviews;
	}
	public String getInterval() {
		return interval;
	}
	public void setInterval(String interval) {
		this.interval = interval;
	}
	public Double getPD_Session() {
		return PD_Session;
	}
	public void setPD_Session(Double pD_Session) {
		PD_Session = pD_Session;
	}
	public Double getUPD_Session() {
		return UPD_Session;
	}
	public void setUPD_Session(Double uPD_Session) {
		UPD_Session = uPD_Session;
	}
	public String getAvg_session_duration() {
		return avg_session_duration;
	}
	public void setAvg_session_duration(String avg_session_duration) {
		this.avg_session_duration = avg_session_duration;
	}
	public String getAvg_page_load_time() {
		return avg_page_load_time;
	}
	public void setAvg_page_load_time(String avg_page_load_time) {
		this.avg_page_load_time = avg_page_load_time;
	}
	public String getBounce_rate_percentage() {
		return bounce_rate_percentage;
	}
	public void setBounce_rate_percentage(String bounce_rate_percentage) {
		this.bounce_rate_percentage = bounce_rate_percentage;
	}
	public String getNew_sessions() {
		return new_sessions;
	}
	public void setNew_sessions(String new_sessions) {
		this.new_sessions = new_sessions;
	}
	public String getOther() {
		return other;
	}
	public void setOther(String other) {
		this.other = other;
	}
	public String getDirect() {
		return direct;
	}
	public void setDirect(String direct) {
		this.direct = direct;
	}
	public String getDisplay() {
		return display;
	}
	public void setDisplay(String display) {
		this.display = display;
	}
	public String getOrganic_search() {
		return organic_search;
	}
	public void setOrganic_search(String organic_search) {
		this.organic_search = organic_search;
	}
	public String getPaid_search() {
		return paid_search;
	}
	public void setPaid_search(String paid_search) {
		this.paid_search = paid_search;
	}
	public String getReferral() {
		return referral;
	}
	public void setReferral(String referral) {
		this.referral = referral;
	}
	public String getSocial() {
		return social;
	}
	public void setSocial(String social) {
		this.social = social;
	}
	public String getPhoto_impressions() {
		return photo_impressions;
	}
	public void setPhoto_impressions(String photo_impressions) {
		this.photo_impressions = photo_impressions;
	}
	public String getVideo_impressions() {
		return video_impressions;
	}
	public void setVideo_impressions(String video_impressions) {
		this.video_impressions = video_impressions;
	}
	public Long getUniquePageviews() {
		return uniquePageviews;
	}
	public void setUniquePageviews(Long uniquePageviews) {
		this.uniquePageviews = uniquePageviews;
	}
	
}
