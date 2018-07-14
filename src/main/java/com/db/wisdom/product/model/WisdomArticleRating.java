package com.db.wisdom.product.model;

import java.util.Map;

public class WisdomArticleRating {
	
	private String storyid;
	private Map<String,Object>breakup;
	private Double average_rating;
	private Long total_users;
	
	
	public String getStoryid() {
		return storyid;
	}
	public void setStoryid(String storyid) {
		this.storyid = storyid;
	}
	public Map<String, Object> getBreakup() {
		return breakup;
	}
	public void setBreakup(Map<String, Object> breakup) {
		this.breakup = breakup;
	}
	public Double getAverage_rating() {
		return average_rating;
	}
	public void setAverage_rating(Double average_rating) {
		this.average_rating = average_rating;
	}
	public Long getTotal_users() {
		return total_users;
	}
	public void setTotal_users(Long total_users) {
		this.total_users = total_users;
	}
	
	
}
