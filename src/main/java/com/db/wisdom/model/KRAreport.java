package com.db.wisdom.model;

import java.util.List;

public class KRAreport {
	
	private Integer author_id;
	
	private String author_name;
	
	private Integer super_cat_id;
	
	private String super_cat_name;

	private Long pvs;
	
	private Long upvs;

	private Long head_count;

	private Long storyCount;	

	private Long mtd;

	private Long projection;
	
	private Long last_day; 
		
	private String  channel_slno;
	
	private Double sharability;
	
	private Long shares;
	
	private Long sessions;

	private Double sessions_growth;
	
	public Integer getAuthor_id() {
		return author_id;
	}

	public void setAuthor_id(Integer author_id) {
		this.author_id = author_id;
	}

	public String getAuthor_name() {
		return author_name;
	}

	public void setAuthor_name(String author_name) {
		this.author_name = author_name;
	}

	public Integer getSuper_cat_id() {
		return super_cat_id;
	}

	public void setSuper_cat_id(Integer super_cat_id) {
		this.super_cat_id = super_cat_id;
	}

	public Long getPvs() {
		return pvs;
	}

	public void setPvs(Long pvs) {
		this.pvs = pvs;
	}

	public Long getUpvs() {
		return upvs;
	}

	public void setUpvs(Long upvs) {
		this.upvs = upvs;
	}

	public Long getHead_count() {
		return head_count;
	}

	public void setHead_count(Long head_count) {
		this.head_count = head_count;
	}

	public Long getStoryCount() {
		return storyCount;
	}

	public void setStoryCount(Long storyCount) {
		this.storyCount = storyCount;
	}

	public Long getMtd() {
		return mtd;
	}

	public void setMtd(Long mtd) {
		this.mtd = mtd;
	}

	public Long getProjection() {
		return projection;
	}

	public void setProjection(Long projection) {
		this.projection = projection;
	}

	public Long getLast_day() {
		return last_day;
	}

	public void setLast_day(Long last_day) {
		this.last_day = last_day;
	}

	public String getChannel_slno() {
		return channel_slno;
	}

	public void setChannel_slno(String channel_slno) {
		this.channel_slno = channel_slno;
	}
	
	public String getSuper_cat_name() {
		return super_cat_name;
	}

	public void setSuper_cat_name(String super_cat_name) {
		this.super_cat_name = super_cat_name;
	}

	public Double getSharability() {
		return sharability;
	}

	public void setSharability(Double sharability) {
		this.sharability = sharability;
	}

	public Long getShares() {
		return shares;
	}

	public void setShares(Long shares) {
		this.shares = shares;
	}

	public Long getSessions() {
		return sessions;
	}

	public void setSessions(Long sessions) {
		this.sessions = sessions;
	}

	public void setSuper_cat_id(List<Integer> super_cat_id2) {
		// TODO Auto-generated method stub
		
	}

	public Double getSessions_growth() {
		return sessions_growth;
	}

	public void setSessions_growth(Double sessions_growth) {
		this.sessions_growth = sessions_growth;
	}	
	
}
