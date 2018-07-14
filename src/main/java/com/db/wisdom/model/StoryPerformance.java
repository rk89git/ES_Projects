package com.db.wisdom.model;

public class StoryPerformance {

	private String date;
	
	private Long tpvs;
	
	private Long mpvs;
	
	private Long wpvs;
	
    private Long tuvs;
	
	private Long muvs;
	
	private Long wuvs;
	
	private Long total_reach;
	
	private Long link_clicks;
	
	private Double ctr;
	
	private Integer version;
	
	private String tracker;

	public Long getTotal_reach() {
		return total_reach;
	}

	public void setTotal_reach(Long total_reach) {
		this.total_reach = total_reach;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public Long getTpvs() {
		return tpvs;
	}

	public void setTpvs(Long tpvs) {
		this.tpvs = tpvs;
	}

	public Long getLink_clicks() {
		return link_clicks;
	}

	public void setLink_clicks(Long link_clicks) {
		this.link_clicks = link_clicks;
	}

	public double getCtr() {
		return ctr;
	}

	public void setCtr(double ctr) {
		this.ctr = ctr;
	}

	public Integer getVersion() {
		return version;
	}

	public void setVersion(Integer version) {
		this.version = version;
	}

	public Long getMpvs() {
		return mpvs;
	}

	public void setMpvs(Long mpvs) {
		this.mpvs = mpvs;
	}

	public Long getWpvs() {
		return wpvs;
	}

	public void setWpvs(Long wpvs) {
		this.wpvs = wpvs;
	}

	public void setCtr(Double ctr) {
		this.ctr = ctr;
	}

	public String getTracker() {
		return tracker;
	}

	public void setTracker(String tracker) {
		this.tracker = tracker;
	}

	public Long getTuvs() {
		return tuvs;
	}

	public void setTuvs(Long tuvs) {
		this.tuvs = tuvs;
	}

	public Long getMuvs() {
		return muvs;
	}

	public void setMuvs(Long muvs) {
		this.muvs = muvs;
	}

	public Long getWuvs() {
		return wuvs;
	}

	public void setWuvs(Long wuvs) {
		this.wuvs = wuvs;
	}	
	
}
