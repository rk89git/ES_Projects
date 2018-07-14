package com.db.wisdom.product.model;

public class StoryPerformance {

	private String date;
	
	private Long tpvs;
	
	private Long mpvs;
	
	private Long wpvs;
	
    private Long tupvs;
	
	private Long mupvs;
	
	private Long wupvs;
	
	private Long total_reach;
	
	private Long link_clicks;
	
	private Double ctr;
	
	private String tracker;

	private String modified_date;
	
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

	public Long getTupvs() {
		return tupvs;
	}

	public void setTupvs(Long tuvs) {
		this.tupvs = tuvs;
	}

	public Long getMupvs() {
		return mupvs;
	}

	public void setMupvs(Long muvs) {
		this.mupvs = muvs;
	}

	public Long getWupvs() {
		return wupvs;
	}

	public void setWupvs(Long wuvs) {
		this.wupvs = wuvs;
	}

	public String getModified_date() {
		return modified_date;
	}

	public void setModified_date(String modified_date) {
		this.modified_date = modified_date;
	}	
	
}
