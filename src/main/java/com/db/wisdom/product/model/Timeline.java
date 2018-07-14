package com.db.wisdom.product.model;

public class Timeline {

	private String tracker;
	
	private String datetime;
	
	private Long tpvs;
	
	private Long tupvs;

	public String getTracker() {
		return tracker;
	}

	public void setTracker(String tracker) {
		this.tracker = tracker;
	}

	public String getDatetime() {
		return datetime;
	}

	public void setDatetime(String datetime) {
		this.datetime = datetime;
	}

	public Long getTpvs() {
		return tpvs;
	}

	public void setTpvs(Long tpvs) {
		this.tpvs = tpvs;
	}

	public Long getTupvs() {
		return tupvs;
	}

	public void setTupvs(Long tuvs) {
		this.tupvs = tuvs;
	}
	
	
}
