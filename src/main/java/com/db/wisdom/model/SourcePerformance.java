package com.db.wisdom.model;

import java.util.List;

public class SourcePerformance {

	private Long total_reach;
	
	private Long link_clicks;
	
	private Long unique_reach;
	
	private Long shares;
	
	private Double ctr;
	
	private Long pvs;	
	
	private Long mpvs;
	
	private Long wpvs;
	
    private Long uvs;	
	
	private Long muvs;
	
	private Long wuvs;
	
	private List<StoryPerformance> performance;

	public Long getTotal_reach() {
		return total_reach;
	}

	public void setTotal_reach(Long total_reach) {
		this.total_reach = total_reach;
	}

	public Long getLink_clicks() {
		return link_clicks;
	}

	public void setLink_clicks(Long link_clicks) {
		this.link_clicks = link_clicks;
	}

	public Long getShares() {
		return shares;
	}

	public void setShares(Long shares) {
		this.shares = shares;
	}

	public Double getCtr() {
		return ctr;
	}

	public void setCtr(Double ctr) {
		this.ctr = ctr;
	}

	public Long getPvs() {
		return pvs;
	}

	public void setPvs(Long pvs) {
		this.pvs = pvs;
	}

	public List<StoryPerformance> getPerformance() {
		return performance;
	}

	public void setPerformance(List<StoryPerformance> performance) {
		this.performance = performance;
	}

	public Long getUnique_reach() {
		return unique_reach;
	}

	public void setUnique_reach(Long unique_reach) {
		this.unique_reach = unique_reach;
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

	public Long getUvs() {
		return uvs;
	}

	public void setUvs(Long uvs) {
		this.uvs = uvs;
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
