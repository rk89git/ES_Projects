package com.db.wisdom.product.model;

import java.util.List;

public class FacebookInsightsPage {

	private Integer totalTotal_reach;

	private Integer totalUnique_reach;

	private Integer totalShares;

	private Integer totalLink_clicks;

	private Double totalCtr;

	private List<FacebookInsights> stories;

	public Integer getTotalTotal_reach() {
		return totalTotal_reach;
	}

	public void setTotalTotal_reach(Integer totalTotal_reach) {
		this.totalTotal_reach = totalTotal_reach;
	}

	public Integer getTotalUnique_reach() {
		return totalUnique_reach;
	}

	public void setTotalUnique_reach(Integer totalUnique_reach) {
		this.totalUnique_reach = totalUnique_reach;
	}

	public Integer getTotalShares() {
		return totalShares;
	}

	public void setTotalShares(Integer totalShares) {
		this.totalShares = totalShares;
	}

	public Integer getTotalLink_clicks() {
		return totalLink_clicks;
	}

	public void setTotalLink_clicks(Integer totalLink_clicks) {
		this.totalLink_clicks = totalLink_clicks;
	}

	public Double getTotalCtr() {
		return totalCtr;
	}

	public void setTotalCtr(Double totalCtr) {
		this.totalCtr = totalCtr;
	}

	public List<FacebookInsights> getStories() {
		return stories;
	}

	public void setStories(List<FacebookInsights> stories) {
		this.stories = stories;
	}

	@Override
	public String toString() {
		return "FacebookInsightsPage [totalTotal_reach=" + totalTotal_reach + ", totalUnique_reach=" + totalUnique_reach
				+ ", totalShares=" + totalShares + ", totalLink_clicks=" + totalLink_clicks + ", totalCtr=" + totalCtr
				+ ", stories=" + stories + "]";
	}

	

}
