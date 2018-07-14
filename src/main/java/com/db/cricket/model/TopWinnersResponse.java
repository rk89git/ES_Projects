package com.db.cricket.model;

public class TopWinnersResponse {
	private String userId;
	private double totalCoinsGained;
	int rank;
	
	
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public double getTotalCoinsGained() {
		return totalCoinsGained;
	}
	public void setTotalCoinsGained(double totalCoinsGained) {
		this.totalCoinsGained = totalCoinsGained;
	}
	public int getRank() {
		return rank;
	}
	public void setRank(int rank) {
		this.rank = rank;
	}
	


}
