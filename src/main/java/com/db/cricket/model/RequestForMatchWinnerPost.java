package com.db.cricket.model;

public class RequestForMatchWinnerPost {

	private String matchId ;
	private int size =12;
	
	public String getMatchId() {
		return matchId;
	}
	public void setMatchId(String matchId) {
		this.matchId = matchId;
	}
	public int getSize() {
		return size;
	}
	public void setSize(int size) {
		this.size = size;
	}
	
	
}
