package com.db.cricket.model;

public class MatchWonTeamDetails {

	private String matchId;
	private String teamId;
	private String winMargin ;
	private Boolean isDraw ;
	private String teamName ;
	
	public String getMatchId() {
		return matchId;
	}
	public void setMatchId(String matchId) {
		this.matchId = matchId;
	}
	public String getTeamId() {
		return teamId;
	}
	public void setTeamId(String teamId) {
		this.teamId = teamId;
	}
	
	public String getTeamName() {
		return teamName;
	}
	public void setTeamName(String teamName) {
		this.teamName = teamName;
	}
	
	public String getWinMargin() {
		return winMargin;
	}
	public void setWinMargin(String winMargin) {
		this.winMargin = winMargin;
	}	

	public Boolean getIsDraw() {
		return isDraw;
	}
	public void setIsDraw(Boolean isDraw) {
		this.isDraw = isDraw;
	}
	
		
}
