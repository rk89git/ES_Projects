package com.db.cricket.model;

public class TeamInningEndDetails {

	private String matchId;
	private String teamId;
	private int scoredRunsByTeam ;
	private String teamName ;
	private boolean isTest = false; 
	private int inning;
	
	public boolean getIsTest() {
		return isTest;
	}
	
	public void setIsTest(boolean isTest) {
		this.isTest = isTest;
	}
	
	
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
	public int getScoredRunsByTeam() {
		return scoredRunsByTeam;
	}
	public void setScoredRunsByTeam(int scoredRunsByTeam) {
		this.scoredRunsByTeam = scoredRunsByTeam;
	}
	public String getTeamName() {
		return teamName;
	}
	public void setTeamName(String teamName) {
		this.teamName = teamName;
	}
	
	public int getInning() {
		return inning;
	}

	public void setInning(int inning) {
		this.inning = inning;
	}
		
}
