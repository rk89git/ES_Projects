package com.db.cricket.model;


public class TossWonDetails {

	private String matchId;
	private String teamId;
	private String electedToByTossWonTeam ;
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
	
	public String getElectedToByTossWonTeam() {
		return electedToByTossWonTeam;
	}
	public void setElectedToByTossWonTeam(String electedToByTossWonTeam) {
		this.electedToByTossWonTeam = electedToByTossWonTeam;
	}
	@Override
	public String toString() {
		return "TossWonDetails [matchId=" + matchId + ", teamId=" + teamId + ", electedToByTossWonTeam="
				+ electedToByTossWonTeam + ", teamName=" + teamName + "]";
	}
	
	
	
		
}
