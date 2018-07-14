package com.db.cricket.model;


public class FellWicketsDetails {

	private String matchId;
	private String playerId;
	private int scoredRunsByPlayer ;
	private String playerName ;
	private boolean isTest = false; 
	private int inning;
	
	public boolean getIsTest() {
		return isTest;
	}
	
	public void setIsTest(boolean isTest) {
		this.isTest = isTest;
	}
	
	public String getPlayerName() {
		return playerName;
	}
	public void setPlayerName(String playerName) {
		this.playerName = playerName;
	}
	public String getMatchId() {
		return matchId;
	}
	public void setMatchId(String matchId) {
		this.matchId = matchId;
	}
	public String getPlayerId() {
		return playerId;
	}
	public void setPlayerId(String playerId) {
		this.playerId = playerId;
	}
	public int getScoredRunsByPlayer() {
		return scoredRunsByPlayer;
	}
	public void setScoredRunsByPlayer(int scoredRunsByPlayer) {
		this.scoredRunsByPlayer = scoredRunsByPlayer;
	}
		
	public String toString(){//overriding the toString() method  
		  return matchId +" "+playerId+" "+scoredRunsByPlayer+" "+playerName;  
		 }

	public int getInning() {
		return inning;
	}

	public void setInning(int inning) {
		this.inning = inning;
	}
}
