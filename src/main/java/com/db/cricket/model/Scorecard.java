
package com.db.cricket.model;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.google.gson.annotations.SerializedName;

public class Scorecard {

	@SerializedName("Matchdetail")
	private Matchdetail matchdetail;

	@SerializedName("Innings")
	private List<Inning> innings = null;

	@SerializedName("Team_Away_Name")
	private String teamAwayName;

	@SerializedName("Team_Home")
	private String teamHome;

	@SerializedName("Team_Home_Name")
	private String teamHomeName;

	@SerializedName("Team_Home_Short_Name")
	private String teamHomeShortName;

	@SerializedName("Team_Away")
	private String teamAway;

	@SerializedName("datetime")
	private String datetime;

	@SerializedName("Team_Away_Short_Name")
	private String teamAwayShortName;

	@SerializedName("current_batting_team")
	private String currentBattingTeam;

	@SerializedName("current_batting_team_name")
	private String currentBattingTeamName;

	@SerializedName("Result")
	private String result = "";

	@SerializedName("Nuggets")
	private List<String> nuggets = null;

	@SerializedName("Winningteam")
	private String winningTeam;

	@SerializedName("Winningteam_Name")
	private String winningTeamName;

	@SerializedName("Winmargin")
	private String winMargin;
	
	private Object Teams;

	/**
	 * @return the teams
	 */
	public Object getTeams() {
		return Teams;
	}

	/**
	 * @param teams the teams to set
	 */
	public void setTeams(Object teams) {
		Teams = teams;
	}

	public String getWinningTeam() {
		return winningTeam;
	}

	public void setWinningTeam(String winningTeam) {
		this.winningTeam = winningTeam;
	}

	public String getWinningTeamName() {
		return winningTeamName;
	}

	public void setWinningTeamName(String winningTeamName) {
		this.winningTeamName = winningTeamName;
	}

	public String getWinMargin() {
		return winMargin;
	}

	public void setWinMargin(String winMargin) {
		this.winMargin = winMargin;
	}

	public List<String> getNuggets() {
		return nuggets;
	}

	public void setNuggets(List<String> nuggets) {
		this.nuggets = nuggets;
	}

	public String getResult() {
		return result;
	}

	public void setResult(String result) {
		this.result = result;
	}

	@SerializedName("Player_Match")
	private String playerMatch;

	public String getPlayerMatch() {
		return playerMatch;
	}

	public void setPlayerMatch(String playerMatch) {
		this.playerMatch = playerMatch;
	}

	// @SerializedName("Teams")
	// private String teams;

	public Matchdetail getMatchdetail() {
		return matchdetail;
	}

	public void setMatchdetail(Matchdetail matchdetail) {
		this.matchdetail = matchdetail;
	}

	public List<Inning> getInnings() {
		return innings;
	}

	public void setInnings(List<Inning> innings) {
		this.innings = innings;
	}

	public String getTeamAwayName() {
		return teamAwayName;
	}

	public void setTeamAwayName(String teamAwayName) {
		this.teamAwayName = teamAwayName;
	}

	public String getTeamHome() {
		return teamHome;
	}

	public void setTeamHome(String teamHome) {
		this.teamHome = teamHome;
	}

	public String getTeamHomeName() {
		return teamHomeName;
	}

	public void setTeamHomeName(String teamHomeName) {
		this.teamHomeName = teamHomeName;
	}

	public String getTeamHomeShortName() {
		return teamHomeShortName;
	}

	public void setTeamHomeShortName(String teamHomeShortName) {
		this.teamHomeShortName = teamHomeShortName;
	}

	public String getTeamAway() {
		return teamAway;
	}

	public void setTeamAway(String teamAway) {
		this.teamAway = teamAway;
	}

	public String getDatetime() {
		return datetime;
	}

	public void setDatetime(String datetime) {
		this.datetime = datetime;
	}

	public String getTeamAwayShortName() {
		return teamAwayShortName;
	}

	public void setTeamAwayShortName(String teamAwayShortName) {
		this.teamAwayShortName = teamAwayShortName;
	}

	public String getCurrentBattingTeam() {
		return currentBattingTeam;
	}

	public void setCurrentBattingTeam(String currentBattingTeam) {
		this.currentBattingTeam = currentBattingTeam;
	}

	public String getCurrentBattingTeamName() {
		return currentBattingTeamName;
	}

	public void setCurrentBattingTeamName(String currentBattingTeamName) {
		this.currentBattingTeamName = currentBattingTeamName;
	}

	// public String getTeams() {
	// return teams;
	// }
	//
	// public void setTeams(String teams) {
	// this.teams = teams;
	// }

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}

}
