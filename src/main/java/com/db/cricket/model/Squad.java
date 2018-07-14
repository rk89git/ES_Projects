package com.db.cricket.model;

import java.util.List;

import com.google.gson.annotations.SerializedName;

public class Squad {
	
	private String teamName;
	
	private String seriesName;
	
	private String teamId;
	
	private String teamShortName;
	
	private String seriesShortName;
	
	private String seriesId;
	
	private List<Player> players;
	
	@SerializedName("support_staff")
	private List<SupportStaff> supportStaff;

	public String getTeamName() {
		return teamName;
	}

	public void setTeamName(String teamName) {
		this.teamName = teamName;
	}

	public String getSeriesName() {
		return seriesName;
	}

	public void setSeriesName(String seriesName) {
		this.seriesName = seriesName;
	}

	public String getTeamId() {
		return teamId;
	}

	public void setTeamId(String teamId) {
		this.teamId = teamId;
	}

	public String getTeamShortName() {
		return teamShortName;
	}

	public void setTeamShortName(String teamShortName) {
		this.teamShortName = teamShortName;
	}

	public String getSeriesShortName() {
		return seriesShortName;
	}

	public void setSeriesShortName(String seriesShortName) {
		this.seriesShortName = seriesShortName;
	}

	public String getSeriesId() {
		return seriesId;
	}

	public void setSeriesId(String seriesId) {
		this.seriesId = seriesId;
	}

	public List<Player> getPlayers() {
		return players;
	}

	public void setPlayers(List<Player> players) {
		this.players = players;
	}

	public List<SupportStaff> getSupportStaff() {
		return supportStaff;
	}

	public void setSupportStaff(List<SupportStaff> supportStaff) {
		this.supportStaff = supportStaff;
	}
}
