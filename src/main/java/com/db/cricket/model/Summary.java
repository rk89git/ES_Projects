package com.db.cricket.model;

import java.util.List;

import com.google.gson.annotations.SerializedName;

public class Summary {

	@SerializedName("Score")
	private String score;

	@SerializedName("Over")
	private String over;

	@SerializedName("Runs")
	private String runs;

	@SerializedName("Wickets")
	private String wickets;

	@SerializedName("Batsmen")
	private List<Batsman> batsmen;

	@SerializedName("Bowlers")
	private List<Bowler> bowlers;

	public String getScore() {
		return score;
	}

	public void setScore(String score) {
		this.score = score;
	}

	public String getOver() {
		return over;
	}

	public void setOver(String over) {
		this.over = over;
	}

	public String getRuns() {
		return runs;
	}

	public void setRuns(String runs) {
		this.runs = runs;
	}

	public String getWickets() {
		return wickets;
	}

	public void setWickets(String wickets) {
		this.wickets = wickets;
	}

	public List<Batsman> getBatsmen() {
		return batsmen;
	}

	public void setBatsmen(List<Batsman> batsmen) {
		this.batsmen = batsmen;
	}

	public List<Bowler> getBowlers() {
		return bowlers;
	}

	public void setBowlers(List<Bowler> bowlers) {
		this.bowlers = bowlers;
	}
}
