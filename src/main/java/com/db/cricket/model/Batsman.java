
package com.db.cricket.model;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.google.gson.annotations.SerializedName;

public class Batsman {

	@SerializedName("Batsman")
	private String batsman;

	@SerializedName("Runs")
	private String runs;

	@SerializedName("Balls")
	private String balls;

	@SerializedName("Fours")
	private String fours;

	@SerializedName("Sixes")
	private String sixes;

	@SerializedName("Dots")
	private String dots;

	@SerializedName("Strikerate")
	private String strikerate;

	@SerializedName("Dismissal")
	private String dismissal;

	@SerializedName("Howout")
	private String howout;

	@SerializedName("Bowler")
	private String bowler;

	@SerializedName("Fielder")
	private String fielder;

	@SerializedName("Batsman_Name")
	private String name;

	@SerializedName("Bowler_Name")
	private String bowlerName;

	@SerializedName("Stats")
	private Stats stats;

	@SerializedName("Fielder_Name")
	private String fielderName;

	@SerializedName("Isbatting")
	private Boolean isBatting;

	@SerializedName("Isonstrike")
	private Boolean isOnStrike;

	public String getBatsman() {
		return batsman;
	}

	public void setBatsman(String batsman) {
		this.batsman = batsman;
	}

	public String getRuns() {
		return runs;
	}

	public void setRuns(String runs) {
		this.runs = runs;
	}

	public String getBalls() {
		return balls;
	}

	public void setBalls(String balls) {
		this.balls = balls;
	}

	public String getFours() {
		return fours;
	}

	public void setFours(String fours) {
		this.fours = fours;
	}

	public String getSixes() {
		return sixes;
	}

	public void setSixes(String sixes) {
		this.sixes = sixes;
	}

	public String getDots() {
		return dots;
	}

	public void setDots(String dots) {
		this.dots = dots;
	}

	public String getStrikerate() {
		return strikerate;
	}

	public void setStrikerate(String strikerate) {
		this.strikerate = strikerate;
	}

	public String getDismissal() {
		return dismissal;
	}

	public void setDismissal(String dismissal) {
		this.dismissal = dismissal;
	}

	public String getHowout() {
		return howout;
	}

	public void setHowout(String howout) {
		this.howout = howout;
	}

	public String getBowler() {
		return bowler;
	}

	public void setBowler(String bowler) {
		this.bowler = bowler;
	}

	public String getFielder() {
		return fielder;
	}

	public void setFielder(String fielder) {
		this.fielder = fielder;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getBowlerName() {
		return bowlerName;
	}

	public void setBowlerName(String bowlerName) {
		this.bowlerName = bowlerName;
	}

	public Stats getStats() {
		return stats;
	}

	public void setStats(Stats stats) {
		this.stats = stats;
	}

	public String getFielderName() {
		return fielderName;
	}

	public void setFielderName(String fielderName) {
		this.fielderName = fielderName;
	}

	public Boolean getIsBatting() {
		return isBatting;
	}

	public void setIsBatting(Boolean isBatting) {
		this.isBatting = isBatting;
	}

	public Boolean getIsOnStrike() {
		return isOnStrike;
	}

	public void setIsOnStrike(Boolean isOnStrike) {
		this.isOnStrike = isOnStrike;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}

}
