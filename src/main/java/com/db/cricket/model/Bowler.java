
package com.db.cricket.model;

import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.google.gson.annotations.SerializedName;

public class Bowler {

	@SerializedName("Bowler")
	private String bowler;

	@SerializedName("Overs")
	private String overs;

	@SerializedName("Maidens")
	private String maidens;

	@SerializedName("Runs")
	private String runs;

	@SerializedName("Wickets")
	private String wickets;

	@SerializedName("Economyrate")
	private Double economyrate;

	@SerializedName("Noballs")
	private String noBalls;

	@SerializedName("Wides")
	private String wides;

	@SerializedName("Dots")
	private String dots;

	@SerializedName("Bowler_Name")
	private String name;

	@SerializedName("Stats")
	private Stats stats;

	@SerializedName("Isbowlingtandem")
	private Boolean isBowlingTandem;

	@SerializedName("Isbowlingnow")
	private Boolean isBowlingNow;

	@SerializedName("ThisOver")
	private List<ThisOver> thisOver;

	public String getBowler() {
		return bowler;
	}

	public void setBowler(String bowler) {
		this.bowler = bowler;
	}

	public String getOvers() {
		return overs;
	}

	public void setOvers(String overs) {
		this.overs = overs;
	}

	public String getMaidens() {
		return maidens;
	}

	public void setMaidens(String maidens) {
		this.maidens = maidens;
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

	public Double getEconomyrate() {
		return economyrate;
	}

	public void setEconomyrate(Double economyrate) {
		this.economyrate = economyrate;
	}

	public String getNoBalls() {
		return noBalls;
	}

	public void setNoBalls(String noballs) {
		this.noBalls = noballs;
	}

	public String getWides() {
		return wides;
	}

	public void setWides(String wides) {
		this.wides = wides;
	}

	public String getDots() {
		return dots;
	}

	public void setDots(String dots) {
		this.dots = dots;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Stats getStats() {
		return stats;
	}

	public void setStats(Stats stats) {
		this.stats = stats;
	}

	public Boolean getIsBowlingTandem() {
		return isBowlingTandem;
	}

	public void setIsBowlingTandem(Boolean isbowlingtandem) {
		this.isBowlingTandem = isbowlingtandem;
	}

	public Boolean getIsBowlingNow() {
		return isBowlingNow;
	}

	public void setIsBowlingNow(Boolean isBowlingNow) {
		this.isBowlingNow = isBowlingNow;
	}

	public List<ThisOver> getThisOver() {
		return thisOver;
	}

	public void setThisOver(List<ThisOver> thisOver) {
		this.thisOver = thisOver;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}

}
