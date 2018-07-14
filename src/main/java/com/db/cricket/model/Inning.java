
package com.db.cricket.model;

import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.google.gson.annotations.SerializedName;

public class Inning {

	@SerializedName("Number")
	private String number;

	@SerializedName("Battingteam")
	private String battingteam;

	@SerializedName("Total")
	private Integer total;

	@SerializedName("Wickets")
	private Integer wickets;

	@SerializedName("Overs")
	private String overs;

	@SerializedName("Runrate")
	private String runrate;

	@SerializedName("Byes")
	private Integer byes;

	@SerializedName("Legbyes")
	private Integer legbyes;

	@SerializedName("Wides")
	private Integer wides;

	@SerializedName("Noballs")
	private Integer noballs;

	@SerializedName("Penalty")
	private Integer penalty;

	@SerializedName("Batsmen")
	private List<Batsman> batsmen = null;

	@SerializedName("Partnership_Current")
	private PartnershipCurrent partnershipCurrent;

	@SerializedName("Bowlers")
	private List<Bowler> bowlers = null;

	@SerializedName("FallofWickets")
	private List<FallofWicket> fallofWickets = null;

	@SerializedName("Team_Name")
	private String teamName;

	@SerializedName("Team_Short_Name")
	private String teamShortName;

	public String getNumber() {
		return number;
	}

	public void setNumber(String number) {
		this.number = number;
	}

	public String getBattingteam() {
		return battingteam;
	}

	public void setBattingteam(String battingteam) {
		this.battingteam = battingteam;
	}

	public Integer getTotal() {
		return total;
	}

	public void setTotal(Integer total) {
		this.total = total;
	}

	public Integer getWickets() {
		return wickets;
	}

	public void setWickets(Integer wickets) {
		this.wickets = wickets;
	}

	public String getOvers() {
		return overs;
	}

	public void setOvers(String overs) {
		this.overs = overs;
	}

	public String getRunrate() {
		return runrate;
	}

	public void setRunrate(String runrate) {
		this.runrate = runrate;
	}

	public Integer getByes() {
		return byes;
	}

	public void setByes(Integer byes) {
		this.byes = byes;
	}

	public Integer getLegbyes() {
		return legbyes;
	}

	public void setLegbyes(Integer legbyes) {
		this.legbyes = legbyes;
	}

	public Integer getWides() {
		return wides;
	}

	public void setWides(Integer wides) {
		this.wides = wides;
	}

	public Integer getNoballs() {
		return noballs;
	}

	public void setNoballs(Integer noballs) {
		this.noballs = noballs;
	}

	public Integer getPenalty() {
		return penalty;
	}

	public void setPenalty(Integer penalty) {
		this.penalty = penalty;
	}

	public List<Batsman> getBatsmen() {
		return batsmen;
	}

	public void setBatsmen(List<Batsman> batsmen) {
		this.batsmen = batsmen;
	}

	public PartnershipCurrent getPartnershipCurrent() {
		return partnershipCurrent;
	}

	public void setPartnershipCurrent(PartnershipCurrent partnershipCurrent) {
		this.partnershipCurrent = partnershipCurrent;
	}

	public List<Bowler> getBowlers() {
		return bowlers;
	}

	public void setBowlers(List<Bowler> bowlers) {
		this.bowlers = bowlers;
	}

	public List<FallofWicket> getFallofWickets() {
		return fallofWickets;
	}

	public void setFallofWickets(List<FallofWicket> fallofWickets) {
		this.fallofWickets = fallofWickets;
	}

	public String getTeamName() {
		return teamName;
	}

	public void setTeamName(String teamName) {
		this.teamName = teamName;
	}

	public String getTeamShortName() {
		return teamShortName;
	}

	public void setTeamShortName(String teamShortName) {
		this.teamShortName = teamShortName;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}

}
