package com.db.cricket.model;

import com.google.gson.annotations.SerializedName;

public class CommentaryItem {

	@SerializedName("Summary")
	private Summary summary;

	@SerializedName("Over")
	private String over;

	@SerializedName("Id")
	private String id;

	@SerializedName("Iswicket")
	private Boolean isWicket = false;

	@SerializedName("Dismissed")
	private String dismissed;

	@SerializedName("Runs")
	private String runs;

	@SerializedName("Detail")
	private String detail;

	@SerializedName("Isball")
	private Boolean isBall;

	@SerializedName("Bowler")
	private String bowler;

	@SerializedName("Batsman")
	private String batsman;

	@SerializedName("Commentary")
	private String commentary;

	public Summary getSummary() {
		return summary;
	}

	public void setSummary(Summary summary) {
		this.summary = summary;
	}

	public String getOver() {
		return over;
	}

	public void setOver(String over) {
		this.over = over;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Boolean getIsWicket() {
		return isWicket;
	}

	public void setIsWicket(Boolean isWicket) {
		this.isWicket = isWicket;
	}

	public String getDismissed() {
		return dismissed;
	}

	public void setDismissed(String dismissed) {
		this.dismissed = dismissed;
	}

	public String getRuns() {
		return runs;
	}

	public void setRuns(String runs) {
		this.runs = runs;
	}

	public String getDetail() {
		return detail;
	}

	public void setDetail(String detail) {
		this.detail = detail;
	}

	public Boolean getIsBall() {
		return isBall;
	}

	public void setIsBall(Boolean isBall) {
		this.isBall = isBall;
	}

	public String getBowler() {
		return bowler;
	}

	public void setBowler(String bowler) {
		this.bowler = bowler;
	}

	public String getBatsman() {
		return batsman;
	}

	public void setBatsman(String batsman) {
		this.batsman = batsman;
	}

	public String getCommentary() {
		return commentary;
	}

	public void setCommentary(String commentary) {
		this.commentary = commentary;
	}

}
