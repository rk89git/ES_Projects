package com.db.cricket.model;

/**
 * Created by Satya on 19-09-2017.
 */
public class CricketNotification {
	private String teamName;
	private String runs;
	private String totalRuns;
	private String overs;
	private String totalWickets;
	private String seriesName;
	private String tourName = null;
	private String match_id = null;

	public String getTourName() {
		return tourName;
	}

	public void setTourName(String tourName) {
		this.tourName = tourName;
	}

	public String getMatch_id() {
		return match_id;
	}

	public void setMatch_id(String match_id) {
		this.match_id = match_id;
	}

	public String getTeamName() {
		return teamName;
	}

	public void setTeamName(String teamName) {
		this.teamName = teamName;
	}

	public String getRuns() {
		return runs;
	}

	public void setRuns(String runs) {
		this.runs = runs;
	}

	public String getTotalRuns() {
		return totalRuns;
	}

	public void setTotalRuns(String totalRuns) {
		this.totalRuns = totalRuns;
	}

	public String getOvers() {
		return overs;
	}

	public void setOvers(String overs) {
		this.overs = overs;
	}

	public String getTotalWickets() {
		return totalWickets;
	}

	public void setTotalWickets(String totalWickets) {
		this.totalWickets = totalWickets;
	}

	public String getSeriesName() {
		return seriesName;
	}

	public void setSeriesName(String seriesName) {
		this.seriesName = seriesName;
	}

	@Override
	public String toString() {
		return "CricketNotification{" + "teamName='" + teamName + '\'' + ", runs='" + runs + '\'' + ", totalRuns='"
				+ totalRuns + '\'' + ", overs='" + overs + '\'' + ", totalWickets='" + totalWickets + '\''
				+ ", seriesName='" + seriesName + '\'' + ", tourName='" + tourName + '\'' + ", match_id='" + match_id
				+ '\'' + '}';
	}

	/**
	 * Method meant to provide for strings for cricket notifications.
	 */
	public String toNotificationString() {
		return null;
	}

	/**
	 * Method meant to provide for strings for cricket notifications.
	 */
	public String toNotificationString(int host) {
		return null;
	}
	
	/**
	 * Method meant to provide slug intro(notification body) for cricket
	 * notifications.
	 */
	public String slugIntro() {
		return teamName + " " + totalRuns + "/" + totalWickets + " (" + overs + " Ovs) - Live Score " + seriesName;
	}
	
	public String slugIntro(int host) {
		return slugIntro();
	}

	public String toCommentDescription() {
		return null;
	}
}
