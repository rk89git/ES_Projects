package com.db.cricket.model;

import com.db.common.constants.Constants.Host;

public class MatchWinNotification extends CricketNotification {

	private String winningTeam;

	private String teamHome;

	private String teamAway;

	private String winMargin;

	private Boolean isDraw = false;

	private String matchNumber;

	public String getMatchNumber() {
		return matchNumber;
	}

	public void setMatchNumber(String matchNumber) {
		this.matchNumber = matchNumber;
	}

	public Boolean getIsDraw() {
		return isDraw;
	}

	public void setIsDraw(Boolean isDraw) {
		this.isDraw = isDraw;
	}

	public String getWinningTeam() {
		return winningTeam;
	}

	public void setWinningTeam(String winningTeam) {
		this.winningTeam = winningTeam;
	}

	public String getWinMargin() {
		return winMargin;
	}

	public void setWinMargin(String winMargin) {
		this.winMargin = winMargin;
	}

	public String getTeamHome() {
		return teamHome;
	}

	public void setTeamHome(String teamHome) {
		this.teamHome = teamHome;
	}

	public String getTeamAway() {
		return teamAway;
	}

	public void setTeamAway(String teamAway) {
		this.teamAway = teamAway;
	}

	@Override
	public String toNotificationString() {
		return teamHome + " Vs " + teamAway + " रिजल्ट";
	}

	@Override
	public String slugIntro() {

		if (isDraw) {
			return teamHome + " और " + teamAway + " के बीच में खेला गया " + matchNumber + " टेस्ट मैच ड्रा हो गया।";
		} else if (teamHome.equals(winningTeam)) {
			return winningTeam + " ने " + teamAway + " को " + winMargin + " से हराया। ";
		}
		return winningTeam + " ने " + teamHome + " को " + winMargin + " से हराया। ";
	}

	@Override
	public String toNotificationString(int host) {
		
		if (Host.DIVYA_APP_ANDROID_HOST == host || Host.DIVYA_MOBILE_WEB_HOST == host || Host.DIVYA_WEB_HOST == host) {
			return teamHome + " Vs " + teamAway + " પરિણામ";
		}
		
		return teamHome + " Vs " + teamAway + " रिजल्ट";
	}

	@Override
	public String slugIntro(int host) {

		if (Host.DIVYA_APP_ANDROID_HOST == host || Host.DIVYA_MOBILE_WEB_HOST == host || Host.DIVYA_WEB_HOST == host) {
			
			if (isDraw) {
				return teamHome + " અને " + teamAway + " ની વચ્ચે રમાયો " + matchNumber + " મેચ ડ્રો થઇ ગયો";
			} else if (teamHome.equals(winningTeam)) {
				return winningTeam + " અને " + teamAway + " ને " + winMargin + " થી હરાવી ";
			}

		}
		
		if (isDraw) {
			return teamHome + " ने " + teamAway + " के बीच में खेला गया " + matchNumber + " मैच ड्रा हो गया।";
		} else if (teamHome.equals(winningTeam)) {
			return winningTeam + " ने " + teamAway + " को " + winMargin + " से हराया। ";
		}
		return winningTeam + " ने " + teamHome + " को " + winMargin + " से हराया। ";
	}

	@Override
	public String toCommentDescription() {
		return toNotificationString() + ": \n" + slugIntro();
	}

	@Override
	public String toString() {
		return "MatchWinNotification [winningTeam=" + winningTeam + ", teamHome=" + teamHome + ", teamAway=" + teamAway
				+ ", winMargin=" + winMargin + ", isDraw=" + isDraw + ", matchNumber=" + matchNumber + "]";
	}
}
