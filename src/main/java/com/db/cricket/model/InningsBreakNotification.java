package com.db.cricket.model;

import com.db.common.constants.Constants.Host;

public class InningsBreakNotification extends CricketNotification {

	private String teamHome;

	private String teamAway;

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
		return teamHome + " Vs " + teamAway + " लाइव अपडेट पहली पारी के बाद";
	}

	@Override
	public String slugIntro() {
		return getTeamName() + " ने " + getOvers() + " ओवर में " + getTotalWickets() + " विकेट खोकर " + getTotalRuns()
				+ " रन बनाए। ";
	}

	@Override
	public String toNotificationString(int host) {

		if (Host.DIVYA_APP_ANDROID_HOST == host || Host.DIVYA_MOBILE_WEB_HOST == host || Host.DIVYA_WEB_HOST == host) {
			return teamHome + " Vs " + teamAway + "લાઇવ અપડેટ પહેલી પારી પછી";
		}

		return teamHome + " Vs " + teamAway + " लाइव अपडेट पहली पारी के बाद";
	}

	@Override
	public String slugIntro(int host) {

		if (Host.DIVYA_APP_ANDROID_HOST == host || Host.DIVYA_MOBILE_WEB_HOST == host || Host.DIVYA_WEB_HOST == host) {
			return getTeamName() + " એ " + getOvers() + " ઓવરમાં " + getTotalWickets() + " વિકેટ ગુમાવીને "
					+ getTotalRuns() + " રન બનાવ્યા";
		}

		return getTeamName() + " ने " + getOvers() + " ओवर में " + getTotalWickets() + " विकेट खोकर " + getTotalRuns()
				+ " रन बनाए। ";
	}

	@Override
	public String toCommentDescription() {
		return toNotificationString() + ": \n" + slugIntro();
	}
}
