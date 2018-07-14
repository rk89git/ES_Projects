package com.db.cricket.model;

import com.db.common.constants.Constants.Host;

public class OversNotification extends CricketNotification {

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
		return teamHome + " Vs " + teamAway + " लाइव अपडेट: " + getOvers() + " ओवर के बाद";
	}
	
	@Override
	public String toNotificationString(int host) {
		
		if (Host.DIVYA_APP_ANDROID_HOST == host || Host.DIVYA_MOBILE_WEB_HOST == host || Host.DIVYA_WEB_HOST == host) {
			return teamHome + " Vs " + teamAway + " લાઇવ અપડેટ: " + getOvers() + " ઓવર  પછી";
		}
		
		return teamHome + " Vs " + teamAway + " लाइव अपडेट: " + getOvers() + " ओवर के बाद";
	}

	@Override
	public String toCommentDescription() {
		return toNotificationString() + ": \n" + slugIntro();
	}
}
