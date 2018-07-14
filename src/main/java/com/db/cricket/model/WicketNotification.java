package com.db.cricket.model;

import com.db.common.constants.Constants.Host;
import com.db.cricket.utils.CricketUtils;

/**
 * Created by Satya on 19-09-2017.
 */
public class WicketNotification extends PlayerNotification {

	private String bowlerName;

	public String getBowlerName() {
		return bowlerName;
	}

	public void setBowlerName(String bowlerName) {
		this.bowlerName = bowlerName;
	}

	@Override
	public String toString() {
		return "WicketNotification{" + "bowlerName='" + bowlerName + '\'' + '}';
	}

	@Override
	public String toNotificationString() {
		return getTeamName() + " का " + CricketUtils.AddOrdinal(getTotalWickets()) + " विकेट गिरा। " + getPlayerName()
				+ " " + getRuns() + " रन बनाकर आउट। ";
	}
	
	@Override
	public String toNotificationString(int host) {
		
		if (Host.DIVYA_APP_ANDROID_HOST == host || Host.DIVYA_MOBILE_WEB_HOST == host || Host.DIVYA_WEB_HOST == host) {
			return getTeamName() + " ની " + CricketUtils.AddOrdinal(getTotalWickets()) + " વિકેટ પડી।  "
					+ getPlayerName() + " " + getRuns() + " રન બનાવીને આઉટ। ";
		}
		
		return getTeamName() + " का " + CricketUtils.AddOrdinal(getTotalWickets()) + " विकेट गिरा। " + getPlayerName()
				+ " " + getRuns() + " रन बनाकर आउट। ";
	}

	@Override
	public String toCommentDescription() {
		return toNotificationString();
	}
}
