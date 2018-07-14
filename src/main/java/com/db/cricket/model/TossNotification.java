package com.db.cricket.model;

import com.db.common.constants.Constants.Host;

public class TossNotification extends CricketNotification {

	private String tossWonByName;

	private String tossElectedTo;

	public String getTossElectedTo() {
		return tossElectedTo;
	}

	public void setTossElectedTo(String tossElectedTo) {
		this.tossElectedTo = tossElectedTo;
	}

	public String getTossWonByName() {
		return tossWonByName;
	}

	public void setTossWonByName(String tossWonByName) {
		this.tossWonByName = tossWonByName;
	}

	@Override
	public String toNotificationString() {
		String notification = tossWonByName + " ने टॉस जीता ";

		if ("bat".equalsIgnoreCase(tossElectedTo)) {
			notification = notification + "और पहले बल्लेबाज़ी का फैसला किया।";
		} else {
			notification = notification + "। " + getTeamName() + " करेगी पहले बल्लेबाजी। ";
		}

		return notification;
	}

	@Override
	public String slugIntro() {
		return "Live Score " + getSeriesName();
	}

	@Override
	public String toNotificationString(int host) {

		String notification = tossWonByName;

		if (Host.DIVYA_APP_ANDROID_HOST == host || Host.DIVYA_MOBILE_WEB_HOST == host || Host.DIVYA_WEB_HOST == host) {
			notification = notification + " એ ટૉસ જીત્યો ";

			if ("bat".equalsIgnoreCase(tossElectedTo)) {
				notification = notification + " અને પહેલા બેટીંગ કરવાનો નિર્ણય લીધો";
			} else {
				notification = notification + "।  " + getTeamName() + " કરશે પહેલા બેટીંગ  ";
			}
		} else {
			notification = notification + " ने टॉस जीता ";

			if ("bat".equalsIgnoreCase(tossElectedTo)) {
				notification = notification + "और पहले बल्लेबाज़ी का फैसला किया।";
			} else {
				notification = notification + "। " + getTeamName() + " करेगी पहले बल्लेबाजी। ";
			}
		}

		return notification;
	}

	@Override
	public String slugIntro(int host) {
		return slugIntro();
	}

	@Override
	public String toCommentDescription() {
		return toNotificationString();
	}
}
