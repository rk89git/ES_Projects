package com.db.cricket.model;

import com.db.common.constants.Constants.Host;

public class TeamNotification extends CricketNotification {

	private String opponentName = null;

	public String getOpponentName() {
		return opponentName;
	}

	public void setOpponentName(String opponentName) {
		this.opponentName = opponentName;
	}

	@Override
	public String toNotificationString() {
		int runFlag = Integer.parseInt(getTotalRuns()) / 50;
		String runs_phrase = null;
		if (runFlag == 1) {
			runs_phrase = "50";
		} else if (runFlag == 2) {
			runs_phrase = "100";
		} else if (runFlag == 3) {
			runs_phrase = "150";
		} else if (runFlag == 4) {
			runs_phrase = "200";
		} else if (runFlag == 5) {
			runs_phrase = "250";
		} else if (runFlag == 6) {
			runs_phrase = "300";
		} else if (runFlag == 7) {
			runs_phrase = "350";
		} else if (runFlag == 8) {
			runs_phrase = "400";
		} else if (runFlag == 9) {
			runs_phrase = "450";
		} else if (runFlag == 10) {
			runs_phrase = "500";
		} else if (runFlag == 11) {
			runs_phrase = "550";
		}

		return opponentName + " के खिलाफ " + getTeamName() + " ने " + getOvers() + " ओवर में " + runs_phrase
				+ " रन पूरे किए। ";
	}

	@Override
	public String toNotificationString(int host) {
		int runFlag = Integer.parseInt(getTotalRuns()) / 50;
		String runs_phrase = null;
		if (runFlag == 1) {
			runs_phrase = "50";
		} else if (runFlag == 2) {
			runs_phrase = "100";
		} else if (runFlag == 3) {
			runs_phrase = "150";
		} else if (runFlag == 4) {
			runs_phrase = "200";
		} else if (runFlag == 5) {
			runs_phrase = "250";
		} else if (runFlag == 6) {
			runs_phrase = "300";
		} else if (runFlag == 7) {
			runs_phrase = "350";
		} else if (runFlag == 8) {
			runs_phrase = "400";
		} else if (runFlag == 9) {
			runs_phrase = "450";
		} else if (runFlag == 10) {
			runs_phrase = "500";
		} else if (runFlag == 11) {
			runs_phrase = "550";
		}

		if (Host.DIVYA_APP_ANDROID_HOST == host || Host.DIVYA_MOBILE_WEB_HOST == host || Host.DIVYA_WEB_HOST == host) {
			return opponentName + " ની વિરુદ્ધ " + getTeamName() + " એ " + getOvers() + " ઓવરમાં  " + runs_phrase
					+ " રન પૂરા કર્યા";
		}

		return opponentName + " के खिलाफ " + getTeamName() + " ने " + getOvers() + " ओवर में " + runs_phrase
				+ " रन पूरे किए। ";
	}

	@Override
	public String toCommentDescription() {
		return toNotificationString();
	}
}
