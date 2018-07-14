package com.db.cricket.model;

import com.db.common.constants.Constants.Host;

public class PlayerNotification extends CricketNotification {

	private String playerName;

	private String ballsPlayed;

	public String getPlayerName() {
		return playerName;
	}

	public void setPlayerName(String playerName) {
		this.playerName = playerName;
	}

	public String getBallsPlayed() {
		return ballsPlayed;
	}

	public void setBallsPlayed(String ballsPlayed) {
		this.ballsPlayed = ballsPlayed;
	}

	@Override
	public String toNotificationString() {
		int runFlag = Integer.parseInt(getRuns()) / 50;
		String runs_phrase = null;
		if (runFlag == 1) {
			runs_phrase = "50";
		} else if (runFlag == 2) {
			runs_phrase = "100";
		} else if (runFlag == 3) {
			runs_phrase = "150";
		} else if (runFlag == 4) {
			runs_phrase = "200";
		}
		return playerName + " ने " + ballsPlayed + " बॉल पर " + runs_phrase + " रन बनाए। ";
	}

	@Override
	public String toNotificationString(int host) {

		int runFlag = Integer.parseInt(getRuns()) / 50;
		String runs_phrase = null;

		if (runFlag == 1) {
			runs_phrase = "50";
		} else if (runFlag == 2) {
			runs_phrase = "100";
		} else if (runFlag == 3) {
			runs_phrase = "150";
		} else if (runFlag == 4) {
			runs_phrase = "200";
		}

		if (Host.DIVYA_APP_ANDROID_HOST == host || Host.DIVYA_MOBILE_WEB_HOST == host || Host.DIVYA_WEB_HOST == host) {
			return playerName + " એ " + ballsPlayed + " બોલ પર " + runs_phrase + " રન બનાવ્યા";
		}

		return playerName + " ने " + ballsPlayed + " बॉल पर " + runs_phrase + " रन बनाए। ";
	}

	@Override
	public String toCommentDescription() {
		return toNotificationString();
	}
}
