package com.db.cricket.model;

import java.util.List;

import com.google.gson.annotations.SerializedName;

public class Commentary {

	@SerializedName("Commentary")
	private List<CommentaryItem> commentary = null;

	private String inningsNumber = null;

	@SerializedName("match_id")
	private String matchId = null;

	public List<CommentaryItem> getCommentary() {
		return commentary;
	}

	public void setCommentary(List<CommentaryItem> commentary) {
		this.commentary = commentary;
	}

	public String getInningsNumber() {
		return inningsNumber;
	}

	public void setInningsNumber(String inningsNumber) {
		this.inningsNumber = inningsNumber;
	}

	public String getMatchId() {
		return matchId;
	}

	public void setMatchId(String matchId) {
		this.matchId = matchId;
	}

	@Override
	public String toString() {
		return "Commentary [commentary=" + commentary + "]";
	}
}
