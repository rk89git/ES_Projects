
package com.db.cricket.model;

import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.google.gson.annotations.SerializedName;

/**
 * This class represents a schedule of cricket matches.
 */
public class Schedule {

	@SerializedName("matches")
	private List<Match> matches = null;

	public List<Match> getMatches() {
		return matches;
	}

	public void setMatches(List<Match> matches) {
		this.matches = matches;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}

}
