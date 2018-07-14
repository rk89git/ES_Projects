package com.db.cricket.model;

import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.google.gson.annotations.SerializedName;

public class Note {

	@SerializedName("inningsNumber")
	private int inningsNumber;

	@SerializedName("notes")
	private List<String> notes = null;

	public int getInningsNumber() {
		return inningsNumber;
	}

	public void setInningsNumber(int inningsNumber) {
		this.inningsNumber = inningsNumber;
	}

	public List<String> getNotes() {
		return notes;
	}

	public void setNotes(List<String> notes) {
		this.notes = notes;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}

}
