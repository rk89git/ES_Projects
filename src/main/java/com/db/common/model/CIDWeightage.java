package com.db.common.model;

public class CIDWeightage implements Comparable<CIDWeightage>{

	private String field;
	private Integer value;
	private double weightage;
	private int count=0;

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}

	public double getWeightage() {
		return weightage;
	}

	public void setWeightage(double weightage) {
		this.weightage = weightage;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}


	@Override
	public int compareTo(CIDWeightage o) {
		// compareTo should return < 0 if this is supposed to be
        // less than other, > 0 if this is supposed to be greater than 
        // other and 0 if they are supposed to be equal
        int last = this.value.compareTo(o.getValue());
        return last;
	}

}
