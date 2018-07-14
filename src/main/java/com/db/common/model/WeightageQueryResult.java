package com.db.common.model;

import java.util.ArrayList;
import java.util.List;

public class WeightageQueryResult {

	List<CIDWeightage> weightages = new ArrayList<CIDWeightage>();

	public List<CIDWeightage> getWeightages() {
		return weightages;
	}

	public void setWeightages(List<CIDWeightage> weightages) {
		this.weightages = weightages;
	}
}
