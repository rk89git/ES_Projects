package com.db.nlp.model;

import java.util.ArrayList;
import java.util.List;

public class NLPResult {

	List<String> metaNouns = new ArrayList<>();

	public List<String> getMetaNouns() {
		return metaNouns;
	}

	public void setMetaNouns(List<String> metaNouns) {
		this.metaNouns = metaNouns;
	}

	List<Entity> entities = new ArrayList<>();

	public List<Entity> getEntities() {
		return entities;
	}

	public void setEntities(List<Entity> entities) {
		this.entities = entities;
	}

	public class Entity {
		String type = "";

		public String getType() {
			return type;
		}

		public void setType(String type) {
			this.type = type;
		}

		List<String> value = new ArrayList<>();

		public List<String> getValue() {
			return value;
		}

		public void setValue(List<String> value) {
			this.value = value;
		}

		@Override
		public String toString() {
			return "Entity [type=" + type + ", value=" + value + "]";
		}

	}

	@Override
	public String toString() {
		return "NLPResult [metaNouns=" + metaNouns + ", entities=" + entities + "]";
	}

}
