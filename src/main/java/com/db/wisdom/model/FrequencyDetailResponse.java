package com.db.wisdom.model;

import java.util.List;

public class FrequencyDetailResponse {

	private String session_count;
	private String date;
	private long user_count;
	private List<Source> sources;
	
	
	
	public String getSession_count() {
		return session_count;
	}

	public void setSession_count(String session_count) {
		this.session_count = session_count;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public long getUser_count() {
		return user_count;
	}

	public void setUser_count(long user_count) {
		this.user_count = user_count;
	}

	public List<Source> getSources() {
		return sources;
	}

	public void setSources(List<Source> sources) {
		this.sources = sources;
	}

	public class Source {
		private String source;
		private long user_count;
		private List<Category> categories;
		public String getSource() {
			return source;
		}
		public void setSource(String source) {
			this.source = source;
		}
		public long getUser_count() {
			return user_count;
		}
		public void setUser_count(long user_count) {
			this.user_count = user_count;
		}
		public List<Category> getCategories() {
			return categories;
		}
		public void setCategories(List<Category> categories) {
			this.categories = categories;
		}
		
		
		
	}
	
	public class Category{
		private String category;
		private long user_count;
		public String getCategory() {
			return category;
		}
		public void setCategory(String category) {
			this.category = category;
		}
		public long getUser_count() {
			return user_count;
		}
		public void setUser_count(long user_count) {
			this.user_count = user_count;
		}
		
		
		
	}
	

}
