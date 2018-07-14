package com.db.cricket.model;

import com.google.gson.annotations.SerializedName;

public class SupportStaff {
	
	private String id;
	
	private String name;
	
	private String nationality;
	
	@SerializedName("nationality_id")
	private String nationalityId;
	
	@SerializedName("role_id")
	private String roleId;
	
	@SerializedName("role_name")
	private String roleName;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getNationality() {
		return nationality;
	}

	public void setNationality(String nationality) {
		this.nationality = nationality;
	}

	public String getNationalityId() {
		return nationalityId;
	}

	public void setNationalityId(String nationalityId) {
		this.nationalityId = nationalityId;
	}

	public String getRoleId() {
		return roleId;
	}

	public void setRoleId(String roleId) {
		this.roleId = roleId;
	}

	public String getRoleName() {
		return roleName;
	}

	public void setRoleName(String roleName) {
		this.roleName = roleName;
	}
}
