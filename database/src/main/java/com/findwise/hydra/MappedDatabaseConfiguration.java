package com.findwise.hydra;

import java.util.HashMap;

public class MappedDatabaseConfiguration extends HashMap<String, Object> implements DatabaseConfiguration {

	private static final long serialVersionUID = 1L;

	public MappedDatabaseConfiguration(
			DatabaseConfiguration databaseConfiguration) {
		setNamespace(databaseConfiguration.getNamespace());
		setDatabaseUrl(databaseConfiguration.getDatabaseUrl());
		setDatabaseUser(databaseConfiguration.getDatabaseUser());
		setDatabasePassword(databaseConfiguration.getDatabasePassword());
		setOldMaxSize(databaseConfiguration.getOldMaxSize());
		setOldMaxCount(databaseConfiguration.getOldMaxCount());
	}

	@Override
	public String getNamespace() {
		return (String) this.get("namespace");
	}
	
	public void setNamespace(String namespace) {
		this.put("namespace", namespace);
	}

	@Override
	public String getDatabaseUrl() {
		return (String) this.get("databaseUrl");
	}
	
	public void setDatabaseUrl(String databaseUrl) {
		this.put("databaseUrl", databaseUrl);
	}
	
	@Override
	public String getDatabaseUser() {
		return (String) this.get("databaseUser");
	}

	public void setDatabaseUser(String databaseUser) {
		this.put("databaseUser", databaseUser);
	}
	
	@Override
	public String getDatabasePassword() {
		return (String) this.get("databasePassword");
	}

	public void setDatabasePassword(String databasePassword) {
		this.put("databasePassword", databasePassword);
	}
	
	@Override
	public int getOldMaxSize() {
		return (Integer) this.get("oldMaxSize");
	}

	public void setOldMaxSize(int oldMaxSize) {
		this.put("oldMaxSize", oldMaxSize);
	}
	
	@Override
	public int getOldMaxCount() {
		return (Integer) this.get("oldMaxCount");
	}

	public void setOldMaxCount(int oldMaxCount) {
		this.put("oldMaxCount", oldMaxCount);
	}
}
