package com.hazelcast.enterprise;

import java.util.Date;

public class Registration {

	private static final long serialVersionUID = 8244902115220303228L;
	
	private String owner;
	private Date registryDate;
	private Date expiryDate;
	private Mode mode;
	private int maxNodes;
	
	public Registration(String owner, Date registryDate,
			Date expiryDate, String mode, int maxNodes) {
		super();
		
		this.owner = owner;
		this.registryDate = registryDate;
		this.expiryDate = expiryDate;
		this.mode = Mode.valueOf(mode);
		this.maxNodes = maxNodes;
	}
	
	public String getOwner() {
		return owner;
	}
	
	public Date getRegistryDate() {
		return registryDate;
	}

	public Date getExpiryDate() {
		return expiryDate;
	}

	public Mode getMode() {
		return mode;
	}
	
	public boolean isExpired() {
		return new Date().after(expiryDate);
	}
	
	public int getMaxNodes() {
		return maxNodes;
	}
	
	public boolean isValid() {
		return (mode == Mode.FULL 
				|| mode == Mode.TRIAL && !isExpired());
	}
	
	public enum Mode {
		TRIAL,
		FULL
	}
}