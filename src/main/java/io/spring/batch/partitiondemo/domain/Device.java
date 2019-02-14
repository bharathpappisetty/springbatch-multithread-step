package io.spring.batch.partitiondemo.domain;

public class Device {

	private String username;
	private String userid;
	private String devicename;
	private String parameter; 
	private String metricvalue;
	private String datetime;
	private String location;
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getUserid() {
		return userid;
	}
	public void setUserid(String userid) {
		this.userid = userid;
	}
	public String getDevicename() {
		return devicename;
	}
	public void setDevicename(String devicename) {
		this.devicename = devicename;
	}
	public String getParameter() {
		return parameter;
	}
	public void setParameter(String parameter) {
		this.parameter = parameter;
	}
	public String getMetricvalue() {
		return metricvalue;
	}
	public void setMetricvalue(String metricvalue) {
		this.metricvalue = metricvalue;
	}
	public String getDatetime() {
		return datetime;
	}
	public void setDatetime(String datetime) {
		this.datetime = datetime;
	}
	public String getLocation() {
		return location;
	}
	public void setLocation(String location) {
		this.location = location;
	}
	@Override
	public String toString() {
		return "Device [username=" + username + ", userid=" + userid + ", devicename=" + devicename + ", parameter="
				+ parameter + ", metricvalue=" + metricvalue + ", datetime=" + datetime + ", location=" + location
				+ "]";
	}
	
	
}
