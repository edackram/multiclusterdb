package com.maprdemo.multiclusterdb;

import com.tdunning.math.stats.ArrayDigest;
import com.tdunning.math.stats.TDigest;

public class ClusterLatency {

	String cluster;
	long defaultSLA;
	ArrayDigest getDigest = TDigest.createArrayDigest(100);
	ArrayDigest putDigest = TDigest.createArrayDigest(100);
	ArrayDigest scanDigest = TDigest.createArrayDigest(100);
	
	public ClusterLatency(String cluster, long startSLA) {
		this.cluster = cluster;
		this.defaultSLA = startSLA;
	}
	
	public long getLatency() {
		//Get 50% percentile if at least 10 entries
		if (getDigest.size() > 10) {
			double latency = getDigest.quantile(0.5d);
			//Latency has to be at least 1 millisecond
			if (latency < 1)
				latency = 1d;
			System.out.println("Get Latency for cluser " + cluster + " is: " + latency);
			return (long)latency; 
		} else {
			System.out.println("Using Default latency.......");
			return defaultSLA;
		}
	}
	
	public long scanLatency() {
		//Get 50% percentile if at least 10 entries
		if (scanDigest.size() > 10) {
			double latency = scanDigest.quantile(0.5d);
			//Latency has to be at least 1 millisecond
			if (latency < 1)
				latency = 1d;
			System.out.println("Get Latency for cluser " + cluster + " is: " + latency);
			return (long)latency; 
		} else {
			System.out.println("Using Default latency.......");
			return defaultSLA;
		}
	}
	
	public void addGetLatency(double latency) {
		System.out.println("Adding get latency of: " + latency + " for cluster " + cluster);
		getDigest.add(latency);
	}
	
	public void addPutLatency(double latency) {
		System.out.println("Adding put latency of: " + latency + " for cluster " + cluster);
		putDigest.add(latency);
	}
	
	public void addScanLatency(double latency) {
		System.out.println("Adding scan latency of: " + latency + " for cluster " + cluster);
		scanDigest.add(latency);
	}
}
