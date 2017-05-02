package com.maprdemo.multiclusterdb;

import com.tdunning.math.stats.ArrayDigest;
import com.tdunning.math.stats.TDigest;

public class ClusterSLA {

	String cluster;
	long defaultSLA;
	ArrayDigest totalDigest = TDigest.createArrayDigest(100);
	
	public ClusterSLA(String cluster, long startSLA) {
		this.cluster = cluster;
		this.defaultSLA = startSLA;
	}
	
	public long getLatency() {
		//Get 50% percentile if at least 10 entries
		if (totalDigest.size() > 10) {
			double latency = totalDigest.quantile(0.5d);
			//Latency has to be at least 1 millisecond
			if (latency < 1)
				latency = 1d;
			System.out.println("Latency for cluser " + cluster + " is: " + latency);
			return (long)latency; 
		} else {
			System.out.println("Using Default latency.......");
			return defaultSLA;
		}
	}
	
	public void addLatency(double latency) {
		System.out.println("Adding a latency of: " + latency + " for cluster " + cluster);
		totalDigest.add(latency);
	}
}
