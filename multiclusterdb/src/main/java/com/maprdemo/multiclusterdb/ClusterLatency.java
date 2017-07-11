package com.maprdemo.multiclusterdb;

import java.util.HashMap;
import java.util.Map;

import com.tdunning.math.stats.ArrayDigest;
import com.tdunning.math.stats.TDigest;

public class ClusterLatency {

	String cluster;
	long defaultSLA;
	Map<String, ArrayDigest> digests = new HashMap<String, ArrayDigest>();
	
	public ClusterLatency(String cluster, long startSLA) {
		this.cluster = cluster;
		this.defaultSLA = startSLA;
	}
	
	public long getLatency(String type) {
		//Get 50% percentile if at least 10 entries
		if (!digests.isEmpty() && digests.get(type).size() > 10) {
			double latency = digests.get(type).quantile(0.5d);
			//Latency has to be at least 1 millisecond
			if (latency < 1)
				latency = 1d;
			//System.out.println(type + " Latency for cluser " + cluster + " is: " + latency);
			return (long)latency; 
		} else {
			System.out.println("Using Default latency.......");
			return defaultSLA;
		}
	}
	
	public void addLatency(String type, double latency) {
		//System.out.println("Adding " + type + " latency of: " + latency + " for cluster " + cluster);
		if (digests.get(type) != null)	
			digests.get(type).add(latency);
		else 
			digests.put(type, TDigest.createArrayDigest(100));
	}

	public void printQuantiles() {
		
		for (String type : digests.keySet()) {
			ArrayDigest digest = digests.get(type);
			System.out.println("Quantiles for " + type + ":");
			System.out.println("50%: " + digest.quantile(0.5d));
			System.out.println("60%: " + digest.quantile(0.6d));
			System.out.println("70%: " + digest.quantile(0.7d));
			System.out.println("80%: " + digest.quantile(0.8d));
			System.out.println("90%: " + digest.quantile(0.9d));
			System.out.println("95%: " + digest.quantile(0.95d));
			System.out.println("99%: " + digest.quantile(0.99d));
			System.out.println("99.9%: " + digest.quantile(0.999d));
		}
		
	}
}
