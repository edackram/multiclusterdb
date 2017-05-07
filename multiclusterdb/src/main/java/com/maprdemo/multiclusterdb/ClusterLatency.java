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
		if (digests.get(type).size() > 10) {
			double latency = digests.get(type).quantile(0.5d);
			//Latency has to be at least 1 millisecond
			if (latency < 1)
				latency = 1d;
			System.out.println(type + " Latency for cluser " + cluster + " is: " + latency);
			return (long)latency; 
		} else {
			System.out.println("Using Default latency.......");
			return defaultSLA;
		}
	}
	
	public void addLatency(String type, double latency) {
		System.out.println("Adding " + type + " latency of: " + latency + " for cluster " + cluster);
		if (digests.get(type) != null)	
			digests.get(type).add(latency);
		else {
			digests.put(type, TDigest.createArrayDigest(100));
		}
	}

}
