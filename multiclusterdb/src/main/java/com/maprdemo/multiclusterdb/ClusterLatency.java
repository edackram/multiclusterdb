package com.maprdemo.multiclusterdb;

import java.util.HashMap;
import java.util.Map;

import com.tdunning.math.stats.ArrayDigest;
import com.tdunning.math.stats.TDigest;

/**
 * Keeps track of the latency for a table in a cluster in order
 * to determine the 50% percentile latency to use as a timeout 
 * for queries.
 * 
 * @author mcade
 *
 */
public class ClusterLatency {

	String cluster;
	long defaultSLA;
	Map<String, ArrayDigest> digests = new HashMap<String, ArrayDigest>();
	
	/**
	 * Constructor for ClusterLatency
	 * 
	 * @param cluster name of cluster with tablepath ie /mapr/test.cluster/tables/testTable
	 * @param startSLA the service level agreement for this table in milliseconds
	 * 
	 */
	public ClusterLatency(String cluster, long startSLA) {
		this.cluster = cluster;
		this.defaultSLA = startSLA;
	}
	
	/**
	 * Returns the current latency for a table in cluster and type of 
	 * access. 
	 * 
	 * @param type the type of access. ie GET, PUT, SCAN
	 * @return the current latency at the 50% percentile for the type
	 * 
	 */
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
	
	/**
	 * Adds a latency to TDigest to be used for calculating latencies
	 * by percentile.
	 * 
	 * @param type the query type that was performed. ie GET, SCAN
	 * @param latency the response type of the query in milliseconds
	 * 
	 */
	public void addLatency(String type, double latency) {
		//System.out.println("Adding " + type + " latency of: " + latency + " for cluster " + cluster);
		if (digests.get(type) != null)	
			digests.get(type).add(latency);
		else 
			digests.put(type, TDigest.createArrayDigest(100));
	}

	/**
	 * Prints the quantiles of the latency for a specific table in a cluster
	 * by type of query.
	 * 
	 */
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
