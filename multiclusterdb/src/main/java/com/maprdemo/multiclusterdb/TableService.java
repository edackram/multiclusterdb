package com.maprdemo.multiclusterdb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

/**
 * 
 * @author mcade
 *
 */
public class TableService {
	
	private static final String GET = "GET";
	private static final String PUT = "PUT";
	private static final String SCAN = "SCAN";
	private static final ExecutorService taskExec = Executors.newCachedThreadPool();
	private static final long DEFAULT_LATENCY = 100;
	private Map<String,Table> clusters = new HashMap<String,Table>();
	private Map<String,ClusterLatency> clusterLatencies = new HashMap<String,ClusterLatency>();
	
	/**
	 * Constructor for the TableService
	 * 
	 * @param table name of the table to be queried
	 * 
	 */
	public TableService(String table) {

		try {
			Configuration conf = HBaseConfiguration.create();
			conf.set("mapr.hbase.default.db", "maprdb");

			Connection conn = ConnectionFactory.createConnection(conf);
			
			loadClusterConfig(conn,table);

		} catch (IOException e) {
			// TODO Auto-generated catch block
		}
	}
	
	/**
	 * Executes the org.apache.hadoop.hbase.client.Put on the table
	 * in the first cluster available.
	 * 
	 * @param put org.apache.hadoop.hbase.client.Put created in application code.
	 * 
	 */
	public void put(Put put) {
		long start_time = System.nanoTime();
		
		try {		
			String cluster = clusters.keySet().iterator().next();
			clusters.get(cluster).put(put);
			clusterLatencies.get(cluster).addLatency(PUT,(System.nanoTime() - start_time)/1e6);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/**
	 * Takes org.apache.hadoop.hbase.client.Get and builds a
	 * Callable for execution against the cluster with a timeout that
	 * is retrieved from ClusterLatency.  If the Get times out, we move
	 * to the next cluster.
	 * 
	 * @param get org.apache.hadoop.hbase.client.Get create in application code
	 * @return org.apache.hadoop.hbase.client.Result
	 * 
	 */
	public Result getFromTable(Get get) {
		long start_time = 0;
		
		Result result = null;
		
		for (String cluster : clusters.keySet()) {
			GetFromTable gft = new GetFromTable(get,clusters.get(cluster));
		
			start_time = System.nanoTime();
	        result = getFromTable(gft, clusterLatencies.get(cluster).getLatency(GET));
	        if (result != null) {
	        	clusterLatencies.get(cluster).addLatency(GET,(System.nanoTime() - start_time)/1e6);
	        	return result; 
	        }
		}
 
		return null;
	}

	/**
	 * Takes org.apache.hadoop.hbase.client.Scan and builds a
	 * Callable for execution against the cluster with a timeout that
	 * is retrieved from ClusterLatency.  If the Scan times out, we move
	 * to the next cluster.
	 * 
	 * @param scan org.apache.hadoop.hbase.client.Scan created in application code
	 * @return org.apache.hadoop.hbase.client.ResultScanner
	 * 
	 */
	public ResultScanner scanTable(Scan s) {
		long start_time = System.nanoTime();
		
		ResultScanner result = null;
		
		for (String cluster : clusters.keySet()) {
			//System.out.println("Working on cluster: " + cluster);
			ScanTable st = new ScanTable(s,clusters.get(cluster));
			
			start_time = System.nanoTime();
	        result = scanTable(st, clusterLatencies.get(cluster).getLatency(SCAN));
	        if (result != null) {
	        	clusterLatencies.get(cluster).addLatency(SCAN,(System.nanoTime() - start_time)/1e6);
	        	return result; 
	        }
		}
 
		return null;
	}
	
	/**
	 * 
	 * @param gft
	 * @param timeout
	 * @return
	 */
	private Result getFromTable(GetFromTable gft, long timeout) {
    		
        Future<Result> task = taskExec.submit(gft);
        
        try {
        	Result result = (Result) task.get(timeout, MILLISECONDS);
            return result;
        } catch (TimeoutException e) {
            // task will be cancelled below
        	System.out.println("Time out on Table: " + gft.getTable() + " timeout was: " + timeout);
        	e.printStackTrace();
        } catch (ExecutionException e) {
            // exception thrown in task; 
            e.printStackTrace();
        } catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
            // Harmless if task already completed
            task.cancel(true); // interrupt if running
        }

        return null;
	}
	
	/**
	 * 
	 * @param st
	 * @param timeout
	 * @return
	 */
	private ResultScanner scanTable(ScanTable st, long timeout) {
		
        Future<ResultScanner> task = taskExec.submit(st);
        
        try {
        	ResultScanner result = (ResultScanner) task.get(timeout, MILLISECONDS);
            return result;
        } catch (TimeoutException e) {
            // task will be cancelled below
        	System.out.println("Time out on Table: " + st.getTable());
        	e.printStackTrace();
        } catch (ExecutionException e) {
            // exception thrown in task; 
            e.printStackTrace();
        } catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
            // Harmless if task already completed
            task.cancel(true); // interrupt if running
        }

        return null;
	}
	
	/**
	 * Creates a Map of the clusters in mapr-clusters.conf with the tablename
	 * to be used for processing queries against different clusters for timeout 
	 * and fail over scenarios.
	 * 
	 * @param conn org.apache.hadoop.hbase.client.Connection
	 * @param table name of the table to be queried
	 * 
	 */
	private void loadClusterConfig(Connection conn,String table) {
		
		try {
	 		// Open the input file and read it line by line
			BufferedReader br = new BufferedReader(new FileReader("/opt/mapr/conf/mapr-clusters.conf"));
			String currentLine;
			String[] tokens; // an array to hold values from one line of the file
	
			while ((currentLine = br.readLine()) != null) {
				System.out.println(currentLine);
				tokens = currentLine.split("\\s* \\s*"); // Split on : boundaries, stripping white space.
				try {
					if ( tokens != null )  {
						clusters.put(tokens[0], conn.getTable(TableName.valueOf("/mapr/" + tokens[0] + table)));
						clusterLatencies.put(tokens[0], new ClusterLatency(tokens[0],DEFAULT_LATENCY));
					} else {
						System.out.println("Ignoring malformed line: " + currentLine);
					}
				} catch (Exception e) {
					//Cluster might be down, don't load it.
					//TODO: Figure out how to check for cluster up and add back into list.
					e.printStackTrace();
				}
			}
			
			// Close the file & exit.
			if (br != null) br.close();
			
		} catch (IOException e) {
			System.out.println("No cluster configuration file found.");
		} 

	}
	
	/**
	 * Print the quantiles of latency for the table across the different clusters
	 */
	public void printQuantiles() {
		for (String cluster : clusterLatencies.keySet()) {
			ClusterLatency cl = clusterLatencies.get(cluster);
			System.out.println("Quantiles for cluster/table " + cluster + ";");
			cl.printQuantiles();
		}
		
	}
}
