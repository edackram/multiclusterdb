package com.maprdemo.multiclusterdb;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

public class ScanTable implements Callable<ResultScanner> {

	private Scan scan;
	private Table table;
	
	public ScanTable(Scan scan, Table table) {
		this.scan = scan;
		this.table = table;
	}
	
	@Override
	public ResultScanner call() throws IOException {
		
		ResultScanner result;
		try {
			result = table.getScanner(scan);

			return result;
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		}
	}

	protected String getTable() {
		return this.table.toString();
	}
}
