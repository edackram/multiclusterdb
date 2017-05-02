package com.maprdemo.multiclusterdb;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

public class GetFromTable implements Callable<Result> {

	private Get get;
	private Table table;
	
	public GetFromTable(Get get, Table table) {
		this.get = get;
		//System.out.println("GET: " + get.toString());
		this.table = table;
		//System.out.println("TABLE: " + table.toString());
	}
	
	@Override
	public Result call() throws IOException {
		
		Result result;
		try {
			result = table.get(get);
			if (result.isEmpty()) {
				return null;
			} else {
				return result;
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw e;
		}
	}
	
	protected String getTable() {
		return this.table.toString();
	}
}
