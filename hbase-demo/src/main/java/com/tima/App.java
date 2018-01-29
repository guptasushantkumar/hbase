package com.tima;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Hello world!
 *
 */
public class App {

	public static void main(String[] args) throws Exception {

		TableName tableName = TableName.valueOf("sushant:filter_master");
		Configuration hConf = HBaseConfiguration.create();
		try {
			Connection conn = ConnectionFactory.createConnection(hConf);
			Table findthis = conn.getTable(tableName);
			Put put = new Put(Bytes.toBytes(3));

			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"),
					Bytes.toBytes(28));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"),
					Bytes.toBytes("Madhu"));

			findthis.put(put);

			Get getThis = new Get(Bytes.toBytes(3));
			Result result = findthis.get(getThis);

			byte[] val = result.getValue(Bytes.toBytes("info"),
					Bytes.toBytes("age"));

			System.out.println("Value: " + Bytes.toInt(val));

			val = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"));

			System.out.println("Value: " + Bytes.toString(val));

			findthis.close();
			conn.close();
		} catch (Exception e) {
			System.out.println("Error");
			e.printStackTrace();
		}
	}
}
