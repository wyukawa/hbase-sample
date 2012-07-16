package sample;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class ExampleClient {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws  
	 */
	public static void main(String[] args) throws IOException {
		Configuration config = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(config);
		HTableDescriptor htd = new HTableDescriptor("test");
		HColumnDescriptor hcd = new HColumnDescriptor("data");
		htd.addFamily(hcd);
		admin.createTable(htd);
		byte[] tableName = htd.getName();
		HTable table = new HTable(config, tableName);
		byte[] row1 = Bytes.toBytes("row1");
		Put p1 = new Put(row1);
		p1.add(Bytes.toBytes("data"), Bytes.toBytes("1"), Bytes.toBytes("value1"));
		table.put(p1);
		Get g = new Get(row1);
		Result result = table.get(g);
		System.out.println("Get: " + result);
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		try {
			for(Result scannerResult: scanner) {
				System.out.println("Scan: " + scannerResult);
			}
		} finally {
			scanner.close();
		}
		
		
		admin.disableTable(tableName);
		admin.deleteTable(tableName);
	}

}
