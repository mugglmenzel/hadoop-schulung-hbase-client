package schulung.praxis.hadoop.hbase.client;

import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

public class HBaseClientExample {

	public static void main(String[] args) throws Exception {
		String tbName = "test-table";
		
		
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "107.22.16.86"); //enter IP/address of your server
		//config.set("hbase.master", "107.22.16.86:60000");
		
		
		HBaseAdmin admin = new HBaseAdmin(config);
		
		HTableDescriptor testTab = new HTableDescriptor(tbName);
		testTab.addFamily(new HColumnDescriptor("author"));
		admin.createTable(testTab);
		
		admin.close();
		
		
		HTable tab = new HTable(config, tbName);
		
		String uuid = UUID.randomUUID().toString();
		Put p = new Put(uuid.getBytes());
		p.add("author".getBytes(), "".getBytes(), "Michael".getBytes());
		tab.put(p);
		
		Get g = new Get(uuid.getBytes());
		System.out.println(g.toJSON());
		
		System.out.println(tab.get(g.addFamily("author".getBytes())));
		tab.close();
		
	}
	
}
