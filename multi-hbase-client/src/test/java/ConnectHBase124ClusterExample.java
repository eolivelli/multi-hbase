/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.security.ShellBasedUnixGroupsMapping;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

/**
 * Try connecting to an hbase 0.94 cluster
 *
 * @author enrico.olivelli
 */
public class ConnectHBase124ClusterExample {

    @Test
    public void test() throws Exception {
        final String tableName = "test_table";
        final String family = "family";
        final String columnQualifier = "qual";
        final String rowValue = "foo";
        final String cellValue = "bar";

        Configuration baseDefaults = new Configuration();
        baseDefaults.set("hbase.defaults.for.version", "1.2.4");
        Configuration configuration = HBaseConfiguration.create(baseDefaults);
        configuration.set("hbase.zookeeper.quorum", "new-hbase-server:2181");
        UserGroupInformation.setConfiguration(configuration);
        try (HBaseAdmin admin = new HBaseAdmin(configuration);) {
            HColumnDescriptor col = new HColumnDescriptor(family);
            if (!admin.isTableAvailable(tableName)) {
                System.out.println("Table " + tableName + " does not exist. Creating");
                HTableDescriptor desc = new HTableDescriptor(tableName);
                desc.addFamily(col);
                admin.createTable(desc);
                System.out.println("Table " + tableName + " created");
            }
            if (!admin.isTableEnabled(tableName)) {
                System.out.println("table " + tableName + " is not enabled. enabling");
                admin.enableTable(tableName);
            }
            try (HTablePool pool = new HTablePool(configuration, 1);
                HTableInterface table = pool.getTable(tableName)) {
//                Put put = new Put(rowValue.getBytes());
//                put.add(family.getBytes(), columnQualifier.getBytes(), cellValue.getBytes());
//                table.put(put);

                Scan scan = new Scan();
                scan.setCacheBlocks(false);
                scan.setCaching(1000);
                scan.setBatch(1000);
                scan.setMaxVersions(1);
                try (ResultScanner scanner = table.getScanner(scan);) {
                    Result result = scanner.next();
                    while (result != null) {
                        KeyValue cell = result.getColumnLatest(family.getBytes(), columnQualifier.getBytes());
                        System.out.println("row:" + new String(cell.getRow()));
                        System.out.println("value:" + new String(cell.getValue()));
                        result = scanner.next();
                    }
                }
            }

        }

    }

}
