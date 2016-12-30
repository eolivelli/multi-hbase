/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import hbase094.org.apache.hadoop.conf.Configuration;
import hbase094.org.apache.hadoop.hbase.HBaseConfiguration;
import hbase094.org.apache.hadoop.hbase.HColumnDescriptor;
import hbase094.org.apache.hadoop.hbase.HConstants;
import hbase094.org.apache.hadoop.hbase.HTableDescriptor;
import hbase094.org.apache.hadoop.hbase.KeyValue;
import hbase094.org.apache.hadoop.hbase.client.HBaseAdmin;
import hbase094.org.apache.hadoop.hbase.client.HTableInterface;
import hbase094.org.apache.hadoop.hbase.client.HTablePool;
import hbase094.org.apache.hadoop.hbase.client.Put;
import hbase094.org.apache.hadoop.hbase.client.Result;
import hbase094.org.apache.hadoop.hbase.client.ResultScanner;
import hbase094.org.apache.hadoop.hbase.client.Scan;
import hbase094.org.apache.hadoop.security.ShellBasedUnixGroupsMapping;
import hbase094.org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

/**
 * Try connecting to an hbase 0.94 cluster
 *
 * @author enrico.olivelli
 */
public class ConnectHBase094ClusterExample {

    static {
        System.setProperty("hadoop.security.group.mapping", ShellBasedUnixGroupsMapping.class.getName());
        System.setProperty("hbase.defaults.for.version.skip", "true");
    }

    @Test
    public void test() throws Exception {
        final String tableName = "test_table_2";
        final String family = "family";
        final String columnQualifier = "qual";
        final String rowValue = "foo";
        final String cellValue = "bar";

        Configuration baseDefaults = new Configuration(false);
        baseDefaults.addResource("hbase094_hbase_default.xml");
        baseDefaults.set("hbase.defaults.for.version", "0.94.27");
        Configuration configuration = HBaseConfiguration.create(baseDefaults);
        configuration.setClass("hadoop.security.group.mapping", ShellBasedUnixGroupsMapping.class, ShellBasedUnixGroupsMapping.class);
        configuration.set("hadoop.rpc.socket.factory.class.default", "hbase094.org.apache.hadoop.net.StandardSocketFactory");
        configuration.set(HConstants.REGION_SERVER_CLASS, HConstants.DEFAULT_REGION_SERVER_CLASS);
        configuration.set("hbase.zookeeper.quorum", "localhost:2181");
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
                Put put = new Put(rowValue.getBytes());
                put.add(family.getBytes(), columnQualifier.getBytes(), cellValue.getBytes());
                table.put(put);

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
