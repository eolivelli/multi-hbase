
import hbase094.org.apache.hadoop.conf.Configuration;
import hbase094.org.apache.hadoop.hbase.HBaseConfiguration;
import hbase094.org.apache.hadoop.hbase.HColumnDescriptor;
import hbase094.org.apache.hadoop.hbase.HConstants;
import hbase094.org.apache.hadoop.hbase.HTableDescriptor;
import hbase094.org.apache.hadoop.hbase.client.HBaseAdmin;
import hbase094.org.apache.hadoop.hbase.client.HTableInterface;
import hbase094.org.apache.hadoop.hbase.client.HTablePool;
import hbase094.org.apache.hadoop.hbase.client.Result;
import hbase094.org.apache.hadoop.hbase.client.ResultScanner;
import hbase094.org.apache.hadoop.hbase.client.Scan;
import hbase094.org.apache.hadoop.security.ShellBasedUnixGroupsMapping;
import hbase094.org.apache.hadoop.security.UserGroupInformation;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Scans an HBase 0.94 table
 *
 * @author enrico.olivelli
 */
public class HBase0943TableScanner {

    public static void scanHBase094Table(String tableName, String zkAddress, int batchSize, HBase124TableSink consumer) throws Exception {
        Configuration baseDefaults = new Configuration(false);
        baseDefaults.addResource("hbase094_hbase_default.xml");
        baseDefaults.set("hbase.defaults.for.version", "0.94.27");
        Configuration configuration = HBaseConfiguration.create(baseDefaults);
        configuration.setClass("hadoop.security.group.mapping", ShellBasedUnixGroupsMapping.class, ShellBasedUnixGroupsMapping.class);
        configuration.set("hadoop.rpc.socket.factory.class.default", "hbase094.org.apache.hadoop.net.StandardSocketFactory");
        configuration.set(HConstants.REGION_SERVER_CLASS, HConstants.DEFAULT_REGION_SERVER_CLASS);
        configuration.set("hbase.zookeeper.quorum", zkAddress);
        UserGroupInformation.setConfiguration(configuration);

        try (HBaseAdmin admin = new HBaseAdmin(configuration);) {
            if (!admin.isTableAvailable(tableName)) {
                throw new Exception("No such table " + tableName + " on source HBase");
            }
            HTableDescriptor tableDescriptor = admin.getTableDescriptor(tableName.getBytes(StandardCharsets.UTF_8));
            List<byte[]> columns = new ArrayList<>();
            for (HColumnDescriptor col : tableDescriptor.getColumnFamilies()) {
                columns.add(col.getName());
            }
            consumer.beginTable(columns);
        }

        try (HTablePool pool = new HTablePool(configuration, 1);
            HTableInterface table = pool.getTable(tableName)) {
            Scan scan = new Scan();
            scan.setCacheBlocks(false);
            scan.setCaching(batchSize);
            scan.setBatch(batchSize);
            try (ResultScanner scanner = table.getScanner(scan);) {
                Result result = scanner.next();
                while (result != null) {
                    byte[] rowKey = result.getRow();
                    NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> row = result.getMap();
                    for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyMap : row.entrySet()) {
                        byte[] cellFamily = familyMap.getKey();
                        for (Map.Entry<byte[], NavigableMap<Long, byte[]>> columnsMap : familyMap.getValue().entrySet()) {
                            byte[] columnName = columnsMap.getKey();
                            for (Map.Entry<Long, byte[]> cell : columnsMap.getValue().entrySet()) {
                                long version = cell.getKey();
                                byte[] value = cell.getValue();
                                RawCell rawcell = new RawCell(rowKey, cellFamily, columnName, version, value);
                                consumer.accept(rawcell);
                            }
                        }
                    }
                    result = scanner.next();
                }
            }
        }

    }
}
