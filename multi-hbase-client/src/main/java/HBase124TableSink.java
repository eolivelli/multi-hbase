
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.security.ShellBasedUnixGroupsMapping;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.function.Consumer;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 * Dumps data to an HBase 1.2.4 table. The table must exist
 *
 * @author enrico.olivelli
 */
public class HBase124TableSink implements AutoCloseable {

    private HTablePool pool;
    private HTableInterface table;
    private Configuration configuration;
    private int batchSize;
    private String tableName;

    private List<Put> buffer;

    public HBase124TableSink(String tableName, String zkAddress, int batchSize) throws Exception {
        this.batchSize = batchSize;
        this.tableName = tableName;
        Configuration baseDefaults = new Configuration();
        baseDefaults.set("hbase.defaults.for.version", "1.2.4");
        configuration = HBaseConfiguration.create(baseDefaults);
        configuration.set("hbase.zookeeper.quorum", zkAddress);
        UserGroupInformation.setConfiguration(configuration);
        pool = new HTablePool(configuration, 1);

        buffer = new ArrayList<>(batchSize);
    }

    public void beginTable(List<byte[]> families) throws Exception {
        try (HBaseAdmin admin = new HBaseAdmin(configuration);) {
            if (!admin.isTableAvailable(tableName)) {
                System.out.println("Table " + tableName + " does not exist on destination server. Creating");
                HTableDescriptor desc = new HTableDescriptor(tableName);
                for (byte[] family : families) {
                    HColumnDescriptor col = new HColumnDescriptor(family);
                    desc.addFamily(col);
                }
                admin.createTable(desc);
                System.out.println("Table " + tableName + " created");
            }
            if (!admin.isTableEnabled(tableName)) {
                System.out.println("table " + tableName + " is not enabled. enabling");
                admin.enableTable(tableName);
            }
        }
        table = pool.getTable(tableName);
    }

    public void accept(RawCell t) throws Exception {
        System.out.println("accept:" + t);

        Put put = new Put(t.row);
        put.addColumn(t.family, t.column, t.version, t.value);
        buffer.add(put);
        if (buffer.size() == batchSize) {
            flushBuffer();
        }
    }

    private void flushBuffer() throws Exception {
        table.batch(buffer);
        buffer.clear();
    }

    @Override
    public void close() throws Exception {
        if (!buffer.isEmpty()) {
            flushBuffer();
        }
    }

}
