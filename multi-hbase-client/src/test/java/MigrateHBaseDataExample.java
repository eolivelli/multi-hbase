
import org.junit.Test;

/**
 * Example of migration
 *
 * @author enrico.olivelli
 */
public class MigrateHBaseDataExample {

    @Test
    public void test() throws Exception {
        TableMigrationManager manager = new TableMigrationManager();
        manager.setSourceTableName("test_table_2");
        manager.setSourceZkAddress("localhost:2181");
        manager.setDestTableName("test_table_copy_2");
        manager.setDestZkAddress("new-hbase-server:2181");
        manager.run();
    }
}
