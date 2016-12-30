
/**
 * @author enrico.olivelli
 */
public class TableMigrationManager {

    private String sourceZkAddress;
    private String destZkAddress;
    private String sourceTableName;
    private String destTableName;
    private int batchSize;

    public String getSourceZkAddress() {
        return sourceZkAddress;
    }

    public void setSourceZkAddress(String sourceZkAddress) {
        this.sourceZkAddress = sourceZkAddress;
    }

    public String getDestZkAddress() {
        return destZkAddress;
    }

    public void setDestZkAddress(String destZkAddress) {
        this.destZkAddress = destZkAddress;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public void setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
    }

    public String getDestTableName() {
        return destTableName;
    }

    public void setDestTableName(String destTableName) {
        this.destTableName = destTableName;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public void run() throws Exception {
        try (HBase124TableSink sink = new HBase124TableSink(destTableName, destZkAddress, batchSize)) {
            HBase0943TableScanner.scanHBase094Table(sourceTableName, sourceZkAddress, batchSize, sink);
        }
    }

}
