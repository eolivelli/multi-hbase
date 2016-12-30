
import org.apache.hadoop.hbase.util.Bytes;


/**
 * A single cell
 *
 * @author enrico.olivelli
 */
public class RawCell {

    public final byte[] row;
    public final byte[] family;
    public final byte[] column;
    public final long version;
    public final byte[] value;

    public RawCell(byte[] row, byte[] family, byte[] column, long version, byte[] value) {
        this.row = row;
        this.column = column;
        this.value = value;
        this.version = version;
        this.family = family;
    }

    @Override
    public String toString() {
        return "RawCell{" + "row=" + Bytes.toString(row)
            + ", family=" + Bytes.toString(family)
            + ", column=" + Bytes.toString(column)
            + ", version=" + version
            + ", value=" + Bytes.toString(value) + '}';
    }



}
