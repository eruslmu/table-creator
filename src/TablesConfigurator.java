import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

public class TablesConfigurator {

    private static final String USER_COLUMN_FAMILY = "data";
    private static final String USER_TABLE = "user";
    private static final String VITAL_SIGN_COLUMN_FAMILY = "records";
    private static final String VITAL_SIGN_TABLE = "vital_sign";

    public static void main(String[] args) {
        try {
            Configuration config = HBaseConfiguration.create();
            HBaseAdmin admin = new HBaseAdmin(config);
            // creates a 'user' table if none exists
            if (!admin.tableExists(USER_TABLE)) {
                createUserTable(config);
            }
            // creates a 'vital_sign' table if none exists
            if (!admin.tableExists(VITAL_SIGN_TABLE)) {
                createVitalSignTable(config);
            }
        } catch (IOException ex) {
            System.err.println("\nError occurred during configuring HBase.\n");
            ex.printStackTrace();
        }
    }

    /**
     * Creates a vital sign table in HBase data store.
     *
     * @param config - configuration of HBase
     */
    private static void createVitalSignTable(Configuration config) {
        try {
            HBaseAdmin admin = new HBaseAdmin(config);
            HTableDescriptor tableDescriptor = new HTableDescriptor(VITAL_SIGN_TABLE);
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(VITAL_SIGN_COLUMN_FAMILY);
            tableDescriptor.addFamily(columnDescriptor);
            admin.createTable(tableDescriptor);
        } catch (IOException ex) {
            System.err.println("\nError occurred during adding row to 'vital_sign' table.\n");
            ex.printStackTrace();
        }
    }

    /**
     * Creates a user table in HBase data store.
     *
     * @param config - configuration of HBase
     */
    private static void createUserTable(Configuration config) {
        try {
            HBaseAdmin admin = new HBaseAdmin(config);
            HTableDescriptor tableDescriptor = new HTableDescriptor(USER_TABLE);
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(USER_COLUMN_FAMILY);
            tableDescriptor.addFamily(columnDescriptor);
            admin.createTable(tableDescriptor);
        } catch (IOException ex) {
            System.err.println("\nError occurred during adding row to 'user' table.\n");
            ex.printStackTrace();
        }
    }
}
