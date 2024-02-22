import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import site.ycsb.*;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static site.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY;
import static site.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY_DEFAULT;

public class CassandraCQLClient extends DB {

    private static Cluster cluster = null;
    private static Session session = null;

    private static PreparedStatement readStmt = null;
    private static PreparedStatement insertStmt = null;
    private static PreparedStatement updateStmt = null;

    // Cassandra interprets QUORUM as ANY QUORUM, as we switched their identifiers
    private static ConsistencyLevel readConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM;
    private static ConsistencyLevel writeConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM;

    public static final String YCSB_KEY = "y_id";
    public static final String KEYSPACE_PROPERTY = "cassandra.keyspace";
    public static final String KEYSPACE_PROPERTY_DEFAULT = "ycsb";

    public static final String HOSTS_PROPERTY = "hosts";
    public static final String PORT_PROPERTY = "port";
    public static final String PORT_PROPERTY_DEFAULT = "9042";

    public static final String READ_CONSISTENCY_LEVEL_PROPERTY =
            "cassandra.readconsistencylevel";
    public static final String READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = readConsistencyLevel.name();
    public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY =
            "cassandra.writeconsistencylevel";
    public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = writeConsistencyLevel.name();

    public static final String MAX_CONNECTIONS_PROPERTY =
            "cassandra.maxconnections";
    public static final String CORE_CONNECTIONS_PROPERTY =
            "cassandra.coreconnections";
    public static final String CONNECT_TIMEOUT_MILLIS_PROPERTY =
            "cassandra.connecttimeoutmillis";
    public static final String READ_TIMEOUT_MILLIS_PROPERTY =
            "cassandra.readtimeoutmillis";

    public static final String TRACING_PROPERTY = "cassandra.tracing";
    public static final String TRACING_PROPERTY_DEFAULT = "false";

    /**
     * Count the number of times initialized to teardown on the last
     * {@link #cleanup()}.
     */
    private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

    private static boolean trace = false;

    private final static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
    /**
     * Initialize any state for this DB. Called once per DB instance; there is one
     * DB instance per client thread.
     */
    @Override
    public void init() throws DBException {

        // Keep track of number of calls to init (for later cleanup)
        INIT_COUNT.incrementAndGet();

        // Synchronized so that we only have a single
        // cluster/session instance for all the threads.
        synchronized (INIT_COUNT) {

            // Check if the cluster has already been initialized
            if (cluster != null) {
                return;
            }

            try {
                trace = Boolean.parseBoolean(getProperties().getProperty(TRACING_PROPERTY, TRACING_PROPERTY_DEFAULT));

                String host = getProperties().getProperty(HOSTS_PROPERTY);
                if (host == null) {
                    throw new DBException(String.format(
                            "Required property \"%s\" missing for CassandraCQLClient",
                            HOSTS_PROPERTY));
                }
                String[] hosts = host.split(",");
                String port = getProperties().getProperty(PORT_PROPERTY, PORT_PROPERTY_DEFAULT);

                String keyspace = getProperties().getProperty(KEYSPACE_PROPERTY,
                        KEYSPACE_PROPERTY_DEFAULT);

                readConsistencyLevel = ConsistencyLevel.valueOf(
                        getProperties().getProperty(READ_CONSISTENCY_LEVEL_PROPERTY,
                                READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
                writeConsistencyLevel = ConsistencyLevel.valueOf(
                        getProperties().getProperty(WRITE_CONSISTENCY_LEVEL_PROPERTY,
                                WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));

                String localDc = host.split("-")[0];
                System.err.println(format.format(new Date()) + " LocalDC: " + localDc);
                cluster = Cluster.builder().withPort(Integer.parseInt(port)).addContactPoints(hosts)
                        .withLoadBalancingPolicy(DCAwareRoundRobinPolicy.builder().withLocalDc(localDc).build()).build();

                cluster.getConfiguration().getPoolingOptions().setHeartbeatIntervalSeconds(0);

                System.err.println(format.format(new Date()) + " Hosts: " + host);
                System.err.println(format.format(new Date()) + " Consistency write: " + writeConsistencyLevel);
                System.err.println(format.format(new Date()) + " Consistency read: " + readConsistencyLevel);

                String maxConnections = getProperties().getProperty(
                        MAX_CONNECTIONS_PROPERTY);
                if (maxConnections != null) {
                    cluster.getConfiguration().getPoolingOptions()
                            .setMaxConnectionsPerHost(HostDistance.LOCAL, Integer.parseInt(maxConnections));
                }

                String coreConnections = getProperties().getProperty(
                        CORE_CONNECTIONS_PROPERTY);
                if (coreConnections != null) {
                    cluster.getConfiguration().getPoolingOptions()
                            .setCoreConnectionsPerHost(HostDistance.LOCAL, Integer.parseInt(coreConnections));
                }

                String connectTimoutMillis = getProperties().getProperty(
                        CONNECT_TIMEOUT_MILLIS_PROPERTY);
                if (connectTimoutMillis != null) {
                    cluster.getConfiguration().getSocketOptions()
                            .setConnectTimeoutMillis(Integer.parseInt(connectTimoutMillis));
                }
                String readTimoutMillis = getProperties().getProperty(
                        READ_TIMEOUT_MILLIS_PROPERTY);
                if (readTimoutMillis != null) {
                    cluster.getConfiguration().getSocketOptions()
                            .setReadTimeoutMillis(Integer.parseInt(readTimoutMillis));
                }

                Metadata metadata = cluster.getMetadata();
                System.err.println(format.format(new Date()) + " Connected to cluster: " + metadata.getClusterName());

                session = cluster.connect(keyspace);

                String table = getProperties().getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);
                System.err.println(format.format(new Date()) + " Preparing statements:");

                Select.Selection selectBuilder = QueryBuilder.select();
                selectBuilder.column("field0");
                readStmt = session.prepare(selectBuilder.from(table).where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker())).limit(1));
                readStmt.setConsistencyLevel(readConsistencyLevel);
                if (trace) readStmt.enableTracing();

                Update updateBuilder = QueryBuilder.update(table);
                updateBuilder.with(QueryBuilder.set("field0", QueryBuilder.bindMarker()));
                updateBuilder.where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker()));
                updateStmt = session.prepare(updateBuilder);
                updateStmt.setConsistencyLevel(writeConsistencyLevel);
                if (trace) updateStmt.enableTracing();

                Insert insertBuilder = QueryBuilder.insertInto(table);
                insertBuilder.value(YCSB_KEY, QueryBuilder.bindMarker());
                insertBuilder.value("field0", QueryBuilder.bindMarker());
                insertStmt = session.prepare(insertBuilder);
                insertStmt.setConsistencyLevel(writeConsistencyLevel);
                if (trace) insertStmt.enableTracing();

            } catch (Exception e) {
                throw new DBException(e);
            }
        } // synchronized
    }

    /**
     * Cleanup any state for this DB. Called once per DB instance; there is one DB
     * instance per client thread.
     */
    @Override
    public void cleanup() throws DBException {
        synchronized (INIT_COUNT) {
            final int curInitCount = INIT_COUNT.decrementAndGet();
            if (curInitCount <= 0) {
                session.close();
                cluster.close();
                cluster = null;
                session = null;
            }
            if (curInitCount < 0) {
                // This should never happen.
                throw new DBException(
                        String.format("initCount is negative: %d", curInitCount));
            }
        }
    }

    /**
     * Read a record from the database. Each field/value pair from the result will
     * be stored in a HashMap.
     *
     * @param table
     *          The name of the table
     * @param key
     *          The record key of the record to read.
     * @param fields
     *          The list of fields to read, or null for all of them
     * @param result
     *          A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error
     */
    @Override
    public Status read(String table, String key, Set<String> fields,
                       Map<String, ByteIterator> result) {
        try {
            ResultSet rs = session.execute(readStmt.bind(key));

            if (rs.isExhausted()) {
                return Status.NOT_FOUND;
            }

            // Should be only 1 row
            Row row = rs.one();
            ColumnDefinitions cd = row.getColumnDefinitions();

            for (ColumnDefinitions.Definition def : cd) {
                ByteBuffer val = row.getBytesUnsafe(def.getName());
                if (val != null) {
                    result.put(def.getName(), new ByteArrayByteIterator(val.array()));
                } else {
                    result.put(def.getName(), null);
                }
            }

            return Status.OK;

        } catch (NoHostAvailableException e) {
            System.err.println(format.format(new Date()) + " Error reading partition: " + table + " " + e);
        } catch (Exception e) {
            System.err.println(format.format(new Date()) + " Error reading partition: " + table + " " + e);
            System.exit(1);
        }
        return Status.ERROR;
    }


    /**
     * Update a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key, overwriting any existing values with the same field name.
     *
     * @param table
     *          The name of the table
     * @param key
     *          The record key of the record to write.
     * @param values
     *          A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error
     */
    @Override
    public Status update(String table, String key, Map<String, ByteIterator> values) {

        try {
            PreparedStatement stmt = updateStmt;

            // Add fields
            ColumnDefinitions vars = stmt.getVariables();
            BoundStatement boundStmt = stmt.bind();
            for (int i = 0; i < vars.size() - 1; i++) {
                boundStmt.setString(i, values.get(vars.getName(i)).toString());
            }

            // Add key
            boundStmt.setString(vars.size() - 1, key);

            session.execute(boundStmt);

            return Status.OK;
        } catch (NoHostAvailableException e) {
            System.err.println(format.format(new Date()) + " Error updating partition: " + table + " " + e);
        } catch (Exception e) {
            System.err.println(format.format(new Date()) + " Error updating partition: " + table + " " + e);
            System.exit(1);
        }
        return Status.ERROR;
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key.
     *
     * @param table
     *          The name of the table
     * @param key
     *          The record key of the record to insert.
     * @param values
     *          A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error
     */
    @Override
    public Status insert(String table, String key, Map<String, ByteIterator> values) {

        try {
            PreparedStatement stmt = insertStmt;
            // Add key
            BoundStatement boundStmt = stmt.bind().setString(0, key);

            // Add fields
            ColumnDefinitions vars = stmt.getVariables();
            for (int i = 1; i < vars.size(); i++) {
                boundStmt.setString(i, values.get(vars.getName(i)).toString());
            }

            session.execute(boundStmt);

            return Status.OK;
        } catch (NoHostAvailableException e) {
            System.err.println(format.format(new Date()) + " Error inserting partition: " + table + " " + e);
        } catch (Exception e) {
            System.err.println(format.format(new Date()) + " Error inserting partition: " + table + " " + e);
            System.exit(1);
        }
        return Status.ERROR;
    }


    @Override
    public Status delete(String table, String key) {
        throw new AssertionError();
    }

    @Override
    public Status scan(String t, String sK, int rC, Set<String> f, Vector<HashMap<String, ByteIterator>> r) {
        throw new AssertionError();
    }
}
