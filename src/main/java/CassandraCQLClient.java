import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class CassandraCQLClient extends DB {

    private static Cluster cluster = null;
    private static Session session = null;

    private static final ConcurrentMap<Set<String>, PreparedStatement> readStmts = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Set<String>, PreparedStatement> insertStmts = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Set<String>, PreparedStatement> updateStmts = new ConcurrentHashMap<>();

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


                cluster = Cluster.builder().withPort(Integer.parseInt(port)).addContactPoints(hosts)
                        .withLoadBalancingPolicy(new RoundRobinPolicy()).build();

                cluster.getConfiguration().getPoolingOptions().setHeartbeatIntervalSeconds(0);

                System.err.println("Hosts: " + host);
                System.err.println("Consistency write: " + writeConsistencyLevel);
                System.err.println("Consistency read: " + readConsistencyLevel);

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
                System.err.println("Connected to cluster: " + metadata.getClusterName());

                session = cluster.connect(keyspace);

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
                readStmts.clear();
                insertStmts.clear();
                updateStmts.clear();
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
            PreparedStatement stmt = readStmts.get(fields);

            // Prepare statement on demand
            if (stmt == null) {
                Select.Builder selectBuilder;

                selectBuilder = QueryBuilder.select();
                for (String col : fields) {
                    ((Select.Selection) selectBuilder).column(col);
                }

                stmt = session.prepare(selectBuilder.from(table)
                        .where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker())).limit(1));
                stmt.setConsistencyLevel(readConsistencyLevel);

                if (trace) stmt.enableTracing();

                PreparedStatement prevStmt = readStmts.putIfAbsent(new HashSet<>(fields), stmt);
                if (prevStmt != null) {
                    stmt = prevStmt;
                }
            }

            ResultSet rs = session.execute(stmt.bind(key));

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
            System.err.println("Error reading partition: " + table + " " + e);
        } catch (Exception e) {
            System.err.println("Error reading partition: " + table + " " + e);
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
            Set<String> fields = values.keySet();
            PreparedStatement stmt = updateStmts.get(fields);

            // Prepare statement on demand
            if (stmt == null) {
                Update updateStmt = QueryBuilder.update(table);

                // Add fields
                for (String field : fields) {
                    updateStmt.with(QueryBuilder.set(field, QueryBuilder.bindMarker()));
                }

                // Add key
                updateStmt.where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker()));

                stmt = session.prepare(updateStmt);
                stmt.setConsistencyLevel(writeConsistencyLevel);
                if (trace) stmt.enableTracing();

                PreparedStatement prevStmt = updateStmts.putIfAbsent(new HashSet<>(fields), stmt);
                if (prevStmt != null) {
                    stmt = prevStmt;
                }
            }

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
            System.err.println("Error updating partition: " + table + " " + e);
        } catch (Exception e) {
            System.err.println("Error updating partition: " + table + " " + e);
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
            Set<String> fields = values.keySet();
            PreparedStatement stmt = insertStmts.get(fields);

            // Prepare statement on demand
            if (stmt == null) {
                Insert insertStmt = QueryBuilder.insertInto(table);

                // Add key
                insertStmt.value(YCSB_KEY, QueryBuilder.bindMarker());

                // Add fields
                for (String field : fields) {
                    insertStmt.value(field, QueryBuilder.bindMarker());
                }

                stmt = session.prepare(insertStmt);
                stmt.setConsistencyLevel(writeConsistencyLevel);
                if (trace) stmt.enableTracing();

                PreparedStatement prevStmt = insertStmts.putIfAbsent(new HashSet(fields), stmt);
                if (prevStmt != null) {
                    stmt = prevStmt;
                }
            }

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
            System.err.println("Error inserting partition: " + table + " " + e);
        } catch (Exception e) {
            System.err.println("Error inserting partition: " + table + " " + e);
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
