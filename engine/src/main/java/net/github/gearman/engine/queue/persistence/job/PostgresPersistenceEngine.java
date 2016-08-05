package net.github.gearman.engine.queue.persistence.job;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;

import net.github.gearman.common.Job;
import net.github.gearman.constants.JobPriority;
import net.github.gearman.engine.core.QueuedJob;

public class PostgresPersistenceEngine implements JobPersistenceEngine {

    private static Logger    LOG           = LoggerFactory.getLogger(PostgresPersistenceEngine.class);
    private static final int JOBS_PER_PAGE = 5000;
    private final String     url;
    private final String     tableName;
    private final BoneCP     connectionPool;

    private final String     updateJobQuery;
    private final String     insertJobQuery;
    private final String     deleteJobQuery;
    private final String     findJobQuery;
    private final String     readAllJobsQuery;
    private final String     countQuery;
    private final String     findAllJobsForFunctionQuery;
    private final String     findJobByHandleQuery;
    private final Timer      writeTimer, readTimer;
    private final Counter    deleteCounter, writeCounter, pendingCounter;

    public PostgresPersistenceEngine(final String hostname, final int port, final String database, final String user,
                                     final String password, final String tableName,
                                     final MetricRegistry metricRegistry) throws SQLException{
        this.pendingCounter = metricRegistry.counter("postgresql.pending");
        this.writeTimer = metricRegistry.timer("postgresql.write");
        this.readTimer = metricRegistry.timer("postgresql.read");
        this.writeCounter = metricRegistry.counter("postgresql.write");
        this.deleteCounter = metricRegistry.counter("postgresql.delete");

        this.url = "jdbc:postgresql://" + hostname + ":" + port + "/" + database;
        this.tableName = tableName;

        this.updateJobQuery = String.format("UPDATE %s SET job_handle = ?, priority = ?, time_to_run = ?, json_data = ? WHERE unique_id = ? AND function_name = ?",
                                            tableName);
        this.insertJobQuery = String.format("INSERT INTO %s (unique_id, function_name, time_to_run, priority, job_handle, json_data) VALUES (?, ?, ?, ?, ?, ?)",
                                            tableName);
        this.deleteJobQuery = String.format("DELETE FROM %s WHERE function_name = ? AND unique_id = ?", tableName);
        this.findJobQuery = String.format("SELECT * FROM %s WHERE function_name = ? AND unique_id = ?", tableName);
        this.readAllJobsQuery = String.format("SELECT function_name, priority, unique_id, time_to_run FROM %s LIMIT ? OFFSET ?",
                                              tableName);
        this.countQuery = String.format("SELECT COUNT(*) AS jobCount FROM %s", tableName);
        this.findAllJobsForFunctionQuery = String.format("SELECT unique_id, time_to_run, priority FROM %s WHERE function_name = ?",
                                                         tableName);
        this.findJobByHandleQuery = String.format("SELECT * FROM %s WHERE job_handle = ?", tableName);

        final BoneCPConfig config = new BoneCPConfig();
        config.setJdbcUrl(this.url);
        config.setUsername(user);
        config.setPassword(password);
        config.setMinConnectionsPerPartition(10);
        config.setMaxConnectionsPerPartition(20);
        config.setPartitionCount(1);

        connectionPool = new BoneCP(config);

        if (!validateOrCreateTable()) {
            throw new SQLException("Unable to validate or create jobs table '" + tableName + "'. Check credentials.");
        }
    }

    @Override
    public String getIdentifier() {
        String result = url;

        try {
            Connection connection = connectionPool.getConnection();
            DatabaseMetaData metaData = connection.getMetaData();
            int majorVersion, minorVersion;
            String productName, productVersion;

            majorVersion = metaData.getDatabaseMajorVersion();
            minorVersion = metaData.getDatabaseMinorVersion();
            productName = metaData.getDatabaseProductName();
            productVersion = metaData.getDatabaseProductVersion();
            result = String.format("%s (%s v%d.%d) - %s", productName, productVersion, majorVersion, minorVersion, url);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return result;
    }

    @Override
    public boolean write(final Job job) {
        Timer.Context context = writeTimer.time();
        PreparedStatement st = null;
        Connection conn = null;
        ObjectMapper mapper = new ObjectMapper();

        try {
            conn = connectionPool.getConnection();
            if (conn != null) {
                String jobJSON = mapper.writeValueAsString(job);

                // Update an existing job if one exists based on unique id
                st = conn.prepareStatement(updateJobQuery);
                st.setString(1, job.getJobHandle());
                st.setString(2, job.getPriority().toString());
                st.setLong(3, job.getTimeToRun());
                st.setString(4, jobJSON);
                st.setString(5, job.getUniqueID());
                st.setString(6, job.getFunctionName());
                int updated = st.executeUpdate();

                // No updates, insert a new record.
                if (updated == 0) {
                    st = conn.prepareStatement(insertJobQuery);
                    st.setString(1, job.getUniqueID());
                    st.setString(2, job.getFunctionName());
                    st.setLong(3, job.getTimeToRun());
                    st.setString(4, job.getPriority().toString());
                    st.setString(5, job.getJobHandle());
                    st.setString(6, jobJSON);
                    int inserted = st.executeUpdate();
                    LOG.debug("Inserted " + inserted + " records for UUID " + job.getUniqueID());
                }
            }

            writeCounter.inc();
            pendingCounter.inc();
            return true;
        } catch (SQLException se) {
            LOG.error("SQL Error writing job: ", se);
            return false;
        } catch (IOException e) {
            LOG.error("I/O Error writing job: ", e);
            return false;
        } finally {
            context.stop();
            try {
                if (st != null) st.close();

                if (conn != null) conn.close();

            } catch (SQLException innerEx) {
                LOG.debug("Error cleaning up: " + innerEx);
            }
        }
    }

    @Override
    public void delete(final Job job) {
        this.delete(job.getFunctionName(), job.getUniqueID());
    }

    @Override
    public void delete(final String functionName, final String uniqueID) {
        PreparedStatement st = null;
        Connection conn = null;

        try {
            conn = connectionPool.getConnection();
            if (conn != null) {
                st = conn.prepareStatement(deleteJobQuery);
                st.setString(1, functionName);
                st.setString(2, uniqueID);
                int deleted = st.executeUpdate();
                LOG.debug("Deleted " + deleted + " records for " + functionName + "/" + uniqueID);
            }
            deleteCounter.inc();
            pendingCounter.dec();
        } catch (SQLException se) {
            LOG.error("SQL Error deleting job: ", se);
        } finally {
            try {
                if (st != null) st.close();

                if (conn != null) conn.close();

            } catch (SQLException innerEx) {
                LOG.debug("Error cleaning up: " + innerEx);
            }
        }
    }

    @Override
    public void deleteAll() {
        Statement st = null;
        Connection conn = null;
        try {
            conn = connectionPool.getConnection();
            if (conn != null) {
                st = conn.createStatement();
                final String deleteAllQuery = String.format("DELETE FROM %s", tableName);
                int deleted = st.executeUpdate(deleteAllQuery);
                LOG.debug("Deleted " + deleted + " jobs...");
            }
        } catch (SQLException se) {
            LOG.error("SQL Error deleting all jobs: ", se);
        } finally {
            try {
                if (st != null) st.close();

                if (conn != null) conn.close();

            } catch (SQLException innerEx) {
                LOG.debug("Error cleaning up: " + innerEx);
            }
        }
    }

    @Override
    public Job findJob(final String functionName, final String uniqueID) {
        PreparedStatement st = null;
        ResultSet rs = null;
        Connection conn = null;
        Timer.Context timer = readTimer.time();
        Job job = null;

        try {
            conn = connectionPool.getConnection();
            if (conn != null) {
                st = conn.prepareStatement(findJobQuery);
                st.setString(1, functionName);
                st.setString(2, uniqueID);

                ObjectMapper mapper = new ObjectMapper();
                rs = st.executeQuery();

                if (rs.next()) {
                    String jobJSON = rs.getString("json_data");
                    job = mapper.readValue(jobJSON, Job.class);
                } else {
                    LOG.warn("No job for unique ID: " + uniqueID
                             + " -- this could be an internal consistency problem...");
                }
            }
        } catch (SQLException se) {
            LOG.debug(se.toString());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            timer.stop();
            try {
                if (rs != null) rs.close();

                if (st != null) st.close();

                if (conn != null) conn.close();

            } catch (SQLException innerEx) {
                LOG.debug("Error cleaning up: " + innerEx);
            }
        }

        return job;
    }

    @Override
    public Collection<QueuedJob> readAll() {
        LinkedList<QueuedJob> jobs = new LinkedList<>();
        PreparedStatement st = null;
        ResultSet rs = null;
        Connection conn = null;
        // Which page of results are we on?
        int pageNum = 0;

        try {
            conn = connectionPool.getConnection();
            if (conn != null) {

                LOG.debug("Reading all job data from PostgreSQL");
                st = conn.prepareStatement(countQuery);
                rs = st.executeQuery();

                if (rs.next()) {
                    int totalJobs = rs.getInt("jobCount");
                    int fetchedJobs = 0;
                    LOG.debug("Reading " + totalJobs + " jobs from PostgreSQL");
                    do {

                        st.setFetchSize(JOBS_PER_PAGE);
                        st.setMaxRows(JOBS_PER_PAGE);

                        st = conn.prepareStatement(readAllJobsQuery);
                        st.setInt(1, JOBS_PER_PAGE);
                        st.setInt(2, (pageNum * JOBS_PER_PAGE));

                        rs = st.executeQuery();

                        while (rs.next()) {

                            try {
                                final String uniqueId = rs.getString("unique_id");
                                final long timeToRun = rs.getLong("time_to_run");
                                final JobPriority jobPriority = JobPriority.valueOf(rs.getString("priority"));
                                final String functionName = rs.getString("function_name");

                                jobs.add(new QueuedJob(uniqueId, timeToRun, jobPriority, functionName));
                            } catch (Exception e) {
                                LOG.error("Unable to load job '" + rs.getString("unique_id") + "'");
                            }
                            fetchedJobs += 1;
                        }

                        pageNum += 1;
                        LOG.debug("Loaded " + fetchedJobs + "...");
                    } while (fetchedJobs != totalJobs);
                }

            }

        } catch (SQLException se) {
            LOG.debug(se.toString());
        } finally {
            try {
                if (rs != null) rs.close();

                if (st != null) st.close();

                if (conn != null) conn.close();

            } catch (SQLException innerEx) {
                LOG.debug("Error cleaning up: " + innerEx);
            }
        }

        return jobs;
    }

    @Override
    public Collection<QueuedJob> getAllForFunction(final String functionName) {
        LinkedList<QueuedJob> jobs = new LinkedList<>();
        PreparedStatement st = null;
        ResultSet rs = null;
        Connection conn = null;
        QueuedJob job;

        try {
            conn = connectionPool.getConnection();
            if (conn != null) {
                st = conn.prepareStatement(findAllJobsForFunctionQuery);
                st.setString(1, functionName);
                ObjectMapper mapper = new ObjectMapper();
                rs = st.executeQuery();

                while (rs.next()) {
                    job = new QueuedJob(rs.getString("unique_id"), rs.getLong("time_to_run"),
                                        JobPriority.valueOf(rs.getString("priority")), functionName);
                    jobs.add(job);
                }
            }
        } catch (SQLException se) {
            LOG.debug(se.toString());
        } finally {
            try {
                if (rs != null) rs.close();

                if (st != null) st.close();

                if (conn != null) conn.close();

            } catch (SQLException innerEx) {
                LOG.debug("Error cleaning up: " + innerEx);
            }
        }

        return jobs;
    }

    public Job findJobByHandle(String jobHandle) {
        PreparedStatement st = null;
        ResultSet rs = null;
        Connection conn = null;

        Job job = null;

        try {
            conn = connectionPool.getConnection();
            if (conn != null) {
                st = conn.prepareStatement(findJobByHandleQuery);
                st.setString(1, jobHandle);

                ObjectMapper mapper = new ObjectMapper();
                rs = st.executeQuery();

                if (rs.next()) {
                    String jobJSON = rs.getString("json_data");
                    job = mapper.readValue(jobJSON, Job.class);
                } else {
                    LOG.warn("No job for job handle: " + jobHandle
                             + " -- this could be an internal consistency problem...");
                }
            }
        } catch (SQLException se) {
            LOG.debug(se.toString());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (rs != null) rs.close();

                if (st != null) st.close();

                if (conn != null) conn.close();

            } catch (SQLException innerEx) {
                LOG.debug("Error cleaning up: " + innerEx);
            }
        }

        return job;
    }

    private boolean validateOrCreateTable() {
        PreparedStatement st = null;
        ResultSet rs = null;
        Connection conn = null;
        ObjectMapper mapper = new ObjectMapper();
        boolean success = false;

        try {
            conn = connectionPool.getConnection();
            if (conn != null) {
                DatabaseMetaData dbm = conn.getMetaData();
                ResultSet tables = dbm.getTables(null, null, tableName, null);
                if (!tables.next()) {
                    final String createQuery = String.format("CREATE TABLE %s(id bigserial, unique_id varchar(255), priority varchar(50), function_name varchar(255), time_to_run bigint, job_handle text, json_data text)",
                                                             tableName);
                    final String indexUidQuery = String.format("CREATE INDEX %s_unique_id ON %s(unique_id)", tableName,
                                                               tableName);
                    final String indexJobHandleQuery = String.format("CREATE INDEX %s_job_handle ON %s(job_handle)",
                                                                     tableName, tableName);
                    st = conn.prepareStatement(createQuery);
                    st.executeUpdate();
                    st = conn.prepareStatement(indexUidQuery);
                    st.executeUpdate();
                    st = conn.prepareStatement(indexJobHandleQuery);
                    st.executeUpdate();

                    // Make sure it worked
                    ResultSet createdTables = dbm.getTables(null, null, tableName, null);
                    if (createdTables.next()) {
                        LOG.debug("Created jobs table '" + tableName + "'");
                        success = true;
                    } else {
                        LOG.debug("Unable to create jobs table '" + tableName + "'");
                        success = false;
                    }
                } else {
                    LOG.debug("Jobs table '" + tableName + "' already exists.");
                    success = true;
                }
            }
        } catch (SQLException se) {
            se.printStackTrace();
        } finally {
            try {
                if (st != null) st.close();

                if (conn != null) conn.close();

            } catch (SQLException innerEx) {
                innerEx.printStackTrace();
            }
        }

        return success;
    }
}
