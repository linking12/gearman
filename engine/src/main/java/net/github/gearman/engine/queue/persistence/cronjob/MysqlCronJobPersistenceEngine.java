package net.github.gearman.engine.queue.persistence.cronjob;

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
import net.github.gearman.engine.core.cronjob.CronJob;

public class MysqlCronJobPersistenceEngine implements CronJobPersistenceEngine {

    private static Logger    LOG           = LoggerFactory.getLogger(CronJobPersistenceEngine.class);
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
    private final Timer      writeTimer, readTimer;
    private final Counter    deleteCounter, writeCounter, pendingCounter;

    public MysqlCronJobPersistenceEngine(final String hostname, final int port, final String database,
                                         final String user, final String password, final String tableName,
                                         final MetricRegistry metricRegistry) throws SQLException{
        this.pendingCounter = metricRegistry.counter("mysql.pending");
        this.writeTimer = metricRegistry.timer("mysql.write");
        this.readTimer = metricRegistry.timer("mysql.read");
        this.writeCounter = metricRegistry.counter("mysql.write.count");
        this.deleteCounter = metricRegistry.counter("mysql.delete.count");
        this.url = "jdbc:mysql://" + hostname + ":" + port + "/" + database;
        this.tableName = tableName;
        this.updateJobQuery = String.format("UPDATE %s SET job_handle = ?, priority = ?, cronExpression = ?, json_data = ? WHERE unique_id = ? AND function_name = ?",
                                            tableName);
        this.insertJobQuery = String.format("INSERT INTO %s (unique_id, function_name, cronExpression, priority, job_handle, json_data) VALUES (?, ?, ?, ?, ?, ?)",
                                            tableName);
        this.deleteJobQuery = String.format("DELETE FROM %s WHERE function_name = ? AND unique_id = ?", tableName);
        this.findJobQuery = String.format("SELECT * FROM %s WHERE function_name = ? AND unique_id = ?", tableName);
        this.readAllJobsQuery = String.format("SELECT function_name, priority, unique_id, cronExpression FROM %s LIMIT ? OFFSET ?",
                                              tableName);
        this.countQuery = String.format("SELECT COUNT(*) AS jobCount FROM %s", tableName);
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

    private boolean validateOrCreateTable() {
        PreparedStatement st = null;
        Connection conn = null;
        boolean success = false;
        try {
            conn = connectionPool.getConnection();
            if (conn != null) {
                DatabaseMetaData dbm = conn.getMetaData();
                ResultSet tables = dbm.getTables(null, null, tableName, null);
                if (!tables.next()) {
                    final String createQuery = String.format("CREATE TABLE %s(id bigint(20) NOT NULL AUTO_INCREMENT, unique_id varchar(255), priority varchar(50), function_name varchar(255), cronExpression varchar(255), job_handle varchar(244), json_data text,KEY(id))",
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

    @Override
    public boolean write(CronJob job) {
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
                st.setString(3, job.getCronExpression());
                st.setString(4, jobJSON);
                st.setString(5, job.getUniqueID());
                st.setString(6, job.getFunctionName());
                int updated = st.executeUpdate();

                // No updates, insert a new record.
                if (updated == 0) {
                    st = conn.prepareStatement(insertJobQuery);
                    st.setString(1, job.getUniqueID());
                    st.setString(2, job.getFunctionName());
                    st.setString(3, job.getCronExpression());
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
    public void delete(CronJob job) {
        this.delete(job.getFunctionName(), job.getUniqueID());
    }

    @Override
    public void delete(String functionName, String uniqueID) {
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
    public CronJob findJob(String functionName, String uniqueID) {
        PreparedStatement st = null;
        ResultSet rs = null;
        Connection conn = null;
        Timer.Context timer = readTimer.time();
        CronJob job = null;
        try {
            conn = connectionPool.getConnection();
            if (conn != null) {
                st = conn.prepareStatement(findJobQuery);
                st.setString(1, functionName);
                st.setString(2, uniqueID);

                ObjectMapper mapper = new ObjectMapper();
                rs = st.executeQuery();

                if (rs.next()) {
                    final String cronExpression = rs.getString("cronExpression");
                    final String jobJSON = rs.getString("json_data");
                    Job realJob = mapper.readValue(jobJSON, Job.class);
                    job = new CronJob(cronExpression, realJob);
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
    public Collection<CronJob> readAll() {
        LinkedList<CronJob> jobs = new LinkedList<>();
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
                                ObjectMapper mapper = new ObjectMapper();
                                final String cronExpression = rs.getString("cronExpression");
                                final String jobJSON = rs.getString("json_data");
                                Job job = mapper.readValue(jobJSON, Job.class);
                                jobs.add(new CronJob(cronExpression, job));
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
}
