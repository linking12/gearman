package net.github.gearman.server.config;

import java.sql.SQLException;

import net.github.gearman.engine.storage.ExceptionStorageEngine;
import net.github.gearman.engine.storage.MemoryExceptionStorageEngine;
import net.github.gearman.engine.storage.NoopExceptionStorageEngine;
import net.github.gearman.engine.storage.PostgresExceptionStorageEngine;
import net.github.gearman.server.config.persistence.DataBaseConfiguration;

public class ExceptionStoreConfiguration {

    private static final String     ENGINE_MEMORY      = "memory";
    private static final String     ENGINE_POSTGRES    = "postgres";
    private static final int        MAX_MEMORY_ENTRIES = 5000;

    private DataBaseConfiguration postgreSQL;
    private String                  engine;
    private ExceptionStorageEngine  exceptionStorageEngine;

    public String getEngine() {
        return engine;
    }

    public void setEngine(String engine) {
        this.engine = engine;
    }

    public DataBaseConfiguration getPostgreSQL() {
        return postgreSQL;
    }

    public void setPostgreSQL(DataBaseConfiguration postgreSQL) {
        this.postgreSQL = postgreSQL;
    }

    public ExceptionStorageEngine getExceptionStorageEngine() {
        if (exceptionStorageEngine == null) {
            switch (getEngine()) {
                case ENGINE_MEMORY:
                    exceptionStorageEngine = new MemoryExceptionStorageEngine(MAX_MEMORY_ENTRIES);
                    break;
                case ENGINE_POSTGRES:
                    try {
                        exceptionStorageEngine = new PostgresExceptionStorageEngine(postgreSQL.getHost(),
                                                                                    postgreSQL.getPort(),
                                                                                    postgreSQL.getDbName(),
                                                                                    postgreSQL.getUser(),
                                                                                    postgreSQL.getPassword(),
                                                                                    postgreSQL.getTable());
                    } catch (SQLException e) {
                        e.printStackTrace();
                        exceptionStorageEngine = new NoopExceptionStorageEngine();
                    }
                    break;
                default:
                    exceptionStorageEngine = new NoopExceptionStorageEngine();
            }
        }

        return exceptionStorageEngine;
    }
}
