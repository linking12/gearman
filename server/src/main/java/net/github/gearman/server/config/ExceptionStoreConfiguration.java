package net.github.gearman.server.config;

import java.sql.SQLException;

import net.github.gearman.engine.storage.ExceptionStorageEngine;
import net.github.gearman.engine.storage.MemoryExceptionStorageEngine;
import net.github.gearman.engine.storage.MysqlExceptionStorageEngine;
import net.github.gearman.engine.storage.NoopExceptionStorageEngine;
import net.github.gearman.engine.storage.PostgresExceptionStorageEngine;
import net.github.gearman.server.config.persistence.DataBaseConfiguration;

public class ExceptionStoreConfiguration {

    private static final String    ENGINE_MEMORY      = "memory";
    private static final String    ENGINE_POSTGRES    = "postgres";
    private static final String    ENGINE_MYSQL       = "mysql";
    private static final int       MAX_MEMORY_ENTRIES = 5000;

    private DataBaseConfiguration  dbSQL;
    private String                 engine;
    private ExceptionStorageEngine exceptionStorageEngine;

    public String getEngine() {
        return engine;
    }

    public void setEngine(String engine) {
        this.engine = engine;
    }

    public DataBaseConfiguration getDbSQL() {
        return dbSQL;
    }

    public void setDbSQL(DataBaseConfiguration dbSQL) {
        this.dbSQL = dbSQL;
    }

    public ExceptionStorageEngine getExceptionStorageEngine() {
        if (exceptionStorageEngine == null) {
            switch (getEngine()) {
                case ENGINE_MEMORY:
                    exceptionStorageEngine = new MemoryExceptionStorageEngine(MAX_MEMORY_ENTRIES);
                    break;
                case ENGINE_POSTGRES:
                    try {
                        exceptionStorageEngine = new PostgresExceptionStorageEngine(dbSQL.getHost(), dbSQL.getPort(),
                                                                                    dbSQL.getDbName(), dbSQL.getUser(),
                                                                                    dbSQL.getPassword(),
                                                                                    dbSQL.getTable());
                    } catch (SQLException e) {
                        e.printStackTrace();
                        exceptionStorageEngine = new NoopExceptionStorageEngine();
                    }
                    break;
                case ENGINE_MYSQL:
                    try {
                        exceptionStorageEngine = new MysqlExceptionStorageEngine(dbSQL.getHost(), dbSQL.getPort(),
                                                                                 dbSQL.getDbName(), dbSQL.getUser(),
                                                                                 dbSQL.getPassword(), dbSQL.getTable());
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
