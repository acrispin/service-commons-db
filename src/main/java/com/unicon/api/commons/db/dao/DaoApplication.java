package com.unicon.api.commons.db.dao;

import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.ResourceBundle;

/**
 *
 * @author acrispin
 * @param <T> tipo del objeto a ser manejado por el DAO
 * @param <U> tipo del Mapper de mybatis que administra los metodos de acceso para el DAO
 */
public abstract class DaoApplication<T, U> extends DaoBase<T, U> {

    private static final Logger LOGGER = LogManager.getLogger(DaoApplication.class);
    private static final String RESOURCE_FILE = "sqlMapConfig.xml";
    private static final String ENVIROMENT_SINGLE = "single";
    private static final String ENVIROMENT_DIRECT = LOGGER.isTraceEnabled() ? "junit" : "pooled";
    private static SqlSessionFactory sqlSessionFactory;
    private static final Map<String, SqlSessionFactory> SQL_SESSION_FACTORY_MAP_SDC = new HashMap<>();
    private static final Map<String, SqlSessionFactory> SQL_SESSION_FACTORY_MAP_CMD = new HashMap<>();

    protected DaoApplication() {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized SqlSessionFactory getSqlSessionFactoryInner() {
        if (sqlSessionFactory == null) {
            sqlSessionFactory  = init();
        }
        return sqlSessionFactory;
    }

    @Override
    public synchronized SqlSessionFactory getSqlSessionFactoryInnerSdc(String idMarca) {
        if (!SQL_SESSION_FACTORY_MAP_SDC.containsKey(idMarca)) {
            SQL_SESSION_FACTORY_MAP_SDC.put(idMarca, init(idMarca, "sdc"));
        }
        return SQL_SESSION_FACTORY_MAP_SDC.get(idMarca);
    }

    @Override
    public synchronized SqlSessionFactory getSqlSessionFactoryInnerCmd(String idMarca) {
        if (!SQL_SESSION_FACTORY_MAP_CMD.containsKey(idMarca)) {
            SQL_SESSION_FACTORY_MAP_CMD.put(idMarca, init(idMarca, "cmd"));
        }
        return SQL_SESSION_FACTORY_MAP_CMD.get(idMarca);
    }

    private static SqlSessionFactory init() {
        SqlSessionFactory ssf;
        try (InputStream inputStream = Resources.getResourceAsStream(RESOURCE_FILE)) {
            ssf = new SqlSessionFactoryBuilder().build(inputStream, ENVIROMENT_SINGLE);
            if (ssf == null ||
                    ssf.getConfiguration() == null ||
                    ssf.getConfiguration().getEnvironment() == null) {
                throw new PersistenceException(String.format("Environment '%s' incorrecto, se usara conexion directa", ENVIROMENT_SINGLE));
            }
        } catch (IOException | RuntimeException ex) {
            LOGGER.error(ex.getMessage(), ex);
            ssf = initDirect();
        }
        return ssf;
    }

    private static SqlSessionFactory init(String idMarca, String plataforma) {
        SqlSessionFactory ssf;
        String environment = String.format("%s-%s", idMarca, plataforma);
        try (InputStream inputStream = Resources.getResourceAsStream(RESOURCE_FILE)) {
            ssf = new SqlSessionFactoryBuilder().build(inputStream, environment);
            if (ssf == null ||
                    ssf.getConfiguration() == null ||
                    ssf.getConfiguration().getEnvironment() == null) {
                throw new PersistenceException(String.format("Environment por marca '%s' incorrecto, se usara conexion directa", environment));
            }
        } catch (IOException | RuntimeException ex) {
            LOGGER.error(ex.getMessage(), ex);
            ssf = initDirect(environment);
        }
        return ssf;
    }

    private static SqlSessionFactory initDirect() {
        SqlSessionFactory ssf = null;
        if (!LOGGER.isDebugEnabled()) {
            LOGGER.info("Solo se usa conexion directa en modo DEBUG.");
            throw new PersistenceException("Solo se usa conexion directa en modo DEBUG.");
        }
        LOGGER.info("----------------- Usando conexión directa -----------------");
        try {
            ResourceBundle bundle = ResourceBundle.getBundle("application");
            Properties defaultProps = new Properties();
            String urlFormat = bundle.getString("database.test.url");
            String url = String.format(urlFormat,
                    bundle.getString("database.test.server"),
                    bundle.getString("database.test.port"),
                    bundle.getString("database.test.dbname"));
            defaultProps.put("driver", bundle.getString("database.test.driver"));
            defaultProps.put("url", url);
            defaultProps.put("username", bundle.getString("database.test.username"));
            defaultProps.put("password", bundle.getString("database.test.password"));
            try (InputStream inputStream = Resources.getResourceAsStream(RESOURCE_FILE)) {
                ssf = new SqlSessionFactoryBuilder().build(inputStream, ENVIROMENT_DIRECT, defaultProps);
                if (ssf == null ||
                        ssf.getConfiguration() == null ||
                        ssf.getConfiguration().getEnvironment() == null) {
                    throw new PersistenceException(String.format("Environment '%s' incorrecto para conexion directa", ENVIROMENT_DIRECT));
                }
            }
        } catch (IOException | RuntimeException ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new PersistenceException(ex);
        }
        return ssf;
    }

    private static SqlSessionFactory initDirect(String environment) {
        SqlSessionFactory ssf = null;
        if (!LOGGER.isDebugEnabled()) {
            LOGGER.info("Solo se usa conexion directa en modo DEBUG.");
            throw new PersistenceException("Solo se usa conexion directa en modo DEBUG.");
        }
        LOGGER.info("----------------- Usando conexión directa -----------------");
        try {
            ResourceBundle bundle = ResourceBundle.getBundle("application");
            Properties defaultProps = new Properties();
            String urlFormat = bundle.getString("database.test.url");
            String url = String.format(urlFormat,
                    bundle.getString(environment+".database.test.server"),
                    bundle.getString(environment+".database.test.port"),
                    bundle.getString(environment+".database.test.dbname"));
            defaultProps.put("driver", bundle.getString("database.test.driver"));
            defaultProps.put("url", url);
            defaultProps.put("username", bundle.getString(environment+".database.test.username"));
            defaultProps.put("password", bundle.getString(environment+".database.test.password"));
            try (InputStream inputStream = Resources.getResourceAsStream(RESOURCE_FILE)) {
                ssf = new SqlSessionFactoryBuilder().build(inputStream, ENVIROMENT_DIRECT, defaultProps);
                if (ssf == null ||
                        ssf.getConfiguration() == null ||
                        ssf.getConfiguration().getEnvironment() == null) {
                    throw new PersistenceException(String.format("Environment '%s' incorrecto para conexion directa", ENVIROMENT_DIRECT));
                }
            }
        } catch (IOException | RuntimeException ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new PersistenceException(ex);
        }
        return ssf;
    }

    public static synchronized SqlSessionFactory getSqlSessionFactory() {
        if (sqlSessionFactory == null) {
            sqlSessionFactory  = init();
        }
        return sqlSessionFactory;
    }

    public static synchronized SqlSessionFactory getSqlSessionFactorySdc(String idMarca) {
        if (!SQL_SESSION_FACTORY_MAP_SDC.containsKey(idMarca)) {
            SQL_SESSION_FACTORY_MAP_SDC.put(idMarca, init(idMarca, "sdc"));
        }
        return SQL_SESSION_FACTORY_MAP_SDC.get(idMarca);
    }

    public static synchronized SqlSessionFactory getSqlSessionFactoryCmd(String idMarca) {
        if (!SQL_SESSION_FACTORY_MAP_CMD.containsKey(idMarca)) {
            SQL_SESSION_FACTORY_MAP_CMD.put(idMarca, init(idMarca, "cmd"));
        }
        return SQL_SESSION_FACTORY_MAP_CMD.get(idMarca);
    }

}
