package com.unicon.api.commons.db.dao;

import com.unicon.api.commons.db.dao.enums.EConnectionType;
import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 * @author acrispin
 * @param <U> Mapper a utilizar por la clase
 */
public abstract class DaoGeneric<U> {

    private String idMarca;

    public String getIdMarca() {
        return idMarca;
    }

    public void setIdMarca(String idMarca) {
        this.idMarca = idMarca;
    }

    public DaoGeneric() {
        this.idMarca = "";
    }

    public DaoGeneric(String idMarca) {
        this.idMarca = idMarca;
    }

    /**
     *
     */
    protected static final Class<?>[] NOPARAMS = {};

    /**
     * @return Logger de la clase hija
     */
    protected abstract Logger getLogger();

    /**
     * @return SqlSessionFactory
     */
    protected abstract SqlSessionFactory getSqlSessionFactoryInnerSingle();

    /**
     * @return SqlSessionFactory
     */
    protected abstract SqlSessionFactory getSqlSessionFactoryInnerSdc(String idMarca);

    /**
     * @return SqlSessionFactory
     */
    protected abstract SqlSessionFactory getSqlSessionFactoryInnerCmd(String idMarca);

    /**
     * @return SqlSessionFactory
     */
    protected abstract EConnectionType getConnectionType();

    /**
     * @return Mapper type
     */
    protected abstract Class<U> getMapperType();

    protected SqlSessionFactory getSqlSessionFactoryInner() {
        switch (getConnectionType()) {
            case SINGLE:
                return getSqlSessionFactoryInnerSingle();
            case SDC:
                return getSqlSessionFactoryInnerSdc(getIdMarca());
            case CMD:
                return getSqlSessionFactoryInnerCmd(getIdMarca());
        }
        return getSqlSessionFactoryInnerSingle();
    }

    private Class<?>[] getParamsClass(Object... params) {
        int max = params.length;
        Class<?>[] paramsClass = new Class[max];
        for (int i = 0; i < max; i++) {
            paramsClass[i] = params[i].getClass();
        }
        return paramsClass;
    }

    /**
     * @param <T>        tipo de los elementos en la lista
     * @param methodName nombre del metodo en el mapper a invocar
     * @param params     parámetros a pasar al método a invocar
     * @return lista de elementos del tipo T
     */
    protected <T> List<T> queryList(String methodName, Object... params) {
        List<T> lista;
        Class<?>[] paramsClass = getParamsClass(params);

        try (SqlSession session = getSqlSessionFactoryInner().openSession(true)) {
            U mapper = session.getMapper(getMapperType());
            Method method = mapper.getClass().getDeclaredMethod(methodName, paramsClass);
            lista = (List<T>) method.invoke(mapper, params);
            if (lista == null) {
                lista = new ArrayList<>(0);
            }
        } catch (PersistenceException | NullPointerException | NoSuchMethodException | IllegalAccessException | InvocationTargetException ex) {
            lista = new ArrayList<>(0);
            getLogger().error(ex.getMessage(), ex);
        }

        return lista;
    }

    /**
     * @param <T>        tipo de los elementos en la lista
     * @param methodName nombre del metodo en el mapper a invocar
     * @param params     parámetros a pasar al método a invocar
     * @return lista de elementos del tipo T
     */
    protected <T> List<T> queryList(String methodName,
                                    Map<String, Object> params) {
        List<T> lista;
        Class<?>[] paramsClass = new Class[1];
        paramsClass[0] = Map.class;
        try (SqlSession session = getSqlSessionFactoryInner().openSession(true)) {
            U mapper = session.getMapper(getMapperType());
            Method method = mapper.getClass().getDeclaredMethod(methodName, paramsClass);
            lista = (List<T>) method.invoke(mapper, params);

        } catch (PersistenceException | NullPointerException | NoSuchMethodException | IllegalAccessException | InvocationTargetException ex) {
            lista = new ArrayList<>(0);
            getLogger().error(ex.getMessage(), ex);
        }

        return lista;
    }

    /**
     * @param <T>        tipo de los elementos en la lista
     * @param methodName nombre del metodo en el mapper a invocar
     * @return lista de elementos del tipo T
     */
    protected <T> List<T> queryList(String methodName) {
        List<T> lista;
        Class<?>[] paramsClass = NOPARAMS;
        try (SqlSession session = getSqlSessionFactoryInner().openSession(true)) {
            U mapper = session.getMapper(getMapperType());
            Method method = mapper.getClass().getDeclaredMethod(methodName, paramsClass);
            lista = (List<T>) method.invoke(mapper);
        } catch (PersistenceException | NullPointerException | NoSuchMethodException | IllegalAccessException | InvocationTargetException ex) {
            lista = new ArrayList<>(0);
            getLogger().error(ex.getMessage(), ex);
        }

        return lista;
    }

    /**
     * @param <T>        tipo del elemento a devolver
     * @param type       clase del objeto a instanciar en caso de excepción
     * @param methodName nombre del método en el mapper a invocar
     * @param params     parámetros a pasar al método a invocar
     * @return objeto del tipo T
     */
    protected <T> T queryObject(Class<T> type, String methodName,
                                Map<String, Object> params) {
        T object = null;
        Class<?>[] paramsClass = new Class[1];
        paramsClass[0] = Map.class;
        try (SqlSession session = getSqlSessionFactoryInner().openSession(true)) {
            U mapper = session.getMapper(getMapperType());
            Method method = mapper.getClass().getDeclaredMethod(methodName, paramsClass);
            if (params != null) {
                object = (T) method.invoke(mapper, params);
            } else {
                object = (T) method.invoke(mapper);
            }
        } catch (PersistenceException | NullPointerException | NoSuchMethodException | IllegalAccessException | InvocationTargetException ex) {
            getLogger().error(ex.getMessage(), ex);
            try {
                object = type.newInstance();
            } catch (InstantiationException | IllegalAccessException ex1) {
                getLogger().fatal(ex1.getMessage(), ex1);
            }
        }

        return object;
    }

    /**
     * @param <T>        tipo del elemento a devolver
     * @param type       clase del objeto a instanciar en caso de excepción
     * @param methodName nombre del método en el mapper a invocar
     * @param params     parámetros a pasar al método a invocar
     * @return objeto del tipo T
     */
    protected <T> T queryObject(Class<T> type, String methodName,
                                Object... params) {
        T object = null;
        Class<?>[] paramsClass = getParamsClass(params);
        try (SqlSession session = getSqlSessionFactoryInner().openSession(true)) {
            U mapper = session.getMapper(getMapperType());
            Method method = mapper.getClass().getDeclaredMethod(methodName, paramsClass);
            if (params != null) {
                object = (T) method.invoke(mapper, params);
            } else {
                object = (T) method.invoke(mapper);
            }

        } catch (PersistenceException | NullPointerException | NoSuchMethodException | IllegalAccessException | InvocationTargetException ex) {
            getLogger().error(ex.getMessage(), ex);
            try {
                object = type.newInstance();
            } catch (InstantiationException | IllegalAccessException ex1) {
                getLogger().fatal(ex1.getMessage(), ex1);
            }
        }

        return object;
    }

    /**
     * @param <T>        tipo del elemento a devolver
     * @param type       clase del objeto a instanciar en caso de excepción
     * @param methodName nombre del método en el mapper a invocar
     * @param bean       objeto cuyos atributos seran usados dentro de la consulta
     * @return objeto del tipo T
     */
    protected <T> T queryObject(Class<T> type, String methodName, T bean) {
        T object = null;
        Class<?>[] paramsClass = new Class[1];
        paramsClass[0] = type;
        try (SqlSession session = getSqlSessionFactoryInner().openSession(true)) {
            U mapper = session.getMapper(getMapperType());
            Method method = mapper.getClass().getDeclaredMethod(methodName, paramsClass);
            if (bean != null) {
                object = (T) method.invoke(mapper, bean);
            } else {
                object = (T) method.invoke(mapper);
            }

        } catch (PersistenceException | NullPointerException | NoSuchMethodException | IllegalAccessException | InvocationTargetException ex) {
            getLogger().error(ex.getMessage(), ex);
            try {
                object = type.newInstance();
            } catch (InstantiationException | IllegalAccessException ex1) {
                getLogger().fatal(ex1.getMessage(), ex1);
            }
        }

        return object;
    }

    /**
     * Ejecuta una sentencia DML (insert,update o delete) como parte de la transaccion a la que pertenece la sesión.
     *
     * @param <T>        objeto que se esta insertando
     * @param <V>        objeto que devuelve el método
     * @param session    sesión del ORM
     * @param param      objeto a insertar
     * @param methodName nombre del método en el mapper a invocar
     * @param paramClass clase del parámetro, para multiparametros usar Map
     * @return Objeto del tipo V
     */
    protected <T, V> V executeDml(SqlSession session, String methodName,
                                  Class<?> paramClass, T param) {

        Class<?>[] paramsClass = new Class[1];
        paramsClass[0] = paramClass;
        V result = null;
        try {
            U mapper = session.getMapper(getMapperType());
            Method method = mapper.getClass().getDeclaredMethod(methodName, paramsClass);

            if (param != null) {
                result = (V) method.invoke(mapper, param);
            } else {
                result = (V) method.invoke(mapper);
            }
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException ex) {
            getLogger().error(ex.getMessage(), ex);
        } catch (InvocationTargetException | PersistenceException | NullPointerException pex) {
            if (getLogger().isDebugEnabled()) {
                getLogger().debug("Error wrapper: %s", pex.getMessage());
            }
            getLogger().error(pex.getCause().getMessage(), pex.getCause());
        }
        return result;
    }

    /**
     * Ejecuta una sentencia DML (insert,update o delete) independiente de la sesión.
     *
     * @param <T>        objeto que se esta insertando
     * @param <V>        objeto que devuelve el método
     * @param param      objeto a insertar
     * @param methodName nombre del método en el mapper a invocar
     * @param paramClass clase del parámetro, para multiparametros usar Map
     * @return true si se ejecuta de forma correcta, caso contrario false
     */
    protected <T, V> V executeDml(String methodName,
                                  Class<?> paramClass, T param) {
        try (SqlSession session = getSqlSessionFactoryInner().openSession(false)) {
            V result = executeDml(session, methodName, paramClass, param);
            if (result != null) {
                session.commit();
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug("Ejecutando commit para methodName %s", methodName);
                }
            } else {
                session.rollback();
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug("Ejecutando rollback para methodName %s", methodName);
                }
            }
            return result;
        }

    }

    /**
     * @param <T>        tipo del elemento a devolver
     * @param session    sesión del ORM
     * @param methodName nombre del método en el mapper a invocar
     * @param params     parámetros a pasar al método a invocar
     * @return objeto del tipo T
     */
    protected <T> T executeDml(SqlSession session, String methodName,
                               Object... params) {
        T result = null;
        Class<?>[] paramsClass = getParamsClass(params);
        try {
            U mapper = session.getMapper(getMapperType());
            Method method = mapper.getClass().getDeclaredMethod(methodName, paramsClass);

            if (params != null) {
                result = (T) method.invoke(mapper, params);
            } else {
                result = (T) method.invoke(mapper);
            }
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException ex) {
            getLogger().error(ex.getMessage(), ex);
        } catch (InvocationTargetException | PersistenceException | NullPointerException pex) {
            if (getLogger().isDebugEnabled()) {
                getLogger().debug("Error wrapper: %s", pex.getMessage());
            }
            getLogger().error(pex.getCause().getMessage(), pex.getCause());
        }
        return result;
    }

    /**
     * Ejecuta una sentencia DML (insert,update o delete) independiente de la sesión.
     *
     * @param <T>        objeto que se esta insertando
     * @param methodName nombre del método en el mapper a invocar
     * @param params     parámetros de la consulta
     * @return true si se ejecuta de forma correcta, caso contrario false
     */
    protected <T> T executeDml(String methodName, Object... params) {
        try (SqlSession session = getSqlSessionFactoryInner().openSession(false)) {
            T result = executeDml(session, methodName, params);
            if (result != null) {
                session.commit();
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug("Ejecutando commit para methodName %s", methodName);
                }
            } else {
                session.rollback();
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug("Ejecutando rollback para methodName %s", methodName);
                }
            }
            return result;
        }

    }
}