package com.unicon.api.commons.db.dao;

import org.apache.ibatis.session.SqlSession;

import java.util.List;
import java.util.Map;

/**
 * Clase utilitaria para usar las sentencias DML junto con myBatis
 *
 * @author acrispin
 * @param <T> tipo del objeto a ser manejado por el DAO
 * @param <U> tipo del Mapper de mybatis que administra los metodos de acceso para el DAO
 */
public abstract class DaoBase<T, U> extends DaoGeneric<U> {
    private static final String DELETE = "delete";
    private static final String INSERT = "insert";
    private static final String UPDATE = "update";
    private static final String SELECT_BY_ID = "selectById";
    private static final String SELECT_ALL = "selectAll";

    /**
     * Obtiene el tipo de la clase que se usa para las operaciones DML de <code>insert</code>, <code>update</code> y los resultados de los <code>select</code>
     *
     * @return Tipo de la clase a usar
     */
    protected abstract Class<T> getClassType();

    public DaoBase() {
    }

    public DaoBase(String idMarca) {
        super(idMarca);
    }

    /**
     * Ejecuta una operación <code>select</code> para obtener una lista de objetos. Se requiere que en mapper se tenga la operación <code>selectAll</code>
     *
     * @param params lista de objetos que son usados dentro la consulta
     * @return lista de objetos
     */
    public List<T> selectAll(Map<String, Object> params) {
        return queryList(SELECT_ALL, params);
    }

    /**
     * Ejecuta una operación <code>select</code> para obtener una lista de objetos. Se requiere que en mapper se tenga la operación <code>selectAll</code>
     *
     * @param params lista de objetos que son usados dentro la consulta
     * @return lista de objetos
     */
    public List<T> selectAll(Object... params) {
        return queryList(SELECT_ALL, params);
    }

    /**
     * Ejecuta una operación <code>select</code> para obtener una lista de objetos. Se requiere que en mapper se tenga la operación <code>selectAll</code>
     *
     * @return lista de objetos
     */
    public List<T> selectAll() {
        return queryList(SELECT_ALL);
    }


    /**
     * Ejecuta una operación <code>select</code> para obtener un único objeto. Se requiere que en mapper se tenga la operación <code>select</code>
     *
     * @param params lista de objetos que son usados dentro la consulta
     * @return objeto
     */
    public T selectById(Map<String, Object> params) {
        return queryObject(getClassType(), SELECT_BY_ID, params);
    }

    /**
     * Ejecuta una operación <code>select</code> para obtener un único objeto. Se requiere que en mapper se tenga la operación <code>select</code>
     *
     * @param params lista de objetos que son usados dentro la consulta
     * @return objeto
     */
    public T selectById(Object... params) {
        return queryObject(getClassType(), SELECT_BY_ID, params);
    }

    /**
     * Ejecuta una operación <code>select</code> para obtener un único objeto. Se requiere que en mapper se tenga la operación <code>select</code>
     *
     * @param obj objeto cuyos atributos son utiliados dentro de la consulta
     * @return objeto
     */
    public T selectById(T obj) {
        return queryObject(getClassType(), SELECT_BY_ID, obj);
    }

    /**
     * Ejecuta una operación <code>insert</code> como parte de una transacción. Se requiere que en mapper se tenga la operación <code>insert</code>
     *
     * @param session referencia a la sesión en la que se esta procesado el insert
     * @param obj objeto a insertar
     * @return true si se pudo insertar bien, caso contrario false
     */
    public boolean insert(SqlSession session, T obj) {
        Integer result = executeDml(session, INSERT, getClassType(), obj);
        return result != null && result >= 0;
    }

    /**
     * Ejecuta una operación <code>insert</code> única. Se requiere que en mapper se tenga la operación <code>insert</code>
     *
     * @param obj objeto a insertar
     * @return true si se pudo insertar bien, caso contrario false
     */
    public boolean insert(T obj) {
        Integer result = executeDml(INSERT, getClassType(), obj);
        return result != null && result >= 0;
    }

    /**
     * Ejecuta una operación <code>update</code> como parte de una transacción. Se requiere que en mapper se tenga la operación <code>update</code>
     *
     * @param session referencia a la sesión en la que se esta procesado el update
     * @param obj objeto a actualizar
     * @return true si se pudo actualizar bien, caso contrario false
     */
    public boolean update(SqlSession session, T obj) {
        Integer result = executeDml(session, UPDATE, getClassType(), obj);
        return result != null && result >= 0;
    }

    /**
     * Ejecuta una operación <code>update</code> única. Se requiere que en mapper se tenga la operación <code>update</code>
     *
     * @param obj objeto a actualizar
     * @return true si se pudo actualizar bien, caso contrario false
     */
    public boolean update(T obj) {
        Integer result = executeDml(UPDATE, getClassType(), obj);
        return result != null && result >= 0;
    }

    /**
     * Ejecuta una operación <code>delete</code> como parte de una transacción. Se requiere que en mapper se tenga la operación <code>delete</code>
     *
     * @param session referencia a la sesión en la que se esta procesado el delete
     * @param obj objeto a eliminar
     * @return true si se pudo eliminar bien, caso contrario false
     */
    public boolean delete(SqlSession session, T obj) {
        Integer result = executeDml(session, DELETE, getClassType(), obj);
        return result != null && result >= 0;
    }

    /**
     * Ejecuta una operación <code>delete</code> única. Se requiere que en mapper se tenga la operación <code>delete</code>
     *
     * @param obj objeto a eliminar
     * @return true si se pudo eliminar bien, caso contrario false
     */
    public boolean delete(T obj) {
        Integer result = executeDml(DELETE, getClassType(), obj);
        return result != null && result >= 0;
    }

    /**
     * Ejecuta una operación <code>delete</code> como parte de una transacción. Se requiere que en mapper se tenga la operación <code>delete</code>
     *
     * @param session referencia a la sesión en la que se esta procesado el update
     * @param params lista de objetos que son usados dentro la operación
     * @return true si se pudo insertar bien, caso contrario false
     */
    public boolean delete(SqlSession session, Map<String, Object> params) {
        Integer result = executeDml(session, DELETE, Map.class, params);
        return result != null && result >= 0;
    }

    /**
     * Ejecuta una operación <code>delete</code> única. Se requiere que en mapper se tenga la operación <code>delete</code>
     *
     * @param params lista de objetos que son usados dentro la operación
     * @return true si se pudo insertar bien, caso contrario false
     */
    public boolean delete(Map<String, Object> params) {
        Integer result = executeDml(DELETE, Map.class, params);
        return result != null && result >= 0;
    }
}
