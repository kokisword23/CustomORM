package orm;

import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;

public interface DbContext<E> {

    <E> void doCreate (Class<E> entity) throws SQLException;

    <E> void doAlter(Class<E> entity) throws SQLException;

    boolean persist(E entity) throws IllegalAccessException, SQLException;

    Iterable<E> find(Class<E> table) throws InvocationTargetException, SQLException, InstantiationException, IllegalAccessException, NoSuchMethodException;

    Iterable<E> find(Class<E> table, String where) throws SQLException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException;

    E findFirst(Class<E> table) throws InvocationTargetException, SQLException, InstantiationException, IllegalAccessException, NoSuchMethodException;

    E findFirst(Class<E> table, String where) throws SQLException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException;

    boolean delete(E entity) throws SQLException, IllegalAccessException;
}
