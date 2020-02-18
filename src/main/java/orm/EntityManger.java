package orm;

import annotations.Column;
import annotations.Entity;
import annotations.Id;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class EntityManger<E> implements DbContext<E> {

    private Connection connection;

    public EntityManger(Connection connection) {
        this.connection = connection;
    }

    @Override
    public <E1> void doCreate(Class<E1> entity) throws SQLException {
        String query = "CREATE TABLE " + this.getTableName(entity) + " (";
        String columnDefinition = "";

        Field[] fields = entity.getDeclaredFields();

        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            field.setAccessible(true);

            columnDefinition += this.getColumnName(field) + " "
                    + this.getDBType(field);

            if (field.isAnnotationPresent(Id.class)) {
                columnDefinition += " PRIMARY KEY AUTO_INCREMENT";
            }

            if (i < fields.length - 1) {
                columnDefinition += ",\n";
            }
        }

        query += columnDefinition + ")";

        connection.prepareStatement(query).execute();
    }

    @Override
    public <E1> void doAlter(Class<E1> entity) throws SQLException {
        String query = "ALTER TABLE " + this.getTableName(entity) + " ADD ";
        Field[] fields = entity.getDeclaredFields();
        List<String> toAdd = new ArrayList<>();
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            field.setAccessible(true);

            if (!this.checkIfColumnExists(field)) {
                toAdd.add(this.getColumnName(field) + " " + this.getDBType(field));
            }
        }

        query += String.join(",", toAdd);

        connection.prepareStatement(query).execute();
    }

    private String getDBType(Field field) {
        String result = "";
        String typeToStr = field.getType().getSimpleName();
        switch (typeToStr) {
            case "int":
            case "Integer":
                result = "int";
                break;
            case "String":
                result = "varchar(50)";
                break;
            case "Date":
                result = "DATETIME";
                break;
        }

        return result;
    }


    public boolean persist(E entity) throws IllegalAccessException, SQLException {
        Field id = this.getId(entity.getClass());
        id.setAccessible(true);
        Object value = id.get(entity);

        if (value == null || (int) value <= 0) {
            return this.doInsert(entity, id);
        }

        return this.doUpdate(entity, id);
    }

    private boolean doInsert(E entity, Field primary) throws IllegalAccessException, SQLException {
        if (!this.checkIfTableExists(entity.getClass())) {
            this.doCreate(entity.getClass());
        } else {
            if (!this.checkIfThereIsNewColumn(entity.getClass().getDeclaredFields())) {
                this.doAlter(entity.getClass());
            }
        }

        String query = "INSERT INTO " + this.getTableName(entity.getClass()) + " ";
        String columns = "(";
        String values = "(";

        Field[] fields = entity.getClass().getDeclaredFields();

        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            field.setAccessible(true);

            if (!field.isAnnotationPresent(Id.class)) {
                columns += "`" + this.getColumnName(field) + "`";

                Object value = field.get(entity);

                if (value instanceof Date) {
                    values += "'" + new SimpleDateFormat("yyyy-MM-dd").format(value) + "'";
                } else if (value instanceof Integer) {
                    values += value;
                } else {
                    values += "'" + value + "'";
                }

                if (i < fields.length - 1) {
                    values += ",";
                    columns += ",";
                }
            }
        }

        query += columns + ") VALUES " + values + ")";
        return connection.prepareStatement(query).execute();
    }

    private boolean doUpdate(E entity, Field primary) throws IllegalAccessException, SQLException {
        String query = "UPDATE " + this.getTableName(entity.getClass()) + " SET ";
        String columnAndValue = "";
        String where = "";
        Field[] fields = entity.getClass().getDeclaredFields();

        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            field.setAccessible(true);

            Object value = field.get(entity);
            if (field.isAnnotationPresent(Id.class)) {
                where += " WHERE " + this.getColumnName(field) + " = " + value;
            } else {

                if (value instanceof Date) {
                    columnAndValue += this.getColumnName(field) + " = '"
                            + new SimpleDateFormat("yyyy-MM-dd").format(value) + "'";
                } else if (value instanceof Integer) {
                    columnAndValue += this.getColumnName(field) + " = " + value;
                } else {
                    columnAndValue += this.getColumnName(field) + " = '" + value + "'";
                }

                if (i < fields.length - 1) {
                    columnAndValue += ",";
                }
            }
        }

        query += columnAndValue + where;
        return connection.prepareStatement(query).execute();
    }


    public Iterable<E> find(Class<E> table) throws InvocationTargetException, SQLException, InstantiationException, IllegalAccessException, NoSuchMethodException {
        return this.find(table, null);
    }

    public Iterable<E> find(Class<E> table, String where) throws SQLException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Statement statement = connection.createStatement();
        String query = "SELECT * FROM " + table.getAnnotation(Entity.class).name() +
                " WHERE 1 " + (where != null ? " AND " + where : "");
        ResultSet resultSet = statement.executeQuery(query);

        List<E> entities = new ArrayList<>();
        while (resultSet.next()) {
            E entity = table.getDeclaredConstructor().newInstance();
            this.fillEntity(table, resultSet, entity);
            entities.add(entity);
        }

        return entities;
    }

    public E findFirst(Class<E> table) throws InvocationTargetException, SQLException, InstantiationException, IllegalAccessException, NoSuchMethodException {
        return this.findFirst(table, null);
    }

    public E findFirst(Class<E> table, String where) throws SQLException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Statement statement = connection.createStatement();
        String query = "SELECT * FROM " + table.getAnnotation(Entity.class).name() +
                " WHERE 1 " + (where != null ? " AND " + where : "") + " LIMIT 1";
        ResultSet resultSet = statement.executeQuery(query);
        E entity = table.getDeclaredConstructor().newInstance();
        resultSet.next();
        this.fillEntity(table, resultSet, entity);
        return entity;
    }

    private Field getId(Class entity) {
        return Arrays.stream(entity.getDeclaredFields())
                .filter(field -> field.isAnnotationPresent(Id.class))
                .findFirst()
                .orElseThrow(() -> new UnsupportedOperationException("Entity doesn't have ID!"));
    }

    private String getTableName(Class entity) {
        String tableName = "";

        tableName = ((Entity) entity.getAnnotation(Entity.class)).name();

        if (tableName.isEmpty()) {
            tableName = entity.getSimpleName();
        }

        return tableName;
    }

    private String getColumnName(Field field) {
        String columnName = field.getAnnotation(Column.class).name();

        if (columnName.isEmpty()) {
            columnName = field.getName();
        }

        return columnName;
    }

    private void fillEntity(Class table, ResultSet resultSet, E entity) throws SQLException, IllegalAccessException {
        Field[] fields = table.getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);

            this.fillField(field, entity, resultSet, this.getColumnName(field));
        }
    }

    private void fillField(Field field, Object instance, ResultSet set, String fieldName) throws SQLException, IllegalAccessException {
        field.setAccessible(true);

        if (field.getType() == Integer.class || field.getType() == int.class) {
            field.set(instance, set.getInt(fieldName));
        } else if (field.getType() == Date.class) {
            field.set(instance, set.getDate(fieldName));
        } else if (field.getType() == String.class) {
            field.set(instance, set.getString(fieldName));
        }
    }

    private boolean checkIfTableExists(Class entity) throws SQLException {
        String query = "SELECT TABLE_NAME FROM information_schema.TABLES\n" +
                "WHERE TABLE_SCHEMA = 'orm_db'\n" +
                "AND TABLE_NAME = '" + this.getTableName(entity) + "'";

        ResultSet rs = connection.prepareStatement(query).executeQuery();

        return rs.next();
    }

    private boolean checkIfColumnExists(Field field) throws SQLException {
        String query = "SELECT `COLUMN_NAME`\n" +
                "FROM `INFORMATION_SCHEMA`.`COLUMNS`\n" +
                "WHERE `TABLE_SCHEMA`='orm_db'\n" +
                "  AND `TABLE_NAME`='users';";

        ResultSet rs = connection.prepareStatement(query).executeQuery();
        while (rs.next()) {

            if (this.getColumnName(field).equals(rs.getString(1))) {
                return true;
            }
        }
        return false;
    }

    private boolean checkIfThereIsNewColumn(Field[] fields) throws SQLException {
        boolean result = true;
        for (Field field : fields) {
            result = this.checkIfColumnExists(field);
            if (!result) {
                return false;
            }
        }

        return result;
    }

    @Override
    public boolean delete(E entity) throws SQLException, IllegalAccessException {
        String query = "DELETE FROM " + this.getTableName(entity.getClass())
                + " WHERE `id` = " ;
        for (Field declaredField : entity.getClass().getDeclaredFields()) {
            declaredField.setAccessible(true);

            if (declaredField.isAnnotationPresent(Id.class)) {
                Object value = declaredField.get(entity);
                query += value;
            }
        }

        return connection.prepareStatement(query).execute();
    }
}
