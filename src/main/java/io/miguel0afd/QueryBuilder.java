package io.miguel0afd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.DataType;

public class QueryBuilder {

    public static String generateCreateTable(
            String table,
            String cluster,
            List<String> columns,
            List<ColumnType> types,
            String... pks){
        if(pks.length < 1){
            throw new RuntimeException("At least one primary key has to be specified.");
        }
        StringBuilder sb = new StringBuilder("CREATE TABLE ");
        sb.append(table);
        sb.append(" ON CLUSTER ").append(cluster);
        sb.append(" (");
        // Include columns
        sb.append(generateColumns(columns, types));
        // Include primary keys
        sb.append(generatePrimaryKeys(pks));
        sb.append(");");
        return sb.toString();
    }

    private static String generateColumns(List<String> columns, List<ColumnType> types) {
        if(columns.size() != types.size()){
            throw new RuntimeException("Column names & column types lists must have the same size");
        }
        if(columns.size() < 1){
            throw new RuntimeException("Column names & column types lists cannot be empty");
        }
        StringBuilder sb = new StringBuilder();
        Iterator<String> columnsIter = columns.iterator();
        Iterator<ColumnType> typesIter = types.iterator();
        while(columnsIter.hasNext()){
            String column = columnsIter.next();
            ColumnType type = typesIter.next();
            String dataType = String.valueOf(type);
            if(type.getDataType() == DataType.NATIVE){
                dataType = type.getDbType();
            }
            sb.append(column).append(" ").append(dataType);
            if(columnsIter.hasNext()){
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    private static String generatePrimaryKeys(String[] pks) {
        StringBuilder sb = new StringBuilder(", PRIMARY KEY (");
        List<String> primaryKeys = new ArrayList<>(Arrays.asList(pks));
        Iterator<String> iter = primaryKeys.iterator();
        while(iter.hasNext()){
            String pk = iter.next();
            sb.append(pk);
            if(iter.hasNext()){
                sb.append(", ");
            }
        }
        sb.append(")");
        return sb.toString();
    }

    public static String generateInsert(String table, List<String> columns, List<ColumnType> types, String[] values) {
        if(columns.size() != types.size()){
            throw new RuntimeException("Columns & Types must have the same size");
        }
        if(columns.size() != values.length){
            throw new RuntimeException("Columns & Values must have the same size");
        }
        StringBuilder sb = new StringBuilder("INSERT INTO ");
        sb.append(table).append("(");
        sb.append(generateColumns(columns));
        sb.append(") VALUES (");
        sb.append(generateValues(types, values));
        sb.append(");");
        return sb.toString();
    }

    private static String generateColumns(List<String> columns) {
        StringBuilder sb = new StringBuilder();
        Iterator<String> columnsIter = columns.iterator();
        while(columnsIter.hasNext()){
            sb.append(columnsIter.next());
            if(columnsIter.hasNext()){
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    private static String generateValues(List<ColumnType> types, String[] values) {
        StringBuilder sb = new StringBuilder();
        Iterator<ColumnType> typesIter = types.iterator();
        int i = 0;
        while(typesIter.hasNext()){
            ColumnType ct = typesIter.next();
            String value = values[i++];
            sb.append(createValue(value, ct));
            if(typesIter.hasNext()){
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    private static String createValue(String value, ColumnType ct) {
        StringBuilder sb = new StringBuilder();
        switch (ct.getDataType()){
        case BIGINT:
        case INT:
        case FLOAT:
        case DOUBLE:
            sb.append(convertToNumericValue(value));
            break;
        case BOOLEAN:
            sb.append(convertToBooleanValue(value));
            break;
        case TEXT:
        case VARCHAR:
            sb.append(convertToTextValue(value));
            break;
        case NATIVE:
            sb.append(convertToNativeValue(value));
            break;
        case SET:
        case LIST:
        case MAP:
            throw new RuntimeException("Collections are not supported.");
        }
        return sb.toString();
    }

    private static String convertToTextValue(String value) {
        String result = value;
        result.replaceAll("\"", "");
        StringBuilder sb = new StringBuilder(result);
        if(!result.startsWith("'")){
            sb.insert(0, "'");
        }
        if(!result.endsWith("'")){
            sb.append("'");
        }
        return sb.toString();
    }

    private static String convertToNativeValue(String value) {
        return convertToTextValue(value);
    }

    private static String convertToBooleanValue(String value) {
        if(value.equalsIgnoreCase("t")){
            return "TRUE";
        } else if (value.equalsIgnoreCase("f")){
            return "FALSE";
        } else {
            String result = value;
            return result.replaceAll("\"", "").toUpperCase();
        }
    }

    private static String convertToNumericValue(String value) {
        String result = value;
        return result.replaceAll("\"", "");
    }

}
