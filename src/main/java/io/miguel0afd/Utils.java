package io.miguel0afd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.stratio.crossdata.common.metadata.ColumnType;

public class Utils {

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
        sb.append(")");
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
            sb.append(column).append(" ").append(type);
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

}
