package io.miguel0afd;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.ManifestException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.exceptions.ValidationException;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.DataType;
import com.stratio.crossdata.common.result.ConnectResult;
import com.stratio.crossdata.common.result.IDriverResultHandler;
import com.stratio.crossdata.common.result.Result;
import com.stratio.crossdata.driver.BasicDriver;

public class App {

    /**
     * Class logger.
     */
    private static final Logger LOG = Logger.getLogger(App.class.getName());

    public static void main(String[] args)
            throws ConnectionException, ValidationException, ExecutionException, ManifestException, IOException,
            UnsupportedException {
        // TODO: BasicDriver can receive servers as parameters, should it be included something else?
        BasicDriver driver = new BasicDriver();
        // TODO: connect should return a ConnectResult instead of a generic Result
        Result result = driver.connect("crossdata", "secret");
        ConnectResult connectResult = (ConnectResult) result;
        // TODO: from this point, the sessionId should be transparent to the user
        String sessionId = connectResult.getSessionId();
        LOG.info("Connected to Crossdata with session="+sessionId);
        // TODO: The "ADD DATASTORE/CONNECTOR" part sounds different
        // TODO: sessionId should be the first parameter
        // TODO: Add symbol ; at the end if missing
        String cassandraDatastoreManifestPath =
                "/home/mafernandez/workspace/stratio-connector-cassandra/cassandra-connector/src/main/config/CassandraDataStore.xml;";
        result = driver.sendManifest(
                "ADD DATASTORE " + cassandraDatastoreManifestPath,
                sessionId);
        LOG.info("Added Cassandra Datastore Manifest");
        String cassandraConnectorManifestPath =
                "/home/mafernandez/workspace/stratio-connector-cassandra/cassandra-connector/src/main/config/CassandraConnector.xml;";
        result = driver.sendManifest(
                "ADD CONNECTOR " + cassandraConnectorManifestPath,
                sessionId);
        LOG.info("Added Cassandra Connector Manifest");
        // TODO: Create a specific method for this command
        result = driver.executeRawQuery(
                "ATTACH CLUSTER cassandra_prod ON DATASTORE Cassandra " +
                        "WITH OPTIONS {'Hosts': '[127.0.0.1]', 'Port': 9042};",
                sessionId);
        LOG.info("Cluster cassandra_prod created");
        // TODO: Create a specific method for this command
        result = driver.executeRawQuery(
                "ATTACH CONNECTOR CassandraConnector TO cassandra_prod WITH OPTIONS {'DefaultLimit': '1000'};",
                sessionId);
        LOG.info("Connector CassandraConnector attached to cassandra_prod");
        // TODO: Create a specific method for this command
        driver.executeRawQuery(
                "CREATE CATALOG catalogTest;",
                sessionId);
        LOG.info("Catalog catalogTest created");
        driver.setCurrentCatalog("catalogTest");
        LOG.info("Current catalog: " + driver.getCurrentCatalog());
        List<String> columns = new ArrayList<>();
        columns.add("donationid");
        columns.add("projectid");
        columns.add("donor_acctid");
        columns.add("cartid");
        columns.add("donor_city");
        columns.add("donor_state");
        columns.add("donor_zip");
        columns.add("is_teacher_acct");
        columns.add("donation_timestamp");
        columns.add("donation_to_project");
        columns.add("donation_optional_support");
        columns.add("donation_total");
        columns.add("dollar_amount");
        columns.add("donation_included_optional_support");
        columns.add("payment_method");
        columns.add("payment_included_acct_credit");
        columns.add("payment_included_campaign_gift_card");
        columns.add("payment_included_web_purchased_gift_card");
        columns.add("payment_was_promo_matched");
        columns.add("via_giving_page");
        columns.add("for_honoree");
        columns.add("donation_message");
        List<ColumnType> types = new ArrayList<>();
        //TODO: Constructor for ColumnType with a String as parameter
        types.add(ColumnType.valueOf("TEXT"));
        types.add(ColumnType.valueOf("TEXT"));
        types.add(ColumnType.valueOf("TEXT"));
        types.add(ColumnType.valueOf("TEXT"));
        types.add(ColumnType.valueOf("TEXT"));
        types.add(ColumnType.valueOf("TEXT"));
        types.add(ColumnType.valueOf("INT"));
        types.add(ColumnType.valueOf("BOOLEAN"));
        //TODO: ColumnType.valueOf could return a native type if conversion fails
        ColumnType ct = new ColumnType(DataType.NATIVE);
        ct.setDbType("Timestamp");
        ct.setDbType("Timestamp");
        types.add(ct);
        types.add(ColumnType.valueOf("INT"));
        types.add(ColumnType.valueOf("INT"));
        types.add(ColumnType.valueOf("INT"));
        types.add(ColumnType.valueOf("TEXT"));
        types.add(ColumnType.valueOf("BOOLEAN"));
        types.add(ColumnType.valueOf("TEXT"));
        types.add(ColumnType.valueOf("BOOLEAN"));
        types.add(ColumnType.valueOf("BOOLEAN"));
        types.add(ColumnType.valueOf("BOOLEAN"));
        types.add(ColumnType.valueOf("BOOLEAN"));
        types.add(ColumnType.valueOf("BOOLEAN"));
        types.add(ColumnType.valueOf("BOOLEAN"));
        types.add(ColumnType.valueOf("TEXT"));
        String table = "donations";
        result = driver.executeQuery(
                QueryBuilder.generateCreateTable(
                        table,
                        "cassandra_prod",
                        columns,
                        types,
                        columns.subList(0, 4).toArray(new String[4])),
                sessionId);
        LOG.info("Table " + table + " created.");
        //TODO: Create a batch insert
        String csvFilePath = "/home/mafernandez/workspace/opendata_donations_little.csv";
        File csvFile = new File(csvFilePath);
        if(!csvFile.exists()){
            LOG.warning("File " + csvFilePath + " not found");
        }
        CSVParser parser = CSVParser.parse(
                csvFile,
                Charset.defaultCharset(),
                CSVFormat.DEFAULT.withHeader());
        Iterator<CSVRecord> csvIter = parser.iterator();
        long lines = parser.getRecordNumber();
        LOG.info(lines + " lines to be inserted.");
        Map<String, Integer> header = parser.getHeaderMap();
        /*
        LOG.info("HEADER MAP: ");
        for(Map.Entry<String, Integer> entry: header.entrySet()){
            LOG.info(entry.getKey() + ": " + entry.getValue());
        }
        */
        long start = System.currentTimeMillis();
        LOG.info("Insert data into table " + table);
        while (csvIter.hasNext()) {
            CSVRecord row = csvIter.next();
            IDriverResultHandler handler = new EmptyHandler();
            if(!csvIter.hasNext()){
                handler = new LastResultHandler(start);
            }
            Map<String, String> rowMap = row.toMap();
            String[] nextLine = rowMap.values().toArray(new String[rowMap.size()]);
            result = driver.asyncExecuteQuery(
                    QueryBuilder.generateInsert(table, columns, types, nextLine),
                    handler,
                    sessionId
            );
        }
        LOG.info("All data was sent");
    }

}
