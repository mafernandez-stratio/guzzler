package io.miguel0afd;

import java.util.ArrayList;
import java.util.List;

import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ManifestException;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.result.ConnectResult;
import com.stratio.crossdata.common.result.Result;
import com.stratio.crossdata.driver.BasicDriver;

public class App {

    public static void main(String[] args) throws ConnectionException, ManifestException {
        // TODO: BasicDriver can receive servers as parameters, should it be included something else?
        BasicDriver driver = new BasicDriver();
        // TODO: connect should return a ConnectResult instead of a generic Result
        Result result = driver.connect("crossdata", "secret");
        ConnectResult connectResult = (ConnectResult) result;
        // TODO: from this point, the sessionId should be transparent to the user
        String sessionId = connectResult.getSessionId();
        // TODO: The "ADD DATASTORE/CONNECTOR" part sounds different
        // TODO: sessionId should be the first parameter
        String cassandraDatastoreManifestPath = "/home/mafernandez/workspace/stratio-connector-cassandra/cassandra-connector/src/main/config/CassandraDataStore.xml";
        result = driver.sendManifest(
                "ADD DATASTORE " + cassandraDatastoreManifestPath,
                sessionId);
        String cassandraConnectorManifestPath = "/home/mafernandez/workspace/stratio-connector-cassandra/cassandra-connector/src/main/config/CassandraConnector.xml";
        result = driver.sendManifest(
                "ADD CONNECTOR " + cassandraConnectorManifestPath,
                sessionId);
        // TODO: Create a specific method for this command
        result = driver.executeRawQuery(
                "ATTACH CLUSTER cassandra_prod ON DATASTORE Cassandra WITH OPTIONS {'Hosts': '[127.0.0.1]', 'Port': 9042}",
                sessionId);
        // TODO: Create a specific method for this command
        result = driver.executeRawQuery(
                "ATTACH CONNECTOR CassandraConnector TO cassandra_prod WITH OPTIONS {'DefaultLimit': '1000'}",
                sessionId);
        // TODO: Create a specific method for this command
        driver.executeRawQuery(
                "CREATE CATALOG catalogTest",
                sessionId);
        driver.setCurrentCatalog("catalogTest");
        //driver.executeRawQuery(
        //"CREATE TABLE tableTest ON CLUSTER cassandra_prod
        // (id int PRIMARY KEY, name text, description text, rating float)",
        //        sessionId);
        List<String> columns = new ArrayList<>();
        columns.add("_donationid");
        columns.add("_projectid");
        columns.add("_donor_acctid");
        columns.add("_cartid");
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
        types.add();
        driver.executeRawQuery(
                Utils.generateCreateTable(
                        "donations",
                        "cassandra_prod",
                        columns,
                        types,
                        columns.subList(0, 4).toArray(new String[4])),
                sessionId);


    }

}
