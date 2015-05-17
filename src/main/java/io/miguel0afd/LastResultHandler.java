package io.miguel0afd;

import org.apache.log4j.Logger;

import com.stratio.crossdata.common.result.IDriverResultHandler;
import com.stratio.crossdata.common.result.QueryStatus;
import com.stratio.crossdata.common.result.Result;

public class LastResultHandler implements IDriverResultHandler {

    /**
     * Class logger.
     */
    private static final Logger LOG = Logger.getLogger(LastResultHandler.class);

    private final long start;

    public LastResultHandler(long start) {
        this.start = start;
    }

    /**
     * Process an acknowledgement message for a specific query.
     *
     * @param queryId The query identifier.
     * @param status  The query status.
     */
    @Override
    public void processAck(String queryId, QueryStatus status) {
        LOG.info("Executing last query: " + status + ", " + queryId);
    }

    /**
     * Process an error result.
     *
     * @param errorResult The error.
     */
    @Override
    public void processError(Result errorResult) {
        LOG.error("Last query failed: " + errorResult);
    }

    /**
     * Process a successful result.
     *
     * @param result The result.
     */
    @Override
    public void processResult(Result result) {
        long end = System.currentTimeMillis();
        long totalTime = end - start;
        long milliseconds = totalTime % 1000;
        long seconds = totalTime % 60000;
        long minutes = totalTime % 360000;
        long hours = totalTime / 360000;
        LOG.info("Ingestion took: "
                + hours + " hours, "
                + minutes + " minutes, "
                + seconds + " seconds, "
                + milliseconds + "milliseconds.");
    }
}
