package io.miguel0afd;

import org.apache.log4j.Logger;

import com.stratio.crossdata.common.result.IDriverResultHandler;
import com.stratio.crossdata.common.result.QueryStatus;
import com.stratio.crossdata.common.result.Result;

public class EmptyHandler implements IDriverResultHandler {

    /**
     * Class logger.
     */
    private static final Logger LOG = Logger.getLogger(EmptyHandler.class);

    /**
     * Process an acknowledgement message for a specific query.
     *
     * @param queryId The query identifier.
     * @param status  The query status.
     */
    @Override
    public void processAck(String queryId, QueryStatus status) {

    }

    /**
     * Process an error result.
     *
     * @param errorResult The error.
     */
    @Override
    public void processError(Result errorResult) {
        LOG.error(errorResult);
    }

    /**
     * Process a successful result.
     *
     * @param result The result.
     */
    @Override public void
    processResult(Result result) {

    }
}
