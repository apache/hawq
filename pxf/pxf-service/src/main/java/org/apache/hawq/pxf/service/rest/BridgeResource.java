package org.apache.hawq.pxf.service.rest;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import org.apache.catalina.connector.ClientAbortException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hawq.pxf.service.Bridge;
import org.apache.hawq.pxf.service.ReadBridge;
import org.apache.hawq.pxf.service.ReadSamplingBridge;
import org.apache.hawq.pxf.service.io.Writable;
import org.apache.hawq.pxf.service.utilities.ProtocolData;
import org.apache.hawq.pxf.service.utilities.SecuredHDFS;

/*
 * This class handles the subpath /<version>/Bridge/ of this
 * REST component
 */
@Path("/" + Version.PXF_PROTOCOL_VERSION + "/Bridge/")
public class BridgeResource extends RestResource {

    private static Log Log = LogFactory.getLog(BridgeResource.class);
    /**
     * Lock is needed here in the case of a non-thread-safe plugin. Using
     * synchronized methods is not enough because the bridge work is called by
     * jetty ({@link StreamingOutput}), after we are getting out of this class's
     * context.
     * <p/>
     * BRIDGE_LOCK is accessed through lock() and unlock() functions, based on
     * the isThreadSafe parameter that is determined by the bridge.
     */
    private static final ReentrantLock BRIDGE_LOCK = new ReentrantLock();

    public BridgeResource() {
    }

    /**
     * Used to be HDFSReader. Creates a bridge instance and iterates over its
     * records, printing it out to outgoing stream. Outputs GPDBWritable or
     * Text.
     *
     * Parameters come through HTTP header.
     *
     * @param servletContext Servlet context contains attributes required by
     *            SecuredHDFS
     * @param headers Holds HTTP headers from request
     * @return response object containing stream that will output records
     * @throws Exception in case of wrong request parameters, or failure to
     *             initialize bridge
     */
    @GET
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response read(@Context final ServletContext servletContext,
                         @Context HttpHeaders headers) throws Exception {
        // Convert headers into a regular map
        Map<String, String> params = convertToCaseInsensitiveMap(headers.getRequestHeaders());

        Log.debug("started with parameters: " + params);

        ProtocolData protData = new ProtocolData(params);
        SecuredHDFS.verifyToken(protData, servletContext);
        Bridge bridge;
        float sampleRatio = protData.getStatsSampleRatio();
        if (sampleRatio > 0) {
            bridge = new ReadSamplingBridge(protData);
        } else {
            bridge = new ReadBridge(protData);
        }
        String dataDir = protData.getDataSource();
        // THREAD-SAFE parameter has precedence
        boolean isThreadSafe = protData.isThreadSafe() && bridge.isThreadSafe();
        Log.debug("Request for " + dataDir + " will be handled "
                + (isThreadSafe ? "without" : "with") + " synchronization");

        return readResponse(bridge, protData, isThreadSafe);
    }

    Response readResponse(final Bridge bridge, ProtocolData protData,
                          final boolean threadSafe) {
        final int fragment = protData.getDataFragment();
        final String dataDir = protData.getDataSource();

        // Creating an internal streaming class
        // which will iterate the records and put them on the
        // output stream
        final StreamingOutput streaming = new StreamingOutput() {
            @Override
            public void write(final OutputStream out) throws IOException,
                    WebApplicationException {
                long recordCount = 0;

                if (!threadSafe) {
                    lock(dataDir);
                }
                try {

                    if (!bridge.beginIteration()) {
                        return;
                    }

                    Writable record;
                    DataOutputStream dos = new DataOutputStream(out);
                    Log.debug("Starting streaming fragment " + fragment
                            + " of resource " + dataDir);
                    while ((record = bridge.getNext()) != null) {
                        record.write(dos);
                        ++recordCount;
                    }
                    Log.debug("Finished streaming fragment " + fragment
                            + " of resource " + dataDir + ", " + recordCount
                            + " records.");
                } catch (ClientAbortException e) {
                    // Occurs whenever client (HAWQ) decides the end the
                    // connection
                    Log.error("Remote connection closed by HAWQ", e);
                } catch (Exception e) {
                    Log.error("Exception thrown when streaming", e);
                    throw new IOException(e.getMessage());
                } finally {
                    Log.debug("Stopped streaming fragment " + fragment
                            + " of resource " + dataDir + ", " + recordCount
                            + " records.");
                    if (!threadSafe) {
                        unlock(dataDir);
                    }
                }
            }
        };

        return Response.ok(streaming, MediaType.APPLICATION_OCTET_STREAM).build();
    }

    /**
     * Locks BRIDGE_LOCK
     *
     * @param path path for the request, used for logging.
     */
    private void lock(String path) {
        Log.trace("Locking BridgeResource for " + path);
        BRIDGE_LOCK.lock();
        Log.trace("Locked BridgeResource for " + path);
    }

    /**
     * Unlocks BRIDGE_LOCK
     *
     * @param path path for the request, used for logging.
     */
    private void unlock(String path) {
        Log.trace("Unlocking BridgeResource for " + path);
        BRIDGE_LOCK.unlock();
        Log.trace("Unlocked BridgeResource for " + path);
    }
}
