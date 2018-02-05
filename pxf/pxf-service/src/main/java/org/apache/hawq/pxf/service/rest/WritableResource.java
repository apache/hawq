package org.apache.hawq.pxf.service.rest;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.DataInputStream;
import java.io.InputStream;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.catalina.connector.ClientAbortException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hawq.pxf.api.utilities.Utilities;
import org.apache.hawq.pxf.service.Bridge;
import org.apache.hawq.pxf.service.WriteBridge;
import org.apache.hawq.pxf.service.utilities.ProtocolData;
import org.apache.hawq.pxf.service.utilities.SecuredHDFS;

/*
 * Running this resource manually:
 *
 * run:
 	curl -i -X post "http://localhost:51200/pxf/{version}/Writable/stream?path=/data/curl/curl`date \"+%h%d_%H%M%s\"`" \
 	--header "X-GP-Accessor: TextFileWAccessor" \
 	--header "X-GP-Resolver: TextWResolver" \
 	--header "Content-Type:application/octet-stream" \
 	--header "Expect: 100-continue" \
  	--header "X-GP-ALIGNMENT: 4" \
 	--header "X-GP-SEGMENT-ID: 0" \
 	--header "X-GP-SEGMENT-COUNT: 3" \
 	--header "X-GP-HAS-FILTER: 0" \
 	--header "X-GP-FORMAT: TEXT" \
 	--header "X-GP-URI: pxf://localhost:51200/data/curl/?Accessor=TextFileWAccessor&Resolver=TextWResolver" \
 	--header "X-GP-URL-HOST: localhost" \
 	--header "X-GP-URL-PORT: 51200" \
 	--header "X-GP-ATTRS: 0" \
 	--header "X-GP-DATA-DIR: data/curl/" \
 	  -d "data111" -d "data222"

 * 	result:

  	HTTP/1.1 200 OK
	Content-Type: text/plain;charset=UTF-8
	Content-Type: text/plain
	Transfer-Encoding: chunked
	Server: Jetty(7.6.10.v20130312)

	wrote 15 bytes to curlAug11_17271376231245

	file content:
	bin/hdfs dfs -cat /data/curl/*45
	data111&data222

 */

/**
 * This class handles the subpath /&lt;version&gt;/Writable/ of this REST component
 */
@Path("/" + Version.PXF_PROTOCOL_VERSION + "/Writable/")
public class WritableResource extends RestResource{
    private static final Log LOG = LogFactory.getLog(WritableResource.class);

    public WritableResource() {
    }

    /**
     * This function is called when http://nn:port/pxf/{version}/Writable/stream?path=...
	 * is used.
	 *
	 * @param servletContext Servlet context contains attributes required by SecuredHDFS
	 * @param headers Holds HTTP headers from request
	 * @param path Holds URI path option used in this request
	 * @param inputStream stream of bytes to write from Hawq
     * @return ok response if the operation finished successfully
     * @throws Exception in case of wrong request parameters, failure to
     *             initialize bridge or to write data
     */
    @POST
    @Path("stream")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public Response stream(@Context final ServletContext servletContext,
                           @Context HttpHeaders headers,
                           @QueryParam("path") String path,
                           InputStream inputStream) throws Exception {

        /* Convert headers into a case-insensitive regular map */
        Map<String, String> params = convertToCaseInsensitiveMap(headers.getRequestHeaders());
        if (LOG.isDebugEnabled()) {
            LOG.debug("WritableResource started with parameters: " + params + " and write path: " + path);
        }

        ProtocolData protData = new ProtocolData(params);
        protData.setDataSource(path);

        SecuredHDFS.verifyToken(protData, servletContext);
        Bridge bridge = new WriteBridge(protData);

        // THREAD-SAFE parameter has precedence
        boolean isThreadSafe = protData.isThreadSafe() && bridge.isThreadSafe();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Request for " + path + " handled " +
                    (isThreadSafe ? "without" : "with") + " synchronization");
        }

        return isThreadSafe ?
                writeResponse(bridge, path, inputStream) :
                synchronizedWriteResponse(bridge, path, inputStream);
    }

    private static synchronized Response synchronizedWriteResponse(Bridge bridge,
                                                                   String path,
                                                                   InputStream inputStream)
            throws Exception {
        return writeResponse(bridge, path, inputStream);
    }

    private static Response writeResponse(Bridge bridge,
                                          String path,
                                          InputStream inputStream)
            throws Exception {
        // Open the output file
        bridge.beginIteration();
        long totalWritten = 0;
        Exception ex = null;

        // dataStream will close automatically in the end of the try.
        // inputStream is closed by dataStream.close().
        try (DataInputStream dataStream = new DataInputStream(inputStream)) {
            while (bridge.setNext(dataStream)) {
                ++totalWritten;
            }
        } catch (ClientAbortException cae) {
            LOG.error("Remote connection closed by HAWQ", cae);
        } catch (Exception e) {
            LOG.error("Exception: totalWritten so far " + totalWritten + " to " + path, e);
            ex = e;
            throw ex;
        } finally {
            try {
                bridge.endIteration();
            } catch (Exception e) {
                throw (ex == null) ? e: ex;
            }
        }

        String censuredPath = Utilities.maskNonPrintables(path);
        String returnMsg = "wrote " + totalWritten + " bulks to " + censuredPath;
        LOG.debug(returnMsg);

        return Response.ok(returnMsg).build();
    }
}
