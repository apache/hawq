package org.apache.hawq.pxf.service;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;

import org.apache.hawq.pxf.api.Fragment;

/**
 * Class for serializing fragments metadata in JSON format.
 * The class implements {@link StreamingOutput} so the serialization will be
 * done in a stream and not in one bulk, this in order to avoid running
 * out of memory when processing a lot of fragments.
 */
public class FragmentsResponse implements StreamingOutput {

    private static Log Log = LogFactory.getLog(FragmentsResponse.class);

    private List<Fragment> fragments;

    /**
     * Constructs fragments response out of a list of fragments
     *
     * @param fragments fragment list
     */
    public FragmentsResponse(List<Fragment> fragments) {
        this.fragments = fragments;
    }

    /**
     * Serializes a fragments list in JSON,
     * To be used as the result string for HAWQ.
     * An example result is as follows:
     * <code>{"PXFFragments":[{"replicas":["sdw1.corp.emc.com","sdw3.corp.emc.com","sdw8.corp.emc.com"],"sourceName":"text2.csv", "index":"0", "metadata":<base64 metadata for fragment>, "userData":"<data_specific_to_third_party_fragmenter>"},{"replicas":["sdw2.corp.emc.com","sdw4.corp.emc.com","sdw5.corp.emc.com"],"sourceName":"text_data.csv","index":"0","metadata":<base64 metadata for fragment>,"userData":"<data_specific_to_third_party_fragmenter>"}]}</code>
     */
    @Override
    public void write(OutputStream output) throws IOException,
            WebApplicationException {
        DataOutputStream dos = new DataOutputStream(output);
        ObjectMapper mapper = new ObjectMapper();

        dos.write("{\"PXFFragments\":[".getBytes());

        String prefix = "";
        for (Fragment fragment : fragments) {
            StringBuilder result = new StringBuilder();
            /* metaData and userData are automatically converted to Base64 */
            result.append(prefix).append(mapper.writeValueAsString(fragment));
            prefix = ",";
            dos.write(result.toString().getBytes());
        }

        dos.write("]}".getBytes());
    }
}
