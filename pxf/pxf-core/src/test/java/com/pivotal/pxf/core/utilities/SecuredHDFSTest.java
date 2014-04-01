package com.pivotal.pxf.core.utilities;

import com.pivotal.pxf.api.utilities.InputData;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.servlet.ServletContext;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({UserGroupInformation.class})
public class SecuredHDFSTest {
    Map<String, String> parameters;
    ServletContext mockContext;

    @Test
    public void nullIdentifierThrows() {
        when(UserGroupInformation.isSecurityEnabled()).thenReturn(true);
        try {
            SecuredHDFS.verifyToken(new InputData(parameters), mockContext);
            fail("null X-GP-TOKEN-IDNT should throw");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(),
                    "Internal server error. Property \"X-GP-TOKEN-IDNT\" has no value in current request");
        }
    }

    @Test
    public void invalidIdentifierThrows() {
        when(UserGroupInformation.isSecurityEnabled()).thenReturn(true);
        parameters.put("X-GP-TOKEN-IDNT", "This is odd");
        try {
            SecuredHDFS.verifyToken(new InputData(parameters), mockContext);
            fail("invalid X-GP-TOKEN-IDNT should throw");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Internal server error. String This is odd isn't a valid hex string");
        }
    }

    @Test
    public void invalidPasswordThrows() {
        when(UserGroupInformation.isSecurityEnabled()).thenReturn(true);
        parameters.put("X-GP-TOKEN-IDNT", "DEAD");
        parameters.put("X-GP-TOKEN-PASS", "This is odd");
        try {
            SecuredHDFS.verifyToken(new InputData(parameters), mockContext);
            fail("invalid X-GP-TOKEN-PASS should throw");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Internal server error. String This is odd isn't a valid hex string");
        }
    }

    @Test
    public void nullKindThrows() {
        when(UserGroupInformation.isSecurityEnabled()).thenReturn(true);
        parameters.put("X-GP-TOKEN-IDNT", "DEAD");
        parameters.put("X-GP-TOKEN-PASS", "DEAD");

        try {
            SecuredHDFS.verifyToken(new InputData(parameters), mockContext);
            fail("null X-GP-TOKEN-KIND should throw");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(),
                    "Internal server error. Property \"X-GP-TOKEN-KIND\" has no value in current request");
        }
    }

    @Test
    public void invalidKindThrows() {
        when(UserGroupInformation.isSecurityEnabled()).thenReturn(true);
        parameters.put("X-GP-TOKEN-IDNT", "DEAD");
        parameters.put("X-GP-TOKEN-PASS", "DEAD");
        parameters.put("X-GP-TOKEN-KIND", "This is odd");

        try {
            SecuredHDFS.verifyToken(new InputData(parameters), mockContext);
            fail("invalid X-GP-TOKEN-KIND should throw");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Internal server error. String This is odd isn't a valid hex string");
        }
    }

    @Test
    public void nullPasswordThrows() {
        when(UserGroupInformation.isSecurityEnabled()).thenReturn(true);
        parameters.put("X-GP-TOKEN-IDNT", "DEAD");

        try {
            SecuredHDFS.verifyToken(new InputData(parameters), mockContext);
            fail("null X-GP-TOKEN-PASS should throw");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(),
                    "Internal server error. Property \"X-GP-TOKEN-PASS\" has no value in current request");
        }
    }


    @Test
    public void nullServiceThrows() {
        when(UserGroupInformation.isSecurityEnabled()).thenReturn(true);
        parameters.put("X-GP-TOKEN-IDNT", "DEAD");
        parameters.put("X-GP-TOKEN-PASS", "DEAD");
        parameters.put("X-GP-TOKEN-KIND", "DEAD");

        try {
            SecuredHDFS.verifyToken(new InputData(parameters), mockContext);
            fail("null X-GP-TOKEN-SRVC should throw");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(),
                    "Internal server error. Property \"X-GP-TOKEN-SRVC\" has no value in current request");
        }
    }

    @Test
    public void invalidServiceThrows() {
        when(UserGroupInformation.isSecurityEnabled()).thenReturn(true);
        parameters.put("X-GP-TOKEN-IDNT", "DEAD");
        parameters.put("X-GP-TOKEN-PASS", "DEAD");
        parameters.put("X-GP-TOKEN-KIND", "DEAD");
        parameters.put("X-GP-TOKEN-SRVC", "This is odd");

        try {
            SecuredHDFS.verifyToken(new InputData(parameters), mockContext);
            fail("invalid X-GP-TOKEN-SRVC should throw");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Internal server error. String This is odd isn't a valid hex string");
        }
    }

    /*
     * setUp function called before each test
	 */
    @Before
    public void setUp() {
        parameters = new HashMap<String, String>();

        parameters.put("X-GP-ALIGNMENT", "all");
        parameters.put("X-GP-SEGMENT-ID", "-44");
        parameters.put("X-GP-SEGMENT-COUNT", "2");
        parameters.put("X-GP-HAS-FILTER", "0");
        parameters.put("X-GP-FORMAT", "TEXT");
        parameters.put("X-GP-URL-HOST", "my://bags");
        parameters.put("X-GP-URL-PORT", "-8020");
        parameters.put("X-GP-ATTRS", "-1");
        parameters.put("X-GP-ACCESSOR", "are");
        parameters.put("X-GP-RESOLVER", "packed");
        parameters.put("X-GP-DATA-DIR", "i'm/ready/to/go");
        parameters.put("X-GP-FRAGMENT-METADATA", "U29tZXRoaW5nIGluIHRoZSB3YXk=");
        parameters.put("X-GP-I'M-STANDING-HERE", "outside-your-door");

        mockContext = mock(ServletContext.class);

        PowerMockito.mockStatic(UserGroupInformation.class);
    }
}
