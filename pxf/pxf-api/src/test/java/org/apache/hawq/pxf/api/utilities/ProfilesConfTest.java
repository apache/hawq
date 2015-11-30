package org.apache.hawq.pxf.api.utilities;

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


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;

import static org.apache.hawq.pxf.api.utilities.ProfileConfException.MessageFormat.NO_PROFILE_DEF;
import static org.apache.hawq.pxf.api.utilities.ProfileConfException.MessageFormat.PROFILES_FILE_NOT_FOUND;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Base test class for all ProfilesConf tests.
 * Each test case is encapsulated inside its own inner class to force reloading of ProfilesConf enum singleton
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ProfilesConf.class, Log.class, LogFactory.class, ClassLoader.class})
public class ProfilesConfTest {
    static ClassLoader classLoader;
    static Log log;
    String mandatoryFileName = "mandatory.xml";
    String optionalFileName = "optional.xml";
    File mandatoryFile;
    File optionalFile;

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        mandatoryFile = testFolder.newFile(mandatoryFileName);
        optionalFile = testFolder.newFile(optionalFileName);
        PowerMockito.mockStatic(LogFactory.class);
        log = mock(Log.class);
        when(LogFactory.getLog(ProfilesConf.class)).thenReturn(log);
        classLoader = mock(ClassLoader.class);
        PowerMockito.stub(PowerMockito.method(ProfilesConf.class, "getClassLoader")).toReturn(classLoader);
    }

    void writeFile(File file, String content) throws IOException {
        Files.write(file.toPath(), content.getBytes());
    }
}

class ProfilesConfTestDefinedProfile extends ProfilesConfTest {
    @Test
    public void definedProfile() throws Exception {
        writeFile(mandatoryFile, "<profiles><profile><name>HBase</name><plugins><plugin1>X</plugin1><plugin2>XX</plugin2></plugins></profile></profiles>");
        writeFile(optionalFile, "<profiles><profile><name>Hive</name><plugins><plugin1>Y</plugin1></plugins></profile></profiles>");
        when(classLoader.getResource("pxf-profiles-default.xml")).thenReturn(mandatoryFile.toURI().toURL());
        when(classLoader.getResource("pxf-profiles.xml")).thenReturn(optionalFile.toURI().toURL());

        Map<String, String> hbaseProfile = ProfilesConf.getProfilePluginsMap("HBase");
        assertEquals(2, hbaseProfile.keySet().size());
        assertEquals(hbaseProfile.get("X-GP-PLUGIN1"), "X");
        assertEquals(hbaseProfile.get("X-GP-PLUGIN2"), "XX");

        Map<String, String> hiveProfile = ProfilesConf.getProfilePluginsMap("hIVe");// case insensitive profile name
        assertEquals(1, hiveProfile.keySet().size());
        assertEquals(hiveProfile.get("X-GP-PLUGIN1"), "Y");

        Mockito.verify(log).info("PXF profiles loaded: [HBase, Hive]");
    }
}

class ProfilesConfTestUndefinedProfile extends ProfilesConfTest {
    @Test
    public void undefinedProfile() throws Exception {
        writeFile(mandatoryFile, "<profiles><profile><name>HBase</name><plugins><plugin1>X</plugin1></plugins></profile></profiles>");
        writeFile(optionalFile, "<profiles><profile><name>Hive</name><plugins><plugin1>Y</plugin1></plugins></profile></profiles>");
        when(classLoader.getResource("pxf-profiles-default.xml")).thenReturn(mandatoryFile.toURI().toURL());
        when(classLoader.getResource("pxf-profiles.xml")).thenReturn(optionalFile.toURI().toURL());
        try {
            ProfilesConf.getProfilePluginsMap("UndefinedProfile");
            fail("undefined profile should have thrown exception");
        } catch (ProfileConfException pce) {
            assertEquals(pce.getMessage(), String.format(NO_PROFILE_DEF.getFormat(), "UndefinedProfile", "pxf-profiles.xml"));
        }
    }
}

class ProfilesConfTestDuplicateProfileDefinition extends ProfilesConfTest {
    @Test
    public void duplicateProfileDefinition() throws Exception {
        writeFile(mandatoryFile, "<profiles><profile><name>HBase</name><plugins><plugin1>Y</plugin1><plugin1>YY</plugin1></plugins></profile><profile><name>HBase</name><plugins><plugin1>Y</plugin1></plugins></profile></profiles>");
        writeFile(optionalFile, "<profiles><profile><name>Hive</name><plugins><plugin1>Y</plugin1></plugins></profile></profiles>");
        when(classLoader.getResource("pxf-profiles-default.xml")).thenReturn(mandatoryFile.toURI().toURL());
        when(classLoader.getResource("pxf-profiles.xml")).thenReturn(optionalFile.toURI().toURL());
        ProfilesConf.getProfilePluginsMap("HBase");
        Mockito.verify(log).warn("Duplicate profile definition found in " + mandatoryFileName + " for: HBase");
    }
}

class ProfilesConfTestOverrideProfile extends ProfilesConfTest {
    @Test
    public void overrideProfile() throws Exception {
        writeFile(mandatoryFile, "<profiles><profile><name>HBase</name><plugins><plugin1>X</plugin1></plugins></profile></profiles>");
        writeFile(optionalFile, "<profiles><profile><name>HBase</name><plugins><plugin1>Y</plugin1><plugin2>YY</plugin2></plugins></profile></profiles>");
        when(classLoader.getResource("pxf-profiles-default.xml")).thenReturn(mandatoryFile.toURI().toURL());
        when(classLoader.getResource("pxf-profiles.xml")).thenReturn(optionalFile.toURI().toURL());
        Map profile = ProfilesConf.getProfilePluginsMap("HBase");
        assertEquals(2, profile.keySet().size());
        assertEquals(profile.get("X-GP-PLUGIN1"), "Y");
        assertEquals(profile.get("X-GP-PLUGIN2"), "YY");
    }
}

class ProfilesConfTestEmptyProfileFile extends ProfilesConfTest {
    @Test
    public void emptyProfileFile() throws Exception {
        writeFile(mandatoryFile, "<profiles/>");
        writeFile(optionalFile, "<profiles><profile><name>HBase</name><plugins><plugin1>Y</plugin1></plugins></profile></profiles>");
        when(classLoader.getResource("pxf-profiles-default.xml")).thenReturn(mandatoryFile.toURI().toURL());
        when(classLoader.getResource("pxf-profiles.xml")).thenReturn(optionalFile.toURI().toURL());
        ProfilesConf.getProfilePluginsMap("HBase");
        Mockito.verify(log).warn("Profile file: " + mandatoryFileName + " is empty");
    }
}

class ProfilesConfTestMalformedProfileFile extends ProfilesConfTest {
    @Test
    public void malformedProfileFile() throws Exception {
        writeFile(mandatoryFile, "I'm a malford x.m.l@#$#<%");
        writeFile(optionalFile, "<profiles><profile><name>HBase</name><plugins><plugin1>Y</plugin1></plugins></profile></profiles>");
        when(classLoader.getResource("pxf-profiles-default.xml")).thenReturn(mandatoryFile.toURI().toURL());
        when(classLoader.getResource("pxf-profiles.xml")).thenReturn(optionalFile.toURI().toURL());
        try {
            ProfilesConf.getProfilePluginsMap("HBase");
            fail("malformed profile file should have thrown exception");
        } catch (ExceptionInInitializerError pce) {
            assertTrue(pce.getCause().getMessage().contains(mandatoryFileName + " could not be loaded: org.xml.sax.SAXParseException"));
        }
    }
}

class ProfilesConfTestMissingMandatoryProfileFile extends ProfilesConfTest {
    @Test
    public void missingMandatoryProfileFile() throws Exception {
        when(classLoader.getResource("pxf-profiles-default.xml")).thenReturn(null);
        try {
            ProfilesConf.getProfilePluginsMap("HBase");
            fail("missing mandatory profile file should have thrown exception");
        } catch (ExceptionInInitializerError pce) {
            Mockito.verify(log).warn("pxf-profiles-default.xml not found in the classpath");
            assertEquals(pce.getCause().getMessage(), String.format(PROFILES_FILE_NOT_FOUND.getFormat(), "pxf-profiles-default.xml"));
        }
    }
}

class ProfilesConfTestMissingOptionalProfileFile extends ProfilesConfTest {
    @Test
    public void missingOptionalProfileFile() throws Exception {
        writeFile(mandatoryFile, "<profiles><profile><name>HBase</name><plugins><plugin1>Y</plugin1></plugins></profile></profiles>");
        when(classLoader.getResource("pxf-profiles-default.xml")).thenReturn(mandatoryFile.toURI().toURL());
        when(classLoader.getResource("pxf-profiles.xml")).thenReturn(null);
        Map<String, String> hbaseProfile = ProfilesConf.getProfilePluginsMap("HBase");
        assertEquals("Y", hbaseProfile.get("X-GP-PLUGIN1"));
        Mockito.verify(log).warn("pxf-profiles.xml not found in the classpath");
    }
}
