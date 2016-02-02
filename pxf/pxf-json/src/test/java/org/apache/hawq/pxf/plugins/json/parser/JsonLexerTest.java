package org.apache.hawq.pxf.plugins.json.parser;


import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.*;
import java.util.List;
import java.util.ListIterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


public class JsonLexerTest {

    @Test
    public void testSimple() throws IOException {
        File testsDir = new File("src/test/resources/lexer-tests");
        File[] jsonFiles = testsDir.listFiles(new FilenameFilter() {
            public boolean accept(File file, String s) {
                return s.endsWith(".json");
            }
        });

        for(File jsonFile: jsonFiles) {
            File stateFile = new File(jsonFile.getAbsolutePath() + ".state");
            if(stateFile.exists()) {
                runTest(jsonFile, stateFile);
            }
        }
    }

    public static Pattern STATE_RECURRENCE = Pattern.compile("^([A-Za-z\\_0-9]+)\\{([0-9]+)\\}$");

    public void runTest(File jsonFile, File stateFile) throws IOException {
        List<String> lexerStates = FileUtils.readLines(stateFile);
        InputStream jsonInputStream = new FileInputStream(jsonFile);

        try {
            JsonLexer lexer = new JsonLexer();

            int byteOffset = 0;
            int i;
            ListIterator<String> stateIterator = lexerStates.listIterator();
            int recurrence = 0;
            JsonLexer.JsonLexerState expectedState = null;
            StringBuilder sb = new StringBuilder();
            int stateFileLineNum = 0;
            while((i = jsonInputStream.read()) != -1) {
                byteOffset++;
                char c = (char) i;

                sb.append(c);

                lexer.lex(c);

                if(lexer.getState() == JsonLexer.JsonLexerState.WHITESPACE) {
                    // optimization to skip over multiple whitespaces
                    //
                    continue;
                }

                if(!stateIterator.hasNext()) {
                    assertFalse(formatStateInfo(jsonFile, sb.toString(), byteOffset, stateFileLineNum) +
                            ": Input stream had character '" + c + "' but no matching state", true);
                }

                if(recurrence <= 0) {
                    String state = stateIterator.next().trim();
                    stateFileLineNum++;

                    while(state.equals("") || state.startsWith("#")) {
                        if(!stateIterator.hasNext()) {
                            assertFalse(formatStateInfo(jsonFile, sb.toString(), byteOffset, stateFileLineNum) +
                                    ": Input stream had character '" + c + "' but no matching state", true);
                        }
                        state = stateIterator.next().trim();
                        stateFileLineNum++;
                    }

                    Matcher m = STATE_RECURRENCE.matcher(state);
                    recurrence = 1;
                    if(m.matches()) {
                        state = m.group(1);
                        recurrence = Integer.valueOf(m.group(2));
                    }
                    expectedState = JsonLexer.JsonLexerState.valueOf(state);
                }

                //System.out.println("state = " + expectedState + " recurrence = " + recurrence);

                assertEquals(formatStateInfo(jsonFile, sb.toString(), byteOffset, stateFileLineNum) +
                        ": Issue for char '" + c + "'", expectedState, lexer.getState());
                recurrence--;
            }

            if(stateIterator.hasNext()) {
                assertFalse(formatStateInfo(jsonFile, sb.toString(), byteOffset, stateFileLineNum) +
                        ": Input stream has ended but more states were expected: '" + stateIterator.next() + "...'", true);
            }

        } finally {
            IOUtils.closeQuietly(jsonInputStream);
        }

        System.out.println("File " + jsonFile.getName() + " passed");

    }

    static String formatStateInfo(File jsonFile, String streamContents, int streamByteOffset, int stateFileLineNum) {
        return jsonFile.getName() + ": Input stream currently at byte-offset " + streamByteOffset + ", contents = '" + streamContents + "'" +
                " state-file line = " + stateFileLineNum;
    }
}