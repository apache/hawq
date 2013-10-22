package com.pivotal.pxf.exception;

/**
 * Thrown when there is a configuration problem with pxf profiles definitions
 * Can arise when pxf-profiles.xml is missing from the CLASSPATH
 * or when pxf-profiles.xml is not valid or when a profile entry or attribute is missing.
 */
public class ProfileConfException extends RuntimeException
{
    public static final String PROFILES_FILE_NOT_FOUND = "%s was not found on the CLASSPATH";
    public static final String PROFILES_FILE_LOAD_ERR = "Profiles configuration %s could not be loaded: %s";
    public static final String NO_PROFILE_DEF = "%s is not defined in %s";

    private String msgFormat;

    public ProfileConfException(String msgFormat, String... msgArgs)
    {
        super(String.format(msgFormat, (Object[]) msgArgs));
        this.msgFormat = msgFormat;
    }

    public String getMsgFormat()
    {
        return msgFormat;
    }
}