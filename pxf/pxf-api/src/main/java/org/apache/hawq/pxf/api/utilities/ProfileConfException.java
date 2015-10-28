package org.apache.hawq.pxf.api.utilities;

/**
 * Thrown when there is a configuration problem with pxf profiles definitions.
 * {@link ProfileConfException.MessageFormat#PROFILES_FILE_NOT_FOUND} when pxf-profiles.xml is missing from the CLASSPATH.
 * {@link ProfileConfException.MessageFormat#PROFILES_FILE_LOAD_ERR} when pxf-profiles.xml is not valid.
 * {@link ProfileConfException.MessageFormat#NO_PROFILE_DEF} when a profile entry or attribute is missing.
 */
public class ProfileConfException extends RuntimeException {
    public static enum MessageFormat {
        PROFILES_FILE_NOT_FOUND("%s was not found on the CLASSPATH"),
        PROFILES_FILE_LOAD_ERR("Profiles configuration %s could not be loaded: %s"),
        NO_PROFILE_DEF("%s is not defined in %s");

        String format;

        MessageFormat(String format) {
            this.format = format;
        }

        public String getFormat() {
            return format;
        }
    }

    private MessageFormat msgFormat;

    /**
     * Constructs a ProfileConfException.
     *
     * @param msgFormat the message format
     * @param msgArgs the message arguments
     */
    public ProfileConfException(MessageFormat msgFormat, String... msgArgs) {
        super(String.format(msgFormat.getFormat(), (Object[]) msgArgs));
        this.msgFormat = msgFormat;
    }

    public MessageFormat getMsgFormat() {
        return msgFormat;
    }
}