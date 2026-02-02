package com.anode.maven.b2extension;

import org.apache.maven.wagon.authentication.AuthenticationInfo;

/**
 * Handles Backblaze B2 authentication credential resolution.
 *
 * Credentials are resolved in the following order of precedence:
 * 1. System properties: b2.applicationKeyId, b2.applicationKey
 * 2. Environment variables: B2_APPLICATION_KEY_ID, B2_APPLICATION_KEY
 * 3. Maven settings.xml server credentials (username/password)
 */
public final class B2Auth {

    private B2Auth() {
        throw new UnsupportedOperationException("Utility class");
    }

    /**
     * Represents resolved B2 credentials.
     */
    public static class B2Credentials {
        private final String applicationKeyId;
        private final String applicationKey;

        public B2Credentials(String applicationKeyId, String applicationKey) {
            this.applicationKeyId = applicationKeyId;
            this.applicationKey = applicationKey;
        }

        public String getApplicationKeyId() {
            return applicationKeyId;
        }

        public String getApplicationKey() {
            return applicationKey;
        }

        public boolean isValid() {
            return applicationKeyId != null && !applicationKeyId.isEmpty()
                    && applicationKey != null && !applicationKey.isEmpty();
        }
    }

    /**
     * Resolves B2 credentials from various sources.
     *
     * @param authenticationInfo Maven authentication info from settings.xml (may be null)
     * @return Resolved B2 credentials
     */
    public static B2Credentials getCredentials(AuthenticationInfo authenticationInfo) {
        // 1. Try system properties first
        String keyId = System.getProperty("b2.applicationKeyId");
        String key = System.getProperty("b2.applicationKey");

        if (keyId != null && key != null) {
            return new B2Credentials(keyId, key);
        }

        // 2. Try environment variables
        keyId = System.getenv("B2_APPLICATION_KEY_ID");
        key = System.getenv("B2_APPLICATION_KEY");

        if (keyId != null && key != null) {
            return new B2Credentials(keyId, key);
        }

        // 3. Try Maven settings.xml credentials
        if (authenticationInfo != null) {
            String username = authenticationInfo.getUserName();
            String password = authenticationInfo.getPassword();

            if (username != null && password != null) {
                return new B2Credentials(username, password);
            }
        }

        // Return empty credentials if nothing found
        return new B2Credentials(null, null);
    }
}
