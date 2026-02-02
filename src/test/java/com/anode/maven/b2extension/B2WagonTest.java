package com.anode.maven.b2extension;

import com.backblaze.b2.client.B2StorageClient;
import com.backblaze.b2.client.exceptions.B2Exception;
import com.backblaze.b2.client.exceptions.B2NotFoundException;
import com.backblaze.b2.client.exceptions.B2UnauthorizedException;
import com.backblaze.b2.client.structures.B2FileVersion;
import com.backblaze.b2.client.structures.B2UploadFileRequest;
import org.apache.maven.wagon.ResourceDoesNotExistException;
import org.apache.maven.wagon.TransferFailedException;
import org.apache.maven.wagon.authentication.AuthenticationInfo;
import org.apache.maven.wagon.authorization.AuthorizationException;
import org.apache.maven.wagon.events.TransferListener;
import org.apache.maven.wagon.repository.Repository;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;

import static org.junit.Assert.*;

/**
 * Unit tests for B2Wagon.
 *
 * Note: Tests that require mocking B2StorageClient use Java's dynamic proxy
 * mechanism instead of Mockito, due to Java 21+ restrictions on mocking
 * interfaces that extend Closeable.
 */
public class B2WagonTest {

    private B2Wagon wagon;
    private TransferListener mockTransferListener;
    private AuthenticationInfo authInfo;
    private Repository repository;

    // Proxy-based mock state
    private B2FileVersion uploadResult;
    private B2Exception uploadException;
    private B2Exception downloadException;
    private B2FileVersion fileInfo;
    private B2Exception fileInfoException;
    private B2UploadFileRequest capturedUploadRequest;

    @Before
    public void setUp() throws Exception {
        wagon = new B2Wagon();

        // Set up authentication
        authInfo = new AuthenticationInfo();
        authInfo.setUserName("testKeyId");
        authInfo.setPassword("testApplicationKey");

        // Set up repository
        repository = new Repository("test-repo", "b2://test-bucket/path/to/repo");

        // Create a simple transfer listener proxy
        mockTransferListener = createTransferListenerProxy();
        wagon.addTransferListener(mockTransferListener);

        // Reset mock state
        uploadResult = null;
        uploadException = null;
        downloadException = null;
        fileInfo = null;
        fileInfoException = null;
        capturedUploadRequest = null;
    }

    private TransferListener createTransferListenerProxy() {
        return (TransferListener) Proxy.newProxyInstance(
                TransferListener.class.getClassLoader(),
                new Class<?>[] { TransferListener.class },
                (proxy, method, args) -> null
        );
    }

    private B2StorageClient createB2ClientProxy() {
        return (B2StorageClient) Proxy.newProxyInstance(
                B2StorageClient.class.getClassLoader(),
                new Class<?>[] { B2StorageClient.class },
                new B2ClientInvocationHandler()
        );
    }

    private class B2ClientInvocationHandler implements InvocationHandler {
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String methodName = method.getName();

            switch (methodName) {
                case "uploadSmallFile":
                    if (uploadException != null) {
                        throw uploadException;
                    }
                    capturedUploadRequest = (B2UploadFileRequest) args[0];
                    return uploadResult;

                case "downloadByName":
                    if (downloadException != null) {
                        throw downloadException;
                    }
                    return null;

                case "getFileInfoByName":
                    if (fileInfoException != null) {
                        throw fileInfoException;
                    }
                    return fileInfo;

                case "close":
                    return null;

                case "toString":
                    return "MockB2StorageClient";

                case "hashCode":
                    return System.identityHashCode(proxy);

                case "equals":
                    return proxy == args[0];

                default:
                    throw new UnsupportedOperationException("Method not mocked: " + methodName);
            }
        }
    }

    @Test
    public void testB2AuthWithSystemProperties() {
        // Test credential chain with system properties
        System.setProperty("b2.applicationKeyId", "sys-key-id");
        System.setProperty("b2.applicationKey", "sys-key");

        try {
            B2Auth.B2Credentials creds = B2Auth.getCredentials(null);
            assertEquals("sys-key-id", creds.getApplicationKeyId());
            assertEquals("sys-key", creds.getApplicationKey());
            assertTrue(creds.isValid());
        } finally {
            System.clearProperty("b2.applicationKeyId");
            System.clearProperty("b2.applicationKey");
        }
    }

    @Test
    public void testB2AuthWithMavenSettings() {
        // Test credential chain with Maven settings (no system props or env vars)
        AuthenticationInfo auth = new AuthenticationInfo();
        auth.setUserName("maven-key-id");
        auth.setPassword("maven-key");

        B2Auth.B2Credentials creds = B2Auth.getCredentials(auth);
        assertEquals("maven-key-id", creds.getApplicationKeyId());
        assertEquals("maven-key", creds.getApplicationKey());
        assertTrue(creds.isValid());
    }

    @Test
    public void testB2AuthWithNullCredentials() {
        B2Auth.B2Credentials creds = B2Auth.getCredentials(null);
        assertFalse(creds.isValid());
    }

    @Test
    public void testB2AuthWithPartialCredentials() {
        AuthenticationInfo auth = new AuthenticationInfo();
        auth.setUserName("only-key-id");
        auth.setPassword(null);

        B2Auth.B2Credentials creds = B2Auth.getCredentials(auth);
        assertFalse(creds.isValid());
    }

    @Test
    public void testPutFile() throws Exception {
        injectMockClient();

        File sourceFile = createTempFile("test-content");
        String destination = "com/example/artifact/1.0.0/artifact-1.0.0.jar";

        uploadResult = createMockFileVersion();

        wagon.put(sourceFile, destination);

        assertNotNull(capturedUploadRequest);
        assertEquals("path/to/repo/com/example/artifact/1.0.0/artifact-1.0.0.jar", capturedUploadRequest.getFileName());
        assertEquals("application/java-archive", capturedUploadRequest.getContentType());

        sourceFile.delete();
    }

    @Test
    public void testPutFilePomContentType() throws Exception {
        injectMockClient();

        File sourceFile = createTempFile("pom-content");
        String destination = "com/example/artifact/1.0.0/artifact-1.0.0.pom";

        uploadResult = createMockFileVersion();

        wagon.put(sourceFile, destination);

        assertNotNull(capturedUploadRequest);
        assertEquals("application/xml", capturedUploadRequest.getContentType());

        sourceFile.delete();
    }

    @Test
    public void testPutFileXmlContentType() throws Exception {
        injectMockClient();

        File sourceFile = createTempFile("xml-content");
        String destination = "com/example/artifact/1.0.0/maven-metadata.xml";

        uploadResult = createMockFileVersion();

        wagon.put(sourceFile, destination);

        assertNotNull(capturedUploadRequest);
        assertEquals("application/xml", capturedUploadRequest.getContentType());

        sourceFile.delete();
    }

    @Test
    public void testPutFileSha1ContentType() throws Exception {
        injectMockClient();

        File sourceFile = createTempFile("checksum");
        String destination = "com/example/artifact/1.0.0/artifact-1.0.0.jar.sha1";

        uploadResult = createMockFileVersion();

        wagon.put(sourceFile, destination);

        assertNotNull(capturedUploadRequest);
        assertEquals("text/plain", capturedUploadRequest.getContentType());

        sourceFile.delete();
    }

    @Test
    public void testPutFileMd5ContentType() throws Exception {
        injectMockClient();

        File sourceFile = createTempFile("checksum");
        String destination = "com/example/artifact/1.0.0/artifact-1.0.0.jar.md5";

        uploadResult = createMockFileVersion();

        wagon.put(sourceFile, destination);

        assertNotNull(capturedUploadRequest);
        assertEquals("text/plain", capturedUploadRequest.getContentType());

        sourceFile.delete();
    }

    @Test
    public void testPutFileWithB2Exception() throws Exception {
        injectMockClient();

        File sourceFile = createTempFile("test-content");
        String destination = "com/example/artifact/1.0.0/artifact-1.0.0.jar";

        uploadException = new B2Exception("test", 500, null, "Upload failed");

        try {
            wagon.put(sourceFile, destination);
            fail("Should throw TransferFailedException on B2Exception");
        } catch (TransferFailedException e) {
            assertTrue(e.getMessage().contains("Failed to upload"));
            assertTrue(e.getCause() instanceof B2Exception);
        }

        sourceFile.delete();
    }

    @Test
    public void testPutFileWithUnauthorizedException() throws Exception {
        injectMockClient();

        File sourceFile = createTempFile("test-content");
        String destination = "com/example/artifact/1.0.0/artifact-1.0.0.jar";

        uploadException = new B2UnauthorizedException("test", null, "Unauthorized");

        try {
            wagon.put(sourceFile, destination);
            fail("Should throw AuthorizationException on B2UnauthorizedException");
        } catch (AuthorizationException e) {
            assertTrue(e.getMessage().contains("Not authorized"));
        }

        sourceFile.delete();
    }

    @Test
    public void testGetFile() throws Exception {
        injectMockClient();

        String resourceName = "com/example/artifact/1.0.0/artifact-1.0.0.jar";
        File destination = File.createTempFile("maven-test-", ".jar");

        wagon.get(resourceName, destination);

        destination.delete();
    }

    @Test
    public void testGetFileNotFound() throws Exception {
        injectMockClient();

        String resourceName = "com/example/artifact/1.0.0/not-found.jar";
        File destination = File.createTempFile("maven-test-", ".jar");

        downloadException = new B2NotFoundException("test", null, "Not found");

        try {
            wagon.get(resourceName, destination);
            fail("Should throw ResourceDoesNotExistException when file not found");
        } catch (ResourceDoesNotExistException e) {
            assertTrue(e.getMessage().contains("Resource not found"));
            assertTrue(e.getCause() instanceof B2NotFoundException);
        }

        destination.delete();
    }

    @Test
    public void testGetFileWithB2Exception() throws Exception {
        injectMockClient();

        String resourceName = "com/example/artifact/1.0.0/artifact-1.0.0.jar";
        File destination = File.createTempFile("maven-test-", ".jar");

        downloadException = new B2Exception("test", 500, null, "Download failed");

        try {
            wagon.get(resourceName, destination);
            fail("Should throw TransferFailedException on B2Exception");
        } catch (TransferFailedException e) {
            assertTrue(e.getMessage().contains("Failed to download"));
        }

        destination.delete();
    }

    @Test
    public void testGetIfNewerWhenRemoteIsNewer() throws Exception {
        injectMockClient();

        String resourceName = "com/example/artifact/1.0.0/artifact-1.0.0.jar";
        File destination = File.createTempFile("maven-test-", ".jar");
        long localTimestamp = 1000000L;

        fileInfo = createMockFileVersion(2000000L);

        boolean result = wagon.getIfNewer(resourceName, destination, localTimestamp);

        assertTrue(result);

        destination.delete();
    }

    @Test
    public void testGetIfNewerWhenLocalIsNewer() throws Exception {
        injectMockClient();

        String resourceName = "com/example/artifact/1.0.0/artifact-1.0.0.jar";
        File destination = File.createTempFile("maven-test-", ".jar");
        long localTimestamp = 2000000L;

        fileInfo = createMockFileVersion(1000000L);

        boolean result = wagon.getIfNewer(resourceName, destination, localTimestamp);

        assertFalse(result);

        destination.delete();
    }

    @Test
    public void testGetIfNewerWhenFileNotFound() throws Exception {
        injectMockClient();

        String resourceName = "com/example/artifact/1.0.0/not-found.jar";
        File destination = File.createTempFile("maven-test-", ".jar");

        fileInfoException = new B2NotFoundException("test", null, "Not found");

        try {
            wagon.getIfNewer(resourceName, destination, 1000000L);
            fail("Should throw ResourceDoesNotExistException when file not found");
        } catch (ResourceDoesNotExistException e) {
            assertTrue(e.getCause() instanceof B2NotFoundException);
        }

        destination.delete();
    }

    @Test
    public void testGetIfNewerWithB2Exception() throws Exception {
        injectMockClient();

        String resourceName = "com/example/artifact/1.0.0/artifact-1.0.0.jar";
        File destination = File.createTempFile("maven-test-", ".jar");

        fileInfoException = new B2Exception("test", 500, null, "Error");

        boolean result = wagon.getIfNewer(resourceName, destination, 1000000L);

        assertTrue(result);

        destination.delete();
    }

    @Test
    public void testResourceExists() throws Exception {
        injectMockClient();

        String resourceName = "com/example/artifact/1.0.0/artifact-1.0.0.jar";

        fileInfo = createMockFileVersion();

        boolean exists = wagon.resourceExists(resourceName);

        assertTrue(exists);
    }

    @Test
    public void testResourceDoesNotExist() throws Exception {
        injectMockClient();

        String resourceName = "com/example/artifact/1.0.0/not-found.jar";

        fileInfoException = new B2NotFoundException("test", null, "Not found");

        boolean exists = wagon.resourceExists(resourceName);

        assertFalse(exists);
    }

    @Test
    public void testResourceExistsWithB2Exception() throws Exception {
        injectMockClient();

        String resourceName = "com/example/artifact/1.0.0/artifact-1.0.0.jar";

        fileInfoException = new B2Exception("test", 500, null, "Error");

        try {
            wagon.resourceExists(resourceName);
            fail("Should throw TransferFailedException on B2Exception");
        } catch (TransferFailedException e) {
            assertTrue(e.getMessage().contains("Error checking resource"));
            assertTrue(e.getCause() instanceof B2Exception);
        }
    }

    @Test
    public void testSupportsDirectoryCopy() {
        assertFalse(wagon.supportsDirectoryCopy());
    }

    @Test
    public void testGetFileListThrowsUnsupportedOperation() {
        try {
            wagon.getFileList("some/directory");
            fail("Should throw UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("not supported"));
        }
    }

    @Test
    public void testPutDirectoryThrowsUnsupportedOperation() {
        File dir = new File("/tmp");
        try {
            wagon.putDirectory(dir, "some/directory");
            fail("Should throw UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("not supported"));
        }
    }

    // Helper methods

    private void injectMockClient() throws Exception {
        // Set the repository first
        setPrivateField(wagon, "repository", repository);

        // Inject proxy-based mock client
        B2StorageClient mockClient = createB2ClientProxy();
        setPrivateField(wagon, "client", mockClient);

        // Inject mock bucket ID
        setPrivateField(wagon, "bucketId", "test-bucket-id");
    }

    private File createTempFile(String content) throws IOException {
        File file = File.createTempFile("test-", ".tmp");
        try (FileWriter writer = new FileWriter(file)) {
            writer.write(content);
        }
        return file;
    }

    private void setPrivateField(Object target, String fieldName, Object value) throws Exception {
        Field field = findFieldInHierarchy(target.getClass(), fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private Field findFieldInHierarchy(Class<?> clazz, String fieldName) throws NoSuchFieldException {
        Class<?> current = clazz;
        while (current != null) {
            try {
                return current.getDeclaredField(fieldName);
            } catch (NoSuchFieldException e) {
                current = current.getSuperclass();
            }
        }
        throw new NoSuchFieldException("Field " + fieldName + " not found in class hierarchy");
    }

    private B2FileVersion createMockFileVersion() {
        return createMockFileVersion(System.currentTimeMillis());
    }

    private B2FileVersion createMockFileVersion(long uploadTimestamp) {
        // Create a real B2FileVersion instance with the required constructor arguments
        return new B2FileVersion(
                "fileId123",           // fileId
                "test/file.jar",       // fileName
                1024L,                 // contentLength
                "application/octet-stream", // contentType
                "sha1hash",            // contentSha1
                null,                  // contentMd5
                Collections.emptyMap(), // fileInfo
                B2FileVersion.UPLOAD_ACTION, // action
                uploadTimestamp,       // uploadTimestamp
                null,                  // fileRetention
                null,                  // legalHold
                null,                  // serverSideEncryption
                null                   // replicationStatus
        );
    }
}
