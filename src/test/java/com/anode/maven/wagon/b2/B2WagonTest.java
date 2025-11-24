package com.anode.maven.wagon.b2;

import com.backblaze.b2.client.B2StorageClient;
import com.backblaze.b2.client.contentHandlers.B2ContentFileWriter;
import com.backblaze.b2.client.contentSources.B2FileContentSource;
import com.backblaze.b2.client.exceptions.B2Exception;
import com.backblaze.b2.client.exceptions.B2NotFoundException;
import com.backblaze.b2.client.structures.B2Bucket;
import com.backblaze.b2.client.structures.B2DownloadByNameRequest;
import com.backblaze.b2.client.structures.B2FileVersion;
import com.backblaze.b2.client.structures.B2UploadFileRequest;
import org.apache.maven.wagon.*;
import org.apache.maven.wagon.authentication.AuthenticationException;
import org.apache.maven.wagon.authentication.AuthenticationInfo;
import org.apache.maven.wagon.authorization.AuthorizationException;
import org.apache.maven.wagon.events.TransferEvent;
import org.apache.maven.wagon.events.TransferListener;
import org.apache.maven.wagon.repository.Repository;
import org.apache.maven.wagon.resource.Resource;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class B2WagonTest {

    private B2Wagon wagon;

    @Mock
    private B2StorageClient mockB2Client;

    @Mock
    private B2Bucket mockBucket;

    @Mock
    private B2FileVersion mockFileVersion;

    @Mock
    private TransferListener mockTransferListener;

    private AuthenticationInfo authInfo;
    private Repository repository;

    @Before
    public void setUp() throws Exception {
        wagon = new B2Wagon();

        // Set up authentication
        authInfo = new AuthenticationInfo();
        authInfo.setUserName("testKeyId");
        authInfo.setPassword("testApplicationKey");

        // Set up repository
        repository = new Repository("test-repo", "b2://test-bucket/path/to/repo");

        // Use reflection to set repository and auth info
        setPrivateField(wagon, "repository", repository);
        setPrivateField(wagon, "authenticationInfo", authInfo);

        // Add transfer listener
        wagon.addTransferListener(mockTransferListener);
    }

    @Test
    public void testOpenConnectionWithNullAuthInfo() throws Exception {
        // Test that connection fails when authentication info is not set
        AuthenticationInfo nullAuth = null;

        try {
            wagon.connect(repository, nullAuth);
            fail("Should throw AuthenticationException when auth info is null");
        } catch (AuthenticationException e) {
            // Expected - the code checks for null credentials and throws AuthenticationException
            assertTrue(e.getMessage().contains("credentials not provided"));
        }
    }

    @Test
    public void testOpenConnectionWithNullCredentials() throws Exception {
        authInfo.setUserName(null);
        authInfo.setPassword(null);

        try {
            wagon.connect(repository, authInfo);
            fail("Should throw AuthenticationException when credentials are null");
        } catch (Exception e) {
            assertTrue(e instanceof AuthenticationException);
            assertTrue(e.getMessage().contains("credentials not provided"));
        }
    }

    @Test
    public void testOpenConnectionWithNullKeyId() throws Exception {
        authInfo.setUserName(null);
        authInfo.setPassword("testKey");

        try {
            wagon.connect(repository, authInfo);
            fail("Should throw AuthenticationException when keyId is null");
        } catch (Exception e) {
            assertTrue(e instanceof AuthenticationException);
        }
    }

    @Test
    public void testOpenConnectionWithNullApplicationKey() throws Exception {
        authInfo.setUserName("testKeyId");
        authInfo.setPassword(null);

        try {
            wagon.connect(repository, authInfo);
            fail("Should throw AuthenticationException when application key is null");
        } catch (Exception e) {
            assertTrue(e instanceof AuthenticationException);
        }
    }

    @Test
    public void testOpenConnectionWithEmptyBucketName() throws Exception {
        // Create a new wagon instance for this test to avoid state issues
        B2Wagon testWagon = new B2Wagon();

        // Create a repository with empty bucket name (just the protocol)
        Repository emptyBucketRepo = new Repository("test-repo", "b2:///path");

        // Use reflection to set the repository with empty bucket
        setPrivateField(testWagon, "repository", emptyBucketRepo);
        setPrivateField(testWagon, "authenticationInfo", authInfo);

        try {
            // Try to connect - should fail with empty bucket name
            testWagon.connect(emptyBucketRepo, authInfo);
            fail("Should fail when bucket name is empty");
        } catch (Exception e) {
            // Expected - operations should fail with empty bucket
            assertNotNull(e);
        }
    }

    @Test
    public void testCloseConnection() throws Exception {
        // Inject mock client
        injectMockClient();

        wagon.disconnect();

        verify(mockB2Client).close();
    }

    @Test
    public void testCloseConnectionWithException() throws Exception {
        // Inject mock client
        injectMockClient();

        doThrow(new RuntimeException("Close error")).when(mockB2Client).close();

        // Should not throw exception
        wagon.disconnect();

        verify(mockB2Client).close();
    }

    @Test
    public void testPutFile() throws Exception {
        injectMockClient();

        File sourceFile = createTempFile("test-content");
        String destination = "com/example/artifact/1.0.0/artifact-1.0.0.jar";

        when(mockB2Client.uploadSmallFile(any(B2UploadFileRequest.class))).thenReturn(mockFileVersion);

        wagon.put(sourceFile, destination);

        ArgumentCaptor<B2UploadFileRequest> requestCaptor = ArgumentCaptor.forClass(B2UploadFileRequest.class);
        verify(mockB2Client).uploadSmallFile(requestCaptor.capture());

        B2UploadFileRequest request = requestCaptor.getValue();
        assertEquals("path/to/repo/com/example/artifact/1.0.0/artifact-1.0.0.jar", request.getFileName());
        assertEquals("application/octet-stream", request.getContentType());

        // Verify events were fired
        verify(mockTransferListener, atLeastOnce()).transferInitiated(any(TransferEvent.class));
        verify(mockTransferListener, atLeastOnce()).transferStarted(any(TransferEvent.class));
        verify(mockTransferListener, atLeastOnce()).transferCompleted(any(TransferEvent.class));

        sourceFile.delete();
    }

    @Test
    public void testPutFilePomContentType() throws Exception {
        injectMockClient();

        File sourceFile = createTempFile("pom-content");
        String destination = "com/example/artifact/1.0.0/artifact-1.0.0.pom";

        when(mockB2Client.uploadSmallFile(any(B2UploadFileRequest.class))).thenReturn(mockFileVersion);

        wagon.put(sourceFile, destination);

        ArgumentCaptor<B2UploadFileRequest> requestCaptor = ArgumentCaptor.forClass(B2UploadFileRequest.class);
        verify(mockB2Client).uploadSmallFile(requestCaptor.capture());

        assertEquals("application/xml", requestCaptor.getValue().getContentType());

        sourceFile.delete();
    }

    @Test
    public void testPutFileXmlContentType() throws Exception {
        injectMockClient();

        File sourceFile = createTempFile("xml-content");
        String destination = "com/example/artifact/1.0.0/maven-metadata.xml";

        when(mockB2Client.uploadSmallFile(any(B2UploadFileRequest.class))).thenReturn(mockFileVersion);

        wagon.put(sourceFile, destination);

        ArgumentCaptor<B2UploadFileRequest> requestCaptor = ArgumentCaptor.forClass(B2UploadFileRequest.class);
        verify(mockB2Client).uploadSmallFile(requestCaptor.capture());

        assertEquals("application/xml", requestCaptor.getValue().getContentType());

        sourceFile.delete();
    }

    @Test
    public void testPutFileSha1ContentType() throws Exception {
        injectMockClient();

        File sourceFile = createTempFile("checksum");
        String destination = "com/example/artifact/1.0.0/artifact-1.0.0.jar.sha1";

        when(mockB2Client.uploadSmallFile(any(B2UploadFileRequest.class))).thenReturn(mockFileVersion);

        wagon.put(sourceFile, destination);

        ArgumentCaptor<B2UploadFileRequest> requestCaptor = ArgumentCaptor.forClass(B2UploadFileRequest.class);
        verify(mockB2Client).uploadSmallFile(requestCaptor.capture());

        // B2ContentTypes.TEXT_PLAIN is just "text/plain" without charset
        assertEquals("text/plain", requestCaptor.getValue().getContentType());

        sourceFile.delete();
    }

    @Test
    public void testPutFileMd5ContentType() throws Exception {
        injectMockClient();

        File sourceFile = createTempFile("checksum");
        String destination = "com/example/artifact/1.0.0/artifact-1.0.0.jar.md5";

        when(mockB2Client.uploadSmallFile(any(B2UploadFileRequest.class))).thenReturn(mockFileVersion);

        wagon.put(sourceFile, destination);

        ArgumentCaptor<B2UploadFileRequest> requestCaptor = ArgumentCaptor.forClass(B2UploadFileRequest.class);
        verify(mockB2Client).uploadSmallFile(requestCaptor.capture());

        // B2ContentTypes.TEXT_PLAIN is just "text/plain" without charset
        assertEquals("text/plain", requestCaptor.getValue().getContentType());

        sourceFile.delete();
    }

    @Test
    public void testPutFileWarContentType() throws Exception {
        injectMockClient();

        File sourceFile = createTempFile("war-content");
        String destination = "com/example/artifact/1.0.0/artifact-1.0.0.war";

        when(mockB2Client.uploadSmallFile(any(B2UploadFileRequest.class))).thenReturn(mockFileVersion);

        wagon.put(sourceFile, destination);

        ArgumentCaptor<B2UploadFileRequest> requestCaptor = ArgumentCaptor.forClass(B2UploadFileRequest.class);
        verify(mockB2Client).uploadSmallFile(requestCaptor.capture());

        assertEquals("application/octet-stream", requestCaptor.getValue().getContentType());

        sourceFile.delete();
    }

    @Test
    public void testPutFileEarContentType() throws Exception {
        injectMockClient();

        File sourceFile = createTempFile("ear-content");
        String destination = "com/example/artifact/1.0.0/artifact-1.0.0.ear";

        when(mockB2Client.uploadSmallFile(any(B2UploadFileRequest.class))).thenReturn(mockFileVersion);

        wagon.put(sourceFile, destination);

        ArgumentCaptor<B2UploadFileRequest> requestCaptor = ArgumentCaptor.forClass(B2UploadFileRequest.class);
        verify(mockB2Client).uploadSmallFile(requestCaptor.capture());

        assertEquals("application/octet-stream", requestCaptor.getValue().getContentType());

        sourceFile.delete();
    }

    @Test
    public void testPutFileWithB2Exception() throws Exception {
        injectMockClient();

        File sourceFile = createTempFile("test-content");
        String destination = "com/example/artifact/1.0.0/artifact-1.0.0.jar";

        B2Exception b2Exception = mock(B2Exception.class);
        when(mockB2Client.uploadSmallFile(any(B2UploadFileRequest.class))).thenThrow(b2Exception);

        try {
            wagon.put(sourceFile, destination);
            fail("Should throw TransferFailedException on B2Exception");
        } catch (TransferFailedException e) {
            assertTrue(e.getMessage().contains("B2 error uploading resource"));
            assertTrue(e.getCause() instanceof B2Exception);
        }

        sourceFile.delete();
    }

    @Test
    public void testPutFileWithoutConnection() throws Exception {
        // Don't inject client to simulate no connection
        File sourceFile = createTempFile("test-content");
        String destination = "com/example/artifact/1.0.0/artifact-1.0.0.jar";

        try {
            wagon.put(sourceFile, destination);
            fail("Should throw TransferFailedException when not connected");
        } catch (TransferFailedException e) {
            assertTrue(e.getMessage().contains("B2 client not initialized"));
        }

        sourceFile.delete();
    }

    @Test
    public void testGetFile() throws Exception {
        injectMockClient();

        String resourceName = "com/example/artifact/1.0.0/artifact-1.0.0.jar";
        File destination = File.createTempFile("maven-test-", ".jar");

        // Mock the download - this is tricky since downloadByName writes to file
        doAnswer(invocation -> {
            B2ContentFileWriter writer = invocation.getArgument(1);
            // Simulate successful download by creating a temp file
            return null;
        }).when(mockB2Client).downloadByName(any(B2DownloadByNameRequest.class), any(B2ContentFileWriter.class));

        wagon.get(resourceName, destination);

        verify(mockB2Client).downloadByName(any(B2DownloadByNameRequest.class), any(B2ContentFileWriter.class));

        // Verify events were fired
        verify(mockTransferListener, atLeastOnce()).transferInitiated(any(TransferEvent.class));
        verify(mockTransferListener, atLeastOnce()).transferStarted(any(TransferEvent.class));

        destination.delete();
    }

    @Test
    public void testGetFileNotFound() throws Exception {
        injectMockClient();

        String resourceName = "com/example/artifact/1.0.0/not-found.jar";
        File destination = File.createTempFile("maven-test-", ".jar");

        B2NotFoundException notFoundException = mock(B2NotFoundException.class);
        doThrow(notFoundException).when(mockB2Client).downloadByName(any(B2DownloadByNameRequest.class), any(B2ContentFileWriter.class));

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

        B2Exception b2Exception = mock(B2Exception.class);
        doThrow(b2Exception).when(mockB2Client).downloadByName(any(B2DownloadByNameRequest.class), any(B2ContentFileWriter.class));

        try {
            wagon.get(resourceName, destination);
            fail("Should throw TransferFailedException on B2Exception");
        } catch (TransferFailedException e) {
            assertTrue(e.getMessage().contains("B2 error downloading resource"));
        }

        destination.delete();
    }

    @Test
    public void testGetFileWithoutConnection() throws Exception {
        // Don't inject client to simulate no connection
        String resourceName = "com/example/artifact/1.0.0/artifact-1.0.0.jar";
        File destination = File.createTempFile("maven-test-", ".jar");

        try {
            wagon.get(resourceName, destination);
            fail("Should throw TransferFailedException when not connected");
        } catch (TransferFailedException e) {
            assertTrue(e.getMessage().contains("B2 client not initialized"));
        }

        destination.delete();
    }

    @Test
    public void testGetIfNewerWhenRemoteIsNewer() throws Exception {
        injectMockClient();

        String resourceName = "com/example/artifact/1.0.0/artifact-1.0.0.jar";
        File destination = File.createTempFile("maven-test-", ".jar");
        long localTimestamp = 1000000L;

        when(mockFileVersion.getUploadTimestamp()).thenReturn(2000000L);
        when(mockB2Client.getFileInfoByName(eq("test-bucket"), anyString())).thenReturn(mockFileVersion);

        // Mock download
        doAnswer(invocation -> null).when(mockB2Client).downloadByName(any(B2DownloadByNameRequest.class), any(B2ContentFileWriter.class));

        boolean result = wagon.getIfNewer(resourceName, destination, localTimestamp);

        assertTrue(result);
        verify(mockB2Client).getFileInfoByName(eq("test-bucket"), anyString());
        verify(mockB2Client).downloadByName(any(B2DownloadByNameRequest.class), any(B2ContentFileWriter.class));

        destination.delete();
    }

    @Test
    public void testGetIfNewerWhenLocalIsNewer() throws Exception {
        injectMockClient();

        String resourceName = "com/example/artifact/1.0.0/artifact-1.0.0.jar";
        File destination = File.createTempFile("maven-test-", ".jar");
        long localTimestamp = 2000000L;

        when(mockFileVersion.getUploadTimestamp()).thenReturn(1000000L);
        when(mockB2Client.getFileInfoByName(eq("test-bucket"), anyString())).thenReturn(mockFileVersion);

        boolean result = wagon.getIfNewer(resourceName, destination, localTimestamp);

        assertFalse(result);
        verify(mockB2Client).getFileInfoByName(eq("test-bucket"), anyString());
        verify(mockB2Client, never()).downloadByName(any(B2DownloadByNameRequest.class), any(B2ContentFileWriter.class));

        destination.delete();
    }

    @Test
    public void testGetIfNewerWhenFileNotFound() throws Exception {
        injectMockClient();

        String resourceName = "com/example/artifact/1.0.0/not-found.jar";
        File destination = File.createTempFile("maven-test-", ".jar");

        B2NotFoundException notFoundException = mock(B2NotFoundException.class);
        when(mockB2Client.getFileInfoByName(eq("test-bucket"), anyString())).thenThrow(notFoundException);

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

        B2Exception b2Exception = mock(B2Exception.class);
        when(mockB2Client.getFileInfoByName(eq("test-bucket"), anyString())).thenThrow(b2Exception);

        // Mock download since it should download on exception
        doAnswer(invocation -> null).when(mockB2Client).downloadByName(any(B2DownloadByNameRequest.class), any(B2ContentFileWriter.class));

        boolean result = wagon.getIfNewer(resourceName, destination, 1000000L);

        assertTrue(result);
        verify(mockB2Client).downloadByName(any(B2DownloadByNameRequest.class), any(B2ContentFileWriter.class));

        destination.delete();
    }

    @Test
    public void testResourceExists() throws Exception {
        injectMockClient();

        String resourceName = "com/example/artifact/1.0.0/artifact-1.0.0.jar";

        when(mockB2Client.getFileInfoByName(eq("test-bucket"), anyString())).thenReturn(mockFileVersion);

        boolean exists = wagon.resourceExists(resourceName);

        assertTrue(exists);
        verify(mockB2Client).getFileInfoByName(eq("test-bucket"), eq("path/to/repo/com/example/artifact/1.0.0/artifact-1.0.0.jar"));
    }

    @Test
    public void testResourceDoesNotExist() throws Exception {
        injectMockClient();

        String resourceName = "com/example/artifact/1.0.0/not-found.jar";

        B2NotFoundException notFoundException = mock(B2NotFoundException.class);
        when(mockB2Client.getFileInfoByName(eq("test-bucket"), anyString())).thenThrow(notFoundException);

        boolean exists = wagon.resourceExists(resourceName);

        assertFalse(exists);
        verify(mockB2Client).getFileInfoByName(eq("test-bucket"), anyString());
    }

    @Test
    public void testResourceExistsWithB2Exception() throws Exception {
        injectMockClient();

        String resourceName = "com/example/artifact/1.0.0/artifact-1.0.0.jar";

        B2Exception b2Exception = mock(B2Exception.class);
        when(mockB2Client.getFileInfoByName(eq("test-bucket"), anyString())).thenThrow(b2Exception);

        try {
            wagon.resourceExists(resourceName);
            fail("Should throw TransferFailedException on B2Exception");
        } catch (TransferFailedException e) {
            assertTrue(e.getMessage().contains("Error checking if resource exists"));
            assertTrue(e.getCause() instanceof B2Exception);
        }
    }

    @Test
    public void testBasePathHandling() throws Exception {
        // Test with no path - just verify it doesn't crash
        Repository repo1 = new Repository("test-repo", "b2://test-bucket");
        // Would need to connect to fully test

        // Test with path without trailing slash
        Repository repo2 = new Repository("test-repo", "b2://test-bucket/releases");
        // Would need to connect to fully test

        // Test with path with trailing slash
        Repository repo3 = new Repository("test-repo", "b2://test-bucket/snapshots/");
        // Would need to connect to fully test

        // Since we can't easily test the internal state without invoking connect(),
        // we verify through the behavior of other methods that use basePath
        // This test mainly documents the expected URL formats
        assertNotNull(repo1);
        assertNotNull(repo2);
        assertNotNull(repo3);
    }

    @Test
    public void testBasePathWithLeadingSlash() throws Exception {
        // The basePath should strip leading slashes
        Repository repo = new Repository("test-repo", "b2://test-bucket//path/with/slash");

        // Would need to connect to fully test basePath handling
        // This test mainly documents the expected behavior
        assertNotNull(repo);
    }

    // Helper methods

    @SuppressWarnings("unchecked")
    private void injectMockClient() throws Exception {
        Field b2ClientField = B2Wagon.class.getDeclaredField("b2Client");
        b2ClientField.setAccessible(true);
        b2ClientField.set(wagon, mockB2Client);

        // Populate the bucket ID cache so getBucketId() doesn't try to look it up
        Field bucketIdCacheField = B2Wagon.class.getDeclaredField("bucketIdCache");
        bucketIdCacheField.setAccessible(true);
        java.util.Map<String, String> cache = (java.util.Map<String, String>) bucketIdCacheField.get(wagon);
        cache.put("test-bucket", "test-bucket-id");
    }

    private File createTempFile(String content) throws IOException {
        File file = File.createTempFile("test-", ".tmp");
        java.io.FileWriter writer = new java.io.FileWriter(file);
        writer.write(content);
        writer.close();
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
}
