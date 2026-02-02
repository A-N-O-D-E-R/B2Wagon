package com.anode.maven.b2extension;

import com.backblaze.b2.client.B2StorageClient;
import com.backblaze.b2.client.B2StorageClientFactory;
import com.backblaze.b2.client.contentHandlers.B2ContentFileWriter;
import com.backblaze.b2.client.contentSources.B2ContentTypes;
import com.backblaze.b2.client.contentSources.B2FileContentSource;
import com.backblaze.b2.client.exceptions.B2Exception;
import com.backblaze.b2.client.exceptions.B2NotFoundException;
import com.backblaze.b2.client.exceptions.B2UnauthorizedException;
import com.backblaze.b2.client.structures.B2Bucket;
import com.backblaze.b2.client.structures.B2DownloadByNameRequest;
import com.backblaze.b2.client.structures.B2FileVersion;
import com.backblaze.b2.client.structures.B2UploadFileRequest;

import org.apache.maven.wagon.ConnectionException;
import org.apache.maven.wagon.ResourceDoesNotExistException;
import org.apache.maven.wagon.TransferFailedException;
import org.apache.maven.wagon.authentication.AuthenticationException;
import org.apache.maven.wagon.authentication.AuthenticationInfo;
import org.apache.maven.wagon.authorization.AuthorizationException;
import org.apache.maven.wagon.proxy.ProxyInfoProvider;
import org.apache.maven.wagon.repository.Repository;
import org.apache.maven.wagon.resource.Resource;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/**
 * Maven Wagon implementation for Backblaze B2 Cloud Storage.
 *
 * <p>Supports the {@code b2://} URL scheme for Maven repositories.</p>
 *
 * <h2>URL Format</h2>
 * <pre>b2://{bucket-name}/{base-path}</pre>
 *
 * <h2>Configuration Example</h2>
 * <pre>{@code
 * <!-- In pom.xml -->
 * <build>
 *   <extensions>
 *     <extension>
 *       <groupId>com.anode.maven</groupId>
 *       <artifactId>b2-maven-extension</artifactId>
 *       <version>1.0.0</version>
 *     </extension>
 *   </extensions>
 * </build>
 *
 * <distributionManagement>
 *   <repository>
 *     <id>b2-releases</id>
 *     <url>b2://my-bucket/maven-releases</url>
 *   </repository>
 * </distributionManagement>
 *
 * <!-- In settings.xml -->
 * <server>
 *   <id>b2-releases</id>
 *   <username>applicationKeyId</username>
 *   <password>applicationKey</password>
 * </server>
 * }</pre>
 */
public class B2Wagon extends ListeningWagon {

    private static final String USER_AGENT = "b2-maven-extension/1.0";

    private B2StorageClient client;
    private AuthenticationInfo authenticationInfo;
    private String bucketId;

    @Override
    public void connect(Repository repository, AuthenticationInfo authenticationInfo, ProxyInfoProvider proxyInfoProvider)
            throws ConnectionException, AuthenticationException {

        setRepository(repository);
        this.authenticationInfo = authenticationInfo;

        fireSessionOpening();

        B2Auth.B2Credentials credentials = B2Auth.getCredentials(authenticationInfo);
        if (!credentials.isValid()) {
            throw new AuthenticationException(
                    "B2 credentials not found. Configure credentials via:\n" +
                            "  1. System properties: -Db2.applicationKeyId=... -Db2.applicationKey=...\n" +
                            "  2. Environment variables: B2_APPLICATION_KEY_ID, B2_APPLICATION_KEY\n" +
                            "  3. Maven settings.xml: <server><username>keyId</username><password>key</password></server>"
            );
        }

        try {
            client = B2StorageClientFactory
                    .createDefaultFactory()
                    .create(credentials.getApplicationKeyId(), credentials.getApplicationKey(), USER_AGENT);

            // Resolve bucket ID
            String bucketName = getBucketName();
            B2Bucket bucket = client.getBucketOrNullByName(bucketName);
            if (bucket == null) {
                throw new ConnectionException("Bucket not found: " + bucketName +
                        ". Ensure the bucket exists and credentials have access.");
            }
            bucketId = bucket.getBucketId();

            fireSessionLoggedIn();
            fireSessionOpened();

        } catch (B2UnauthorizedException e) {
            throw new AuthenticationException("B2 authentication failed: " + e.getMessage(), e);
        } catch (B2Exception e) {
            throw new ConnectionException("Failed to connect to B2: " + e.getMessage(), e);
        }
    }

    @Override
    public void disconnect() throws ConnectionException {
        fireSessionDisconnecting();

        if (client != null) {
            client.close();
            client = null;
        }

        bucketId = null;
        authenticationInfo = null;

        fireSessionLoggedOff();
        fireSessionDisconnected();
    }

    @Override
    public void get(String resourceName, File destination)
            throws TransferFailedException, ResourceDoesNotExistException, AuthorizationException {

        Resource resource = new Resource(resourceName);
        fireGetTransferInitiated(resource, destination);
        fireGetTransferStarted(resource, destination);

        try {
            String key = getKey(resourceName);

            B2DownloadByNameRequest downloadRequest = B2DownloadByNameRequest
                    .builder(getBucketName(), key)
                    .build();

            // Download to temporary file first
            File tempFile = File.createTempFile("b2-maven-", ".tmp");
            try {
                B2ContentFileWriter fileWriter = B2ContentFileWriter.builder(tempFile).build();
                client.downloadByName(downloadRequest, fileWriter);

                // Move to destination
                writeStreamToFile(tempFile, destination);

                resource.setContentLength(destination.length());
                fireGetTransferCompleted(resource, destination);

            } finally {
                if (tempFile.exists()) {
                    tempFile.delete();
                }
            }

        } catch (B2NotFoundException e) {
            throw new ResourceDoesNotExistException("Resource not found: " + resourceName, e);
        } catch (B2UnauthorizedException e) {
            throw new AuthorizationException("Not authorized to access: " + resourceName, e);
        } catch (B2Exception e) {
            throw new TransferFailedException("Failed to download: " + resourceName, e);
        } catch (IOException e) {
            throw new TransferFailedException("IO error downloading: " + resourceName, e);
        }
    }

    @Override
    public boolean getIfNewer(String resourceName, File destination, long timestamp)
            throws TransferFailedException, ResourceDoesNotExistException, AuthorizationException {

        try {
            String key = getKey(resourceName);
            B2FileVersion fileVersion = client.getFileInfoByName(getBucketName(), key);

            if (fileVersion.getUploadTimestamp() > timestamp) {
                get(resourceName, destination);
                return true;
            }

            return false;

        } catch (B2NotFoundException e) {
            throw new ResourceDoesNotExistException("Resource not found: " + resourceName, e);
        } catch (B2UnauthorizedException e) {
            throw new AuthorizationException("Not authorized to access: " + resourceName, e);
        } catch (B2Exception e) {
            // If we can't determine, download anyway
            get(resourceName, destination);
            return true;
        }
    }

    @Override
    public void put(File source, String destination)
            throws TransferFailedException, ResourceDoesNotExistException, AuthorizationException {

        Resource resource = new Resource(destination);
        firePutTransferInitiated(resource, source);

        try {
            String key = getKey(destination);
            String contentType = getContentType(key);

            B2FileContentSource contentSource = B2FileContentSource.build(source);

            B2UploadFileRequest uploadRequest = B2UploadFileRequest
                    .builder(bucketId, key, contentType, contentSource)
                    .build();

            firePutTransferStarted(resource, source);

            // Use PutInputStream to track progress
            client.uploadSmallFile(uploadRequest);

            firePutTransferCompleted(resource, source);

        } catch (B2UnauthorizedException e) {
            throw new AuthorizationException("Not authorized to upload: " + destination, e);
        } catch (B2Exception e) {
            throw new TransferFailedException("Failed to upload: " + destination, e);
        }
    }

    @Override
    public boolean resourceExists(String resourceName) throws TransferFailedException, AuthorizationException {
        try {
            String key = getKey(resourceName);
            B2FileVersion fileVersion = client.getFileInfoByName(getBucketName(), key);
            return fileVersion != null;
        } catch (B2NotFoundException e) {
            return false;
        } catch (B2UnauthorizedException e) {
            throw new AuthorizationException("Not authorized to check: " + resourceName, e);
        } catch (B2Exception e) {
            throw new TransferFailedException("Error checking resource: " + resourceName, e);
        }
    }

    @Override
    public boolean supportsDirectoryCopy() {
        return false;
    }

    /**
     * Gets the bucket name from the repository URL.
     * URL format: b2://bucket-name/path
     */
    private String getBucketName() {
        return getRepository().getHost();
    }

    /**
     * Constructs the B2 object key from resource name.
     */
    private String getKey(String resourceName) {
        String basePath = getRepository().getBasedir();
        if (basePath == null) {
            basePath = "";
        }
        if (basePath.startsWith("/")) {
            basePath = basePath.substring(1);
        }
        if (!basePath.isEmpty() && !basePath.endsWith("/")) {
            basePath += "/";
        }
        return basePath + resourceName;
    }

    /**
     * Determines content type based on file extension.
     */
    private String getContentType(String fileName) {
        if (fileName.endsWith(".pom") || fileName.endsWith(".xml")) {
            return "application/xml";
        } else if (fileName.endsWith(".jar") || fileName.endsWith(".war") || fileName.endsWith(".ear")) {
            return "application/java-archive";
        } else if (fileName.endsWith(".sha1") || fileName.endsWith(".md5") || fileName.endsWith(".sha256") || fileName.endsWith(".sha512")) {
            return "text/plain";
        } else if (fileName.endsWith(".asc")) {
            return "text/plain";
        }
        return B2ContentTypes.B2_AUTO;
    }

    /**
     * Safely writes content from source file to destination.
     * Uses atomic move if possible, falls back to copy.
     */
    private void writeStreamToFile(File source, File destination) throws IOException {
        try {
            Files.move(source.toPath(), destination.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            // Fallback to copy if move fails (e.g., across filesystems)
            try (FileInputStream in = new FileInputStream(source);
                 FileOutputStream out = new FileOutputStream(destination)) {
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = in.read(buffer)) != -1) {
                    out.write(buffer, 0, bytesRead);
                }
            }
        }
    }

    /**
     * Inner class that wraps FileInputStream to fire progress events during upload.
     */
    private class PutInputStream extends FileInputStream {
        private final Resource resource;
        private final File file;

        PutInputStream(File file, Resource resource) throws IOException {
            super(file);
            this.file = file;
            this.resource = resource;
        }

        @Override
        public int read(byte[] b) throws IOException {
            int length = super.read(b);
            if (length > 0) {
                firePutTransferProgress(resource, file, b, length);
            }
            return length;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int length = super.read(b, off, len);
            if (length > 0) {
                byte[] buffer = new byte[length];
                System.arraycopy(b, off, buffer, 0, length);
                firePutTransferProgress(resource, file, buffer, length);
            }
            return length;
        }
    }
}
