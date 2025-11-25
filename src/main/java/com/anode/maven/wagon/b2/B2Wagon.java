package com.anode.maven.wagon.b2;

import org.codehaus.plexus.component.annotations.Component;
import org.apache.maven.wagon.AbstractWagon;
import org.apache.maven.wagon.ConnectionException;
import org.apache.maven.wagon.ResourceDoesNotExistException;
import org.apache.maven.wagon.TransferFailedException;
import org.apache.maven.wagon.authentication.AuthenticationException;
import org.apache.maven.wagon.authorization.AuthorizationException;
import org.apache.maven.wagon.resource.Resource;

import com.backblaze.b2.client.B2StorageClient;
import com.backblaze.b2.client.B2StorageClientFactory;
import com.backblaze.b2.client.contentHandlers.B2ContentFileWriter;
import com.backblaze.b2.client.contentSources.B2ContentTypes;
import com.backblaze.b2.client.contentSources.B2FileContentSource;
import com.backblaze.b2.client.exceptions.B2Exception;
import com.backblaze.b2.client.exceptions.B2NotFoundException;
import com.backblaze.b2.client.structures.B2Bucket;
import com.backblaze.b2.client.structures.B2DownloadByNameRequest;
import com.backblaze.b2.client.structures.B2FileVersion;
import com.backblaze.b2.client.structures.B2UploadFileRequest;

import java.io.*;

/**
 * Maven Wagon implementation for Backblaze B2 Cloud Storage using the official B2 SDK.
 * 
 * Usage in pom.xml:
 * <pre>
 * {@code
 * <distributionManagement>
 *   <repository>
 *     <id>b2-releases</id>
 *     <url>b2://bucket-name/path/to/repo</url>
 *   </repository>
 * </distributionManagement>
 * 
 * In settings.xml:
 * <server>
 *   <id>b2-releases</id>
 *   <username>applicationKeyId</username>
 *   <password>applicationKey</password>
 * </server>
 * }
 * </pre>
 */
@Component(role = org.apache.maven.wagon.Wagon.class, hint = "b2")
public class B2Wagon extends AbstractWagon {

    // Connection pool: maps repository URL to B2StorageClient
    private static java.util.Map<String, B2StorageClient> connectionPool = new java.util.concurrent.ConcurrentHashMap<>();
    // Cache bucket IDs to avoid repeated lookups for the same bucket
    private static java.util.Map<String, String> bucketIdCache = new java.util.concurrent.ConcurrentHashMap<>();

    // Current connection info (instance-specific)
    private String currentRepositoryUrl = null;
    private String currentBucketName = null;
    private String currentBasePath = null;

    /**
     * Parses repository information from a URL.
     * Format: b2://bucket-name/base/path
     */
    private void parseRepositoryUrl(String url) {
        if (url == null || !url.startsWith("b2://")) {
            throw new IllegalArgumentException("Invalid B2 URL: " + url);
        }

        // Remove b2:// prefix
        String remaining = url.substring(5);

        // Split into bucket and path
        int slashIndex = remaining.indexOf('/');
        if (slashIndex == -1) {
            currentBucketName = remaining;
            currentBasePath = "";
        } else {
            currentBucketName = remaining.substring(0, slashIndex);
            currentBasePath = remaining.substring(slashIndex + 1);
            if (!currentBasePath.isEmpty() && !currentBasePath.endsWith("/")) {
                currentBasePath += "/";
            }
        }

        currentRepositoryUrl = url;
    }

    /**
     * Gets the bucket name from the parsed repository URL.
     */
    private String getBucketName() {
        return currentBucketName != null ? currentBucketName : getRepository().getHost();
    }

    /**
     * Gets the base path from the parsed repository URL.
     */
    private String getBasePath() {
        return currentBasePath != null ? currentBasePath : getDefaultBasePath();
    }

    private String getDefaultBasePath() {
        String path = getRepository().getBasedir();
        String basePath = path != null && !path.isEmpty() ? path : "";
        if (basePath.startsWith("/")) {
            basePath = basePath.substring(1);
        }
        if (!basePath.isEmpty() && !basePath.endsWith("/")) {
            basePath += "/";
        }
        return basePath;
    }

    /**
     * Gets the bucket ID for the current bucket, using cache to avoid repeated lookups.
     */
    private String getBucketId() throws TransferFailedException {
        String bucketName = getBucketName();
        if (!bucketIdCache.containsKey(bucketName)) {
            try {
                B2StorageClient client = getOrCreateClient();
                B2Bucket bucket = client.getBucketOrNullByName(bucketName);
                if (bucket == null) {
                    throw new TransferFailedException("Bucket not found: " + bucketName + ". Ensure the bucket exists and credentials have access.");
                }
                bucketIdCache.put(bucketName, bucket.getBucketId());
            } catch (B2Exception e) {
                throw new TransferFailedException("Failed to get bucket ID for: " + bucketName, e);
            } catch (ConnectionException | AuthenticationException e) {
                throw new TransferFailedException("Failed to establish connection", e);
            }
        }
        return bucketIdCache.get(bucketName);
    }

    /**
     * Gets or creates a B2 client for the current bucket.
     * Uses a connection pool to reuse clients across repositories.
     */
    private B2StorageClient getOrCreateClient() throws ConnectionException, AuthenticationException {
        String bucketKey = currentBucketName != null ? currentBucketName : getBucketName();

        if (!connectionPool.containsKey(bucketKey)) {

            String keyId = getAuthenticationInfo().getUserName();
            String applicationKey = getAuthenticationInfo().getPassword();

            if (keyId == null || applicationKey == null) {
                throw new AuthenticationException("B2 credentials not provided. Ensure server credentials are configured in settings.xml");
            }

            try {
                B2StorageClient client = B2StorageClientFactory
                    .createDefaultFactory()
                    .create(keyId, applicationKey, "maven-wagon-b2/1.0");

                // Verify bucket exists and cache its ID
                B2Bucket bucket = client.getBucketOrNullByName(bucketKey);
                if (bucket == null) {
                    throw new ConnectionException("Bucket not found: " + bucketKey + ". Ensure the bucket exists and credentials have access.");
                }
                bucketIdCache.put(bucketKey, bucket.getBucketId());

                connectionPool.put(bucketKey, client);

            } catch (B2Exception e) {
                throw new ConnectionException("Failed to connect to B2: " + e.getMessage(), e);
            }
        }

        return connectionPool.get(bucketKey);
    }

    /**
     * Ensures the B2 client is initialized and connected.
     * This is a safety mechanism in case openConnection wasn't called.
     */
    private void ensureConnected() throws ConnectionException, AuthenticationException {
        // Use connection pool instead of instance variable
        getOrCreateClient();
    }

    @Override
    public void connect(org.apache.maven.wagon.repository.Repository repository)
            throws ConnectionException, AuthenticationException {
        super.connect(repository);
    }

    @Override
    public void connect(org.apache.maven.wagon.repository.Repository repository,
                       org.apache.maven.wagon.authentication.AuthenticationInfo authenticationInfo)
            throws ConnectionException, AuthenticationException {
        super.connect(repository, authenticationInfo);
    }

    @Override
    public void connect(org.apache.maven.wagon.repository.Repository repository,
                       org.apache.maven.wagon.authentication.AuthenticationInfo authenticationInfo,
                       org.apache.maven.wagon.proxy.ProxyInfo proxyInfo)
            throws ConnectionException, AuthenticationException {
        super.connect(repository, authenticationInfo, proxyInfo);
    }

    @Override
    public void connect(org.apache.maven.wagon.repository.Repository repository,
                       org.apache.maven.wagon.proxy.ProxyInfo proxyInfo)
            throws ConnectionException, AuthenticationException {
        super.connect(repository, proxyInfo);
    }

    @Override
    protected void openConnectionInternal() throws ConnectionException, AuthenticationException {
        // Parse repository URL when Maven calls connect
        String repositoryUrl = getRepository() != null ? getRepository().getUrl() : null;
        if (repositoryUrl != null) {
            parseRepositoryUrl(repositoryUrl);
        }

        // Create connection using the pool
        getOrCreateClient();
    }

    @Override
    protected void closeConnection() {
        // Don't close connections - they're pooled
        // Clear instance-specific state
        currentRepositoryUrl = null;
        currentBucketName = null;
        currentBasePath = null;
    }

    @Override
    public void get(String resourceName, File destination)
            throws TransferFailedException, ResourceDoesNotExistException, AuthorizationException {

        // Ensure connection is established
        try {
            ensureConnected();
        } catch (Exception e) {
            throw new TransferFailedException("Failed to establish connection", e);
        }

        Resource resource = new Resource(resourceName);
        fireGetInitiated(resource, destination);
        fireGetStarted(resource, destination);

        try {
            String fileName = getBasePath() + resourceName;

            // Create download request
            B2DownloadByNameRequest downloadRequest = B2DownloadByNameRequest
                .builder(getBucketName(), fileName)
                .build();

            // Download to temporary file first, then move to destination
            File tempFile = File.createTempFile("maven-b2-", ".tmp");
            try {
                // Use B2ContentFileWriter to handle the download
                B2ContentFileWriter fileWriter = B2ContentFileWriter.builder(tempFile).build();
                getOrCreateClient().downloadByName(downloadRequest, fileWriter);
                
                // Move temp file to destination
                if (!tempFile.renameTo(destination)) {
                    // If rename fails, copy manually
                    try (FileInputStream in = new FileInputStream(tempFile);
                         FileOutputStream out = new FileOutputStream(destination)) {
                        byte[] buffer = new byte[8192];
                        int bytesRead;
                        while ((bytesRead = in.read(buffer)) != -1) {
                            out.write(buffer, 0, bytesRead);
                        }
                    }
                    tempFile.delete();
                }
                
                resource.setContentLength(destination.length());
                fireGetCompleted(resource, destination);
                
            } finally {
                if (tempFile.exists()) {
                    tempFile.delete();
                }
            }
            
        } catch (B2NotFoundException e) {
            throw new ResourceDoesNotExistException("Resource not found: " + resourceName, e);
        } catch (B2Exception e) {
            throw new TransferFailedException("B2 error downloading resource: " + resourceName, e);
        } catch (IOException e) {
            throw new TransferFailedException("IO error downloading resource: " + resourceName, e);
        } catch (ConnectionException | AuthenticationException e) {
            throw new TransferFailedException("Failed to establish connection", e);
        }
    }

    @Override
    public boolean getIfNewer(String resourceName, File destination, long timestamp)
            throws TransferFailedException, ResourceDoesNotExistException, AuthorizationException {

        try {
            String fileName = getBasePath() + resourceName;

            // Get file info to check modification time
            B2FileVersion fileVersion = getOrCreateClient().getFileInfoByName(getBucketName(), fileName);
            long uploadTimestamp = fileVersion.getUploadTimestamp();

            // If remote file is newer, download it
            if (uploadTimestamp > timestamp) {
                get(resourceName, destination);
                return true;
            }

            return false;

        } catch (B2NotFoundException e) {
            throw new ResourceDoesNotExistException("Resource not found: " + resourceName, e);
        } catch (B2Exception e) {
            // If we can't determine, download anyway
            get(resourceName, destination);
            return true;
        } catch (ConnectionException | AuthenticationException e) {
            throw new TransferFailedException("Failed to establish connection", e);
        }
    }

    /**
     * Detects if this is a snapshot deployment based on the destination path.
     * Maven includes the version in the path (e.g., com/example/artifact/1.0-SNAPSHOT/...).
     */
    private boolean isSnapshotDeployment(String destination) {
        return destination != null && destination.contains("-SNAPSHOT/");
    }

    /**
     * Gets the correct repository URL based on whether this is a snapshot or release.
     * This is a WORKAROUND for Maven reusing wagon instances without calling connect().
     */
    private String getCorrectRepositoryUrl(boolean isSnapshot) {
        // Try to get both repository URLs from Maven settings
        // We'll look at the current repository and try to infer the other one
        String currentUrl = getRepository() != null ? getRepository().getUrl() : null;
        if (currentUrl == null) {
            return null;
        }

        // If the current URL matches what we need, use it
        if (isSnapshot && currentUrl.contains("snapshot")) {
            return currentUrl;
        }
        if (!isSnapshot && currentUrl.contains("release")) {
            return currentUrl;
        }

        // Otherwise, try to transform the URL to the correct one
        // This is a heuristic approach
        if (isSnapshot) {
            // Transform releases -> snapshots
            return currentUrl.replace("maven-releases", "maven-snapshots").replace("releases", "snapshots");
        } else {
            // Transform snapshots -> releases
            return currentUrl.replace("maven-snapshots", "maven-releases").replace("snapshots", "releases");
        }
    }

    @Override
    public void put(File source, String destination)
            throws TransferFailedException, ResourceDoesNotExistException, AuthorizationException {

        // WORKAROUND: Maven reuses wagon instances without calling connect()
        // Detect if this is a snapshot or release based on the destination path
        boolean isSnapshot = isSnapshotDeployment(destination);
        String correctRepositoryUrl = getCorrectRepositoryUrl(isSnapshot);
        if (correctRepositoryUrl != null) {
            parseRepositoryUrl(correctRepositoryUrl);
        }

        // Ensure connection is established
        try {
            ensureConnected();
        } catch (Exception e) {
            throw new TransferFailedException("B2 client not initialized. Connection may not have been opened.", e);
        }

        // Get current repository's path dynamically (not cached)
        String basePath = getBasePath();
        String bucketName = getBucketName();

        Resource resource = new Resource(destination);
        firePutInitiated(resource, source);
        firePutStarted(resource, source);

        try {
            String fileName = basePath + destination;
           
            // Create content source from file
            B2FileContentSource contentSource = B2FileContentSource.build(source);

            // Determine content type
            String contentType = B2ContentTypes.B2_AUTO;
            if (fileName.endsWith(".pom")) {
                contentType = "application/xml";
            } else if (fileName.endsWith(".jar") || fileName.endsWith(".war") || fileName.endsWith(".ear")) {
                contentType = "application/octet-stream";
            } else if (fileName.endsWith(".xml")) {
                contentType = "application/xml";
            } else if (fileName.endsWith(".sha1") || fileName.endsWith(".md5")) {
                contentType = B2ContentTypes.TEXT_PLAIN;
            }

            // Create upload request
            B2UploadFileRequest uploadRequest = B2UploadFileRequest
                .builder(getBucketId(), fileName, contentType, contentSource)
                .build();

            // Upload file
            getOrCreateClient().uploadSmallFile(uploadRequest);

            firePutCompleted(resource, source);

        } catch (B2Exception e) {
            throw new TransferFailedException("B2 error uploading resource: " + destination, e);
        } catch (ConnectionException | AuthenticationException e) {
            throw new TransferFailedException("Failed to establish connection", e);
        }
    }

    @Override
    public boolean resourceExists(String resourceName) throws TransferFailedException, AuthorizationException {
        try {
            ensureConnected();
            String fileName = getBasePath() + resourceName;
            B2FileVersion fileVersion = getOrCreateClient().getFileInfoByName(getBucketName(), fileName);
            return fileVersion != null;
        } catch (B2NotFoundException e) {
            return false;
        } catch (B2Exception e) {
            throw new TransferFailedException("Error checking if resource exists: " + resourceName, e);
        } catch (ConnectionException | AuthenticationException e) {
            throw new TransferFailedException("Failed to establish connection", e);
        }
    }
}