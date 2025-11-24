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

    private B2StorageClient b2Client;
    // Cache bucket IDs to avoid repeated lookups for the same bucket
    private java.util.Map<String, String> bucketIdCache = new java.util.HashMap<>();

    /**
     * Gets the bucket name from the current repository URL.
     * This is called dynamically to support repository switching during a session.
     */
    private String getBucketName() {
        return getRepository().getHost();
    }

    /**
     * Gets the base path from the current repository URL.
     * This is called dynamically to support repository switching during a session.
     */
    private String getBasePath() {
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
                B2Bucket bucket = b2Client.getBucketOrNullByName(bucketName);
                if (bucket == null) {
                    throw new TransferFailedException("Bucket not found: " + bucketName + ". Ensure the bucket exists and credentials have access.");
                }
                bucketIdCache.put(bucketName, bucket.getBucketId());
            } catch (B2Exception e) {
                throw new TransferFailedException("Failed to get bucket ID for: " + bucketName, e);
            }
        }
        return bucketIdCache.get(bucketName);
    }

    /**
     * Ensures the B2 client is initialized and connected.
     * This is a safety mechanism in case openConnection wasn't called.
     */
    private void ensureConnected() throws ConnectionException, AuthenticationException {
        if (b2Client == null) {
            System.out.println("[B2 Wagon] Connection not established, attempting to connect...");
            openConnectionInternal();
        }
    }

    @Override
    protected void openConnectionInternal() throws ConnectionException, AuthenticationException {
        String keyId = getAuthenticationInfo().getUserName();
        String applicationKey = getAuthenticationInfo().getPassword();

        if (keyId == null || applicationKey == null) {
            throw new AuthenticationException("B2 credentials not provided. Ensure server credentials are configured in settings.xml");
        }

        String bucketName = getBucketName();
        if (bucketName == null || bucketName.isEmpty()) {
            throw new ConnectionException("Bucket name not specified in repository URL");
        }

        try {
            // Create B2 client
            b2Client = B2StorageClientFactory
                .createDefaultFactory()
                .create(keyId, applicationKey, "maven-wagon-b2/1.0");

            // Get and cache the bucket ID for the initial bucket
            B2Bucket bucket = b2Client.getBucketOrNullByName(bucketName);
            if (bucket == null) {
                throw new ConnectionException("Bucket not found: " + bucketName + ". Ensure the bucket exists and credentials have access.");
            }
            bucketIdCache.put(bucketName, bucket.getBucketId());

            // Log successful connection (visible with -X flag)
            System.out.println("[B2 Wagon] Successfully connected to bucket: " + bucketName + " (ID: " + bucket.getBucketId() + ")");

        } catch (B2Exception e) {
            throw new ConnectionException("Failed to connect to B2: " + e.getMessage(), e);
        }
    }

    @Override
    protected void closeConnection() {
        if (b2Client != null) {
            try {
                b2Client.close();
            } catch (Exception e) {
                // Log but don't throw
            }
            b2Client = null;
        }

        bucketIdCache.clear();
    }

    @Override
    public void get(String resourceName, File destination)
            throws TransferFailedException, ResourceDoesNotExistException, AuthorizationException {

        // Ensure connection is established
        if (b2Client == null) {
            throw new TransferFailedException("B2 client not initialized. Connection may not have been opened.");
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
                b2Client.downloadByName(downloadRequest, fileWriter);
                
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
        }
    }

    @Override
    public boolean getIfNewer(String resourceName, File destination, long timestamp)
            throws TransferFailedException, ResourceDoesNotExistException, AuthorizationException {

        try {
            String fileName = getBasePath() + resourceName;

            // Get file info to check modification time
            B2FileVersion fileVersion = b2Client.getFileInfoByName(getBucketName(), fileName);
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
        }
    }

    @Override
    public void put(File source, String destination)
            throws TransferFailedException, ResourceDoesNotExistException, AuthorizationException {

        // Ensure connection is established
        if (b2Client == null) {
            try {
                ensureConnected();
            } catch (Exception e) {
                throw new TransferFailedException("B2 client not initialized. Connection may not have been opened.", e);
            }
        }

        // Get current repository's path dynamically (not cached)
        String basePath = getBasePath();
        String bucketName = getBucketName();
        System.out.println("[B2 Wagon] put() - Repository ID: " + getRepository().getId());
        System.out.println("[B2 Wagon] put() - Repository URL: " + getRepository().getUrl());
        System.out.println("[B2 Wagon] put() - Bucket: " + bucketName);
        System.out.println("[B2 Wagon] put() - BasePath: " + basePath);
        System.out.println("[B2 Wagon] put() - Destination: " + destination);
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
            b2Client.uploadSmallFile(uploadRequest);

            firePutCompleted(resource, source);

        } catch (B2Exception e) {
            throw new TransferFailedException("B2 error uploading resource: " + destination, e);
        }
    }

    @Override
    public boolean resourceExists(String resourceName) throws TransferFailedException, AuthorizationException {
        try {
            String fileName = getBasePath() + resourceName;
            B2FileVersion fileVersion = b2Client.getFileInfoByName(getBucketName(), fileName);
            return fileVersion != null;
        } catch (B2NotFoundException e) {
            return false;
        } catch (B2Exception e) {
            throw new TransferFailedException("Error checking if resource exists: " + resourceName, e);
        }
    }
}