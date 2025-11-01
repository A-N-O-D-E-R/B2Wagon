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

    private String bucketName;
    private String basePath;
    private B2StorageClient b2Client;
    private String bucketId;

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

        // Parse bucket name and path from repository URL
        // Format: b2://bucket-name/path/to/repo
        String host = getRepository().getHost();
        String path = getRepository().getBasedir();
        
        if (host == null || host.isEmpty()) {
            throw new ConnectionException("Bucket name not specified in repository URL");
        }
        
        bucketName = host;
        basePath = path != null && !path.isEmpty() ? path : "";
        if (basePath.startsWith("/")) {
            basePath = basePath.substring(1);
        }
        if (!basePath.isEmpty() && !basePath.endsWith("/")) {
            basePath += "/";
        }

        try {
            // Create B2 client
            b2Client = B2StorageClientFactory
                .createDefaultFactory()
                .create(keyId, applicationKey, "maven-wagon-b2/1.0");
            
            // Get bucket ID
            B2Bucket bucket = b2Client.getBucketOrNullByName(bucketName);
            if (bucket == null) {
                throw new ConnectionException("Bucket not found: " + bucketName + ". Ensure the bucket exists and credentials have access.");
            }
            bucketId = bucket.getBucketId();
            
            // Log successful connection (visible with -X flag)
            System.out.println("[B2 Wagon] Successfully connected to bucket: " + bucketName + " (ID: " + bucketId + ")");
            
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
        bucketId = null;
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
            String fileName = basePath + resourceName;
            
            // Create download request
            B2DownloadByNameRequest downloadRequest = B2DownloadByNameRequest
                .builder(bucketName, fileName)
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
            String fileName = basePath + resourceName;
            
            // Get file info to check modification time
            B2FileVersion fileVersion = b2Client.getFileInfoByName(bucketName, fileName);
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
            try{
                ensureConnected();
            }catch(Exception e){
                throw new TransferFailedException("B2 client not initialized. Connection may not have been opened.", e);
            }
        }
        
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
                .builder(bucketId, fileName, contentType, contentSource)
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
            String fileName = basePath + resourceName;
            B2FileVersion fileVersion = b2Client.getFileInfoByName(bucketName, fileName);
            return fileVersion != null;
        } catch (B2NotFoundException e) {
            return false;
        } catch (B2Exception e) {
            throw new TransferFailedException("Error checking if resource exists: " + resourceName, e);
        }
    }
}