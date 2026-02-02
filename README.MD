# B2 Maven Extension

A Maven extension which provides support for Backblaze B2, instead of an artifact repository like Artifactory or Nexus.

## Installation

Add the extension to your `pom.xml`:

```xml
<build>
    <extensions>
        <extension>
            <groupId>com.anode.maven</groupId>
            <artifactId>b2-maven-extension</artifactId>
            <version>1.0.0</version>
        </extension>
    </extensions>
</build>
```

Or add to `.mvn/extensions.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<extensions>
    <extension>
        <groupId>com.anode.maven</groupId>
        <artifactId>b2-maven-extension</artifactId>
        <version>1.0.0</version>
    </extension>
</extensions>
```

## Configuration

### Credentials

Credentials are resolved in the following order (highest to lowest precedence):

1. **System properties**
   ```
   -Db2.applicationKeyId=your-key-id -Db2.applicationKey=your-key
   ```

2. **Environment variables**
   ```bash
   export B2_APPLICATION_KEY_ID=your-key-id
   export B2_APPLICATION_KEY=your-key
   ```

3. **Maven settings.xml**
   ```xml
   <server>
       <id>b2-releases</id>
       <username>your-application-key-id</username>
       <password>your-application-key</password>
   </server>
   ```

### Getting B2 Credentials

1. Log in to your Backblaze account at https://secure.backblaze.com
2. Navigate to **App Keys** (https://secure.backblaze.com/app_keys.htm)
3. Create a new application key with access to your Maven repository bucket
4. Save the **Application Key ID** and **Application Key**

## URL Format

```
b2://{bucket-name}/{base-directory}
```

**Examples:**
- `b2://my-maven-repo/releases` - Releases stored in `releases/` folder
- `b2://my-maven-repo/snapshots` - Snapshots stored in `snapshots/` folder
- `b2://company-artifacts/maven` - All artifacts in `maven/` folder

## Usage

### Deploying Artifacts

Add distribution management to your `pom.xml`:

```xml
<distributionManagement>
    <repository>
        <id>b2-releases</id>
        <name>B2 Releases Repository</name>
        <url>b2://your-bucket-name/maven-releases</url>
    </repository>

    <snapshotRepository>
        <id>b2-snapshots</id>
        <name>B2 Snapshots Repository</name>
        <url>b2://your-bucket-name/maven-snapshots</url>
    </snapshotRepository>
</distributionManagement>
```

Then deploy:

```bash
mvn deploy
```

### Downloading Dependencies

Add repositories to your `pom.xml`:

```xml
<repositories>
    <repository>
        <id>b2-releases</id>
        <name>B2 Maven Repository</name>
        <url>b2://your-bucket-name/maven-releases</url>
    </repository>
</repositories>
```

Or configure in `settings.xml`:

```xml
<profiles>
    <profile>
        <id>b2-repositories</id>
        <repositories>
            <repository>
                <id>b2-releases</id>
                <url>b2://your-bucket-name/maven-releases</url>
                <releases><enabled>true</enabled></releases>
                <snapshots><enabled>false</enabled></snapshots>
            </repository>
            <repository>
                <id>b2-snapshots</id>
                <url>b2://your-bucket-name/maven-snapshots</url>
                <releases><enabled>false</enabled></releases>
                <snapshots><enabled>true</enabled></snapshots>
            </repository>
        </repositories>
    </profile>
</profiles>

<activeProfiles>
    <activeProfile>b2-repositories</activeProfile>
</activeProfiles>
```

**Important:** The `<id>` in repositories must match the `<id>` in your `settings.xml` `<server>` configuration.

## Complete Example

### settings.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<settings>
    <servers>
        <server>
            <id>b2-releases</id>
            <username>${env.B2_APPLICATION_KEY_ID}</username>
            <password>${env.B2_APPLICATION_KEY}</password>
        </server>
        <server>
            <id>b2-snapshots</id>
            <username>${env.B2_APPLICATION_KEY_ID}</username>
            <password>${env.B2_APPLICATION_KEY}</password>
        </server>
    </servers>
</settings>
```

### pom.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project>
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.example</groupId>
    <artifactId>my-library</artifactId>
    <version>1.0.0</version>

    <build>
        <extensions>
            <extension>
                <groupId>com.anode.maven</groupId>
                <artifactId>b2-maven-extension</artifactId>
                <version>1.0.0</version>
            </extension>
        </extensions>
    </build>

    <distributionManagement>
        <repository>
            <id>b2-releases</id>
            <url>b2://my-bucket/maven-releases</url>
        </repository>
        <snapshotRepository>
            <id>b2-snapshots</id>
            <url>b2://my-bucket/maven-snapshots</url>
        </snapshotRepository>
    </distributionManagement>
</project>
```

## Limitations

- `getFileList()` is not supported
- `putDirectory()` is not supported
- Buckets must already exist (won't create them automatically)

## Building from Source

```bash
git clone <repository-url>
cd B2Wagon
mvn clean install
```

## Requirements

- Maven 3.3.1 or higher
- Java 11 or higher
- Backblaze B2 account with application keys

## Security Best Practices

1. **Use Application Keys with Limited Scope**: Create keys with access only to specific buckets
2. **Use Environment Variables**: Don't hard-code credentials
3. **Use Private Buckets**: Keep Maven repositories private
4. **Rotate Keys Regularly**: Periodically rotate your B2 application keys
5. **CI/CD**: Store credentials as secrets in your CI/CD platform

## Troubleshooting

### Enable Debug Mode

```bash
mvn deploy -X
```

### Common Issues

**"B2 credentials not found"**
- Check credentials are set via system properties, environment variables, or settings.xml
- Verify the server `<id>` matches the repository `<id>`

**"Bucket not found"**
- Verify bucket name in URL is correct
- Ensure application key has access to the bucket

**"Cannot access b2:// with type default"**
- Extension not loaded - check `.mvn/extensions.xml` or `<build><extensions>` configuration
- Run `mvn clean install` to rebuild and install the extension

## License

[Apache License 2.0](./LICENCE)

## Acknowledgments

- Built using the official [Backblaze B2 Java SDK](https://github.com/Backblaze/b2-sdk-java)
- Inspired by [s3-maven-extension](https://github.com/michaelboyles/s3-maven-extension)
