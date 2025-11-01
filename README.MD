# Maven Wagon for Backblaze B2

A Maven Wagon implementation that enables deploying and retrieving Maven artifacts from Backblaze B2 Cloud Storage.

## Features

- 🚀 Deploy Maven artifacts to B2 buckets
- 📦 Retrieve dependencies from B2
- 🔐 Secure authentication using B2 application keys
- ♻️ Automatic retry and connection management via official B2 SDK
- 📊 Support for releases and snapshots
- ✅ SHA-1 verification for uploads

## Prerequisites

- Maven 3.3.1 or higher
- Java 8 or higher
- Backblaze B2 account with application keys
- A B2 bucket for storing Maven artifacts

## Installation

### 1. Build and Install the Wagon

```bash
git clone <repository-url>
cd wagon-b2
mvn clean install
```

This installs the wagon to your local Maven repository (`~/.m2/repository`).

### 2. Get B2 Credentials

1. Log in to your Backblaze account at https://secure.backblaze.com
2. Navigate to **App Keys** (https://secure.backblaze.com/app_keys.htm)
3. Create a new application key with access to your Maven repository bucket
4. Save the **Application Key ID** and **Application Key**

### 3. Configure Maven Settings

Edit `~/.m2/settings.xml` (create it if it doesn't exist):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
          http://maven.apache.org/xsd/settings-1.0.0.xsd">

    <servers>
        <!-- B2 Credentials -->
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

    <profiles>
        <profile>
            <id>b2-repositories</id>
            <repositories>
                <repository>
                    <id>b2-releases</id>
                    <name>B2 Maven Repository - Releases</name>
                    <url>b2://your-bucket-name/releases</url>
                    <releases>
                        <enabled>true</enabled>
                    </releases>
                    <snapshots>
                        <enabled>false</enabled>
                    </snapshots>
                </repository>
                
                <repository>
                    <id>b2-snapshots</id>
                    <name>B2 Maven Repository - Snapshots</name>
                    <url>b2://your-bucket-name/snapshots</url>
                    <releases>
                        <enabled>false</enabled>
                    </releases>
                    <snapshots>
                        <enabled>true</enabled>
                    </snapshots>
                </repository>
            </repositories>
        </profile>
    </profiles>

    <activeProfiles>
        <activeProfile>b2-repositories</activeProfile>
    </activeProfiles>

</settings>
```

### 4. Set Environment Variables

```bash
export B2_APPLICATION_KEY_ID="your_application_key_id"
export B2_APPLICATION_KEY="your_application_key"
```

Or add them to your `~/.bashrc`, `~/.zshrc`, or equivalent.

**Alternative:** Hard-code credentials in `settings.xml` (not recommended for security):

```xml
<server>
    <id>b2-releases</id>
    <username>your_application_key_id_here</username>
    <password>your_application_key_here</password>
</server>
```

## Usage

### Configure Your Project

#### Option 1: Using .mvn/extensions.xml (Recommended)

Create `.mvn/extensions.xml` in your project root:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<extensions>
    <extension>
        <groupId>com.example.maven.wagon</groupId>
        <artifactId>wagon-b2</artifactId>
        <version>1.0.0</version>
    </extension>
</extensions>
```

#### Option 2: Using build extensions in pom.xml

Add to your `pom.xml`:

```xml
<build>
    <extensions>
        <extension>
            <groupId>com.example.maven.wagon</groupId>
            <artifactId>wagon-b2</artifactId>
            <version>1.0.0</version>
        </extension>
    </extensions>
</build>
```

### Configure Distribution Management

Add to your `pom.xml`:

```xml
<distributionManagement>
    <repository>
        <id>b2-releases</id>
        <name>B2 Releases Repository</name>
        <url>b2://your-bucket-name/releases</url>
    </repository>
    
    <snapshotRepository>
        <id>b2-snapshots</id>
        <name>B2 Snapshots Repository</name>
        <url>b2://your-bucket-name/snapshots</url>
    </snapshotRepository>
</distributionManagement>
```

**Important:** The `<id>` in `distributionManagement` must match the `<id>` in your `settings.xml` `<server>` configuration.

### Deploy Artifacts

```bash
# Deploy release version
mvn clean deploy

# Deploy snapshot version (ensure version ends with -SNAPSHOT)
mvn clean deploy
```

### Download Dependencies

If you've configured the repositories in your `settings.xml` profile (as shown above), Maven will automatically download dependencies from B2:

```bash
mvn clean install
```

To force re-download from B2:

```bash
mvn dependency:purge-local-repository
mvn clean install
```

## URL Format

The B2 wagon uses the following URL format:

```
b2://<bucket-name>/<path>
```

**Examples:**

- `b2://my-maven-repo/releases` - Releases stored in `releases/` folder
- `b2://my-maven-repo/snapshots` - Snapshots stored in `snapshots/` folder
- `b2://company-artifacts/maven` - All artifacts in `maven/` folder

## Project Structure in B2

Artifacts are stored following the standard Maven repository layout:

```
releases/
└── com/
    └── example/
        └── my-artifact/
            └── 1.0.0/
                ├── my-artifact-1.0.0.jar
                ├── my-artifact-1.0.0.jar.md5
                ├── my-artifact-1.0.0.jar.sha1
                ├── my-artifact-1.0.0.pom
                ├── my-artifact-1.0.0.pom.md5
                ├── my-artifact-1.0.0.pom.sha1
                ├── my-artifact-1.0.0-sources.jar
                ├── my-artifact-1.0.0-sources.jar.md5
                ├── my-artifact-1.0.0-sources.jar.sha1
                └── maven-metadata.xml
```

## Troubleshooting

### Connection Issues

Enable debug mode to see detailed connection information:

```bash
mvn deploy -X
```

Look for lines like:
```
[B2 Wagon] Successfully connected to bucket: your-bucket-name (ID: ...)
```

### Common Issues

**1. "Cannot access b2:// with type default"**

The wagon extension isn't loaded. Ensure:
- `.mvn/extensions.xml` exists and is correct
- OR `<build><extensions>` is in your `pom.xml`
- The wagon-b2 is installed: `ls ~/.m2/repository/com/example/maven/wagon/wagon-b2/`

**2. "B2 client not initialized"**

Authentication failed. Check:
- Server `<id>` in settings.xml matches repository `<id>` in pom.xml
- Environment variables are set: `echo $B2_APPLICATION_KEY_ID`
- Credentials are correct and have access to the bucket

**3. "Bucket not found"**

Verify:
- Bucket name is correct in the URL
- Application key has access to the bucket
- Bucket exists in your B2 account

**4. "NoSuchElementException: roleHint: b2"**

The Plexus component descriptor wasn't generated. Rebuild the wagon:
```bash
cd wagon-b2
mvn clean install
jar tf target/wagon-b2-1.0.0.jar | grep components.xml
```

You should see `META-INF/plexus/components.xml` in the output.

### Verify Installation

```bash
# Check wagon is installed
ls -la ~/.m2/repository/com/example/maven/wagon/wagon-b2/1.0.0/

# Check component descriptor exists
unzip -l ~/.m2/repository/com/example/maven/wagon/wagon-b2/1.0.0/wagon-b2-1.0.0.jar | grep components.xml

# View component descriptor
unzip -p ~/.m2/repository/com/example/maven/wagon/wagon-b2/1.0.0/wagon-b2-1.0.0.jar META-INF/plexus/components.xml
```

## Security Best Practices

1. **Use Application Keys with Limited Scope**: Create application keys that only have access to specific buckets
2. **Use Environment Variables**: Don't hard-code credentials in `settings.xml` or commit them to version control
3. **Bucket Permissions**: Use private buckets for Maven repositories
4. **Rotate Keys Regularly**: Periodically rotate your B2 application keys
5. **CI/CD**: Store credentials as secrets in your CI/CD platform

## Testing

A test project is included to verify the wagon functionality:

```bash
cd b2-wagon-test

# Build and deploy
mvn clean deploy

# Test downloading
mvn dependency:purge-local-repository -DmanualInclude=com.example.test:b2-wagon-test
mvn dependency:resolve
```

## Dependencies

- Maven Wagon API 3.5.3
- Backblaze B2 SDK 6.2.0
- Java 8+

## License

[APACHE LICENCE 2.0](./LICENCE)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

For issues related to:
- **This wagon**: Open an issue in this repository
- **B2 SDK**: See https://github.com/Backblaze/b2-sdk-java
- **Maven Wagon**: See https://maven.apache.org/wagon/

## Acknowledgments

- Built using the official [Backblaze B2 Java SDK](https://github.com/Backblaze/b2-sdk-java)
- Based on the Maven Wagon Provider API