Google Cloud POC
=================

Examples for google cloud storage and pubsub

Quickstart
----------

[//]: # ({x-version-update-start:google-cloud-examples:released})
Added the depedency in pom.xml
https://cloud.google.com/pubsub/docs/quickstart-client-libraries
```xml
<dependency>
    <groupId>com.google.cloud</groupId>
    <artifactId>google-cloud-pubsub</artifactId>
    <version>1.114.5</version>
    </dependency>
      <!-- https://mvnrepository.com/artifact/com.google.cloud/google-cloud-storage -->
    <dependency>
      <groupId>com.google.cloud</groupId>
      <artifactId>google-cloud-storage</artifactId>
      <version>2.2.2</version>
    </dependency>
  </dependencies>

  <dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>com.google.cloud</groupId>
      <artifactId>libraries-bom</artifactId>
      <version>24.0.0</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
  </dependencyManagement>
  ```
  [//]: # ({x-version-update-end})

  Before running the maven project, below setup is needed:
  
    1. Install Cloud SDK shell
  
    2. Create a service account and get the JSON Keys from GCP portal
  
    3. Create a bucket in GCS in GCP portal
    
    4. Create a topic in Pubsub in GCP portal

  Run the maven project using mvn compile and provide the needed arguments
  
  ```
  [INFO] Scanning for projects...
[INFO]
[INFO] ------------------------< com.example:DataLoad >------------------------
[INFO] Building DataLoad 1
[INFO] --------------------------------[ jar ]---------------------------------
[INFO]
[INFO] --- maven-resources-plugin:3.0.2:resources (default-resources) @ DataLoad ---
[INFO] Using 'UTF-8' encoding to copy filtered resources.
[INFO] skip non existing resourceDirectory C:\Users\1029565\Documents\GCP Learning\PubsubandGCS\code\demo\src\main\resources
[INFO]
[INFO] --- maven-compiler-plugin:3.8.0:compile (default-compile) @ DataLoad ---
[INFO] Changes detected - recompiling the module!
[INFO] Compiling 1 source file to C:\Users\1029565\Documents\GCP Learning\PubsubandGCS\code\demo\target\classes
[INFO]
[INFO] --- exec-maven-plugin:3.0.0:java (default-cli) @ DataLoad ---
Inside main class - Processing begins

--- Started - Upload processing to GCS ---
--- Completed - Upload processing to GCS ---

--- Started - Read processing from GCS ---
Name of the object is Blob{bucket=ikea-bucket-1, name=dog1.jpg, generation=1639113942346730, size=95901, content-type=application/octet-stream, metadata=null}
Name of the object is Blob{bucket=ikea-bucket-1, name=dog2.jpg, generation=1639113942994571, size=105048, content-type=application/octet-stream, metadata=null}
Name of the object is Blob{bucket=ikea-bucket-1, name=dog3.jpg, generation=1639113943183493, size=117321, content-type=application/octet-stream, metadata=null}
Name of the object is Blob{bucket=ikea-bucket-1, name=dog4.jpg, generation=1639113943330956, size=80862, content-type=application/octet-stream, metadata=null}
--- Completed - Read processing from GCS ---

--- Started - pubsub processing ---
Published message id = 3713753761091580
Published message id = 3713877373179199
Published message id = 3713876979325504
Published message id = 3714078212611888
--- Completed - pubsub processing ---

--- Started - Receiver processing ---

Id: 3714078212611888
Id: 3713753761091580
Id: 3713877373179199
Id: 3713876979325504
--- Completed - Receiver processing ---
Inside main class - Processed successfully
```
