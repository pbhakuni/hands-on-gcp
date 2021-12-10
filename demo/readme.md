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

  Before running the maven project, below setup is needed:
    1. Install Cloud SDK shell
    2. Create a service account and get the JSON Keys from GCP portal
    2. Create a bucket in GCS in GCP portal
    3. Create a topic in Pubsub in GCP portal

  Run the maven project using mvn compile and provide the needed arguments

