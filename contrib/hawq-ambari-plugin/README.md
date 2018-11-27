# HAWQ Ambari Plugin

hawq-ambari-plugin helps users install HAWQ and PXF using Ambari.

To ensure that Ambari recognizes HAWQ and PXF as services that can be installed for a specific stack, the following steps are required:
 * Add HAWQ and PXF metainfo.xml files (containing metadata about the service) under the stack to be installed.
 * Add repositories where HAWQ and PXF rpms reside, so that Ambari can use it during installation. This requires updating repoinfo.xml under the stack HAWQ and PXF is to be added.
 * If a stack is already installed using Ambari, add repositories to the existing stack using the Ambari REST API.

The above steps are taken care of by the hawq-ambari-plugin when the ```./add-hawq.py``` command is used.

## Source Code
Source code directory structure of the hawq-ambari-plugin is as follows:
```
hawq-ambari-plugin
+-- README.md
+-- build.properties
+-- pom.xml
+-- src
    +-- main
        +-- resources
            +-- services
            ¦   +-- HAWQ
            ¦   ¦   +-- metainfo.xml
            ¦   +-- PXF
            ¦       +-- metainfo.xml
            +-- utils
                +-- add-hawq.py
```

### build.properties
[build.properties](build.properties) contains properties required for building the plugin.

### metainfo.xml
[metainfo.xml](src/main/resources/services/HAWQ/metainfo.xml) contains the metadata about the service. The metainfo.xml specifies that the service definition is to be inherited from Ambari common-services. HAWQ and PXF common-services code can be found under [Apache Ambari repository](https://github.com/apache/ambari/tree/trunk/ambari-server/src/main/resources/common-services/).

### add-hawq<i></i>.py
[add-hawq.py](src/main/resources/utils/add-hawq.py) deploys HAWQ and PXF metainfo.xml files under the stack and adds the repositories to Ambari.


## Building the plugin
***Build Environment***: centos6 is the typical operating system used for building.

Properties specified in the [build.properties](build.properties) file:

| Property | Description | Value |
| --- | --- | --- |
| hawq.release.version | Release version of HAWQ | 2.0.1 |
| hawq.common.services.version | HAWQ common services code in Ambari to be inherited | 2.0.0 |
| pxf.release.version | Release version of PXF | 3.0.1 |
| pxf.common.services.version | PXF common services code in Ambari to be inherited | 3.0.0 |
| hawq.repo.prefix | Repository name for HAWQ core repository  | hawq |
| hawq.addons.repo.prefix | Repository name for HAWQ Add Ons repository  | hawq-add-ons |
| repository.version | Repository Version to be used in repository information | 2.0.1<i></i>.0 |
| default.stack | Default stack under which, metainfo.xml and repositories have to be added | HDP-2.4 |

To build the rpm for hawq-ambari-plugin, change the [build.properties](build.properties) file with the required parameters and run ```mvn install``` command under hawq-ambari-plugin directory:
```
$ pwd
incubator-hawq/contrib/hawq-ambari-plugin
$ mvn clean resources:copy-resources rpm:rpm -Dbuild_number=1
```

## Usage

Installing the hawq-ambari-plugin rpm would lay down the following directory:
```
/var/lib/hawq
+-- add-hawq.py
+-- staging
    +-- HAWQ
    ¦   +-- metainfo.xml
    +-- PXF
        +-- metainfo.xml
```

***Prerequisite***: Ensure that the script is run on the host where Ambari server is **running**.

*Replace* ```<ambari-username>``` *and* ```<ambari-password>``` *with login Ambari credentials*.

If hawq-2.0.1.0 and hawq-add-ons-2.0.1.0 repository have been set up on the Ambari server host, run the following command:

```
$ ./add-hawq.py --user <ambari-username> --password <ambari-password> --stack HDP-2.5
```
If ```--stack``` is not mentioned, *HDP-2.4* stack will be used as default parameter.

If hawq-2.0.1.0 and hawq-add-ons-2.0.1.0 repository have been set up on a different host than the Ambari server host, run the following command:


```
$ ./add-hawq.py --user <ambari-username> --password <ambari-password> --stack HDP-2.5 --hawqrepo http://my.host.address/hawq-2.0.1.0 --addonsrepo http://my.host.address/hawq-add-ons-2.0.1.0
```

**Please restart ambari-server after running the script so that the changes take effect:**
```
$ ambari-server restart
```