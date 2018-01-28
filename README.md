Cresco-Agent-Controller-Plugin
==============================

### Status
[![Build Status](http://128.163.188.129:9998/buildStatus/icon?job=Cresco-Agent-Controller-Plugin)](http://128.163.188.129:9998/job/Cresco-Agent-Controller-Plugin/)

---
### Install

1. [Build](https://github.com/ResearchWorx/Cresco-Agent/) or [download](http://128.163.188.129:9998/job/Cresco-Agent/lastSuccessfulBuild/com.researchworx.cresco$cresco-agent/) the latest Cresco-Agent and place it in the _cresco_ directory.
1. [Build](#build-section) or [download](http://128.163.188.129:9998/job/Cresco-Agent-Controller-Plugin/lastStableBuild/com.researchworx.cresco$cresco-agent-controller-plugin/) the latest Cresco-Agent-Controller and place it in the _cresco_/_plugin_ subdirectory of the agent directory.
1. Create a [agent configuration file](#agent-config-section) or modify Cresco-Agent-Plugins.ini.sample
1. Create a [agent configuration file](https://github.com/ResearchWorx/Cresco-Agent/) or modify Cresco-Agent-Plugins.ini.sample
1. Create a [plugin configuration file](#controller-config-section) or modify Cresco-Agent-Controller.ini.sample
1. Either restart the agent to load the plug-in or enable the agent through the controller.

---

### <a name="build-section"></a>Build

1. Confirm you have a working [Java Development Environment](https://www.java.com/en/download/faq/develop.xml) (JDK) 8 or greater.  [Oracle](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) and [OpenJDK](http://openjdk.java.net/) are known to work. 
1. Confirm you have a working [Apache Maven 3](https://maven.apache.org) environment.
1. ```git clone https://github.com/ResearchWorx/Cresco-Agent-Controller-Plugin.git```
1. ```cd Cresco-Agent-Controller-Plugin```
1. ```mvn clean package```
1. ```cp target/cresco-agent-controller-plugin-[cresco version].jar [plugin directory]```

### <a name="controller-config-section"></a>Controller Configuration

#### General Section

|  agentname |   |
|---|---|
| required  | False  |
| type  | String  |
| default  | [random UUID]  |
| desc  |  Name of the agent |

|  regionname |   |
|---|---|
| required  | False  |
| type  | String  |
| default  | [random UUID]  |
| desc  |  Name of the region |

|  is_agent |   |
|---|---|
| required  | False  |
| type  | Boolean  |
| default  | False  |
| desc  |  Force agent mode, requires [regional_controller_host] != null |

|  is_region |   |
|---|---|
| required  | False  |
| type  | Boolean  |
| default  | False  |
| desc  |  Force region mode, no agent discovery |

|  is_global |   |
|---|---|
| required  | False  |
| type  | Boolean  |
| default  | False  |
| desc  |  Force global mode, not agent or global discovery |

| perftimer  |   |
|---|---|
| required  | False  |
| type  | Long  |
| default  | 10000  |
| desc  |  Time in milliseconds between performance information update to regional controller. |

| forward_global_kpi  |   |
|---|---|
| required  | False  |
| type  | Boolean  |
| default  | True  |
| desc  |  Forward Agent KPI information from regional controller to global controller |


| watchdogtimer  |   |
|---|---|
| required  | False  |
| type  | Long  |
| default  | 5000  |
| desc  |  Time in milliseconds between watchdog messages to regional controller. |


| localpluginrepo |   |
|---|---|
| required  | False  |
| type  | String  |
| default  | [plugin directory]  |
| mode  | Region & Global |
| desc  | Location of plugin repository for regional and global scheduling |

##### Message Broker

| broker_username |   |
|---|---|
| required  | False  |
| type  | String  |
| default  | [random UUID]  |
| mode  | Region & Global |
| desc  | User of message broker user |

| broker_password |   |
|---|---|
| required  | False  |
| type  | String  |
| default  | [random UUID]  |
| mode  | Region & Global |
| desc  | Password of message broker user |


##### Database

| gdb_host |   |
|---|---|
| required  | False  |
| type  | String  |
| example  | 127.0.0.1  |
| mode  | Region & Global |
| desc  | Host name for remote graph database. |

| gdb_username |   |
|---|---|
| required  | False  |
| type  | String  |
| example  | dbuser  |
| mode  | Region & Global |
| desc  | Password for remote graph database. |

| gdb_password |   |
|---|---|
| required  | False  |
| type  | String  |
| example  | mypassword!  |
| mode  | Region & Global |
| desc  | Password for remote graph database. |

| gdb_dbname |   |
|---|---|
| required  | False  |
| type  | String  |
| example  | cresco  |
| mode  | Region & Global |
| desc  | Graph name for remote graph database. |

| db_retry_count |   |
|---|---|
| required  | False  |
| type  | Integer  |
| default  | 50  |
| mode  | Region & Global |
| desc  | Number of DB transaction attempts before raising exception |


##### Network Discovery

|  regional_controller_host |   |
|---|---|
| required  | False  |
| type  | String  |
| default  | [some host/ip]  |
| mode | Agent |
| desc  |  Hostname or IP of regional controller |

|  gc_host | need to merge with *global_controller_host* |
|---|---|
| required  | False  |
| type  | String  |
| default  | [some host/ip]  |
| mode | Region |
| desc  |  Hostname or IP of global controller |

|  global_controller_host | need to merge with *gc_host*  |
|---|---|
| required  | False  |
| type  | String  |
| default  | [some host/ip]  |
| mode | Region |
| desc  |  Hostname or IP of global controller |


|  networkdiscoveryport |   |
|---|---|
| required  | False  |
| type  | Integer  |
| default  | 32005  |
| desc  |  TCP/UDP port used for network discovery |

|  max_region_size |   |
|---|---|
| required  | False  |
| type  | Integer  |
| default  | 250  |
| desc  |  Sets the maximum/cutoff value where a region will not respond to network discovery request |


|  discovery_static_agent_timeout |   |
|---|---|
| required  | False  |
| type  | Long  |
| default  | 10000  |
| desc  |  Timeout in miliseconds to wait for static network discovery of host |

|  discovery_static_agent_timeout |   |
|---|---|
| required  | False  |
| type  | Long  |
| default  | 10000  |
| desc  |  Timeout in miliseconds to wait for static network discovery of host |

|  discovery_ipv6_agent_timeout |   |
|---|---|
| required  | False  |
| type  | Long  |
| default  | 10000  |
| desc  |  Timeout in miliseconds to wait for ipv6 network discovery of host |

|  discovery_ipv4_agent_timeout |   |
|---|---|
| required  | False  |
| type  | Long  |
| default  | 10000  |
| desc  |  Timeout in miliseconds to wait for ipv4 network discovery of host |

|  discovery_static_global_timeout |   |
|---|---|
| required  | False  |
| type  | Long  |
| default  | 10000  |
| desc  |  Timeout in miliseconds to wait for static network discovery of host |

|  discovery_ipv6_global_timeout |   |
|---|---|
| required  | False  |
| type  | Long  |
| default  | 2000  |
| desc  |  Timeout in miliseconds to wait for ipv6 network discovery of global host |

|  discovery_ipv4_global_timeout |   |
|---|---|
| required  | False  |
| type  | Long  |
| default  | 2000  |
| desc  |  Timeout in miliseconds to wait for ipv4 network discovery of global host |


| discovery_secret_agent |   |
|---|---|
| required  | True  |
| type  | String  |
| example  | MyAgentDiscoveryPassword!  |
| desc  | Shared secret used for agent-to-region network discovery |

| discovery_secret_region |   |
|---|---|
| required  | True  |
| type  | String  |
| example  | MyRegionDiscoveryPassword!  |
| desc  | Shared secret used for region-to-region network discovery |

| discovery_secret_global |   |
|---|---|
| required  | True  |
| type  | String  |
| example  | MyGlobalDiscoveryPassword!  |
| desc  | Shared secret used for region-to-global network discovery |

---