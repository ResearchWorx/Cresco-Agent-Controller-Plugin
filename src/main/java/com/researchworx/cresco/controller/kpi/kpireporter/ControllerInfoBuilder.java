package com.researchworx.cresco.controller.kpi.kpireporter;

import com.google.gson.Gson;
import oshi.SystemInfo;
import oshi.hardware.HWDiskStore;
import oshi.hardware.HWPartition;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.hardware.NetworkIF;
import oshi.software.os.OSFileStore;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.util.*;

class ControllerInfoBuilder {
    private Gson gson;

    private MBeanServer server;


    public ControllerInfoBuilder() {
        gson = new Gson();
        server = ManagementFactory.getPlatformMBeanServer();
    }



    public String getControllerInfoMap() {

        String returnStr = null;
        try {
            StringBuilder sb = new StringBuilder();

            Set<ObjectInstance> instances = server.queryMBeans(null, null);


            Iterator<ObjectInstance> iterator = instances.iterator();
            while (iterator.hasNext()) {
                ObjectInstance instance = iterator.next();
                sb.append(instance.getObjectName() + "\n");
                if(instance.getObjectName().toString().contains("agent-")) {
                    try {

                        //MBeanInfo info = server.getMBeanInfo(instance.getObjectName());
                        //MBeanAttributeInfo[] attrInfo = info.getAttributes();
                        //(ObjectName[])server.getAttribute(http, attr.getName()))
                        System.out.println(server.getAttribute(instance.getObjectName(), "Subscriptions"));
                        for(ObjectName on : (ObjectName[])server.getAttribute(instance.getObjectName(), "Subscriptions")) {
                            System.out.println(on.getCanonicalName());
                        }
                            //WriteAttributes(server, instance.getObjectName(), false);

                    } catch(Exception ex) {
                        System.out.println("get error " + ex.getMessage());
                        ex.printStackTrace();
                    }
                }
                //System.out.println("MBean Found:");
                //System.out.println("Class Name:t" + instance.getClassName());
                //System.out.println("Object Name:t" + instance.getObjectName());
                //System.out.println("****************************************");
                //if(instance.getClassName().equals("com.orientechnologies.orient.core.storage.impl.local.statistic.OPerformanceStatisticManagerMBean")) {

            /*
            try {
                WriteAttributes(server, instance.getObjectName());
            } catch(Exception ex) {

            }
            */

                //}
            }
            returnStr = sb.toString();

        } catch(Exception ex) {

        }
        return returnStr;
    }

    private void WriteAttributes(final MBeanServer mBeanServer, final ObjectName http, Boolean indent)
            throws InstanceNotFoundException, IntrospectionException, ReflectionException
    {
        MBeanInfo info = mBeanServer.getMBeanInfo(http);
        MBeanAttributeInfo[] attrInfo = info.getAttributes();

        System.out.println("\n --Attributes for object: " + http);
        for (MBeanAttributeInfo attr : attrInfo)
        {
            try {
                MBeanServer server = ManagementFactory.getPlatformMBeanServer();
                System.out.println(attr.getName() + "=" + server.getAttribute(http, attr.getName()) + " " + indent);
                if(attr.getName().equals("Subscriptions")){

                    for(ObjectName on : (ObjectName[])server.getAttribute(http, attr.getName())) {

                        //System.out.println("--- " + on.toString());
                        try {
                            WriteAttributes(mBeanServer, on, true);
                        } catch(Exception ex) {
                            System.out.println("Error Write Attb 2: " + ex.toString());

                            ex.printStackTrace();
                        }
                    }
                    /*
                    System.out.println("--- " + server.getAttribute(http, attr.getName()).getClass().getCanonicalName());
                    System.out.println("--- " + server.getAttribute(http, attr.getName()).getClass().getName());
                    System.out.println("--- " + server.getAttribute(http, attr.getName()).getClass().getTypeName());
                    System.out.println("--- " + server.getAttribute(http, attr.getName()).getClass().getSimpleName());
                    */

                }
            } catch(Exception ex) {
                System.out.println("Error Write Attb : " + ex.toString());
                ex.printStackTrace();
            }
            //System.out.println("  " + attr.getName() + "\n");
        }
    }

}
