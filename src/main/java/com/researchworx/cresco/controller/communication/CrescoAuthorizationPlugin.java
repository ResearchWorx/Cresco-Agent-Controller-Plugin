package com.researchworx.cresco.controller.communication;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.security.AuthorizationBroker;
import org.apache.activemq.security.AuthorizationEntry;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class CrescoAuthorizationPlugin implements BrokerPlugin {
    private CrescoAuthorizationMap map;
    private Map<String, Set<AuthorizationEntry>> entries = new ConcurrentHashMap<>();

    public CrescoAuthorizationPlugin() {
        map = new CrescoAuthorizationMap();
    }

    public CrescoAuthorizationPlugin(CrescoAuthorizationMap map) {
        this.map = map;
    }

    public Broker installPlugin(Broker broker) {
        if (map == null) {
            throw new IllegalArgumentException("You must configure a 'map' property");
        }
        return new AuthorizationBroker(broker, map);
    }

    public CrescoAuthorizationMap getMap() {
        return map;
    }

    public void setMap(CrescoAuthorizationMap map) {
        this.map = map;
    }


    public void addEntry(String channelName, String groupName) throws Exception {

        AuthorizationEntry authEntryTopic = new AuthorizationEntry();
        authEntryTopic.setGroupClass("org.apache.activemq.jaas.GroupPrincipal");
        authEntryTopic.setTopic(channelName);
        authEntryTopic.setRead(groupName);
        authEntryTopic.setWrite(groupName);
        authEntryTopic.setAdmin(groupName);
        map.addAuthorizationEntry(authEntryTopic);

        AuthorizationEntry authEntryQueue = new AuthorizationEntry();
        authEntryQueue.setGroupClass("org.apache.activemq.jaas.GroupPrincipal");
        authEntryQueue.setQueue(channelName);
        authEntryQueue.setRead(groupName);
        authEntryQueue.setWrite(groupName);
        authEntryQueue.setAdmin(groupName);
        map.addAuthorizationEntry(authEntryQueue);

        Set<AuthorizationEntry> currentEntries = new HashSet<>();
        currentEntries.add(authEntryTopic);
        currentEntries.add(authEntryQueue);
        entries.put(channelName, currentEntries);
    }

    public void removeEntry(String channelName) {
        for (AuthorizationEntry entry : entries.get(channelName)) {
            map.removeAuthorizationEntry(entry);
        }
    }
}
