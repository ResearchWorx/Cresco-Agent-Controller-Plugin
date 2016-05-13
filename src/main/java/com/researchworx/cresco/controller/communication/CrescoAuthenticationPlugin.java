package com.researchworx.cresco.controller.communication;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.jaas.GroupPrincipal;
import org.apache.activemq.security.AuthenticationUser;
import org.apache.activemq.security.SimpleAuthenticationBroker;

import java.security.Principal;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class CrescoAuthenticationPlugin implements BrokerPlugin {
    private Map<String, String> userPasswords = new ConcurrentHashMap<String, String>();
    private Map<String, Set<Principal>> userGroups = new ConcurrentHashMap<String, Set<Principal>>();
    private static final String DEFAULT_ANONYMOUS_USER = "anonymous";
    private static final String DEFAULT_ANONYMOUS_GROUP = "anonymous";
    private String anonymousUser = DEFAULT_ANONYMOUS_USER;
    private String anonymousGroup = DEFAULT_ANONYMOUS_GROUP;
    private boolean anonymousAccessAllowed = false;

    public CrescoAuthenticationPlugin() {
    }

    public CrescoAuthenticationPlugin(List<?> users) {
        setUsers(users);
    }

    public Broker installPlugin(Broker parent) {
        SimpleAuthenticationBroker broker = new SimpleAuthenticationBroker(parent, userPasswords, userGroups);
        broker.setAnonymousAccessAllowed(anonymousAccessAllowed);
        broker.setAnonymousUser(anonymousUser);
        broker.setAnonymousGroup(anonymousGroup);
        return broker;
    }

    public Map<String, Set<Principal>> getUserGroups() {
        return userGroups;
    }

    /**
     * Sets individual users for authentication
     *
     * @org.apache.xbean.ElementType class="org.apache.activemq.security.AuthenticationUser"
     */
    public void setUsers(List<?> users) {
        userPasswords.clear();
        userGroups.clear();
        for (Iterator<?> it = users.iterator(); it.hasNext();) {
            AuthenticationUser user = (AuthenticationUser)it.next();
            userPasswords.put(user.getUsername(), user.getPassword());
            Set<Principal> groups = new HashSet<Principal>();
            StringTokenizer iter = new StringTokenizer(user.getGroups(), ",");
            while (iter.hasMoreTokens()) {
                String name = iter.nextToken().trim();
                groups.add(new GroupPrincipal(name));
            }
            userGroups.put(user.getUsername(), groups);
        }
    }

    public void addUser(String username, String password, String groups) {
        AuthenticationUser user = new AuthenticationUser(username, password, groups);
        addUser(user);
    }

    public void addUser(AuthenticationUser user) {
        userPasswords.put(user.getUsername(), user.getPassword());
        Set<Principal> groups = new HashSet<Principal>();
        StringTokenizer iter = new StringTokenizer(user.getGroups(), ",");
        while (iter.hasMoreTokens()) {
            String name = iter.nextToken().trim();
            groups.add(new GroupPrincipal(name));
        }
        userGroups.put(user.getUsername(), groups);
    }

    public void removeUser(String username) {
        userPasswords.remove(username);
        userGroups.remove(username);
    }

    public void setAnonymousAccessAllowed(boolean anonymousAccessAllowed) {
        this.anonymousAccessAllowed = anonymousAccessAllowed;
    }

    public boolean isAnonymousAccessAllowed() {
        return anonymousAccessAllowed;
    }

    public void setAnonymousUser(String anonymousUser) {
        this.anonymousUser = anonymousUser;
    }

    public String getAnonymousUser() {
        return anonymousUser;
    }

    public void setAnonymousGroup(String anonymousGroup) {
        this.anonymousGroup = anonymousGroup;
    }

    public String getAnonymousGroup() {
        return anonymousGroup;
    }

    /**
     * Sets the groups a user is in. The key is the user name and the value is a
     * Set of groups
     */
    public void setUserGroups(Map<String, Set<Principal>> userGroups) {
        this.userGroups = userGroups;
    }

    public Map<String, String> getUserPasswords() {
        return userPasswords;
    }

    /**
     * Sets the map indexed by user name with the value the password
     */
    public void setUserPasswords(Map<String, String> userPasswords) {
        this.userPasswords = userPasswords;
    }
}
