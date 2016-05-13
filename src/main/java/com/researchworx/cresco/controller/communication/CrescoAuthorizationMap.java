package com.researchworx.cresco.controller.communication;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.filter.DestinationMap;
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.security.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.security.Principal;
import java.util.*;

public class CrescoAuthorizationMap extends DestinationMap implements AuthorizationMap {

    public static final String DEFAULT_GROUP_CLASS = "org.apache.activemq.jaas.GroupPrincipal";

    private AuthorizationEntry defaultEntry;

    private TempDestinationAuthorizationEntry tempDestinationAuthorizationEntry;

    protected String groupClass = DEFAULT_GROUP_CLASS;

    public CrescoAuthorizationMap() {
    }

    @SuppressWarnings("rawtypes")
    public CrescoAuthorizationMap(List<DestinationMapEntry> authorizationEntries) {
        setAuthorizationEntries(authorizationEntries);

    }

    public void setTempDestinationAuthorizationEntry(TempDestinationAuthorizationEntry tempDestinationAuthorizationEntry) {
        this.tempDestinationAuthorizationEntry = tempDestinationAuthorizationEntry;
    }

    public TempDestinationAuthorizationEntry getTempDestinationAuthorizationEntry() {
        return this.tempDestinationAuthorizationEntry;
    }

    @Override
    public Set<Object> getTempDestinationAdminACLs() {
        if (tempDestinationAuthorizationEntry != null) {
            Set<Object> answer = new CrescoAuthorizationMap.WildcardAwareSet<Object>();
            answer.addAll(tempDestinationAuthorizationEntry.getAdminACLs());
            return answer;
        } else {
            return null;
        }
    }

    @Override
    public Set<Object> getTempDestinationReadACLs() {
        if (tempDestinationAuthorizationEntry != null) {
            Set<Object> answer = new CrescoAuthorizationMap.WildcardAwareSet<Object>();
            answer.addAll(tempDestinationAuthorizationEntry.getReadACLs());
            return answer;
        } else {
            return null;
        }
    }

    @Override
    public Set<Object> getTempDestinationWriteACLs() {
        if (tempDestinationAuthorizationEntry != null) {
            Set<Object> answer = new CrescoAuthorizationMap.WildcardAwareSet<Object>();
            answer.addAll(tempDestinationAuthorizationEntry.getWriteACLs());
            return answer;
        } else {
            return null;
        }
    }

    @Override
    public Set<Object> getAdminACLs(ActiveMQDestination destination) {
        Set<AuthorizationEntry> entries = getAllEntries(destination);
        Set<Object> answer = new CrescoAuthorizationMap.WildcardAwareSet<Object>();

        // now lets go through each entry adding individual
        for (Iterator<AuthorizationEntry> iter = entries.iterator(); iter.hasNext();) {
            AuthorizationEntry entry = iter.next();
            answer.addAll(entry.getAdminACLs());
        }
        return answer;
    }

    @Override
    public Set<Object> getReadACLs(ActiveMQDestination destination) {
        Set<AuthorizationEntry> entries = getAllEntries(destination);
        Set<Object> answer = new CrescoAuthorizationMap.WildcardAwareSet<Object>();

        // now lets go through each entry adding individual
        for (Iterator<AuthorizationEntry> iter = entries.iterator(); iter.hasNext();) {
            AuthorizationEntry entry = iter.next();
            answer.addAll(entry.getReadACLs());
        }
        return answer;
    }

    @Override
    public Set<Object> getWriteACLs(ActiveMQDestination destination) {
        Set<AuthorizationEntry> entries = getAllEntries(destination);
        Set<Object> answer = new CrescoAuthorizationMap.WildcardAwareSet<Object>();

        // now lets go through each entry adding individual
        for (Iterator<AuthorizationEntry> iter = entries.iterator(); iter.hasNext();) {
            AuthorizationEntry entry = iter.next();
            answer.addAll(entry.getWriteACLs());
        }
        return answer;
    }

    public AuthorizationEntry getEntryFor(ActiveMQDestination destination) {
        AuthorizationEntry answer = (AuthorizationEntry)chooseValue(destination);
        if (answer == null) {
            answer = getDefaultEntry();
        }
        return answer;
    }


    /**
     * Looks up the value(s) matching the given Destination key. For simple
     * destinations this is typically a List of one single value, for wildcards
     * or composite destinations this will typically be a Union of matching
     * values.
     *
     * @param key the destination to lookup
     * @return a Union of matching values or an empty list if there are no
     *         matching values.
     */
    @Override
    @SuppressWarnings("rawtypes")
    public synchronized Set get(ActiveMQDestination key) {
        if (key.isComposite()) {
            ActiveMQDestination[] destinations = key.getCompositeDestinations();
            Set answer = null;
            for (int i = 0; i < destinations.length; i++) {
                ActiveMQDestination childDestination = destinations[i];
                answer = union(answer, get(childDestination));
                if (answer == null  || answer.isEmpty()) {
                    break;
                }
            }
            return answer;
        }

        return findWildcardMatches(key, false);
    }

    public void addAuthorizationEntry(AuthorizationEntry entry) {
        put(entry.getDestination(), entry.getValue());
    }

    public void removeAuthorizationEntry(AuthorizationEntry entry) {
        remove(entry.getDestination(), entry.getValue());
    }

    /**
     * Sets the individual entries on the authorization map
     */
    @SuppressWarnings("rawtypes")
    public void setAuthorizationEntries(List<DestinationMapEntry> entries) {
        super.setEntries(entries);
    }

    public AuthorizationEntry getDefaultEntry() {
        return defaultEntry;
    }

    public void setDefaultEntry(AuthorizationEntry defaultEntry) {
        this.defaultEntry = defaultEntry;
    }

    @Override
    @SuppressWarnings("rawtypes")
    protected Class<? extends DestinationMapEntry> getEntryClass() {
        return AuthorizationEntry.class;
    }

    @SuppressWarnings("unchecked")
    protected Set<AuthorizationEntry> getAllEntries(ActiveMQDestination destination) {
        Set<AuthorizationEntry> entries = get(destination);
        if (defaultEntry != null) {
            entries.add(defaultEntry);
        }
        return entries;
    }

    public String getGroupClass() {
        return groupClass;
    }

    public void setGroupClass(String groupClass) {
        this.groupClass = groupClass;
    }

    final static String WILDCARD = "*";
    public static Object createGroupPrincipal(String name, String groupClass) throws Exception {
        if (WILDCARD.equals(name)) {
            // simple match all group principal - match any name and class
            return new Principal() {
                @Override
                public String getName() {
                    return WILDCARD;
                }
                @Override
                public boolean equals(Object other) {
                    return true;
                }

                @Override
                public int hashCode() {
                    return WILDCARD.hashCode();
                }
            };
        }
        Object[] param = new Object[]{name};

        Class<?> cls = Class.forName(groupClass);

        Constructor<?>[] constructors = cls.getConstructors();
        int i;
        Object instance;
        for (i = 0; i < constructors.length; i++) {
            Class<?>[] paramTypes = constructors[i].getParameterTypes();
            if (paramTypes.length != 0 && paramTypes[0].equals(String.class)) {
                break;
            }
        }
        if (i < constructors.length) {
            instance = constructors[i].newInstance(param);
        } else {
            instance = cls.newInstance();
            Method[] methods = cls.getMethods();
            i = 0;
            for (i = 0; i < methods.length; i++) {
                Class<?>[] paramTypes = methods[i].getParameterTypes();
                if (paramTypes.length != 0 && methods[i].getName().equals("setName") && paramTypes[0].equals(String.class)) {
                    break;
                }
            }

            if (i < methods.length) {
                methods[i].invoke(instance, param);
            } else {
                throw new NoSuchMethodException();
            }
        }

        return instance;
    }

    class WildcardAwareSet<T> extends HashSet<T> {
        boolean hasWildcard = false;

        @Override
        public boolean contains(Object e) {
            if (hasWildcard) {
                return true;
            } else {
                return super.contains(e);
            }
        }

        @Override
        public boolean addAll(Collection<? extends T> collection) {
            boolean modified = false;
            Iterator<? extends T> e = collection.iterator();
            while (e.hasNext()) {
                final T item = e.next();
                if (isWildcard(item)) {
                    hasWildcard = true;
                }
                if (add(item)) {
                    modified = true;
                }
            }
            return modified;
        }

        private boolean isWildcard(T item) {
            try {
                if (item.getClass().getMethod("getName", new Class[]{}).invoke(item).equals("*")) {
                    return true;
                }
            } catch (Exception ignored) {
            }
            return false;
        }
    }
}
