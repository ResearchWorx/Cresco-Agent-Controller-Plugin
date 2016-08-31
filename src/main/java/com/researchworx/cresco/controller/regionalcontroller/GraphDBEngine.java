package com.researchworx.cresco.controller.regionalcontroller;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class GraphDBEngine {
	
	public OrientGraphFactory factory;
	private OrientGraph odb;
	//public Cache<String, String> nodePathCache;
	//public Cache<String, Edge> appPathCache;
	public String[] aNodeIndexParams = {"platform","environment","location"};
	
	public int retryCount = 50;
	
	public GraphDBEngine()
	{
		/*
		nodePathCache = CacheBuilder.newBuilder()
			    .concurrencyLevel(4)
			    .softValues()
			    .maximumSize(100000)
			    .expireAfterWrite(15, TimeUnit.MINUTES)
			    .build();
		
		appPathCache = CacheBuilder.newBuilder()
			    .concurrencyLevel(4)
			    .softValues()
			    .maximumSize(100000)
			    .expireAfterWrite(15, TimeUnit.MINUTES)
			    .build();
		*/
		
		//String connection_string = "remote:" + Launcher.conf.getGraphDBServer() + "/" + Launcher.conf.getGraphDBName();
		//String username = Launcher.conf.getGraphDBLogin();
		//String password = Launcher.conf.getGraphDBPassword();
		/*
        String host = ControllerEngine.config.getParam("gdb_host");
		String username = ControllerEngine.config.getParam("gdb_username");
		String password = ControllerEngine.config.getParam("gdb_password");
		String dbname = ControllerEngine.config.getParam("gdb_dbname");
		
		String connection_string = "remote:" + host + "/" + dbname;


        if(dropDBIfExists(connection_string, username, password)) {
            System.out.println("Dropped existing database!");
            //System.exit(0);
        }

        if(!dbCheck(connection_string, username, password))
        {
            System.out.println("DBCheck failed");
            System.exit(0);
        }

        factory = new OrientGraphFactory(connection_string,username,password).setupPool(10, 100);

        initCrescoDB();
        System.out.println("Post Init!");
        ControllerEngine.GDBActive = true;
        */
        ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:MyDb").create();
        factory = new OrientGraphFactory("memory:MyDb");
        initCrescoDB();
        System.out.println("Post Init!");

    }

    public boolean dbCheck(String connection_string, String username, String password) {

        Boolean isValid = false;
        try {
            OServerAdmin server = new OServerAdmin(connection_string).connect(username, password);
            if (!server.existsDatabase("plocal")) {
                server.createDatabase("graph", "plocal");
                isValid = true;
            }
            else {
                isValid = true;
            }
            server.close();
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
        return isValid;
    }

    public boolean dropDBIfExists(String connection_string, String username, String password) {
        Boolean isValid = false;
        try {
            OServerAdmin server = new OServerAdmin(connection_string).connect(username, password);
            if (server.existsDatabase("plocal")) {
                server.dropDatabase("plocal");
                isValid = true;
            }
            server.close();
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
        return isValid;
    }

    //new database functions
    //READS
    public String getINodeId(String resource_id, String inode_id)
    {
        String node_id = null;
        OrientGraph graph = null;
        try
        {
            graph = factory.getTx();

            Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:iNode.nodePath WHERE key = [\"" + resource_id + "\",\"" + inode_id + "\"]")).execute();

            Iterator<Vertex> iter = resultIterator.iterator();
            if(iter.hasNext())
            {
                Vertex v = iter.next();
                node_id = v.getProperty("rid").toString();
            }
            if(node_id != null)
            {
                node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
            }
        }
        catch(Exception ex)
        {
            System.out.println("GraphDBEngine : getINodeID : Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return node_id;
    }

    public List<String> getANodeFromIndex(String indexName, String indexValue)
    {
        List<String> nodeList = null;
        OrientGraph graph = null;
        try
        {
            nodeList = new ArrayList<String>();
            graph = factory.getTx();

            Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:aNode." + indexName + " WHERE key = '" + indexValue + "'")).execute();

            Iterator<Vertex> iter = resultIterator.iterator();
            while(iter.hasNext())
            //if(iter.hasNext())
            {
                Vertex v = iter.next();
                String node_id = v.getProperty("rid").toString();
                if(node_id != null)
                {
                    node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                    nodeList.add(node_id);
                }
            }

        }
        catch(Exception ex)
        {
            System.out.println("GraphDBEngine : getANodeFromIndex : Error " + ex.toString());
            nodeList = null;
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return nodeList;
    }

    public String getResourceNodeId(String resource_id)
    {
        String node_id = null;
        OrientGraph graph = null;
        try
        {
            graph = factory.getTx();

            Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:resourceNode.nodePath WHERE key = '" + resource_id + "'")).execute();

            Iterator<Vertex> iter = resultIterator.iterator();
            if(iter.hasNext())
            {
                Vertex v = iter.next();
                node_id = v.getProperty("rid").toString();
            }
            if(node_id != null)
            {
                node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
            }
        }
        catch(Exception ex)
        {
            System.out.println("GraphDBEngine : getResourceNodeID : Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return node_id;
    }

    public String getNodeId(String region, String agent, String plugin)
    {
        String node_id = null;
        OrientGraph graph = null;

        try
        {

            if((region != null) && (agent == null) && (plugin == null))
            {
                //OrientGraphNoTx graph = factory.getNoTx();
                graph = factory.getTx();

                Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:rNode.nodePath WHERE key = '" + region + "'")).execute();

                Iterator<Vertex> iter = resultIterator.iterator();
                if(iter.hasNext())
                {
                    Vertex v = iter.next();
                    node_id = v.getProperty("rid").toString();
                }
                if(node_id != null)
                {
                    node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                }
                //return isFound;
            }
            else if((region != null) && (agent != null) && (plugin == null))
            {
                //OrientGraph graph = factory.getTx();
                //OrientGraphNoTx graph = factory.getNoTx();
                graph = factory.getTx();
                Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:aNode.nodePath WHERE key = [\"" + region + "\",\"" + agent + "\"]")).execute();
                Iterator<Vertex> iter = resultIterator.iterator();
                if(iter.hasNext())
                {
                    Vertex v = iter.next();
                    node_id = v.getProperty("rid").toString();
                }
                //graph.shutdown();
                //return node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                //node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                if(node_id != null)
                {
                    node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                }
            }
            else if((region != null) && (agent != null) && (plugin != null))
            {
                //OrientGraph graph = factory.getTx();
                //OrientGraphNoTx graph = factory.getNoTx();
                graph = factory.getTx();
                Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:pNode.nodePath WHERE key = [\"" + region + "\",\"" + agent + "\",\"" + plugin +"\"]")).execute();
                Iterator<Vertex> iter = resultIterator.iterator();
                if(iter.hasNext())
                {
                    Vertex v = iter.next();
                    node_id = v.getProperty("rid").toString();
                }
                //graph.shutdown();
                //return node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                //node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                if(node_id != null)
                {
                    node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                }
            }

        }
        catch(Exception ex)
        {
            System.out.println("GraphDBEngine : getNodeID : Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return node_id;
    }

    public String getResourceEdgeId(String resource_id, String inode_id, String region, String agent)
    {
        String edge_id = null;
        OrientGraph graph = null;

        try
        {
            if((resource_id != null) && (inode_id != null) && (region != null) && (agent != null))
            {
                //OrientGraph graph = factory.getTx();
                //OrientGraphNoTx graph = factory.getNoTx();
                graph = factory.getTx();
                Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:isAssigned.edgeProp WHERE key = [\""+ resource_id + "\",\""+ inode_id + "\",\"" + region + "\",\"" + agent +"\"]")).execute();
                Iterator<Vertex> iter = resultIterator.iterator();
                if(iter.hasNext())
                {
                    Vertex v = iter.next();
                    edge_id = v.getProperty("rid").toString();
                }
                //graph.shutdown();
                //return node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                //node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                if(edge_id != null)
                {
                    edge_id = edge_id.substring(edge_id.indexOf("[") + 1, edge_id.indexOf("]"));
                }
            }

        }
        catch(Exception ex)
        {
            System.out.println("GraphDBEngine : getResourceEdgeId : Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return edge_id;
    }

    public String getResourceEdgeId(String resource_id, String inode_id)
    {
        String edge_id = null;
        OrientGraph graph = null;

        try
        {
            if((resource_id != null) && (inode_id != null))
            {
                //OrientGraph graph = factory.getTx();
                //OrientGraphNoTx graph = factory.getNoTx();
                graph = factory.getTx();
                Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:isAssigned.edgeProp WHERE key = [\""+ resource_id + "\",\""+ inode_id +"\"]")).execute();
                Iterator<Vertex> iter = resultIterator.iterator();
                if(iter.hasNext())
                {
                    Vertex v = iter.next();
                    edge_id = v.getProperty("rid").toString();
                }
                //graph.shutdown();
                //return node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                //node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                if(edge_id != null)
                {
                    edge_id = edge_id.substring(edge_id.indexOf("[") + 1, edge_id.indexOf("]"));
                }
            }

        }
        catch(Exception ex)
        {
            System.out.println("GraphDBEngine : getIsAssignedEdgeId : Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return edge_id;
    }

    public List<String> getIsAssignedEdgeIds(String resource_id, String inode_id)
    {
        String edge_id = null;
        OrientGraph graph = null;
        List<String> edge_list = null;
        try
        {
            if((resource_id != null) && (inode_id != null))
            {
                edge_list = new ArrayList<String>();
                //OrientGraph graph = factory.getTx();
                //OrientGraphNoTx graph = factory.getNoTx();
                graph = factory.getTx();
                Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:isAssigned.edgeProp WHERE key = [\""+ resource_id + "\",\""+ inode_id +"\"]")).execute();
                Iterator<Vertex> iter = resultIterator.iterator();
                while(iter.hasNext())
                {
                    Vertex v = iter.next();
                    edge_id = v.getProperty("rid").toString();
                    if(edge_id != null)
                    {
                        edge_id = edge_id.substring(edge_id.indexOf("[") + 1, edge_id.indexOf("]"));
                        edge_list.add(edge_id);
                    }

                }
                //graph.shutdown();
                //return node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                //node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
            }

        }
        catch(Exception ex)
        {
            System.out.println("GraphDBEngine : getIsAssignedEdgeId : Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return edge_list;
    }

    public String getIsAssignedParam(String edge_id,String param_name)
    {
        String param = null;
        OrientGraph graph = null;

        try
        {
            if((edge_id != null) && (param_name != null))
            {
                graph = factory.getTx();
                Edge e = graph.getEdge(edge_id);
                param = e.getProperty(param_name).toString();
            }

        }
        catch(Exception ex)
        {
            System.out.println("GraphDBEngine : getIsAssignedParam : Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return param;
    }

    public List<String> getNodeList(String region, String agent, String plugin)
    {
        List<String> node_list = null;
        OrientGraph graph = null;
        try
        {

            if((region == null) && (agent == null) && (plugin == null))
            {
                //OrientGraph graph = factory.getTx();
                //OrientGraphNoTx graph = factory.getNoTx();
                graph = factory.getTx();
                Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:rNode.nodePath")).execute();

                Iterator<Vertex> iter = resultIterator.iterator();
                if(iter.hasNext())
                {
                    node_list = new ArrayList<String>();
                    while(iter.hasNext())
                    {
                        Vertex v = iter.next();
                        String node_id = v.getProperty("rid").toString();
                        node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                        Vertex rNode = graph.getVertex(node_id);
                        node_list.add(rNode.getProperty("region").toString());
                    }
                }

            }
            else if((region != null) && (agent == null) && (plugin == null))
            {
                graph = factory.getTx();
                //OrientGraphNoTx graph = factory.getNoTx();

                Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:aNode.nodePath WHERE key = [\"" + region + "\"]")).execute();
                Iterator<Vertex> iter = resultIterator.iterator();
                if(iter.hasNext())
                {
                    node_list = new ArrayList<String>();
                    while(iter.hasNext())
                    {
                        Vertex v = iter.next();
                        String node_id = v.getProperty("rid").toString();
                        node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                        Vertex aNode = graph.getVertex(node_id);
                        node_list.add(aNode.getProperty("agent").toString());
                    }
                }

            }
            else if((region != null) && (agent != null) && (plugin == null))
            {
                graph = factory.getTx();
                //OrientGraphNoTx graph = factory.getNoTx();

                Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:pNode.nodePath WHERE key = [\"" + region + "\",\"" + agent +"\"]")).execute();
                Iterator<Vertex> iter = resultIterator.iterator();
                if(iter.hasNext())
                {
                    node_list = new ArrayList<String>();
                    while(iter.hasNext())
                    {
                        Vertex v = iter.next();
                        String node_id = v.getProperty("rid").toString();
                        node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                        Vertex pNode = graph.getVertex(node_id);
                        node_list.add(pNode.getProperty("plugin").toString());
                    }
                }
                //graph.shutdown();
                //return node_list;

            }

        }
        catch(Exception ex)
        {
            System.out.println("GrapgDBEngine : getNodeList : Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return node_list;
    }

    public List<String> getresourceNodeList(String resource_id, String inode_id)
    {
        List<String> node_list = null;
        OrientGraph graph = null;
        try
        {

            if((resource_id == null) && (inode_id == null))
            {
                graph = factory.getTx();
                Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:resourceNode.nodePath")).execute();

                Iterator<Vertex> iter = resultIterator.iterator();
                if(iter.hasNext())
                {
                    node_list = new ArrayList<String>();
                    while(iter.hasNext())
                    {
                        Vertex v = iter.next();
                        String node_id = v.getProperty("rid").toString();
                        node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                        Vertex rNode = graph.getVertex(node_id);
                        node_list.add(rNode.getProperty("resource_id").toString());
                    }
                }

            }
            else if((resource_id != null) && (inode_id == null))
            {
                graph = factory.getTx();
                //OrientGraphNoTx graph = factory.getNoTx();

                Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:iNode.nodePath WHERE key = [\"" + resource_id + "\"]")).execute();
                Iterator<Vertex> iter = resultIterator.iterator();
                if(iter.hasNext())
                {
                    node_list = new ArrayList<String>();
                    while(iter.hasNext())
                    {
                        Vertex v = iter.next();
                        String node_id = v.getProperty("rid").toString();
                        node_id = node_id.substring(node_id.indexOf("[") + 1, node_id.indexOf("]"));
                        Vertex aNode = graph.getVertex(node_id);
                        node_list.add(aNode.getProperty("inode_id").toString());
                    }
                }

            }

        }
        catch(Exception ex)
        {
            System.out.println("GrapgDBEngine : getNodeList : Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return node_list;
    }

    public String getINodeParam(String resource_id,String inode_id, String param)
    {
        String iNode_param = null;
        String node_id = null;
        OrientGraph graph = null;

        try
        {
            node_id = getINodeId(resource_id,inode_id);
            if(node_id != null)
            {
                graph = factory.getTx();
                Vertex iNode = graph.getVertex(node_id);
                iNode_param = iNode.getProperty(param).toString();
            }

        }
        catch(Exception ex)
        {
            System.out.println("getINodeParam: Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return iNode_param;
    }

    public String getNodeParam(String node_id, String param)
    {
        String paramVal = null;

        int count = 0;
        try
        {

            while((paramVal == null) && (count != retryCount))
            {
                if(count > 0)
                {
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                paramVal = IgetNodeParam(node_id,param);
                count++;

            }

            if((paramVal == null) && (count == retryCount))
            {
                System.out.println("GraphDBEngine : getNodeParam : Failed to add node in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            System.out.println("GraphDBEngine : getNodeParam : Error " + ex.toString());
        }

        return paramVal;
    }

    public String getNodeParam(String region, String agent, String plugin, String param)
    {
        String paramVal = null;
        String node_id  = getNodeId(region,agent,plugin);

        int count = 0;
        try
        {

            while((paramVal == null) && (count != retryCount))
            {
                if(count > 0)
                {
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                paramVal = IgetNodeParam(node_id,param);
                count++;

            }

            if((paramVal == null) && (count == retryCount))
            {
                System.out.println("GraphDBEngine : getNodeParam : Failed to add node in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            System.out.println("GraphDBEngine : getNodeParam : Error " + ex.toString());
        }

        return paramVal;
    }

    public String IgetNodeParam(String node_id, String param)
    {
        String node_param = null;
        OrientGraph graph = null;

        try
        {
            graph = factory.getTx();
            Vertex iNode = graph.getVertex(node_id);
            node_param = iNode.getProperty(param).toString();

        }
        catch(Exception ex)
        {
            System.out.println("IgetNodeParam: Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return node_param;
    }


    //WRITES

    public String addIsAttachedEdge(String resource_id, String inode_id, String region, String agent, String plugin)
    {
        String edge_id = null;

        int count = 0;
        try
        {

            while((edge_id == null) && (count != retryCount))
            {
                if(count > 0)
                {
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                edge_id = IaddIsAttachedEdge(resource_id, inode_id, region, agent, plugin);
                count++;

            }

            if((edge_id == null) && (count == retryCount))
            {
                System.out.println("GraphDBEngine : addIsAttachedEdge : Failed to add edge in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            System.out.println("GraphDBEngine : addIsAttachedEdge : Error " + ex.toString());
        }

        return edge_id;
    }

    private String IaddIsAttachedEdge(String resource_id, String inode_id, String region, String agent, String plugin)
    {
        String edge_id = null;
        OrientGraph graph = null;
        try
        {
            edge_id = getResourceEdgeId(resource_id,inode_id,region,agent);
            if(edge_id != null)
            {
                //System.out.println("Node already Exist: region=" + region + " agent=" + agent + " plugin=" + plugin);
            }
            else
            {

                if((resource_id != null) && (inode_id != null) && (region != null) && (agent != null) && (plugin != null))
                {
                    String inode_node_id = getINodeId(resource_id,inode_id);
                    String pnode_node_id = getNodeId(region,agent,plugin);
                    if((inode_node_id != null) && (pnode_node_id != null))
                    {
                        graph = factory.getTx();

                        Vertex fromV = graph.getVertex(pnode_node_id);
                        Vertex toV = graph.getVertex(inode_node_id);
                        if((fromV != null) && (toV != null))
                        {
                            Edge e = graph.addEdge("class:isAssigned", fromV, toV, "isAssigned");
                            e.setProperty("resource_id", resource_id);
                            e.setProperty("inode_id", inode_id);
                            e.setProperty("region", region);
                            e.setProperty("agent", agent);
                            e.setProperty("plugin", plugin);
                            graph.commit();
                            edge_id = e.getId().toString();
                        }
                    }
                    else
                    {
                        if(inode_node_id != null)
                        {
                            System.out.println("IaddIsAttachedEdge: iNode does not exist : " + inode_id);
                        }
                        if(pnode_node_id != null)
                        {
                            System.out.println("IaddIsAttachedEdge: pNode does not exist : " + region + agent + plugin);
                        }
                    }
                }
                else
                {
                    System.out.println("IaddIsAttachedEdge: required input is null : " + resource_id + "," + inode_id + "," + region + "," + agent + "," + plugin);

                }

            }

        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException exc)
        {
            //eat exception.. this is not normal and should log somewhere
            System.out.println("IaddIsAttachedEdge: ORecordDuplicatedException : " + exc.toString());
        }
        catch(com.orientechnologies.orient.core.exception.OConcurrentModificationException exc)
        {
            //eat exception.. this is normal
            System.out.println("IaddIsAttachedEdge: OConcurrentModificationException : " + exc.toString());
        }
        catch(Exception ex)
        {
            long threadId = Thread.currentThread().getId();
            System.out.println("IaddIsAttachedEdge: thread_id: " + threadId + " Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return edge_id;

    }

    public String addINode(String resource_id, String inode_id)
    {
        String node_id = null;

        int count = 0;
        try
        {

            while((node_id == null) && (count != retryCount))
            {
                if(count > 0)
                {
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                node_id = IaddINode(resource_id, inode_id);
                count++;

            }

            if((node_id == null) && (count == retryCount))
            {
                System.out.println("GraphDBEngine : addINode : Failed to add node in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            System.out.println("GraphDBEngine : addINode : Error " + ex.toString());
        }

        return node_id;
    }

    private String IaddNode(String region, String agent, String plugin)
    {
        String node_id = null;
        OrientGraph graph = null;
        try
        {
            node_id = getNodeId(region,agent,plugin);

            if(node_id != null)
            {
                //System.out.println("Node already Exist: region=" + region + " agent=" + agent + " plugin=" + plugin);
            }
            else
            {
                //System.out.println("Adding Node : region=" + region + " agent=" + agent + " plugin=" + plugin);
                if((region != null) && (agent == null) && (plugin == null))
                {
                    graph = factory.getTx();
                    Vertex v = graph.addVertex("class:rNode");
                    v.setProperty("region", region);
                    graph.commit();
                    node_id = v.getId().toString();
                }
                else if((region != null) && (agent != null) && (plugin == null))
                {
                    String region_id = getNodeId(region,null,null);

                    if(region_id == null)
                    {
                        //System.out.println("Must add region=" + region + " before adding agent=" + agent);
                        region_id = addNode(region,null,null);

                    }
                    if(region_id != null)
                    {
                        graph = factory.getTx();
                        Vertex v = graph.addVertex("class:aNode");
                        v.setProperty("region", region);
                        v.setProperty("agent", agent);

                        Vertex fromV = graph.getVertex(v.getId().toString());
                        Vertex toV = graph.getVertex(region_id);

                        graph.addEdge("class:isAgent", fromV, toV, "isAgent");
                        graph.commit();
                        node_id = v.getId().toString();
						/*
				    	//add edges

				    	String edge_id = addEdge(region,agent,null,region,null,null,"isAgent");
				    	if(edge_id == null)
				    	{
				    		System.out.println("Unable to add isAgent Edge between region=" + region + " and agent=" + agent);
				    	}
						 */
                    }
                }
                else if((region != null) && (agent != null) && (plugin != null))
                {
                    //System.out.println("Adding Plugin : region=" + region + " agent=" + agent + " plugin=" + plugin);

                    String agent_id = getNodeId(region,agent,null);
                    if(agent_id == null)
                    {
                        //System.out.println("For region=" + region + " we must add agent=" + agent + " before adding plugin=" + plugin);
                        agent_id = addNode(region,agent,null);

                    }

                    if(agent_id != null)
                    {
                        graph = factory.getTx();
                        Vertex v = graph.addVertex("class:pNode");
                        v.setProperty("region", region);
                        v.setProperty("agent", agent);
                        v.setProperty("plugin", plugin);

                        Vertex fromV = graph.getVertex(v.getId().toString());
                        Vertex toV = graph.getVertex(agent_id);

                        graph.addEdge("class:isPlugin", fromV, toV, "isPlugin");
                        graph.commit();
					    /*
					    //add Edge
					    String edge_id = addEdge(region,agent,plugin,region,agent,null,"isPlugin");
					    if(edge_id == null)
					    {
					    	System.out.println("Unable to add isPlugin Edge between region=" + region + " agent=" + "agent=" + region + " and agent=" + agent);
					    }
					    */
                        node_id = v.getId().toString();
                    }
                }
            }

        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException exc)
        {
            //eat exception.. this is not normal and should log somewhere
        }
        catch(com.orientechnologies.orient.core.exception.OConcurrentModificationException exc)
        {
            //eat exception.. this is normal
        }
        catch(Exception ex)
        {
            long threadId = Thread.currentThread().getId();
            System.out.println("IaddNode: thread_id: " + threadId + " Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return node_id;

    }

    public String addNode(String region, String agent, String plugin)
    {
        String node_id = null;
        int count = 0;
        try
        {

            while((node_id == null) && (count != retryCount))
            {
                if(count > 0)
                {
                    //System.out.println("ADDNODE RETRY : region=" + region + " agent=" + agent + " plugin" + plugin);
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                node_id = IaddNode(region, agent, plugin);
                count++;

            }

            if((node_id == null) && (count == retryCount))
            {
                System.out.println("GraphDBEngine : addNode : Failed to add node in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            System.out.println("GraphDBEngine : addNode : Error " + ex.toString());
        }

        return node_id;
    }

    private String IaddINode(String resource_id, String inode_id)
    {
        String node_id = null;
        String resource_node_id = null;

        OrientGraph graph = null;
        try
        {
            node_id = getINodeId(resource_id,inode_id);
            if(node_id != null)
            {
                //System.out.println("Node already Exist: region=" + region + " agent=" + agent + " plugin=" + plugin);
            }
            else
            {

                resource_node_id = getResourceNodeId(resource_id);
                if(resource_node_id == null)
                {
                    resource_node_id = addResourceNode(resource_id);
                }

                if(resource_node_id != null)
                {
                    graph = factory.getTx();

                    Vertex fromV = graph.addVertex("class:iNode");
                    fromV.setProperty("inode_id", inode_id);
                    fromV.setProperty("resource_id", resource_id);


                    //ADD EDGE TO RESOURCE
                    Vertex toV = graph.getVertex(resource_node_id);
                    graph.addEdge("class:isResource", fromV, toV, "isResource");
                    graph.commit();
                    node_id = fromV.getId().toString();
                }
            }
        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException exc)
        {
            //eat exception.. this is not normal and should log somewhere
            System.out.println("Error 0 " + exc.getMessage());
        }
        catch(com.orientechnologies.orient.core.exception.OConcurrentModificationException exc)
        {
            //eat exception.. this is normal
            System.out.println("Error 1 " + exc.getMessage());
        }
        catch(Exception ex)
        {
            long threadId = Thread.currentThread().getId();
            System.out.println("IaddINode: thread_id: " + threadId + " Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return node_id;

    }

    public String addResourceNode(String resource_id)
    {
        String node_id = null;

        int count = 0;
        try
        {

            while((node_id == null) && (count != retryCount))
            {
                if(count > 0)
                {
                    //System.out.println("ADDNODE RETRY : region=" + region + " agent=" + agent + " plugin" + plugin);
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                node_id = IaddResourceNode(resource_id);
                count++;

            }

            if((node_id == null) && (count == retryCount))
            {
                System.out.println("GraphDBEngine : addINode : Failed to add node in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            System.out.println("GraphDBEngine : addINode : Error " + ex.toString());
        }

        return node_id;
    }

    private String IaddResourceNode(String resource_id)
    {
        String node_id = null;
        OrientGraph graph = null;
        try
        {
            node_id = getResourceNodeId(resource_id);

            if(node_id != null)
            {
                //System.out.println("Node already Exist: region=" + region + " agent=" + agent + " plugin=" + plugin);
            }
            else
            {
                graph = factory.getTx();
                //add something

                Vertex v = graph.addVertex("class:resourceNode");
                v.setProperty("resource_id", resource_id);
                graph.commit();
                node_id = v.getId().toString();
            }
        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException exc)
        {
            //eat exception.. this is not normal and should log somewhere
        }
        catch(com.orientechnologies.orient.core.exception.OConcurrentModificationException exc)
        {
            //eat exception.. this is normal
        }
        catch(Exception ex)
        {
            long threadId = Thread.currentThread().getId();
            System.out.println("addResourceNode: thread_id: " + threadId + " Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return node_id;

    }

    public String addEdge(String src_region, String src_agent, String src_plugin, String dst_region, String dst_agent, String dst_plugin, String className)
    {
        String edge_id = null;
        int count = 0;
        try
        {

            while((edge_id == null) && (count != retryCount))
            {
                edge_id = IaddEdge(src_region, src_agent, src_plugin, dst_region, dst_agent, dst_plugin, className);
                count++;
            }

            if((edge_id == null) && (count == retryCount))
            {
                System.out.println("GraphDBEngine : addEdge : Failed to add node in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            System.out.println("GraphDBEngine : addEdge : Error " + ex.toString());
        }

        return edge_id;
    }

    private String IaddEdge(String src_region, String src_agent, String src_plugin, String dst_region, String dst_agent, String dst_plugin, String className)
    {
        String edge_id = null;
        try
        {
            String src_node_id = getNodeId(src_region,src_agent,src_plugin);
            String dst_node_id = getNodeId(dst_region,dst_agent,dst_plugin);

            OrientGraph graph = factory.getTx();
            Vertex fromV = graph.getVertex(src_node_id);
            Vertex toV = graph.getVertex(dst_node_id);

            Edge isEdge = graph.addEdge("class:" + className, fromV, toV, className);
            graph.commit();
            graph.shutdown();
            edge_id = isEdge.getId().toString();
        }
        catch(com.orientechnologies.orient.core.exception.OConcurrentModificationException exc)
        {
            //eat exception
        }
        catch(Exception ex)
        {
            System.out.println("addEdge Error: " + ex.toString());

        }
        return edge_id;

    }

    public boolean removeNode(String region, String agent, String plugin)
    {
        boolean nodeRemoved = false;
        int count = 0;
        try
        {

            while((!nodeRemoved) && (count != retryCount))
            {
                if(count > 0)
                {
                    //System.out.println("REMOVENODE RETRY : region=" + region + " agent=" + agent + " plugin" + plugin);
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                nodeRemoved = IremoveNode(region, agent, plugin);
                count++;

            }

            if((!nodeRemoved) && (count == retryCount))
            {
                System.out.println("GraphDBEngine : removeNode : Failed to add node in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            System.out.println("GraphDBEngine : removeNode : Error " + ex.toString());
        }

        return nodeRemoved;
    }

    private boolean IremoveNode(String region, String agent, String plugin)
    {
        boolean nodeRemoved = false;
        OrientGraph graph = null;
        try
        {
            //String pathname = getPathname(region,agent,plugin);
            String node_id = getNodeId(region,agent,plugin);
            if(node_id == null)
            {
                //System.out.println("Tried to remove missing node : " + pathname);
                nodeRemoved = true;
            }
            else
            {
                if((region != null) && (agent == null) && (plugin == null))
                {
                    List<String> agentList = getNodeList(region,null,null);
                    if(agentList != null)
                    {
                        for(String removeAgent : agentList)
                        {
                            removeNode(region,removeAgent,null);
                        }
                    }
                    agentList = getNodeList(region,null,null);
                    if(agentList == null)
                    {
                        graph = factory.getTx();
                        Vertex rNode = graph.getVertex(node_id);
                        graph.removeVertex(rNode);
                        graph.commit();
                        nodeRemoved = true;
                    }

                }
                if((region != null) && (agent != null) && (plugin == null))
                {

                    List<String> pluginList = getNodeList(region,agent,null);
                    if(pluginList != null)
                    {
                        for(String removePlugin : pluginList)
                        {
                            removeNode(region,agent,removePlugin);
                        }
                    }
                    pluginList = getNodeList(region,agent,null);
                    if(pluginList == null)
                    {
                        graph = factory.getTx();
                        Vertex aNode = graph.getVertex(node_id);
                        graph.removeVertex(aNode);
                        graph.commit();
                        nodeRemoved = true;
                    }
                }
                if((region != null) && (agent != null) && (plugin != null))
                {
                    graph = factory.getTx();
                    Vertex pNode = graph.getVertex(node_id);
                    graph.removeVertex(pNode);
                    graph.commit();
                    nodeRemoved = true;
                }

            }
        }
        catch(com.orientechnologies.orient.core.exception.OConcurrentModificationException exc)
        {
            //eat exception
        }
        catch(Exception ex)
        {
            long threadId = Thread.currentThread().getId();
            System.out.println("GrapgDBEngine : removeNode :  thread_id: " + threadId + " Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return nodeRemoved;

    }

    public boolean removeINode(String resource_id, String inode_id)
    {
        boolean nodeRemoved = false;
        int count = 0;
        try
        {

            while((!nodeRemoved) && (count != retryCount))
            {
                if(count > 0)
                {
                    //System.out.println("REMOVENODE RETRY : region=" + region + " agent=" + agent + " plugin" + plugin);
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                nodeRemoved = IremoveINode(resource_id, inode_id);
                count++;

            }

            if((!nodeRemoved) && (count == retryCount))
            {
                System.out.println("GraphDBEngine : removeINode : Failed to add node in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            System.out.println("GraphDBEngine : removeINode : Error " + ex.toString());
        }

        return nodeRemoved;
    }

    private boolean IremoveINode(String resource_id, String inode_id)
    {
        boolean nodeRemoved = false;
        OrientGraph graph = null;
        try
        {
            //String pathname = getPathname(region,agent,plugin);
            String node_id = getINodeId(resource_id, inode_id);
            if(node_id == null)
            {
                System.out.println("Tried to remove missing node : resource_id=" + resource_id + " inode_id=" + inode_id);
                nodeRemoved = true;
            }
            else
            {
                graph = factory.getTx();
                Vertex rNode = graph.getVertex(node_id);
                graph.removeVertex(rNode);
                graph.commit();
                nodeRemoved = true;
            }
        }
        catch(com.orientechnologies.orient.core.exception.OConcurrentModificationException exc)
        {
            //eat exception
        }
        catch(Exception ex)
        {
            long threadId = Thread.currentThread().getId();
            System.out.println("GrapgDBEngine : removeNode :  thread_id: " + threadId + " Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return nodeRemoved;

    }

    public boolean removeResourceNode(String resource_id)
    {
        boolean nodeRemoved = false;
        int count = 0;
        try
        {

            while((!nodeRemoved) && (count != retryCount))
            {
                if(count > 0)
                {
                    //System.out.println("REMOVENODE RETRY : region=" + region + " agent=" + agent + " plugin" + plugin);
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                nodeRemoved = IremoveResourceNode(resource_id);
                count++;

            }

            if((!nodeRemoved) && (count == retryCount))
            {
                System.out.println("GraphDBEngine : removeResourceNode : Failed to add node in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            System.out.println("GraphDBEngine : removeResourceNode : Error " + ex.toString());
        }

        return nodeRemoved;
    }

    private boolean IremoveResourceNode(String resource_id)
    {
        boolean nodeRemoved = false;
        OrientGraph graph = null;
        try
        {
            //String pathname = getPathname(region,agent,plugin);
            String node_id = getResourceNodeId(resource_id);
            if(node_id == null)
            {
                System.out.println("Tried to remove missing node : resource_id=" + resource_id);
                nodeRemoved = true;
            }
            else
            {
                //remove iNodes First
                List<String> inodes = getresourceNodeList(resource_id,null);
                if(inodes != null)
                {
                    for(String inode_id : inodes)
                    {
                        removeINode(resource_id,inode_id);
                    }
                }
                inodes = getresourceNodeList(resource_id,null);
                if(inodes == null)
                {
                    graph = factory.getTx();
                    Vertex resourceNode = graph.getVertex(node_id);
                    graph.removeVertex(resourceNode);
                    graph.commit();
                    nodeRemoved = true;
                }

            }
        }
        catch(com.orientechnologies.orient.core.exception.OConcurrentModificationException exc)
        {
            //eat exception
        }
        catch(Exception ex)
        {
            long threadId = Thread.currentThread().getId();
            System.out.println("GraphDBEngine : IremoveResourceNode :  thread_id: " + threadId + " Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return nodeRemoved;

    }

    public boolean IsetINodeParams(String resource_id, String inode_id, Map<String,String> paramMap)
    {
        boolean isUpdated = false;
        OrientGraph graph = null;
        String node_id = null;
        try
        {
            node_id = getINodeId(resource_id, inode_id);
            if(node_id != null)
            {
                graph = factory.getTx();
                Vertex iNode = graph.getVertex(node_id);
                Iterator it = paramMap.entrySet().iterator();
                while (it.hasNext())
                {
                    Entry pairs = (Entry)it.next();
                    iNode.setProperty( pairs.getKey().toString(), pairs.getValue().toString());
                }
                graph.commit();
                isUpdated = true;
            }
        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException exc)
        {
            //eat exception.. this is not normal and should log somewhere
        }
        catch(com.orientechnologies.orient.core.exception.OConcurrentModificationException exc)
        {
            //eat exception.. this is normal
        }
        catch(Exception ex)
        {
            long threadId = Thread.currentThread().getId();
            System.out.println("setINodeParams: thread_id: " + threadId + " Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return isUpdated;
    }

    public boolean setINodeParams(String resource_id, String inode_id, Map<String,String> paramMap)
    {
        boolean isUpdated = false;
        int count = 0;
        try
        {

            while((!isUpdated) && (count != retryCount))
            {
                if(count > 0)
                {
                    //System.out.println("iNODEUPDATE RETRY : region=" + region + " agent=" + agent + " plugin" + plugin);
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                isUpdated = IsetINodeParams(resource_id,inode_id, paramMap);
                count++;

            }

            if((!isUpdated) && (count == retryCount))
            {
                System.out.println("GraphDBEngine : setINodeParams : Failed to add node in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            System.out.println("GraphDBEngine : setINodeParams : Error " + ex.toString());
        }

        return isUpdated;
    }

    public boolean setNodeParams(String region, String agent, String plugin, Map<String,String> paramMap)
    {
        boolean isUpdated = false;
        int count = 0;
        try
        {

            while((!isUpdated) && (count != retryCount))
            {
                if(count > 0)
                {
                    //System.out.println("iNODEUPDATE RETRY : region=" + region + " agent=" + agent + " plugin" + plugin);
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                isUpdated = IsetNodeParams(region, agent, plugin, paramMap);
                count++;

            }

            if((!isUpdated) && (count == retryCount))
            {
                System.out.println("GraphDBEngine : setINodeParams : Failed to add node in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            System.out.println("GraphDBEngine : setINodeParams : Error " + ex.toString());
        }

        return isUpdated;
    }

    public boolean IsetNodeParams(String region, String agent, String plugin, Map<String,String> paramMap)
    {

        boolean isUpdated = false;
        OrientGraph graph = null;
        String node_id = null;
        try
        {
            node_id = getNodeId(region,agent,plugin);
            if(node_id != null)
            {
                graph = factory.getTx();
                Vertex iNode = graph.getVertex(node_id);
                Iterator it = paramMap.entrySet().iterator();
                while (it.hasNext())
                {
                    Entry pairs = (Entry)it.next();
                    iNode.setProperty( pairs.getKey().toString(), pairs.getValue().toString());
                }
                graph.commit();
                isUpdated = true;
            }
        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException exc)
        {
            //eat exception.. this is not normal and should log somewhere
        }
        catch(com.orientechnologies.orient.core.exception.OConcurrentModificationException exc)
        {
            //eat exception.. this is normal
        }
        catch(Exception ex)
        {
            long threadId = Thread.currentThread().getId();
            System.out.println("setINodeParams: thread_id: " + threadId + " Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return isUpdated;
    }

    public boolean setINodeParam(String resource_id, String inode_id, String paramKey, String paramValue)
    {
        boolean isUpdated = false;
        int count = 0;
        try
        {

            while((!isUpdated) && (count != retryCount))
            {
                if(count > 0)
                {
                    //System.out.println("iNODEUPDATE RETRY : region=" + region + " agent=" + agent + " plugin" + plugin);
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                isUpdated = IsetINodeParam(resource_id, inode_id, paramKey, paramValue);
                count++;

            }

            if((!isUpdated) && (count == retryCount))
            {
                System.out.println("GraphDBEngine : setINodeParam : Failed to add node in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            System.out.println("GraphDBEngine : setINodeParam : Error " + ex.toString());
        }

        return isUpdated;
    }

    public boolean IsetINodeParam(String resource_id, String inode_id, String paramKey, String paramValue)
    {
        boolean isUpdated = false;
        OrientGraph graph = null;
        String node_id = null;
        try
        {
            node_id = getINodeId(resource_id, inode_id);
            if(node_id != null)
            {
                graph = factory.getTx();
                Vertex iNode = graph.getVertex(node_id);
                iNode.setProperty( paramKey, paramValue);
                graph.commit();
                isUpdated = true;
            }
        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException exc)
        {
            //eat exception.. this is not normal and should log somewhere
        }
        catch(com.orientechnologies.orient.core.exception.OConcurrentModificationException exc)
        {
            //eat exception.. this is normal
        }
        catch(Exception ex)
        {
            long threadId = Thread.currentThread().getId();
            System.out.println("setINodeParams: thread_id: " + threadId + " Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return isUpdated;
    }

    public boolean setNodeParam(String region, String agent, String plugin, String paramKey, String paramValue)
    {
        boolean isUpdated = false;
        int count = 0;
        try
        {

            while((!isUpdated) && (count != retryCount))
            {
                if(count > 0)
                {
                    //System.out.println("iNODEUPDATE RETRY : region=" + region + " agent=" + agent + " plugin" + plugin);
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                isUpdated = IsetNodeParam(region, agent, plugin, paramKey, paramValue);
                count++;

            }

            if((!isUpdated) && (count == retryCount))
            {
                System.out.println("GraphDBEngine : setINodeParam : Failed to add node in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            System.out.println("GraphDBEngine : setINodeParam : Error " + ex.toString());
        }

        return isUpdated;
    }

    public boolean IsetNodeParam(String region, String agent, String plugin, String paramKey, String paramValue)
    {
        boolean isUpdated = false;
        OrientGraph graph = null;
        String node_id = null;
        try
        {
            node_id = getNodeId(region, agent, plugin);
            if(node_id != null)
            {
                graph = factory.getTx();
                Vertex iNode = graph.getVertex(node_id);
                //set envparams if aNode
                if((paramKey.equals("configparams")) && (region != null) && (agent != null) && (plugin == null))
                {
                    String[] configParams = paramValue.split(",");
                    for(String cParam : configParams)
                    {
                        String[] cPramKV = cParam.split("=");
                        for(String indexParam : aNodeIndexParams)
                        {
                            if(cPramKV[0].equals(indexParam))
                            {
                                iNode.setProperty(cPramKV[0],cPramKV[1]);
                            }
                        }
                    }
                }
                iNode.setProperty( paramKey, paramValue);
                graph.commit();
                isUpdated = true;
            }
        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException exc)
        {
            //eat exception.. this is not normal and should log somewhere
        }
        catch(com.orientechnologies.orient.core.exception.OConcurrentModificationException exc)
        {
            //eat exception.. this is normal
        }
        catch(Exception ex)
        {
            long threadId = Thread.currentThread().getId();
            System.out.println("setINodeParams: thread_id: " + threadId + " Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return isUpdated;
    }

    //INIT Functions
    public void initCrescoDB()
    {
        try
        {
            //index properties

            System.out.println("Create Region Vertex Class");
            String[] rProps = {"region"}; //Property names
            createVertexClass("rNode", rProps);

            System.out.println("Create Agent Vertex Class");
            String[] aProps = {"region", "agent"}; //Property names
            createVertexClass("aNode", aProps);

            //indexes for searching
            System.out.println("Create Agent Vertex Class Index's");
            for(String indexName : aNodeIndexParams)
            {
                createVertexIndex("aNode", indexName, false);
            }

            System.out.println("Create Plugin Vertex Class");
            String[] pProps = {"region", "agent", "plugin"}; //Property names
            createVertexClass("pNode", pProps);


            System.out.println("Create resourceNode Vertex Class");
            String[] resourceProps = {"resource_id"}; //Property names
            createVertexClass("resourceNode", resourceProps);

            System.out.println("Create iNode Vertex Class");
            String[] iProps = {"resource_id", "inode_id"}; //Property names
            createVertexClass("iNode", iProps);


            System.out.println("Create isAgent Edge Class");
            String[] isAgentProps = {"edge_id"}; //Property names
            //createEdgeClass("isAgent",isAgentProps);
            createEdgeClass("isAgent",null);

            System.out.println("Create isPlugin Edge Class");
            String[] isPluginProps = {"edge_id"}; //Property names
            //createEdgeClass("isPlugin",isPluginProps);
            createEdgeClass("isPlugin",null);

            System.out.println("Create isConnected Edge Class");
            String[] isConnectedProps = {"edge_id"}; //Property names
            //createEdgeClass("isConnected",isConnectedProps);
            createEdgeClass("isConnected",null);

            System.out.println("Create isResource Edge Class");
            String[] isResourceProps = {"edge_id"}; //Property names
            //createEdgeClass("isResource",isResourceProps);
            createEdgeClass("isResource",null);

            System.out.println("Create isReachable Edge Class");
            String[] isReachableProps = {"edge_id"}; //Property names
            //createEdgeClass("isReachable",isReachableProps);
            createEdgeClass("isReachable",null);

            System.out.println("Create isAssigned Edge Class");
            String[] isAssignedProps = {"resource_id","inode_id","region", "agent"}; //Property names
            createEdgeClass("isAssigned",isAssignedProps);


            //create plugin anchor resource and inodes
            String[]  pluginAnchors= {"sysinfo","netdiscovery"}; //Property names
            for(String pAnchor : pluginAnchors)
            {
                String resource_id = pAnchor + "_resource";
                String inode_id = pAnchor + "_inode";

                System.out.println("Creating " + pAnchor + " resource node.");
                if(getResourceNodeId(resource_id) == null)
                {
                    //create resource
                    addResourceNode(resource_id);
                }
                System.out.println("Creating " + pAnchor + " iNode.");
                if(getINodeId(resource_id, inode_id) == null)
                {
                    //create inode
                    addINode(resource_id, inode_id);
                }
            }


        }
        catch(Exception ex)
        {
            System.out.println("initCrescoDB Error: " + ex.toString());
        }
    }

    boolean createVertexIndex(String className, String indexName, boolean isUnique)
    {
        boolean wasCreated = false;
        try
        {
            OrientGraphNoTx txGraph = factory.getNoTx();
            //OSchema schema = ((OrientGraph)odb).getRawGraph().getMetadata().getSchema();
            OSchema schema = ((OrientGraphNoTx)txGraph).getRawGraph().getMetadata().getSchema();

            if (schema.existsClass(className))
            {
                OClass vt = txGraph.getVertexType(className);
                //OClass vt = txGraph.createVertexType(className);
                vt.createProperty(indexName, OType.STRING);

                if(isUnique)
                {
                    vt.createIndex(className + "." + indexName, OClass.INDEX_TYPE.UNIQUE, indexName);
                }
                else
                {
                    vt.createIndex(className + "." + indexName, OClass.INDEX_TYPE.NOTUNIQUE, indexName);
                }

                wasCreated = true;
            }
            txGraph.commit();
            txGraph.shutdown();
        }
        catch(Exception ex)
        {
            System.out.println("GraphDBEngine : createVertexIndex : Error " + ex.toString());
        }

        return wasCreated;
    }

    boolean createVertexIndex(String className, String[] props, String indexName, boolean isUnique)
    {
        boolean wasCreated = false;
        OrientGraphNoTx txGraph = factory.getNoTx();
        //OSchema schema = ((OrientGraph)odb).getRawGraph().getMetadata().getSchema();
        OSchema schema = ((OrientGraphNoTx)txGraph).getRawGraph().getMetadata().getSchema();

        if (schema.existsClass(className))
        {
            OClass vt = txGraph.getVertexType(className);
            //OClass vt = txGraph.createVertexType(className);
            for(String prop : props)
            {
                vt.createProperty(prop, OType.STRING);
            }
            if(isUnique)
            {
                vt.createIndex(className + "." + indexName, OClass.INDEX_TYPE.UNIQUE, props);
            }
            else
            {
                vt.createIndex(className + "." + indexName, OClass.INDEX_TYPE.NOTUNIQUE, props);
            }

            wasCreated = true;
        }
        txGraph.commit();
        txGraph.shutdown();
        return wasCreated;
    }

    boolean createVertexClass(String className, String[] props)
    {
        boolean wasCreated = false;
        OrientGraphNoTx txGraph = factory.getNoTx();
        //OSchema schema = ((OrientGraph)odb).getRawGraph().getMetadata().getSchema();
        OSchema schema = ((OrientGraphNoTx)txGraph).getRawGraph().getMetadata().getSchema();

        if (!schema.existsClass(className))
        {
            OClass vt = txGraph.createVertexType(className);
            for(String prop : props)
                vt.createProperty(prop, OType.STRING);
            vt.createIndex(className + ".nodePath", OClass.INDEX_TYPE.UNIQUE, props);

            wasCreated = true;
        }
        txGraph.commit();
        txGraph.shutdown();
        return wasCreated;
    }

    boolean createEdgeClass(String className, String[] props)
    {
        boolean wasCreated = false;
        OrientGraphNoTx txGraph = factory.getNoTx();
        //OSchema schema = ((OrientGraph)odb).getRawGraph().getMetadata().getSchema();
        OSchema schema = ((OrientGraphNoTx)txGraph).getRawGraph().getMetadata().getSchema();

        if (!schema.existsClass(className))
        {
            OClass et = txGraph.createEdgeType(className);
            if(props != null) {
                for (String prop : props) {
                    et.createProperty(prop, OType.STRING);
                }
                et.createIndex(className + ".edgeProp", OClass.INDEX_TYPE.UNIQUE, props);
            }
            wasCreated = true;
        }
        txGraph.commit();
        txGraph.shutdown();
        return wasCreated;
    }

    //CLIENT FUNCTIONS

    //client DB


    public String getNodeClass(String region, String agent, String plugin)
    {
        try
        {
            if((region != null) && (agent == null) && (plugin == null))
            {
                return "rNode";
            }
            else if((region != null) && (agent != null) && (plugin == null))
            {
                return "aNode";
            }
            else if((region != null) && (agent != null) && (plugin != null))
            {
                return "pNode";
            }
        }
        catch(Exception ex)
        {
            System.out.println("getNodeClass: Error " + ex.toString());
        }
        return null;

    }

    public boolean updateEdge(String edge_id, Map<String,String> params)
    {
        boolean isUpdated = false;
        int count = 0;
        try
        {

            while((!isUpdated) && (count != retryCount))
            {
                if(count > 0)
                {
                    //System.out.println("ADDNODE RETRY : region=" + region + " agent=" + agent + " plugin" + plugin);
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                isUpdated = IupdateEdge(edge_id, params);
                count++;

            }

            if((!isUpdated) && (count == retryCount))
            {
                System.out.println("GraphDBEngine : updateEdge : Failed to update edge in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            System.out.println("GraphDBEngine : updateEdge : Error " + ex.toString());
        }

        return isUpdated;
    }

    private boolean IupdateEdge(String edge_id, Map<String,String> params)
    {
        boolean isUpdated = false;
        OrientGraph graph = null;
        try
        {
            graph = factory.getTx();
            Edge edge = graph.getEdge(edge_id);
            if(edge != null)
            {
                for (Entry<String, String> entry : params.entrySet())
                {
                    edge.setProperty(entry.getKey(), entry.getValue());
                }
                graph.commit();
                isUpdated = true;
            }
            else
            {
                System.out.println("IupdateEdge: no edge found for edge_id=" + edge_id);
            }

        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException exc)
        {
            //eat exception.. this is not normal and should log somewhere
        }
        catch(com.orientechnologies.orient.core.exception.OConcurrentModificationException exc)
        {
            //eat exception.. this is normal
        }
        catch(Exception ex)
        {
            long threadId = Thread.currentThread().getId();
            System.out.println("IupdateEdge: thread_id: " + threadId + " Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return isUpdated;

    }

    private boolean updateEdgeNoTx(String edge_id, Map<String,String> params)
    {
        boolean isUpdated = false;
        OrientGraphNoTx graph = null;
        try
        {
            graph = factory.getNoTx();
            Edge edge = graph.getEdge(edge_id);
            if(edge != null)
            {
                for (Entry<String, String> entry : params.entrySet())
                {
                    edge.setProperty(entry.getKey(), entry.getValue());
                }
                graph.commit();
                isUpdated = true;
            }
            else
            {
                System.out.println("IupdateEdge: no edge found for edge_id=" + edge_id);
            }

        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException exc)
        {
            //eat exception.. this is not normal and should log somewhere
        }
        catch(com.orientechnologies.orient.core.exception.OConcurrentModificationException exc)
        {
            //eat exception.. this is normal
        }
        catch(Exception ex)
        {
            long threadId = Thread.currentThread().getId();
            System.out.println("IupdateEdge: thread_id: " + threadId + " Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return isUpdated;

    }

    public boolean updatePerf(String region, String agent, String plugin, String resource_id, String inode_id, Map<String,String> params)
    {
        boolean isUpdated = false;
        String edge_id = null;
        try
        {
            //make sure nodes exist
            String resource_node_id = getResourceNodeId(resource_id);
            String inode_node_id = getINodeId(resource_id,inode_id);
            String plugin_node_id = getNodeId(region,agent,plugin);

            //create node if not seen.. this needs to be changed.
            if(plugin_node_id == null) {
                plugin_node_id = addNode(region,agent,plugin);
                System.out.println("Added Node" + region + " " + agent + " " + plugin + " = " + plugin_node_id);
            }

            if((resource_node_id != null) && (inode_node_id != null) && (plugin_node_id != null))
            {
                //check if edge is found, if not create it
                edge_id = getResourceEdgeId(resource_id, inode_id, region, agent);
                if(edge_id == null)
                {
                    edge_id = addIsAttachedEdge(resource_id, inode_id, region, agent, plugin);
                }
                //check again if edge is found
                if(edge_id != null)
                {
                    //if(updateEdge(edge_id, params))
                    if(updateEdgeNoTx(edge_id, params))
                    {
                        isUpdated = true;
                    }
                    else
                    {
                        System.out.println("Controller : GraphDBEngine : Failed to updatePerf : Failed to update Edge params!");
                    }
                }
                else
                {
                    System.out.println("Controller : GraphDBEngine : Failed to updatePerf : edge_id not found!");
                }
            }
            else
            {
                System.out.println("Can't update missing nodes : " + resource_id + "," + inode_id + "," + plugin);
                System.out.println("Can't update missing nodes : " + resource_node_id + "," + inode_node_id + "," + plugin_node_id);
            }

        }
        catch(Exception ex)
        {
            System.out.println("Controller : GraphDBEngine : Failed to updatePerf");

        }
        return isUpdated;
    }


}
