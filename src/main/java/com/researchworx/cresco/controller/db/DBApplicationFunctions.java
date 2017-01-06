package com.researchworx.cresco.controller.db;

import app.gEdge;
import app.gNode;
import app.gPayload;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.utilities.CLogger;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class DBApplicationFunctions {

    private Launcher plugin;
    private CLogger logger;
    private OrientGraphFactory factory;
    private ODatabaseDocumentTx db;
    private int retryCount;
    private DBEngine dbe;
    private OrientGraph odb;
    private Cache<String, String> cookieCache;

    public DBApplicationFunctions(Launcher plugin, DBEngine dbe) {
        this.logger = new CLogger(DBApplicationFunctions.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
        this.plugin = plugin;
        this.factory = dbe.factory;
        this.db = dbe.db;
        this.retryCount = plugin.getConfig().getIntegerParam("db_retry_count",50);
        odb = factory.getTx();
        //create basic cresco constructs
        //initCrescoDB();

        cookieCache = CacheBuilder.newBuilder()
                .concurrencyLevel(4)
                .softValues()
                .maximumSize(10000)
                .expireAfterWrite(15, TimeUnit.MINUTES)
                .build();
    }


    public void populateDB()
    {
        try
        {
            //adding custom classes and constraints
            System.out.println("Create User Class");
            if(createClass("User"))
            {
                createKeyIndex("User","username");
            }
            System.out.println("Create Cookie Class");

            if(createClass("Cookie"))
            {
                createKeyIndex("Cookie","cookie");
            }
            System.out.println("Create Pipeline Class");

            if(createClass("Pipeline"))
            {
                createKeyIndex("Pipeline","pipelineid");
            }
            //implementation node
            System.out.println("Create iNode Class");

            if(createClass("iNode"))
            {
                createKeyIndex("iNode","node_id");
            }
            //virtual node
            System.out.println("Create vNode Class");

            if(createClass("vNode"))
            {
                createKeyIndex("vNode","node_id");
            }
            //edge node (in,out)
            System.out.println("Create eNode Class");

            if(createClass("eNode"))
            {
                createKeyIndex("eNode","node_id");
                //createKeyIndex("eNode","iNode_id");

            }
            //template node
            System.out.println("Create tNode Class");

            if(createClass("tNode"))
            {
                createKeyIndex("tNode","node_id");
                //createKeyIndex("eNode","iNode_id");

            }
            //make sure one user exist
            System.out.println("Create default user");

            createUser("cody@uky.edu","cody01");

            //add templates
            System.out.println("Create Default Templates");

            Map<String, String> params = new HashMap<String,String>();
            params.put("query_string", "");
            params.put("query_class", "netFlow");
            gNode node = new gNode("query", "ESPER Query", "9a11036f-35ba-419b-b03a-05abe35b1ab9", params);
            createTNode(node);
            params.clear();

            node = new gNode("membuffer", "Memory Output Buffer", "43c98b9f-cab2-47ad-a989-0c8dbb350cb6", params);
            params.put("data_url", "http://www.wuzzlemix.com/API?Woot");
            createTNode(node);
            params.clear();

            node = new gNode("amqp", "AMQP Exchange", "43c98b9f-cab2-47ad-a989-0c8dbb350cb7", params);
            params.put("outExchange", "");
            params.put("amqp_server", "");
            params.put("amqp_password", "");
            params.put("amqp_login", "");

            createTNode(node);
            params.clear();

        }
        catch(Exception ex)
        {
            System.out.println("populateDB Error: " + ex.toString());
        }

    }
    public boolean isAuthenicated(String username, String password)
    {
        //odb.getVertex(arg0)
		/*
		 OrientVertexType type = graph.createVertexType("Government");
		type.createProperty("itemid", OType.INTEGER);
		graph.createKeyIndex("itemid", Vertex.class, new Parameter<>("class", "Government"));
		 */
        try
        {
            Vertex exists = odb.getVertexByKey("User.username", username);
            String storedPass = exists.getProperty("password");
            if(storedPass.equals(password))
            {
                return true;
            }
        }
        catch(Exception ex)
        {
            System.out.println("isAuthenicated Error: " + ex.toString());
        }
        return false;
    }
    public boolean isSession(String cookie)
    {
        try
        {
            Vertex exists = odb.getVertexByKey("Cookie.cookie", cookie);
            if(exists != null)
            {
                String username = exists.getProperty("username");
                System.out.println("cookie exist:" + username);
                //update cache
                cookieCache.put(cookie, username);
                return true;
            }
        }
        catch(Exception ex)
        {
            System.out.println("isSession Error: " + ex.toString());
        }
        return false;
    }
    public boolean createCookie(String cookie, String username)
    {
        try
        {
            Vertex vCookie = odb.addVertex("class:Cookie");
            vCookie.setProperty("cookie", cookie);
            vCookie.setProperty("username", username);
            odb.commit();

            Vertex vUser = odb.getVertexByKey("User.username", username);
            Edge eLives = odb.addEdge(null, vCookie, vUser, "session");

            odb.commit();
            return true;
        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException se)
        {
            System.out.println("DUPE " + se.toString());
        }
        catch(Exception ex)
        {
            System.out.println(ex.toString());
        }
        return false;
    }

    public String getPipeline(String pipeline_id)
    {
        String json = null;
        Vertex vPipeline = odb.getVertexByKey("Pipeline.pipelineid", pipeline_id);
        if(vPipeline != null)
        {
            json = vPipeline.getProperty("submission");
        }
        return json;

    }
    public gPayload getPipelineObj(String pipeline_id)
    {

        gPayload gpay = null;
        Vertex vPipeline = odb.getVertexByKey("Pipeline.pipelineid", pipeline_id);
        if(vPipeline != null)
        {
            String json = vPipeline.getProperty("submission");
            gpay = gPayLoadFromJson(json);
        }
        return gpay;

    }

    public boolean setPipelineStatus(String pipeline_id, String status_code, String status_desc)
    {
        try
        {
            Vertex vPipeline = odb.getVertexByKey("Pipeline.pipelineid", pipeline_id);
            if(vPipeline != null)
            {
                vPipeline.setProperty("status_code", status_code);
                vPipeline.setProperty("status_desc", status_desc);
                odb.commit();
                return true;
            }
        }
        catch(Exception ex)
        {
            System.out.println("setPipelineStatus Error: " + ex.toString());
        }
        return false;

    }

    public boolean setPipelineStatus(gPayload gpay)
    {
        try
        {
            String pipeline_id = gpay.pipeline_id;
            String status_code = gpay.status_code;
            String status_desc = gpay.status_desc;
            String submission = JsonFromgPayLoad(gpay);
            Vertex vPipeline = odb.getVertexByKey("Pipeline.pipelineid", pipeline_id);
            if(vPipeline != null)
            {
                vPipeline.setProperty("status_code", status_code);
                vPipeline.setProperty("status_desc", status_desc);
                vPipeline.setProperty("submission", submission);

                odb.commit();
                return true;
            }
        }
        catch(Exception ex)
        {
            System.out.println("setPipelineStatus Error: " + ex.toString());
        }
        return false;

    }

    public List<gPayload> getPipelineList()
    {
        List<gPayload> pipelineList = new ArrayList<gPayload>();
        try
        {
            //String username = cookieCache.getIfPresent(cookie);
            //odb.getv
            Iterable<Vertex> pipelines = odb.getVerticesOfClass("Pipeline");
            Iterator<Vertex> iter = pipelines.iterator();
            while (iter.hasNext())
            {
                Vertex vPipeline = iter.next();
                String submission = vPipeline.getProperty("submission");
                gPayload gpay = gPayLoadFromJson(submission);
                pipelineList.add(gpay);
                //gPayLoadFromJson(String json)

				/*
				System.out.println("Found pipeline");
				//canidate node for match

				String pipelineid = vPipeline.getProperty("pipelineid");
				System.out.println("Found pipeline id: " + pipelineid);

				String pipeline_name = vPipeline.getProperty("name");
				String pipeline_status_code = vPipeline.getProperty("status_code");
				String pipeline_status_desc = vPipeline.getProperty("status_desc");

				gPayload gpay = new gPayload();
				gpay.pipeline_id = pipelineid;
				gpay.pipeline_name = pipeline_name;
				gpay.status_code = pipeline_status_code;
				gpay.status_desc = pipeline_status_desc;
				System.out.println("Pipeline list pre: " + pipelineList.size());

				pipelineList.add(gpay);
				System.out.println("Pipeline list post: " + pipelineList.size());
				*/
            }
        }
        catch(Exception ex)
        {
            System.out.println("getPipelineList Error: " + ex.toString());
        }
        System.out.println("Pipeline list return: " + pipelineList.size());

        return pipelineList;
    }

    public List<gPayload> getPipelineList(String cookie)
    {
        List<gPayload> pipelineList = new ArrayList<gPayload>();
        try
        {
            String username = cookieCache.getIfPresent(cookie);

            Vertex vUser = odb.getVertexByKey("User.username", username);
            Iterable<Edge> pipelineEdges = vUser.getEdges(Direction.IN, "ispipeline");
            Iterator<Edge> iter = pipelineEdges.iterator();
            while (iter.hasNext())
            {
                Edge isPipeLineEdge = iter.next();
                Vertex vPipeline = isPipeLineEdge.getVertex(Direction.OUT);

                System.out.println("Found pipeline");
                //canidate node for match

                String pipelineid = vPipeline.getProperty("pipelineid");
                System.out.println("Found pipeline id: " + pipelineid);

                String pipeline_name = vPipeline.getProperty("name");
                String pipeline_status_code = vPipeline.getProperty("status_code");
                String pipeline_status_desc = vPipeline.getProperty("status_desc");

                gPayload gpay = new gPayload();
                gpay.pipeline_id = pipelineid;
                gpay.pipeline_name = pipeline_name;
                gpay.status_code = pipeline_status_code;
                gpay.status_desc = pipeline_status_desc;
                System.out.println("Pipeline list pre: " + pipelineList.size());

                pipelineList.add(gpay);
                System.out.println("Pipeline list post: " + pipelineList.size());
            }
        }
        catch(Exception ex)
        {
            System.out.println("getPipelineList Error: " + ex.toString());
        }
        System.out.println("Pipeline list return: " + pipelineList.size());

        return pipelineList;
    }

    public Map<String,String> getMapFromString(String param, boolean isRestricted)
    {
        Map<String,String> paramMap = null;

        System.out.println("PARAM: " + param);
        //URLEncoder.encode(q, "UTF-8");

        try{
            String[] sparam = param.split(",");
            System.out.println("PARAM LENGTH: " + sparam.length);

            paramMap = new HashMap<String,String>();

            for(String str : sparam)
            {
                //URLEncoder.encode(str, "UTF-8");
                String[] sstr = str.split(":");

                //URLDecoder.decode(str, "UTF-8");
                //sstr.length


                if(isRestricted)
                {
                    paramMap.put(URLDecoder.decode(sstr[0], "UTF-8"), "");
                }
                else
                {
                    if(sstr.length > 1)
                    {
                        paramMap.put(URLDecoder.decode(sstr[0], "UTF-8"), URLDecoder.decode(sstr[1], "UTF-8"));
                    }
                    else
                    {
                        paramMap.put(URLDecoder.decode(sstr[0], "UTF-8"), "");
                    }
                }
            }
        }
        catch(Exception ex)
        {
            System.out.println("getMapFromString Error: " + ex.toString());
        }

        return paramMap;
    }

    public List<gNode> getNodeTypes(String cookie)
    {
        List<gNode> nodeTypeList = new ArrayList<gNode>();
        try
        {
            //String username = cookieCache.getIfPresent(cookie);
            Iterable<Vertex> gnodes = odb.getVertices("vNode.isSource", "true");

            Iterator<Vertex> iter = gnodes.iterator();
            while (iter.hasNext())
            {
                System.out.println("WOOT NODE isSource");
                //canidate node for match
                Vertex vNode = iter.next();
                Map<String,String> params = getMapFromString(vNode.getProperty("params").toString(), true);
                //Map<String,String> params = new HashMap<String,String>();
                String node_name = vNode.getProperty("node_name");
                String node_type = vNode.getProperty("node_type");
                String node_id = vNode.getProperty("node_id");
                Boolean isSource = Boolean.parseBoolean(vNode.getProperty("isSource").toString());
                gNode node = new gNode(node_type, node_name, node_id, params);
                node.isSource = isSource;
                nodeTypeList.add(node);
                //public gNode(String type, String node_name, String node_id, String top, String left, HashMap<String, String> params)

            }
            gnodes = odb.getVerticesOfClass("tNode");
            iter = gnodes.iterator();
            while (iter.hasNext())
            {
                System.out.println("WOOT NODE tNode");

                //canidate node for match
                Vertex tNode = iter.next();
                System.out.println("CODY0");

                Map<String,String> params = getMapFromString(tNode.getProperty("params").toString(), false);
                System.out.println("CODY1");

                String node_name = tNode.getProperty("node_name");
                String node_type = tNode.getProperty("node_type");
                String node_id = tNode.getProperty("node_id");
                gNode node = new gNode(node_type, node_name, node_id, params);
                nodeTypeList.add(node);
                //public gNode(String type, String node_name, String node_id, String top, String left, HashMap<String, String> params)
            }

        }
        catch(Exception ex)
        {
            System.out.println("getNodeType Error: " + ex.toString());
        }
        return nodeTypeList;
    }

    public gPayload createPipelineNodes(gPayload gpay)
    {

        try
        {
            //create nodes and links for pipeline records
            //List<gNode> nodes = gpay.nodes;
            //<node_name>,<node_id>
            HashMap<String,String> iNodeHm = new HashMap<String,String>();
            //<node_name>,<node_id>
            HashMap<String,String> vNodeHm = new HashMap<String,String>();

            List<gEdge> edges = gpay.edges;
            for(gNode node : gpay.nodes)
            {
                try
                {
                    String vNode_id = createVNode(gpay.pipeline_id, node,true);
                    vNodeHm.put(node.node_id, vNode_id);

                    System.out.println("Submitted Node:" + node.node_id);
                    System.out.println("vNode:" + vNode_id);

                    String iNode_id = createINode(vNode_id, node);
                    //node.node_id = iNode_id;
                    iNodeHm.put(node.node_id, iNode_id);

                    System.out.println("iNode:" + node.node_id);

                    //keep the vnodeID
                    node.node_id = vNode_id;

                    //test finding nodes
                    //getNode(node);
                }
                catch(Exception ex)
                {
                    System.out.println("createPipelineNodes Node: " + ex.toString());
                }
            }

            for(gEdge edge : gpay.edges)
            {
                try
                {
                    System.out.println(edge.edge_id + " " + edge.node_from + " " + edge.node_to);
                    //createGEdge(edge);
                    //vEdge
                    String edge_id = createVEdge(vNodeHm.get(edge.node_from),vNodeHm.get(edge.node_to));
                    edge.edge_id = edge_id;

                    createIEdge(iNodeHm.get(edge.node_from),iNodeHm.get(edge.node_to));
                    //assign vEdge ID
                    edge.node_from = vNodeHm.get(edge.node_from);
                    edge.node_to = vNodeHm.get(edge.node_to);

                    //iEdge
                }
                catch(Exception ex)
                {
                    System.out.println("createPipelineNodes Edge: " + ex.toString());
                }
            }
            //return gpay;
            gpay.status_code = "3";
            gpay.status_desc = "Pipeline Nodes Created.";
            setPipelineStatus(gpay);

            return gpay;
        }
        catch(Exception ex)
        {
            System.out.println("createPipelineNodes: " + ex.toString());
        }
        gpay.status_code = "1";
        gpay.status_desc = "Failed to create Pipeline.";
        setPipelineStatus(gpay);
        return gpay;
    }

    public gPayload createPipelineRecord(String cookie, String gPayload, gPayload gpay)
    {
        logger.error("createPipelineRecord : Launcher.gPayloadQueue.offer(gpay) COMMENTED OUT");

        try
        {
            String username = cookieCache.getIfPresent(cookie);
            System.out.println("Creating vPipeline");

            Vertex vPipeline = odb.addVertex("class:Pipeline");
            vPipeline.setProperty("pipelineid", gpay.pipeline_id);
            vPipeline.setProperty("name", gpay.pipeline_name);
            //vPipeline.setProperty("username", username);
            vPipeline.setProperty("submission", gPayload);
            vPipeline.setProperty("status_code", "3");
            vPipeline.setProperty("status_desc", "Record added to DB.");

            odb.commit();
            System.out.println("Post vPipeline commit");

            Vertex vUser = odb.getVertexByKey("User.username", username);
            System.out.println("Select vUser");

            Edge eLives = odb.addEdge(null, vPipeline, vUser, "ispipeline");
            System.out.println("Add Edge");
            odb.commit();

            System.out.println("Offer Pipeline to Scheduler Queue");
            //Launcher.gPayloadQueue.offer(gpay);
            return gpay;
        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException se)
        {
            System.out.println("DUPE " + se.toString());
        }
        catch(Exception ex)
        {
            System.out.println(ex.toString());
        }
        return null;
    }

    boolean createUser(String username, String password)
    {
        try
        {
            Vertex vUser = odb.addVertex("class:User");
            vUser.setProperty("username", username);
            vUser.setProperty("password", password);
            odb.commit();
            return true;
        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException se)
        {
            System.out.println("DUPE " + se.toString());
        }
        catch(Exception ex)
        {
            System.out.println(ex.toString());
        }
        return false;
    }

    public String getINodefromVNode(String vNode_id)
    {
        String iNode_id = null;
        try
        {
            Vertex vNode = odb.getVertexByKey("vNode.node_id", vNode_id);
            if(vNode != null)
            {
                //json = vPipeline.getProperty("submission");
                Iterable<Edge> vNodeEdges = vNode.getEdges(Direction.OUT, "isINode");
                Iterator<Edge> iter = vNodeEdges.iterator();

                Edge isINode = iter.next();
                Vertex iNode = isINode.getVertex(Direction.IN);
                return iNode.getProperty("node_id");
            }
        }
        catch(Exception ex)
        {
            System.out.println("getINodefromVNode: Exception ex:" + ex.toString());
        }
        return iNode_id;
    }

    public boolean iNodeIsActive(String iNode_id)
    {
        try
        {
            Vertex iNode = odb.getVertexByKey("iNode.node_id", iNode_id);
            if(iNode != null)
            {
                if(iNode.getProperty("node_id").toString().equals("4"))
                {
                    return true;
                }
            }
        }
        catch(Exception ex)
        {
            System.out.println("iNodeIsActive: Exception ex:" + ex.toString());
        }
        return false;
    }
    public boolean setINodeStatus(String iNode_id, String status_code, String status_desc)
    {
        try
        {
            Vertex iNode = odb.getVertexByKey("iNode.node_id", iNode_id);
            if(iNode == null)
            {
                System.out.println("setINodeStatus Error: Node not found!");
                return false;
            }
            else
            {
                iNode.setProperty("status_code", status_code);
                iNode.setProperty("status_desc", status_desc);
                odb.commit();
                System.out.println("setINodeStatus iNode_id:" + iNode_id + " status_code:" + status_code + " status_desc:" + status_desc);
                return true;
            }

        }
        catch(Exception ex)
        {
            System.out.println("setINodeStatus: Exception ex:" + ex.toString());
        }
        return false;
    }

    public boolean addINodeParams(String iNode_id, Map<String,String> newparams)
    {
        try
        {
            Vertex iNode = odb.getVertexByKey("iNode.node_id", iNode_id);
            Map<String,String> params = getMapFromString(iNode.getProperty("params").toString(), false);

            params.putAll(newparams);

            iNode.setProperty("params", encodeParams(params));
            odb.commit();
            return true;
        }
        catch(Exception ex)
        {
            System.out.println("addINodeParams: Exception ex:" + ex.toString());
        }
        return false;
    }
    public String encodeParams(Map<String,String> params)
    {
        String params_string = null;
        try{
            StringBuilder sb = new StringBuilder();

            for (Map.Entry<String, String> entry : params.entrySet())
            {
                String sKey = URLEncoder.encode(entry.getKey(), "UTF-8");
                String sValue = null;
                if(entry.getValue() != null)
                {
                    sValue = URLEncoder.encode(entry.getValue(), "UTF-8");
                }


                //String sKey = entry.getKey() + ":" + entry.getValue();
                //str = URLEncoder.encode(str, "UTF-8");

                //System.out.println(entry.getKey() + "/" + entry.getValue());
                sb.append(sKey + ":" + sValue + ",");
            }
            params_string = sb.substring(0, sb.length() -1);

        }
        catch(Exception ex)
        {
            System.out.println("encodeParams: Exception ex:" + ex.toString());
        }
        return params_string;
    }
    public Map<String,String> getNodeManifest(String iNode_id)
    {
        Map<String,String> manifest = null;
        try
        {
            Vertex iNode = odb.getVertexByKey("iNode.node_id", iNode_id);
            if(iNode != null)
            {
                manifest = getMapFromString(iNode.getProperty("params").toString(), false);
                //set type in manifest
                manifest.put("node_id", iNode.getProperty("node_id").toString());
                manifest.put("node_name", iNode.getProperty("node_name").toString());
                manifest.put("node_type", iNode.getProperty("node_type").toString());
                manifest.put("status_code", iNode.getProperty("status_code").toString());
                manifest.put("status_desc", iNode.getProperty("status_desc").toString());

                //add type-specific information This should be done a better way
                if((manifest.get("node_type").equals("membuffer")) || (manifest.get("node_type").equals("query")))
                {
                    //String nextNodeId = getUpstreamNode(iNode.getProperty("node_id").toString());

                    //WALK BACK AND FIND AMQP
                    String upstreamINode_id = getUpstreamNode(iNode_id);
                    if(upstreamINode_id == null)
                    {
                        System.out.println("getNodeManifest: Error: null nextNodeID before AMQP Node");
                        return null;
                    }
                    else
                    {
                        Vertex uINode = odb.getVertexByKey("iNode.node_id", upstreamINode_id);
                        if(uINode == null)
                        {
                            System.out.println("getNodeManifest: Error: null uINode Node");
                            return null;
                        }
                        else
                        {
                            if(uINode.getProperty("status_code") != null)
                            {
                                if(uINode.getProperty("status_code").toString().equals("4"))
                                {
                                    System.out.println("Found Active Upstream iNode:" + upstreamINode_id);
                                    Map<String,String> params = getMapFromString(uINode.getProperty("params").toString(), false);
                                    manifest.put("outExchange", params.get("outExchange"));
                                    manifest.put("amqp_server", params.get("amqp_server"));
                                    manifest.put("amqp_login", params.get("amqp_login"));
                                    manifest.put("amqp_password", params.get("amqp_password"));
                                    return manifest;
                                }
                            }
                            else
                            {
                                System.out.println("Upstream iNode is InActive: " + upstreamINode_id);
                                return null;
                            }

                        }
                    }
                }

            }
        }
        catch(Exception ex)
        {
            System.out.println("getNodeManifest: Exception ex:" + ex.toString());
        }
        return manifest;
    }


    public String getUpstreamNode(String iNode_id)
    {
        String uINode = null;
        try
        {
            Vertex iNode = odb.getVertexByKey("iNode.node_id", iNode_id);
            if(iNode != null)
            {
                //Get Enode for this iNode
                Iterable<Edge> iNodeEdges = iNode.getEdges(Direction.IN, "in");
                Iterator<Edge> iterE = iNodeEdges.iterator();
                Edge isIn = iterE.next();
                Vertex eNode = isIn.getVertex(Direction.OUT);
                System.out.println("ENode: " + eNode.getId().toString());

                //Get Enode for the upstream iNode
                Iterable<Edge> eNodeEdges = eNode.getEdges(Direction.IN, "isEConnected");
                Iterator<Edge> iterE2 = eNodeEdges.iterator();
                Edge isEConnected = iterE2.next();
                Vertex eNode2 = isEConnected.getVertex(Direction.OUT);
                System.out.println("ENode2: " + eNode2.getId().toString());

                //Get Upstream iNode
                Iterable<Edge> iNodeEdges2 = eNode2.getEdges(Direction.IN, "out");
                Iterator<Edge> iter2 = iNodeEdges2.iterator();
                Edge isOut = iter2.next();
                Vertex iNode2 = isOut.getVertex(Direction.OUT);
                uINode = iNode2.getProperty("node_id");
            }
            else
            {
                System.out.println("getUpstreamNode: Error: Incoming iNode: " + iNode_id + " is null");
            }
        }
        catch(Exception ex)
        {
            System.out.println("getUpstreamNode: Exception ex:" + ex.toString());
        }
        return uINode;
    }

    boolean getNode(gNode node)
    {
        try
        {
            //Vertex node = odb.getVertex(arg0)
            //Iterable resultset = getVertices("Person",new String[] {"name","surname"},new Object[] { "Sherlock" ,"Holmes"});
            Iterable<Vertex> resultset = odb.getVertices("iNode",node.getIdentKey(),node.getIdentValue());
            Iterator<Vertex> iter = resultset.iterator();
            //Iterable<Vertex> resultset = odb.getVertices("node_name", node.node_name);
            while (iter.hasNext())
            {
                //canidate node for match
                Vertex iNode_to = iter.next();
                //get the In edge to check and see if anything is connected
                Iterable<Edge> toEdges = iNode_to.getEdges(Direction.IN, "In");
                Edge eEdge_to = toEdges.iterator().next();
                //here is the eNode for the incoming connections
                Vertex eNode_to = eEdge_to.getVertex(Direction.OUT);
                Iterable<Edge> incomingEdges = iNode_to.getEdges(Direction.IN, "In");

                //System.out.println("results:" + iNode.getProperty("node_name"));
            }
        }
        catch(Exception ex)
        {
            System.out.println("CODY: getNode Exception ex:" + node.node_name + " " + ex.toString());
        }
        return true;
    }
    Boolean createTNode(gNode node)
    {
        try
        {

            String params = encodeParams(node.params);



            Vertex tNode = odb.addVertex("class:tNode");
            tNode.setProperty("node_name", node.node_name);
            tNode.setProperty("node_type", node.type);
            tNode.setProperty("node_id", node.node_id);

            tNode.setProperty("params", params);

            odb.commit();

            return true;
        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException se)
        {
            System.out.println("DUPE " + se.toString());
        }
        catch(Exception ex)
        {
            System.out.println("createTNode Error: " + ex.toString());
        }
        return false;
    }

    String createVNode(String pipelineid, gNode node, boolean isNew)
    {
        try
        {

            String node_id = null;

            String params = encodeParams(node.params);


            Vertex vNode = odb.addVertex("class:vNode");
            vNode.setProperty("node_name", node.node_name);
            vNode.setProperty("node_type", node.type);
            if(isNew)
            {
                node_id = UUID.randomUUID().toString();
            }
            else
            {
                node_id = node.node_id;
            }
            vNode.setProperty("node_id", node_id);

            vNode.setProperty("top", node.top);
            vNode.setProperty("left", node.left);
            vNode.setProperty("params", params);
            vNode.setProperty("isSource", String.valueOf(node.isSource));
            odb.commit();

            Vertex vPipeline = odb.getVertexByKey("Pipeline.pipelineid", pipelineid);
            Edge ePipeline = odb.addEdge(null, vPipeline, vNode, "isVNode");
            odb.commit();

            return node_id;
        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException se)
        {
            System.out.println("DUPE " + se.toString());
        }
        catch(Exception ex)
        {
            System.out.println("createVNode Error: " + ex.toString());
        }
        return null;
    }
    String createINode(String vNode_id, gNode node)
    {
        try
        {

            String node_id = UUID.randomUUID().toString();

            String params = encodeParams(node.params);

            Vertex iNode = odb.addVertex("class:iNode");
            iNode.setProperty("node_name", node.node_name);
            iNode.setProperty("node_type", node.type);
            iNode.setProperty("node_id", node_id);
            iNode.setProperty("params", params);
            System.out.println("iNode Params: " + params);
            iNode.setProperty("status_code", "0");
            iNode.setProperty("status_desc", "Added to DB");


            odb.commit();
            //Link to vNode
            Vertex vNode = odb.getVertexByKey("vNode.node_id", vNode_id);

            Edge eNodeEdge = odb.addEdge(null, vNode, iNode, "isINode");

            //Create iNode I/O Nodes
            String eNode_id = createENode(node_id, true);

            System.out.println("Added new eNode in: " + eNode_id);
            eNode_id = createENode(node_id, false);
            System.out.println("Added new eNode out: " + eNode_id);

            odb.commit();
            return node_id;
        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException se)
        {
            System.out.println("createINode DUPE " + se.toString());
        }
        catch(Exception ex)
        {
            System.out.println("createINode: " + ex.toString());
        }
        return null;
    }
    String createENode(String iNode_id, boolean isIn)
    {
        try
        {


            String node_id = UUID.randomUUID().toString();

            Vertex eNode = odb.addVertex("class:eNode");
            eNode.setProperty("node_id", node_id);
            eNode.setProperty("iNode_id", iNode_id);
            odb.commit();

            Vertex iNode = odb.getVertexByKey("iNode.node_id", iNode_id);
            if(isIn)
            {
                Edge ePipeline = odb.addEdge(null, eNode, iNode, "In");
            }
            else
            {
                Edge ePipeline = odb.addEdge(null, iNode, eNode, "out");
            }
            odb.commit();

            return node_id;
        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException se)
        {
            System.out.println("createENode DUPE " + se.toString());
        }
        catch(Exception ex)
        {
            System.out.println("createENode: " + ex.toString());
        }
        return null;
    }
    Boolean createIEdge(String node_from, String node_to)
    {
        try
        {
            System.out.println("createIedge from_node: " + node_from + " node_to: " + node_to);
            System.out.println("-I0 ");
            Vertex iNode_from = odb.getVertexByKey("iNode.node_id", node_from);
            System.out.println("-I1 ");

            Vertex iNode_to = odb.getVertexByKey("iNode.node_id", node_to);
            System.out.println("-I2 ");

            if((iNode_from != null) && (iNode_to != null))
            {
                System.out.println("iEdge: node_from:" + node_from + " node_to:" + node_to);
            }
            System.out.println("-I3 ");

            Iterable<Edge> fromEdges = iNode_from.getEdges(Direction.OUT, "Out");
            Edge eEdge_from = fromEdges.iterator().next();
            Vertex eNode_from = eEdge_from.getVertex(Direction.IN);
            if(eNode_from != null)
            {
                System.out.println("From eNode_id:" +  eNode_from.getProperty("node_id") + " iNode_id:" + eNode_from.getProperty("iNode_id"));
            }
            System.out.println("-I4 ");

            Iterable<Edge> toEdges = iNode_to.getEdges(Direction.IN, "In");
            Edge eEdge_to = toEdges.iterator().next();
            Vertex eNode_to = eEdge_to.getVertex(Direction.OUT);
            System.out.println("-I5 ");

            if(eNode_to != null)
            {
                System.out.println("To eNode_id:" +  eNode_to.getProperty("node_id") + " iNode_id:" + eNode_to.getProperty("iNode_id"));
            }
            System.out.println("-I6 ");

            Edge iEdge = odb.addEdge(null, eNode_from, eNode_to, "isEConnected");
			/*
			 public Iterable<Edge> getEdges(OrientVertex iDestination,
                               Direction iDirection,
                               String... iLabels)
			 */
            System.out.println("-I7 ");

            //Edge vEdge = odb.addEdge(null, eNode_from, eNode_to, "isIConnected");
            //Edge vEdge = eNode_from.getEdges(arg0, arg1)
            odb.commit();
            return true;
        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException se)
        {
            System.out.println("createIEdge DUPE " + se.toString());
        }
        catch(Exception ex)
        {
            System.out.println("createIEdge: " + ex.toString());
        }
        return false;
    }
    String createVEdge(String node_from, String node_to)
    {
        String edge_id = null;
        try
        {

            System.out.println("node_from: " + node_from + " node_to:" + node_to);

            System.out.println("-0");
            Vertex vNode_from = odb.getVertexByKey("vNode.node_id", node_from);
            System.out.println("-1");
            Vertex vNode_to = odb.getVertexByKey("vNode.node_id", node_to);
            System.out.println("-2");
            Edge vEdge = odb.addEdge(null, vNode_from, vNode_to, "isVConnected");
            System.out.println("-3");
            edge_id = UUID.randomUUID().toString();
            System.out.println("-4");
            vEdge.setProperty("edge_id", edge_id);
            System.out.println("-5");
            odb.commit();
            System.out.println("-6");
        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException se)
        {
            System.out.println("createVEdge DUPE " + se.toString());
        }
        catch(Exception ex)
        {
            System.out.println("createVedge: " + ex.toString());
        }
        return edge_id;
    }
    boolean createClass(String className)
    {
        boolean wasCreated = false;
        OrientGraphNoTx txGraph = factory.getNoTx();
        //OSchema schema = ((OrientGraph)odb).getRawGraph().getMetadata().getSchema();
        OSchema schema = ((OrientGraphNoTx)txGraph).getRawGraph().getMetadata().getSchema();

        if (!schema.existsClass(className))
        {
            txGraph.createVertexType(className);
            //txGraph.createKeyIndex(key, Vertex.class, new Parameter("type", "UNIQUE"), new Parameter("class", className));
            wasCreated = true;
        }
        txGraph.commit();
        txGraph.shutdown();
        return wasCreated;
    }
    void createKeyIndex(String className, String key)
    {
        OrientGraphNoTx txGraph = factory.getNoTx();
        //OSchema schema = ((OrientGraph)odb).getRawGraph().getMetadata().getSchema();
        OSchema schema = ((OrientGraphNoTx)txGraph).getRawGraph().getMetadata().getSchema();

        if (schema.existsClass(className))
        {
            txGraph.createKeyIndex(key, Vertex.class, new Parameter("type", "UNIQUE"), new Parameter("class", className));
        }
        txGraph.commit();
        txGraph.shutdown();
    }
    //firstName
    private String JsonFromgPayLoad(gPayload gpay)
    {
        Gson gson = new GsonBuilder().create();
        //gPayload me = gson.fromJson(json, gPayload.class);
        //System.out.println(p);
        return gson.toJson(gpay);

    }
    private gPayload gPayLoadFromJson(String json)
    {
        Gson gson = new GsonBuilder().create();
        gPayload me = gson.fromJson(json, gPayload.class);
        //System.out.println(p);
        return me;
    }


    //Base INIT Functions
    public void initCrescoDB()
    {
        try
        {
            //index properties

            logger.debug("Create Region Vertex Class");
            String[] rProps = {"zregion"}; //Property names
            createVertexClass("zNode", rProps);


        }
        catch(Exception ex)
        {
            logger.debug("initCrescoDB Error: " + ex.toString());
        }
    }

    //Class Create Functions
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
            logger.debug("DBEngine : createVertexIndex : Error " + ex.toString());
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
            txGraph.commit();
            if (schema.existsClass(className)) {
                wasCreated = true;
            }
        }
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



}
