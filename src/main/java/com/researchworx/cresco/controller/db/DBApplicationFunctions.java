package com.researchworx.cresco.controller.db;

import app.gEdge;
import app.gNode;
import app.gPayload;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.utilities.CLogger;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.*;


public class DBApplicationFunctions {

    private Launcher plugin;
    private CLogger logger;
    private OrientGraphFactory factory;
    private ODatabaseDocumentTx db;
    private int retryCount;
    private DBEngine dbe;
    private OrientGraph odb;

    public DBApplicationFunctions(Launcher plugin, DBEngine dbe) {
        this.logger = new CLogger(DBApplicationFunctions.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Debug);
        this.plugin = plugin;
        this.factory = dbe.factory;
        this.db = dbe.db;
        this.retryCount = plugin.getConfig().getIntegerParam("db_retry_count",50);
        odb = factory.getTx();

        //create basic application constructs
        initCrescoDB();
    }

    public String getTenantNodeId(String tenantId) {
        String node_id = null;
        OrientGraph graph = null;

        try
        {

            //OrientGraphNoTx graph = factory.getNoTx();
            graph = factory.getTx();

            Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:Tenant.tenant_id WHERE key = '" + tenantId + "'")).execute();

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
            logger.error("getTenantNodeID : Error " + ex.toString());
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

    public String getPipelineNodeId(String pipelineId) {
        String node_id = null;
        OrientGraph graph = null;

        try
        {

            //OrientGraphNoTx graph = factory.getNoTx();
            graph = factory.getTx();

            Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:Pipeline.pipeline_id WHERE key = '" + pipelineId + "'")).execute();

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
            logger.error("getPipelineNodeID : Error " + ex.toString());
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

    public String getVNodeNodeId(String vNodeId) {
        String node_id = null;
        OrientGraph graph = null;

        try
        {

            //OrientGraphNoTx graph = factory.getNoTx();
            graph = factory.getTx();

            Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:vNode.vnode_id WHERE key = '" + vNodeId + "'")).execute();

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
            logger.error("getVNodeID : Error " + ex.toString());
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

    public String getINodeNodeId(String iNodeId) {
        String node_id = null;
        OrientGraph graph = null;

        try
        {

            //OrientGraphNoTx graph = factory.getNoTx();
            graph = factory.getTx();

            Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:iNode.inode_id WHERE key = '" + iNodeId + "'")).execute();

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
            logger.error("getINodeID : Error " + ex.toString());
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

    public String getENodeNodeId(String eNodeId) {
        String node_id = null;
        OrientGraph graph = null;

        try
        {

            //OrientGraphNoTx graph = factory.getNoTx();
            graph = factory.getTx();

            Iterable<Vertex> resultIterator = graph.command(new OCommandSQL("SELECT rid FROM INDEX:eNode.enode_id WHERE key = '" + eNodeId + "'")).execute();

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
            logger.error("getENodeID : Error " + ex.toString());
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

    public String getPipeline(String pipelineId) {
        String json = null;

        String pipelineNodeId = getPipelineNodeId(pipelineId);
        if(pipelineNodeId != null) {
            //if(getPipelineNodeId(pipelineId != null)
            Vertex vPipeline = odb.getVertex(getPipelineNodeId(pipelineId));
            if (vPipeline != null) {
                json = vPipeline.getProperty("submission");
            }
        }
        return json;
    }

    Boolean createIEdge(String node_from, String node_to) {
        try
        {
            logger.debug("createIedge from_node: " + node_from + " node_to: " + node_to);
            logger.debug("-I0 ");
            //Vertex iNode_from = odb.getVertexByKey("iNode.node_id", node_from);
            Vertex iNode_from = odb.getVertex(getINodeNodeId(node_from));
            logger.debug("-I1 ");

            //Vertex iNode_to = odb.getVertexByKey("iNode.node_id", node_to);
            Vertex iNode_to = odb.getVertex(getINodeNodeId(node_to));

            logger.debug("-I2 ");

            if((iNode_from != null) && (iNode_to != null))
            {
                logger.debug("iEdge: node_from:" + node_from + " node_to:" + node_to);
            }
            logger.debug("-I3 ");

            Iterable<Edge> fromEdges = iNode_from.getEdges(Direction.OUT, "Out");
            Edge eEdge_from = fromEdges.iterator().next();
            Vertex eNode_from = eEdge_from.getVertex(Direction.IN);
            if(eNode_from != null)
            {
                logger.debug("From eNode_id:" +  eNode_from.getProperty("node_id") + " iNode_id:" + eNode_from.getProperty("inode_id"));
            }
            logger.debug("-I4 ");

            Iterable<Edge> toEdges = iNode_to.getEdges(Direction.IN, "in");
            Edge eEdge_to = toEdges.iterator().next();
            Vertex eNode_to = eEdge_to.getVertex(Direction.OUT);
            logger.debug("-I5 ");

            if(eNode_to != null)
            {
                logger.debug("To eNode_id:" +  eNode_to.getProperty("node_id") + " iNode_id:" + eNode_to.getProperty("inode_id"));
            }
            logger.debug("-I6 ");

            Edge iEdge = odb.addEdge(null, eNode_from, eNode_to, "isEConnected");
			/*
			 public Iterable<Edge> getEdges(OrientVertex iDestination,
                               Direction iDirection,
                               String... iLabels)
			 */
            logger.debug("-I7 ");

            //Edge vEdge = odb.addEdge(null, eNode_from, eNode_to, "isIConnected");
            //Edge vEdge = eNode_from.getEdges(arg0, arg1)
            odb.commit();
            return true;
        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException se)
        {
            logger.debug("createIEdge DUPE " + se.toString());
        }
        catch(Exception ex)
        {
            logger.debug("createIEdge: " + ex.toString());
        }
        return false;
    }

    String createVEdge(String node_from, String node_to) {
        String edge_id = null;
        try
        {

            logger.debug("node_from: " + node_from + " node_to:" + node_to);

            logger.debug("-0");
            //Vertex vNode_from = odb.getVertexByKey("vNode.node_id", node_from);
            Vertex vNode_from = odb.getVertex(getVNodeNodeId(node_from));
            logger.debug("-1");
            //Vertex vNode_to = odb.getVertexByKey("vNode.node_id", node_to);
            Vertex vNode_to = odb.getVertex(getVNodeNodeId(node_to));
            logger.debug("-2");
            Edge vEdge = odb.addEdge(null, vNode_from, vNode_to, "isVConnected");
            logger.debug("-3");
            edge_id = UUID.randomUUID().toString();
            logger.debug("-4");
            vEdge.setProperty("edge_id", edge_id);
            logger.debug("-5");
            odb.commit();
            logger.debug("-6");
        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException se)
        {
            logger.debug("createVEdge DUPE " + se.toString());
        }
        catch(Exception ex)
        {
            logger.debug("createVedge: " + ex.toString());
        }
        return edge_id;
    }

    public boolean setPipelineStatus(gPayload gpay) {
        try
        {
            String pipelineId = gpay.pipeline_id;
            String status_code = gpay.status_code;
            String status_desc = gpay.status_desc;
            String submission = JsonFromgPayLoad(gpay);
            //Vertex vPipeline = odb.getVertexByKey("Pipeline.pipelineid", pipeline_id);
            Vertex vPipeline = odb.getVertex(getPipelineNodeId(pipelineId));
            
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
            logger.debug("setPipelineStatus Error: " + ex.toString());
        }
        return false;

    }

    public boolean setPipelineStatus(String pipelineId, String status_code, String status_desc) {
        try
        {
            //Vertex vPipeline = odb.getVertexByKey("Pipeline.pipelineid", pipeline_id);
            Vertex vPipeline = odb.getVertex(getPipelineNodeId(pipelineId));

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
            logger.debug("setPipelineStatus Error: " + ex.toString());
        }
        return false;

    }

    public gPayload getPipelineObj(String pipelineId) {

        gPayload gpay = null;
        //Vertex vPipeline = odb.getVertexByKey("Pipeline.pipelineid", pipeline_id);
        Vertex vPipeline = odb.getVertex(getPipelineNodeId(pipelineId));
        if(vPipeline != null)
        {
            String json = vPipeline.getProperty("submission");
            gpay = gPayLoadFromJson(json);
        }
        return gpay;

    }

    public String getINodefromVNode(String vNode_id) {
        String iNode_id = null;
        try
        {
            //Vertex vNode = odb.getVertexByKey("vNode.node_id", vNode_id);
            Vertex vNode = odb.getVertex(getVNodeNodeId(vNode_id));
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
            logger.error("getINodefromVNode: Exception ex:" + ex.toString());
        }
        return iNode_id;
    }

    public boolean setINodeStatus(String iNode_id, String status_code, String status_desc) {
        try
        {
            //Vertex iNode = odb.getVertexByKey("iNode.node_id", iNode_id);
            Vertex iNode = odb.getVertex(getINodeNodeId(iNode_id));
            if(iNode == null)
            {
                logger.debug("setINodeStatus Error: Node not found!");
                return false;
            }
            else
            {
                iNode.setProperty("status_code", status_code);
                iNode.setProperty("status_desc", status_desc);
                odb.commit();
                logger.debug("setINodeStatus iNode_id:" + iNode_id + " status_code:" + status_code + " status_desc:" + status_desc);
                return true;
            }

        }
        catch(Exception ex)
        {
            logger.error("setINodeStatus: Exception ex:" + ex.toString());
        }
        return false;
    }

    public List<gPayload> getPipelineList() {
        List<gPayload> pipelineList = new ArrayList<gPayload>();
        try
        {
            Iterable<Vertex> pipelines = odb.getVerticesOfClass("Pipeline");
            Iterator<Vertex> iter = pipelines.iterator();
            while (iter.hasNext())
            {
                Vertex vPipeline = iter.next();
                String submission = vPipeline.getProperty("submission");
                gPayload gpay = gPayLoadFromJson(submission);
                pipelineList.add(gpay);

            }
        }
        catch(Exception ex)
        {
            logger.debug("getPipelineList Error: " + ex.toString());
        }
        logger.debug("Pipeline list return: " + pipelineList.size());

        return pipelineList;
    }

    public boolean iNodeIsActive(String iNode_id) {
        try
        {
            //Vertex iNode = odb.getVertexByKey("iNode.node_id", iNode_id);
            Vertex iNode = odb.getVertex(getINodeNodeId(iNode_id));

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
            logger.debug("iNodeIsActive: Exception ex:" + ex.toString());
        }
        return false;
    }

    public Map<String,String> getMapFromString(String param, boolean isRestricted) {
        Map<String,String> paramMap = null;

        logger.debug("PARAM: " + param);

        try{
            String[] sparam = param.split(",");
            logger.debug("PARAM LENGTH: " + sparam.length);

            paramMap = new HashMap<String,String>();

            for(String str : sparam)
            {
                String[] sstr = str.split(":");

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
            logger.error("getMapFromString Error: " + ex.toString());
        }

        return paramMap;
    }

    public String getUpstreamNode(String iNode_id) {
        String uINode = null;
        try
        {
            //Vertex iNode = odb.getVertexByKey("iNode.node_id", iNode_id);
            Vertex iNode = odb.getVertex(getINodeNodeId(iNode_id));
            if(iNode != null)
            {
                //Get Enode for this iNode
                Iterable<Edge> iNodeEdges = iNode.getEdges(Direction.IN, "in");
                Iterator<Edge> iterE = iNodeEdges.iterator();
                Edge isIn = iterE.next();
                Vertex eNode = isIn.getVertex(Direction.OUT);
                logger.debug("ENode: " + eNode.getId().toString());

                //Get Enode for the upstream iNode
                Iterable<Edge> eNodeEdges = eNode.getEdges(Direction.IN, "isEConnected");
                Iterator<Edge> iterE2 = eNodeEdges.iterator();
                Edge isEConnected = iterE2.next();
                Vertex eNode2 = isEConnected.getVertex(Direction.OUT);
                logger.debug("ENode2: " + eNode2.getId().toString());

                //Get Upstream iNode
                Iterable<Edge> iNodeEdges2 = eNode2.getEdges(Direction.IN, "out");
                Iterator<Edge> iter2 = iNodeEdges2.iterator();
                Edge isOut = iter2.next();
                Vertex iNode2 = isOut.getVertex(Direction.OUT);
                uINode = iNode2.getProperty("node_id");
            }
            else
            {
                logger.error("getUpstreamNode: Error: Incoming iNode: " + iNode_id + " is null");
            }
        }
        catch(Exception ex)
        {
            logger.error("getUpstreamNode: Exception ex:" + ex.toString());
        }
        return uINode;
    }

    public Map<String,String> getNodeManifest(String iNode_id) {
        Map<String,String> manifest = null;
        try
        {
            //Vertex iNode = odb.getVertexByKey("iNode.node_id", iNode_id);
            Vertex iNode = odb.getVertex(getINodeNodeId(iNode_id));
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
                        logger.error("getNodeManifest: Error: null nextNodeID before AMQP Node");
                        return null;
                    }
                    else
                    {
                        //Vertex uINode = odb.getVertexByKey("iNode.node_id", upstreamINode_id);
                        Vertex uINode = odb.getVertex(getINodeNodeId(upstreamINode_id));
                        if(uINode == null)
                        {
                            logger.error("getNodeManifest: Error: null uINode Node");
                            return null;
                        }
                        else
                        {
                            if(uINode.getProperty("status_code") != null)
                            {
                                if(uINode.getProperty("status_code").toString().equals("4"))
                                {
                                    logger.debug("Found Active Upstream iNode:" + upstreamINode_id);
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
                                logger.debug("Upstream iNode is InActive: " + upstreamINode_id);
                                return null;
                            }

                        }
                    }
                }

            }
        }
        catch(Exception ex)
        {
            logger.debug("getNodeManifest: Exception ex:" + ex.toString());
        }
        return manifest;
    }

    public gPayload createPipelineNodes(gPayload gpay) {

        try
        {
            //create nodes and links for pipeline records
            HashMap<String,String> iNodeHm = new HashMap<>();
            HashMap<String,String> vNodeHm = new HashMap<>();

            List<gEdge> edges = gpay.edges;
            for(gNode node : gpay.nodes)
            {
                try
                {
                    String vNode_id = createVNode(gpay.pipeline_id, node,true);
                    vNodeHm.put(node.node_id, vNode_id);

                    logger.debug("Submitted Node:" + node.node_id);
                    logger.debug("vNode:" + vNode_id);


                    String iNode_id = createINode(vNode_id, node);
                    //node.node_id = iNode_id;
                    iNodeHm.put(node.node_id, iNode_id);

                    logger.debug("iNode:" + node.node_id);

                    //keep the vnodeID
                    node.node_id = vNode_id;

                    //test finding nodes
                    //getNode(node);
                }
                catch(Exception ex)
                {
                    logger.debug("createPipelineNodes Node: " + ex.toString());
                }
            }

            for(gEdge edge : gpay.edges)
            {
                try
                {
                    logger.debug(edge.edge_id + " " + edge.node_from + " " + edge.node_to);
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
                    logger.debug("createPipelineNodes Edge: " + ex.toString());
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
            logger.debug("createPipelineNodes: " + ex.toString());
        }
        gpay.status_code = "1";
        gpay.status_desc = "Failed to create Pipeline.";
        setPipelineStatus(gpay);
        return gpay;
    }

    String createVNode(String pipelineId, gNode node, boolean isNew) {
        try
        {

            String node_id = null;

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
            vNode.setProperty("vnode_id", node_id);

            if(node.params.size() > 0) {
                vNode.setProperty("params",encodeParams(node.params));
            }

            vNode.setProperty("isSource", String.valueOf(node.isSource));
            odb.commit();

            logger.debug("Created vNode " + node_id + " Node ID " + vNode.getId().toString() + " getvNode = " + getVNodeNodeId(node_id));

            Vertex vPipeline = odb.getVertex(getPipelineNodeId(pipelineId));
            Edge ePipeline = odb.addEdge(null, vPipeline, vNode, "isVNode");
            odb.commit();

            return node_id;
        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException se)
        {
            logger.error("DUPE " + se.toString());
        }
        catch(Exception ex)
        {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());

            logger.error("createVNode Error: " + ex.toString());
        }
        return null;
    }

    String createINode(String vNode_id, gNode node) {
        try
        {

            String node_id = UUID.randomUUID().toString();

            Vertex iNode = odb.addVertex("class:iNode");
            iNode.setProperty("node_name", node.node_name);
            iNode.setProperty("node_type", node.type);
            iNode.setProperty("inode_id", node_id);
            if(node.params.size() > 0) {
                iNode.setProperty("params",encodeParams(node.params));
                logger.debug("iNode Params: " + encodeParams(node.params));
            }

            iNode.setProperty("status_code", "0");
            iNode.setProperty("status_desc", "Added to DB");


            odb.commit();

            logger.debug("Created iNode " + node_id + " Node ID " + iNode.getId().toString() + " getiNode = " + getINodeNodeId(node_id));

            //Link to vNode
            logger.debug("Connecting to vNode = " + vNode_id + " node ID " + getVNodeNodeId(vNode_id));
            Vertex vNode = odb.getVertex(getVNodeNodeId(vNode_id));

            Edge eNodeEdge = odb.addEdge(null, vNode, iNode, "isINode");

            //Create iNode I/O Nodes
            String eNode_id = createENode(node_id, true);

            logger.debug("Added new eNode in: " + eNode_id);
            eNode_id = createENode(node_id, false);
            logger.debug("Added new eNode out: " + eNode_id);

            odb.commit();
            return node_id;
        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException se)
        {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            se.printStackTrace(pw);
            logger.error(sw.toString());

            logger.error("createINode DUPE " + se.toString());
        }
        catch(Exception ex)
        {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());

            logger.error("createINode: " + ex.toString());
        }
        return null;
    }

    String createENode(String iNode_id, boolean isIn) {
        try
        {
            String node_id = UUID.randomUUID().toString();

            Vertex eNode = odb.addVertex("class:eNode");
            eNode.setProperty("enode_id", node_id);
            eNode.setProperty("inode_id", iNode_id);
            odb.commit();

            Vertex iNode = odb.getVertex(getINodeNodeId(iNode_id));

            if(isIn)
            {
                Edge ePipeline = odb.addEdge(null, eNode, iNode, "in");
            }
            else
            {
                Edge ePipeline = odb.addEdge(null, iNode, eNode, "out");
            }
            odb.commit();

            logger.debug("Created eNode " + node_id + " Node ID " + eNode.getId().toString() + " geteNode = " + getENodeNodeId(node_id));


            return node_id;
        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException se)
        {
            logger.error("createENode DUPE " + se.toString());
        }
        catch(Exception ex)
        {
            logger.error("createENode: " + ex.toString());
        }
        return null;
    }

    public String encodeParams(Map<String,String> params) {
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

                sb.append(sKey + ":" + sValue + ",");
            }
            params_string = sb.substring(0, sb.length() -1);

        }
        catch(Exception ex)
        {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
            logger.error("encodeParams: Exception ex:" + ex.toString());
        }
        return params_string;
    }

    public gPayload createPipelineRecord(String tenant_id, String gPayload) {
        logger.info("createPipelineRecord...");

        try
        {
            gPayload gpay = gPayLoadFromJson(gPayload);
            gpay.pipeline_id = UUID.randomUUID().toString();
            //inject real pipelineID
            gPayload = JsonFromgPayLoad(gpay);


            if(getPipelineNodeId(gpay.pipeline_id) == null) {
                logger.debug("Creating vPipeline");

                Vertex vPipeline = odb.addVertex("class:Pipeline");
                vPipeline.setProperty("pipeline_id", gpay.pipeline_id);
                vPipeline.setProperty("pipeline_name", gpay.pipeline_name);
                vPipeline.setProperty("submission", gPayload);
                vPipeline.setProperty("status_code", "3");
                vPipeline.setProperty("status_desc", "Record added to DB.");
                vPipeline.setProperty("tenant_id", tenant_id);

                odb.commit();

                if(getPipelineNodeId(gpay.pipeline_id) == null) {
                    logger.error("Pipeline record was not saved to database!");
                }
                else {
                    logger.debug("Post vPipeline commit of node " + getPipelineNodeId(gpay.pipeline_id));
                }

                Vertex vTenant = odb.getVertex(getTenantNodeId(tenant_id));
                Edge eLives = odb.addEdge(null, vPipeline, vTenant, "isPipeline");
                odb.commit();

                logger.info("Offer Pipeline to Scheduler Queue");
                plugin.getAppScheduleQueue().offer(gpay);
                return gpay;
            }
            else {
                logger.error("createPipelineRecord : Duplicate pipeline_id!" );
            }
        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException se)
        {
            logger.error("createPipelineRecord DUPE " + se.toString());
        }
        catch(Exception ex)
        {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());

            logger.error("createPipelineRecord " + ex.toString());

        }
        return null;
    }

    boolean createTenant(String tenantName, String tenantId) {
        try
        {
            Vertex vTenant = odb.addVertex("class:Tenant");
            if(tenantId == null){
                vTenant.setProperty("tenant_id", UUID.randomUUID().toString());
            }
            else {
                vTenant.setProperty("tenant_id", tenantId);
            }
            odb.commit();
            return true;
        }
        catch(com.orientechnologies.orient.core.storage.ORecordDuplicatedException se)
        {
            logger.error("DUPE " + se.toString());
        }
        catch(Exception ex)
        {
            logger.error(ex.toString());
        }
        return false;
    }

    public void initCrescoDB() {
        try
        {

            odb.executeOutsideTx(new OCallable<Object, OrientBaseGraph>() {
                @Override
                public Object call(OrientBaseGraph iArgument) {

                    logger.debug("Create Tenant Vertex Class");
                    createVertexClass("Tenant");
                    createVertexIndex("Tenant", "tenant_id", true);

                    logger.debug("Create Pipeline Vertex Class");
                    createVertexClass("Pipeline");
                    createVertexIndex("Pipeline", "pipeline_id", true);

                    logger.debug("Create eNode Vertex Class");
                    createVertexClass("eNode");
                    createVertexIndex("eNode", "enode_id", true);

                    logger.debug("Create vNode Vertex Class");
                    createVertexClass("vNode");
                    createVertexIndex("vNode", "vnode_id", true);

                    logger.debug("Create isVNode Edge Class");
                    createEdgeClass("isVNode",null);

                    logger.debug("Create isINode Edge Class");
                    createEdgeClass("isINode",null);

                    logger.debug("Create isVConnected Edge Class");
                    createEdgeClass("isVConnected",null);

                    logger.debug("Create isPipeline Edge Class");
                    createEdgeClass("isPipeline",null);

                    logger.debug("Create isEConnected Edge Class");
                    createEdgeClass("isEConnected",null);

                    logger.debug("Create in Edge Class");
                    createEdgeClass("in",null);

                    logger.debug("Create out Edge Class");
                    createEdgeClass("out",null);

                    return null;
                }
            });

            createTenant("default", "0");

        }
        catch(Exception ex)
        {
            logger.debug("initCrescoDB Error: " + ex.toString());
        }
    }

    boolean createEdgeClass(String className, String[] props) {
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

    boolean createVertexClass(String className) {
        boolean wasCreated = false;
        OrientGraphNoTx txGraph = factory.getNoTx();
        OSchema schema = ((OrientGraphNoTx)txGraph).getRawGraph().getMetadata().getSchema();

        if (!schema.existsClass(className))
        {
            OClass vt = txGraph.createVertexType(className);
            txGraph.commit();
            if (schema.existsClass(className)) {
                wasCreated = true;
            }
        }
        txGraph.shutdown();
        return wasCreated;
    }

    boolean createVertexIndex(String className, String indexName, boolean isUnique) {
        boolean wasCreated = false;
        try
        {
            OrientGraphNoTx txGraph = factory.getNoTx();
            OSchema schema = ((OrientGraphNoTx)txGraph).getRawGraph().getMetadata().getSchema();

            if (schema.existsClass(className))
            {
                OClass vt = txGraph.getVertexType(className);
                if(vt.getProperty(indexName) == null) {
                    vt.createProperty(indexName, OType.STRING);
                }
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
            logger.error("createVertexIndex : Error " + ex.toString());
        }

        return wasCreated;
    }

    public String JsonFromgPayLoad(gPayload gpay) {
        Gson gson = new GsonBuilder().create();
        //gPayload me = gson.fromJson(json, gPayload.class);
        //logger.debug(p);
        return gson.toJson(gpay);

    }

    public gPayload gPayLoadFromJson(String json) {
        Gson gson = new GsonBuilder().create();
        gPayload me = gson.fromJson(json, gPayload.class);
        //logger.debug(p);
        return me;
    }
}
