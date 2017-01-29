package com.researchworx.cresco.controller.db;

import com.researchworx.cresco.controller.app.gEdge;
import com.researchworx.cresco.controller.app.gNode;
import com.researchworx.cresco.controller.app.gPayload;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.*;
import java.util.zip.GZIPInputStream;


public class DBApplicationFunctions {

    private Launcher plugin;
    private CLogger logger;
    private OrientGraphFactory factory;
    private ODatabaseDocumentTx db;
    private int retryCount;
    private DBEngine dbe;
    //private OrientGraph odb;

    public DBApplicationFunctions(Launcher plugin, DBEngine dbe) {
        this.logger = new CLogger(DBApplicationFunctions.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
        this.plugin = plugin;
        this.factory = dbe.factory;
        this.db = dbe.db;
        this.retryCount = plugin.getConfig().getIntegerParam("db_retry_count",50);
        //odb = factory.getTx();

        //create basic application constructs
        initCrescoDB();
    }

    public List<String> removePipeline(String pipelineId) {
        List<String> iNodeList = null;
        try {
            iNodeList = new ArrayList<>();
            plugin.getGDB().gdb.getresourceNodeList(pipelineId,null);
            List<String> vNodeList = getNodeIdFromEdge("pipeline", "isVNode", "vnode_id", true, "pipeline_id", pipelineId);
            for(String vNodeId : vNodeList) {
                logger.debug("vnodes " + vNodeId);
                iNodeList.addAll(getNodeIdFromEdge("vnode", "isINode", "inode_id", true, "vnode_id", vNodeId));
                for(String iNodeId : iNodeList) {
                    logger.debug("inodes " + iNodeId);
                    List<String> eNodeList = getNodeIdFromEdge("inode", "in", "enode_id", false, "inode_id",iNodeId);
                    eNodeList.addAll(getNodeIdFromEdge("inode", "out", "enode_id",true, "inode_id",iNodeId));
                    for(String eNodeId : eNodeList) {
                        logger.debug("enodes " + eNodeId);
                        removeNode(getENodeNodeId(eNodeId));
                    }
                }
                removeNode(getVNodeNodeId(vNodeId));
            }

            removeNode(getPipelineNodeId(pipelineId));
        }
        catch(Exception ex) {
            logger.error("removePipeline " + ex.getMessage());
        }
        return iNodeList;
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
        String edge_id = null;

        OrientGraph graph = null;


        int count = 0;
        try
        {
            graph = factory.getTx();
            while((edge_id == null) && (count != retryCount))
            {
                if(count > 0)
                {
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                String tmp_edge_id = IgetVNodeNodeId(vNodeId);
                if(tmp_edge_id == null) {
                    return null;
                }
                if(!tmp_edge_id.equals("*")) {
                    edge_id = tmp_edge_id;
                }
                count++;

            }

            if((edge_id == null) && (count == retryCount))
            {
                logger.debug("getVNodeNodeId : failed to get node " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("getVNodeNodeId : Error " + ex.toString());
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

    public String IgetVNodeNodeId(String vNodeId) {
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
            node_id = "*";
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
        OrientGraph graph = null;
        try {
            graph = factory.getTx();
            String pipelineNodeId = getPipelineNodeId(pipelineId);
            if (pipelineNodeId != null) {
                //if(getPipelineNodeId(pipelineId != null)
                Vertex vPipeline = graph.getVertex(getPipelineNodeId(pipelineId));
                if (vPipeline != null) {
                    json = vPipeline.getProperty("submission");
                }
            }
        }
        catch(Exception ex) {
            logger.error("getPipeline " + ex.getMessage());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return json;
    }

    public String createIEdge(String node_from, String node_to) {
        OrientGraph graph = null;
        String edge_id = null;
        int count = 0;
        try
        {
            graph = factory.getTx();
            while((edge_id == null) && (count != retryCount))
            {
                if(count > 0)
                {
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                edge_id = IcreateIEdge(node_from,node_to);
                count++;

            }

            if((edge_id == null) && (count == retryCount))
            {
                logger.debug("createIEdge : Failed to add edge in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("createIEdge : Error " + ex.toString());
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

    String IcreateIEdge(String node_from, String node_to) {
        String edge_id = null;
        OrientGraph graph = null;

        try
        {
            graph = factory.getTx();

            logger.debug("createIedge from_node: " + node_from + " node_to: " + node_to);
            logger.debug("-I0 ");
            //Vertex iNode_from = odb.getVertexByKey("iNode.node_id", node_from);
            Vertex iNode_from = graph.getVertex(getINodeNodeId(node_from));
            logger.debug("-I1 ");

            //Vertex iNode_to = odb.getVertexByKey("iNode.node_id", node_to);
            Vertex iNode_to = graph.getVertex(getINodeNodeId(node_to));

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
                logger.debug("From eNode_id:" +  eNode_from.getProperty("enode_id") + " iNode_id:" + eNode_from.getProperty("inode_id"));
            }
            logger.debug("-I4 ");

            Iterable<Edge> toEdges = iNode_to.getEdges(Direction.IN, "in");
            Edge eEdge_to = toEdges.iterator().next();
            Vertex eNode_to = eEdge_to.getVertex(Direction.OUT);
            logger.debug("-I5 ");

            if(eNode_to != null)
            {
                logger.debug("To eNode_id:" +  eNode_to.getProperty("enode_id") + " iNode_id:" + eNode_to.getProperty("inode_id"));
            }
            logger.debug("-I6 ");

            Edge iEdge = graph.addEdge(null, eNode_from, eNode_to, "isEConnected");
			/*
			 public Iterable<Edge> getEdges(OrientVertex iDestination,
                               Direction iDirection,
                               String... iLabels)
			 */
            logger.debug("-I7 ");

            //Edge vEdge = odb.addEdge(null, eNode_from, eNode_to, "isIConnected");
            //Edge vEdge = eNode_from.getEdges(arg0, arg1)
            //odb.commit();
            //return true;
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
            logger.debug("IcreateVNode: thread_id: " + threadId + " Error " + ex.toString());
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

    String createVEdge(String node_from, String node_to) {
        String edge_id = null;
        OrientGraph graph = null;

        try
        {
            graph = factory.getTx();

            logger.debug("createVEdge node_from: " + node_from + " node_to:" + node_to);

            logger.debug("-0");
            //Vertex vNode_from = odb.getVertexByKey("vNode.node_id", node_from);
            Vertex vNode_from = graph.getVertex(getVNodeNodeId(node_from));
            logger.debug("-1");
            //Vertex vNode_to = odb.getVertexByKey("vNode.node_id", node_to);
            Vertex vNode_to = graph.getVertex(getVNodeNodeId(node_to));
            logger.debug("-2");
            Edge vEdge = graph.addEdge(null, vNode_from, vNode_to, "isVConnected");
            logger.debug("-3");
            edge_id = UUID.randomUUID().toString();
            logger.debug("-4");
            vEdge.setProperty("edge_id", edge_id);
            logger.debug("-5");
            //odb.commit();
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
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return edge_id;
    }

    public boolean setPipelineStatus(gPayload gpay) {
        OrientGraph graph = null;
        try
        {
            graph = factory.getTx();
            String pipelineId = gpay.pipeline_id;
            String status_code = gpay.status_code;
            String status_desc = gpay.status_desc;
            String submission = JsonFromgPayLoad(gpay);
            //Vertex vPipeline = odb.getVertexByKey("Pipeline.pipelineid", pipeline_id);
            Vertex vPipeline = graph.getVertex(getPipelineNodeId(pipelineId));
            
            if(vPipeline != null)
            {
                vPipeline.setProperty("status_code", status_code);
                vPipeline.setProperty("status_desc", status_desc);
                vPipeline.setProperty("submission", submission);

                //odb.commit();
                return true;
            }
        }
        catch(Exception ex)
        {
            logger.debug("setPipelineStatus Error: " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return false;

    }

    public boolean setPipelineStatus(String pipelineId, String status_code, String status_desc) {
        OrientGraph graph = null;
        try
        {
            graph = factory.getTx();
            //Vertex vPipeline = odb.getVertexByKey("Pipeline.pipelineid", pipeline_id);
            Vertex vPipeline = graph.getVertex(getPipelineNodeId(pipelineId));

            if(vPipeline != null)
            {
                vPipeline.setProperty("status_code", status_code);
                vPipeline.setProperty("status_desc", status_desc);
                //odb.commit();
                return true;
            }
        }
        catch(Exception ex)
        {
            logger.debug("setPipelineStatus Error: " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return false;

    }

    public String getINodeParams(String iNode_id) {
        String params = null;
        OrientGraph graph = null;

        try {
            graph = factory.getTx();

            String node_id = getINodeNodeId(iNode_id);
            if(node_id != null) {
                Vertex iNode = graph.getVertex(node_id);
                params = iNode.getProperty("params");
            }
        }
        catch(Exception ex) {
            logger.error("getINodeConfigParams " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return params;
    }

    public gPayload getPipelineObj(String pipelineId) {
        OrientGraph graph = null;
        gPayload gpay = null;
        try {
            graph = factory.getTx();

            //Vertex vPipeline = odb.getVertexByKey("Pipeline.pipelineid", pipeline_id);
            Vertex vPipeline = graph.getVertex(getPipelineNodeId(pipelineId));
            if (vPipeline != null) {
                String json = vPipeline.getProperty("submission");
                gpay = gPayLoadFromJson(json);
            }
        }
        catch(Exception ex) {
            logger.error("getPipelineObj " + ex.getMessage());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return gpay;

    }

    public String getINodefromVNode(String vNode_id) {
        String edge_id = null;

        int count = 0;
        try
        {
            if(vNode_id != null) {
                while ((edge_id == null) && (count != retryCount)) {
                    if (count > 0) {
                        Thread.sleep((long) (Math.random() * 1000)); //random wait to prevent sync error
                    }
                    String tmp_edge_id = IgetINodefromVNode(vNode_id);
                    if (tmp_edge_id.equals("*")) {
                        return null;
                    }
                    else {
                        edge_id = tmp_edge_id;
                    }
                    count++;

                }

                if ((edge_id == null) && (count == retryCount)) {
                    logger.debug("createINode : Failed to add edge in " + count + " retrys");
                }
            }
        }
        catch(Exception ex)
        {
            logger.debug("createINode : Error " + ex.toString());
        }

        return edge_id;
    }

    public String IgetINodefromVNode(String vNode_id) {
        String iNode_id = null;

        OrientGraph graph = null;
        try
        {
            List<String> iNodeList = getNodeIdFromEdge("vnode", "isINode", "inode_id", true, "vnode_id", vNode_id);
            if(iNodeList.size() > 0) {
                iNode_id = iNodeList.get(0);
            }
            /*
            graph = factory.getTx();

            //Vertex vNode = odb.getVertexByKey("vNode.node_id", vNode_id);
            Vertex vNode = graph.getVertex(getVNodeNodeId(vNode_id));
            if(vNode != null)
            {
                //json = vPipeline.getProperty("submission");
                Iterable<Edge> vNodeEdges = vNode.getEdges(Direction.OUT, "isINode");
                Iterator<Edge> iter = vNodeEdges.iterator();

                Edge isINode = iter.next();
                Vertex iNode = isINode.getVertex(Direction.IN);
                return iNode.getProperty("inode_id");
            }
            */
        }
        catch(Exception ex)
        {
            logger.error("getINodefromVNode: Exception ex:" + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return iNode_id;
    }

    public boolean setINodeStatus(String iNode_id, String status_code, String status_desc) {
        OrientGraph graph = null;

        try
        {
            graph = factory.getTx();
            //Vertex iNode = odb.getVertexByKey("iNode.node_id", iNode_id);
            Vertex iNode = graph.getVertex(getINodeNodeId(iNode_id));
            if(iNode == null)
            {
                logger.debug("setINodeStatus Error: Node not found!");
                return false;
            }
            else
            {
                iNode.setProperty("status_code", status_code);
                iNode.setProperty("status_desc", status_desc);
                //odb.commit();
                logger.debug("setINodeStatus iNode_id:" + iNode_id + " status_code:" + status_code + " status_desc:" + status_desc);
                return true;
            }

        }
        catch(Exception ex)
        {
            logger.error("setINodeStatus: Exception ex:" + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return false;
    }

    public List<String> getPipelineIdList() {
        OrientGraph graph = null;

        List<String> pipelineList = null;
        try
        {
            graph = factory.getTx();

            pipelineList = new ArrayList<>();
            Iterable<Vertex> pipelines = graph.getVerticesOfClass("Pipeline");
            Iterator<Vertex> iter = pipelines.iterator();
            while (iter.hasNext())
            {
                Vertex vPipeline = iter.next();
                String pipelineId = vPipeline.getProperty("pipeline_id");
                //gPayload gpay = gPayLoadFromJson(submission);
                pipelineList.add(pipelineId);
            }
        }
        catch(Exception ex)
        {
            logger.debug("getPipelineIdList Error: " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }

        logger.debug("PipelineIdlist return: " + pipelineList.size());

        return pipelineList;
    }

    public List<gPayload> getPipelineList() {
        OrientGraph graph = null;

        List<gPayload> pipelineList = null;
        try
        {
            graph = factory.getTx();

            pipelineList = new ArrayList<gPayload>();
            Iterable<Vertex> pipelines = graph.getVerticesOfClass("Pipeline");
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
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }

        logger.debug("Pipeline list return: " + pipelineList.size());

        return pipelineList;
    }

    public boolean iNodeIsActive(String iNode_id) {
        OrientGraph graph = null;

        try
        {
            graph = factory.getTx();

            //Vertex iNode = odb.getVertexByKey("iNode.node_id", iNode_id);
            Vertex iNode = graph.getVertex(getINodeNodeId(iNode_id));

            if(iNode != null)
            {
                if(iNode.getProperty("inode_id").toString().equals("4"))
                {
                    return true;
                }
            }
        }
        catch(Exception ex)
        {
            logger.debug("iNodeIsActive: Exception ex:" + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
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
        OrientGraph graph = null;
        try
        {
            graph = factory.getTx();
            //Vertex iNode = odb.getVertexByKey("iNode.node_id", iNode_id);
            Vertex iNode = graph.getVertex(getINodeNodeId(iNode_id));
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
                uINode = iNode2.getProperty("inode_id");
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
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return uINode;
    }

    public Map<String,String> getNodeManifest(String iNode_id) {
        Map<String,String> manifest = null;
        OrientGraph graph = null;

        try
        {
            graph = factory.getTx();
            //Vertex iNode = odb.getVertexByKey("iNode.node_id", iNode_id);
            Vertex iNode = graph.getVertex(getINodeNodeId(iNode_id));
            if(iNode != null)
            {
                manifest = getMapFromString(iNode.getProperty("params").toString(), false);
                //set type in manifest
                manifest.put("inode_id", iNode.getProperty("inode_id").toString());
                manifest.put("node_name", iNode.getProperty("node_name").toString());
                manifest.put("node_type", iNode.getProperty("node_type").toString());
                manifest.put("status_code", iNode.getProperty("status_code").toString());
                manifest.put("status_desc", iNode.getProperty("status_desc").toString());

                //add type-specific information This should be done a better way
                if((manifest.get("node_type").equals("membuffer")) || (manifest.get("node_type").equals("query")))
                {
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
                        Vertex uINode = graph.getVertex(getINodeNodeId(upstreamINode_id));
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
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
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
                    //add resource_id to configs
                    node.params.put("resource_id",gpay.pipeline_id);
                    //

                    String vNode_id = createVNode(gpay.pipeline_id, node,true);
                    vNodeHm.put(node.node_id, vNode_id);

                    logger.debug("vNode:" + vNode_id + " pipelineNodeID: " + node.node_id + " dbNodeId: " + getVNodeNodeId(vNode_id));

                    String iNode_id = createINode(vNode_id, node);
                    iNodeHm.put(node.node_id, iNode_id);

                    logger.debug("iNode:" + iNode_id + " pipelineNodeID: " + node.node_id + " dbNodeId: " +  getINodeNodeId(iNode_id));

                    //Create iNode I/O Nodes
                    String eNodeIn_id = createENode(iNode_id, true);
                    String eNodeOut_id = createENode(iNode_id, false);
                    logger.debug("eNodeIn:" + getENodeNodeId(eNodeIn_id) + " eNodeOut: " + getENodeNodeId(eNodeOut_id));

                    //assoicateNtoV(vNode_id,iNode_id);
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
                    //createGEdge(edge);
                    //vEdge
                    if((edge.node_from != null) && (edge.node_to != null)) {
                        if ((vNodeHm.containsKey(edge.node_from)) && (vNodeHm.containsKey(edge.node_to))) {
                            logger.debug("From vID : " + edge.node_from + " TO vID: " + edge.node_to);

                            String edge_id = createVEdge(vNodeHm.get(edge.node_from), vNodeHm.get(edge.node_to));
                            edge.edge_id = edge_id;

                            logger.debug("vedgeid: " + edge_id + " from: " + iNodeHm.get(edge.node_from) + " to:" + vNodeHm.get(edge.node_to));

                            String iedge_id = createIEdge(iNodeHm.get(edge.node_from), iNodeHm.get(edge.node_to));
                            //assign vEdge ID
                            edge.node_from = vNodeHm.get(edge.node_from);
                            edge.node_to = vNodeHm.get(edge.node_to);

                            logger.debug("iedgeid: " + iedge_id + " from: " + iNodeHm.get(edge.node_from) + " to:" + vNodeHm.get(edge.node_to));

                        }
                    }
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

    public String createVNode(String pipelineId, gNode node, boolean isNew) {
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
                edge_id = IcreateVNode(pipelineId,node,isNew);
                count++;

            }

            if((edge_id == null) && (count == retryCount))
            {
                logger.debug("createVNode : Failed to add edge in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("createVNode : Error " + ex.toString());
        }

        return edge_id;
    }

    String IcreateVNode(String pipelineId, gNode node, boolean isNew) {
        OrientGraph graph = null;
        String node_id = null;
        try
        {
            graph = factory.getTx();

            Vertex vNode = graph.addVertex("class:vNode");
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
            //graph.commit();

            //logger.debug("Created vNode " + node_id + " Node ID " + vNode.getId().toString() );
            //+ " getvNode = " + getVNodeNodeId(node_id));

            Vertex vPipeline = graph.getVertex(getPipelineNodeId(pipelineId));
            Edge ePipeline = graph.addEdge(null, vPipeline, vNode, "isVNode");
            //graph.commit();

            //    return node_id;
            graph.commit();
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
            logger.debug("IcreateVNode: thread_id: " + threadId + " Error " + ex.toString());
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

    public boolean removeNode(String rid) {
        boolean nodeRemoved = false;
        int count = 0;
        try
        {

            while((!nodeRemoved) && (count != retryCount))
            {
                if(count > 0)
                {
                    //logger.debug("REMOVENODE RETRY : region=" + region + " agent=" + agent + " plugin" + plugin);
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                nodeRemoved = IremoveNode(rid);
                count++;

            }

            if((!nodeRemoved) && (count == retryCount))
            {
                logger.debug("removeNodes : Failed to add node in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("removeNodes : Error " + ex.toString());
        }

        return nodeRemoved;
    }

    private boolean IremoveNode(String rid) {
        boolean nodeRemoved = false;
        OrientGraph graph = null;
        try
        {
            if(rid != null) {
                graph = factory.getTx();
                Vertex rNode = graph.getVertex(rid);
                graph.removeVertex(rNode);

                graph.commit();
            }
            nodeRemoved = true;
        }
        catch(com.orientechnologies.orient.core.exception.OConcurrentModificationException exc)
        {
            //eat exception
            logger.error("concurrent " + exc.getMessage());
        }
        catch(Exception ex)
        {
            long threadId = Thread.currentThread().getId();
            logger.error("removeNodes :  thread_id: " + threadId + " Error " + ex.toString());
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

    public String createINode(String iNode_id, gNode node) {
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
                edge_id = IcreateINode(iNode_id,node);
                count++;

            }

            if((edge_id == null) && (count == retryCount))
            {
                logger.debug("createINode : Failed to add edge in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("createINode : Error " + ex.toString());
        }

        return edge_id;
    }

    String IcreateINode(String iNode_id, gNode node) {

        String node_id = null;
        OrientGraph graph = null;

        try
        {
            graph = factory.getTx();

            node_id = UUID.randomUUID().toString();

            Vertex iNode = graph.addVertex("class:iNode");
            iNode.setProperty("node_name", node.node_name);
            iNode.setProperty("node_type", node.type);
            iNode.setProperty("inode_id", node_id);
            node.params.put("inode_id",node_id);
            //if(node.params.size() > 0) {
                iNode.setProperty("params",encodeParams(node.params));
                logger.debug("iNode Params: " + encodeParams(node.params));
            //}

            iNode.setProperty("status_code", "0");
            iNode.setProperty("status_desc", "Added to DB");


            //graph.commit();

            logger.debug("Created iNode " + node_id + " Node ID " + iNode.getId().toString());
            //+ " getiNode = " + getINodeNodeId(node_id));


            //Link to vNode
            logger.debug("Connecting to vNode = " + iNode_id + " node ID " + getVNodeNodeId(iNode_id));
            Vertex vNode = graph.getVertex(getVNodeNodeId(iNode_id));
            //Vertex vNode = graph.getVertex(iNode_id);

            Edge eNodeEdge = graph.addEdge(null, vNode, iNode, "isINode");

            graph.commit();

        //    return node_id;
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
            logger.debug("IcreateVNode: thread_id: " + threadId + " Error " + ex.toString());
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

    public String createENode(String iNode_id, boolean isIn) {
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
                edge_id = IcreateENode(iNode_id,isIn);
                count++;

            }

            if((edge_id == null) && (count == retryCount))
            {
                logger.debug("createENode : Failed to add edge in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("createENode : Error " + ex.toString());
        }

        return edge_id;
    }

    String IcreateENode(String iNode_id, boolean isIn) {
        OrientGraph graph = null;

        try
        {
            graph = factory.getTx();

            String node_id = UUID.randomUUID().toString();

            Vertex eNode = graph.addVertex("class:eNode");
            eNode.setProperty("enode_id", node_id);
            eNode.setProperty("inode_id", iNode_id);
            graph.commit();

            Vertex iNode = graph.getVertex(getINodeNodeId(iNode_id));

            if(isIn)
            {
                Edge ePipeline = graph.addEdge(null, eNode, iNode, "in");
            }
            else
            {
                Edge ePipeline = graph.addEdge(null, iNode, eNode, "out");
            }
            //graph.commit();

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

    String assoicateNtoV(String vNode_id, String iNode_id) {

        String node_id = null;
        OrientGraph graph = null;

        try
        {
            graph = factory.getTx();

            //Link to vNode
            logger.debug("Connecting to vNode = " + iNode_id + " node ID " + getVNodeNodeId(iNode_id));
            Vertex vNode = graph.getVertex(getVNodeNodeId(vNode_id));
            Vertex iNode = graph.getVertex(getVNodeNodeId(iNode_id));

            Edge eNodeEdge = graph.addEdge(null, vNode, iNode, "isINode");

            //Create iNode I/O Nodes
            String eNode_id = createENode(node_id, true);

            logger.debug("Added new eNode in: " + eNode_id);
            eNode_id = createENode(node_id, false);
            logger.debug("Added new eNode out: " + eNode_id);

            //graph.commit();

            //    return node_id;
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
            logger.debug("IcreateVNode: thread_id: " + threadId + " Error " + ex.toString());
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
        OrientGraph graph = null;
        try
        {
            graph = factory.getTx();

            gPayload gpay = gPayLoadFromJson(gPayload);
            gpay.pipeline_id = UUID.randomUUID().toString();
            //inject real pipelineID
            gPayload = JsonFromgPayLoad(gpay);


            if(getPipelineNodeId(gpay.pipeline_id) == null) {
                logger.debug("Creating vPipeline");

                Vertex vPipeline = graph.addVertex("class:Pipeline");
                vPipeline.setProperty("pipeline_id", gpay.pipeline_id);
                vPipeline.setProperty("pipeline_name", gpay.pipeline_name);
                vPipeline.setProperty("submission", gPayload);
                vPipeline.setProperty("status_code", "3");
                vPipeline.setProperty("status_desc", "Record added to DB.");
                vPipeline.setProperty("tenant_id", tenant_id);

                graph.commit();
                //odb.commit();

                if(getPipelineNodeId(gpay.pipeline_id) == null) {
                    logger.error("Pipeline record was not saved to database!");
                }
                else {
                    logger.debug("Post vPipeline commit of node " + getPipelineNodeId(gpay.pipeline_id));
                }

                Vertex vTenant = graph.getVertex(getTenantNodeId(tenant_id));
                Edge eLives = graph.addEdge(null, vPipeline, vTenant, "isPipeline");
                graph.commit();
                //odb.commit();

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
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return null;
    }

    boolean createTenant(String tenantName, String tenantId) {
        OrientGraph graph = null;
        try
        {
            graph = factory.getTx();

            Vertex vTenant = graph.addVertex("class:Tenant");
            if(tenantId == null){
                vTenant.setProperty("tenant_id", UUID.randomUUID().toString());
            }
            else {
                vTenant.setProperty("tenant_id", tenantId);
            }
            //odb.commit();
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
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return false;
    }

    public List<String> getNodeIdFromEdge(String nodeClass, String edgeLabel, String property, boolean in, String key, String value) {
        String edge_id = null;
        OrientGraph graph = null;
        List<String> edge_list = null;
        try
        {
            if((nodeClass != null) && (edgeLabel != null)) {
                nodeClass = nodeClass.toLowerCase();
                edgeLabel.toLowerCase();

                edge_list = new ArrayList<String>();
                //OrientGraph graph = factory.getTx();
                //OrientGraphNoTx graph = factory.getNoTx();
                graph = factory.getTx();//SELECT rid, expand(outE('isVNode')) from pipeline
                String queryString = null;
                if(in) {
                    queryString = "SELECT rid, expand(outE('" + edgeLabel + "').inV()) from " + nodeClass + " where " + key + " = '" + value + "'";
                } else {
                    queryString = "SELECT rid, expand(inE('" + edgeLabel + "').outV()) from " + nodeClass + " where " + key + " = '" + value + "'";
                }
                logger.debug("querystring " + queryString);
                Iterable<Vertex> resultIterator = graph.command(new OCommandSQL(queryString)).execute();

                Iterator<Vertex> iter = resultIterator.iterator();
                while (iter.hasNext()) {
                    Vertex v = iter.next();

                    edge_id = v.getProperty(property).toString();
                    if (edge_id != null) {

                        //edge_id = edge_id.substring(edge_id.indexOf("[") + 1, edge_id.indexOf("]"));
                        logger.debug("parameter = " + edge_id);
                        edge_list.add(edge_id);
                    }

                }
            }
        }
        catch(Exception ex)
        {
            logger.debug("getNodeIdFromEdge : Error " + ex.toString());
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

    public Map<String,String> getpNodeINode(String iNode_id) {
        Map<String,String> nodeMap = null;
        OrientGraph graph = null;
        try
        {
            nodeMap = new HashMap<>();
            if(iNode_id != null) {

                //OrientGraph graph = factory.getTx();
                //OrientGraphNoTx graph = factory.getNoTx();
                graph = factory.getTx();//SELECT rid, expand(outE('isVNode')) from pipeline
                String queryString = "SELECT rid, expand(inE('isAssigned').outV()) from inode";

                logger.debug("querystring " + queryString);
                Iterable<Vertex> resultIterator = graph.command(new OCommandSQL(queryString)).execute();

                Iterator<Vertex> iter = resultIterator.iterator();
                while (iter.hasNext()) {
                    Vertex v = iter.next();

                    String region = v.getProperty("region").toString();
                    String agent = v.getProperty("agent").toString();
                    String plugin = v.getProperty("plugin").toString();

                    if ((region != null) && (agent != null)  && (plugin != null)) {
                        //edge_id = edge_id.substring(edge_id.indexOf("[") + 1, edge_id.indexOf("]"));
                        logger.debug("parameter = " + region + " agent " + "plugin " + plugin);
                        nodeMap.put("region",region);
                        nodeMap.put("agent",agent);
                        nodeMap.put("plugin",plugin);
                    }

                }
            }
        }
        catch(Exception ex)
        {
            logger.debug("getNodeIdFromEdge : Error " + ex.toString());
        }
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
        }
        return nodeMap;
    }

    public void initCrescoDB() {
        OrientGraph graph = null;

        try
        {
            graph = factory.getTx();

            graph.executeOutsideTx(new OCallable<Object, OrientBaseGraph>() {
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
        finally
        {
            if(graph != null)
            {
                graph.shutdown();
            }
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
