package com.researchworx.cresco.controller.db;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.utilities.CLogger;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;


public class DBEngine {

	public OrientGraphFactory factory;
	public ODatabaseDocumentTx db;
    private Launcher plugin;
    private CLogger logger;
    private boolean isMemory = true;

	private int retryCount;

	//upload

	public DBEngine(Launcher plugin) {

        this.plugin = plugin;
        logger = new CLogger(DBEngine.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
        this.retryCount = plugin.getConfig().getIntegerParam("db_retry_count",50);

        String host = plugin.getConfig().getStringParam("gdb_host");
        String username = plugin.getConfig().getStringParam("gdb_username");
        String password = plugin.getConfig().getStringParam("gdb_password");
        String dbname = plugin.getConfig().getStringParam("gdb_dbname");


        String connection_string = "remote:" + host + "/" + dbname;

        if((host != null) && (username != null) && (password != null) && (dbname != null))
        {
            isMemory = false;
        }

        if(isMemory) {
            db = new ODatabaseDocumentTx("memory:internalDb").create();

//            factory = new OrientGraphFactory("memory:internalDb").setupPool(100,1000);
            factory = new OrientGraphFactory("memory:internalDb");


        }
        else {

            //String connection_string = "plocal:/opt/cresco/db";
            //String connection_string = "plocal:/Users/vcbumg2/Downloads/orientdb-community-2.2.14/databases/cresco";
            db = new  ODatabaseDocumentTx(connection_string).open(username, password);
            factory = new OrientGraphFactory(connection_string, username, password).setupPool(10, 100);


        }

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


    /*

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
            logger.debug("getNodeClass: Error " + ex.toString());
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
                    //logger.debug("ADDNODE RETRY : region=" + region + " agent=" + agent + " plugin" + plugin);
                    Thread.sleep((long)(Math.random() * 1000)); //random wait to prevent sync error
                }
                isUpdated = IupdateEdge(edge_id, params);
                count++;

            }

            if((!isUpdated) && (count == retryCount))
            {
                logger.debug("DBEngine : updateEdge : Failed to update edge in " + count + " retrys");
            }
        }
        catch(Exception ex)
        {
            logger.debug("DBEngine : updateEdge : Error " + ex.toString());
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
                logger.debug("IupdateEdge: no edge found for edge_id=" + edge_id);
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
            logger.debug("IupdateEdge: thread_id: " + threadId + " Error " + ex.toString());
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

*/
}
