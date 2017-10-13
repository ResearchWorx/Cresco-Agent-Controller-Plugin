package com.researchworx.cresco.controller.netdiscovery;

import com.google.gson.Gson;
import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;

import java.io.*;
import java.net.*;
import java.security.cert.Certificate;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TCPDiscoveryEngine implements Runnable {
    private Launcher plugin;
    private DiscoveryCrypto discoveryCrypto;
    private Gson gson;
    private CLogger logger;
    private int discoveryPort;
    protected Thread       runningThread= null;
    protected static ServerSocket serverSocket = null;
    protected static boolean      isStopped    = false;

    public TCPDiscoveryEngine(Launcher plugin) {
        this.logger = new CLogger(TCPDiscoveryEngine.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(),CLogger.Level.Info);
        logger.trace("Initializing");
        this.plugin = plugin;
        discoveryCrypto = new DiscoveryCrypto(plugin);
        gson = new Gson();
        this.discoveryPort = plugin.getConfig().getIntegerParam("netdiscoveryport",32005);
    }

    public TCPDiscoveryEngine(Launcher plugin, int discoveryPort) {
        this.logger = new CLogger(TCPDiscoveryEngine.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(),CLogger.Level.Info);
        logger.trace("Initializing");
        this.plugin = plugin;
        discoveryCrypto = new DiscoveryCrypto(plugin);
        gson = new Gson();
        this.discoveryPort = discoveryPort;
    }


    private class WorkerRunnable implements Runnable{

        protected Socket clientSocket = null;
        protected String serverText   = null;

        public WorkerRunnable(Socket clientSocket, String serverText) {
            this.clientSocket = clientSocket;
            this.serverText   = serverText;
        }

        public void run() {
            try {
                InputStream input  = clientSocket.getInputStream();
                OutputStream output = clientSocket.getOutputStream();
                long time = System.currentTimeMillis();
                output.write(("HTTP/1.1 200 OK\n\nWorkerRunnable: " +
                        this.serverText + " - " +
                        time +
                        "").getBytes());
                output.close();
                input.close();
                System.out.println("Request processed: " + time);
            } catch (IOException e) {
                //report exception somewhere.
                e.printStackTrace();
            }
        }
    }

    public void run(){
        synchronized(this){
            this.runningThread = Thread.currentThread();
        }
        openServerSocket();
        plugin.setTCPDiscoveryActive(true);
        while(! isStopped()){
            Socket clientSocket = null;
            try {
                clientSocket = this.serverSocket.accept();
            } catch (IOException e) {
                if(isStopped()) {
                    System.out.println("Server Stopped.") ;
                    return;
                }
                throw new RuntimeException(
                        "Error accepting client connection", e);
            }
            new Thread(
                    new WorkerRunnable(
                            clientSocket, "Multithreaded Server")
            ).start();
        }
        System.out.println("Server Stopped.") ;
    }

    public static synchronized void shutdown(){
        isStopped = true;
        try {
            serverSocket.close();
        } catch (IOException e) {
            throw new RuntimeException("Error closing server", e);
        }
    }

    private synchronized boolean isStopped() {
        return this.isStopped;
    }

    public synchronized void stop(){
        this.isStopped = true;
        try {
            this.serverSocket.close();
        } catch (IOException e) {
            throw new RuntimeException("Error closing server", e);
        }
    }

    private void openServerSocket() {
        try {
            this.serverSocket = new ServerSocket(this.discoveryPort);
        } catch (IOException e) {
            throw new RuntimeException("Cannot open port 8080", e);
        }
    }

}