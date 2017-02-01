package com.researchworx.cresco.controller.shell;

import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.utilities.CLogger;
import jline.console.ConsoleReader;
import jline.console.completer.AggregateCompleter;
import jline.console.completer.ArgumentCompleter;
import jline.console.completer.Completer;
import jline.console.completer.StringsCompleter;
import org.apache.sshd.common.Factory;
import org.apache.sshd.server.Command;
import org.apache.sshd.server.Environment;
import org.apache.sshd.server.ExitCallback;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class AppShellFactory implements Factory {
    private static CLogger logger;

    //public static AgentControlChannel cs;
    private static String word;
    private static List<Completer> completors;
    private static Map<String, String> cmdMap;
    private Launcher plugin;


    public AppShellFactory(Launcher plugin) {
        this.plugin = plugin;
        if (logger == null)
            logger = new CLogger(AppShellFactory.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID());
    }

    public Command create() {
        //Get command channel
        try {
            //cs = new AgentControlChannel();
            completors = new LinkedList<>();
            cmdMap = new ConcurrentHashMap<>();

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return new InAppShell(plugin);
    }

    public static void genCompletors() {
        completors.clear();

    	/*
        String[] StringArray0 = new String[]{"agent","plugins","version"};
    	String[] myStringArray1 = new String[]{"o1","o2","o3"};

    	StringsCompleter[] sc = new StringsCompleter[]{new StringsCompleter("show"),new StringsCompleter(StringArray0)};

		completors.add(
    			 new AggregateCompleter(
    					  new ArgumentCompleter(
    				//			new StringsCompleter("show"), new StringsCompleter(StringArray0), new NullCompleter())
    					  //new StringsCompleter("show"), new StringsCompleter(StringArray0))
    							sc)

    							  //new StringsCompleter("show"), new StringsCompleter("agent", "plugins"), new NullCompleter())
    			                       )
    		                          );
		*/
        completors.add(
                new AggregateCompleter(
                        new ArgumentCompleter(new StringsCompleter(InAppShell.SHELL_CMD_QUESTION,InAppShell.SHELL_CMD_DATABASE,InAppShell.SHELL_CMD_RESOURCES,InAppShell.SHELL_CMD_QUIT,InAppShell.SHELL_CMD_EXIT, InAppShell.SHELL_CMD_VERSION, InAppShell.SHELL_CMD_HELP))));
    }

    private class InAppShell implements Command, Runnable {
        private Launcher plugin;
        public InAppShell(Launcher plugin) {
            this.plugin = plugin;
        }

        //private static final Logger log = LoggerFactory.getLogger(InAppShell.class);

        public final boolean IS_MAC_OSX = System.getProperty("os.name").startsWith("Mac OS X");

        private static final String SHELL_THREAD_NAME = "InAppShell";
        private static final String SHELL_PROMPT = "cresco> ";
        private static final String SHELL_CMD_QUIT = "quit";
        private static final String SHELL_CMD_EXIT = "exit";
        private static final String SHELL_CMD_VERSION = "version";
        private static final String SHELL_CMD_HELP = "help";
        private static final String SHELL_CMD_QUESTION = "?";


        private static final String SHELL_CMD_RESOURCES = "resources";
        private static final String SHELL_CMD_DATABASE = "com/researchworx/cresco/controller/db";

        private InputStream in;
        private OutputStream out;
        private OutputStream err;
        private ExitCallback callback;
        private Environment environment;
        private Thread thread;
        private ConsoleReader reader;
        private PrintWriter writer;


        public InputStream getIn() {
            return in;
        }

        public OutputStream getOut() {
            return out;
        }

        public OutputStream getErr() {
            return err;
        }

        public Environment getEnvironment() {
            return environment;
        }

        public void setInputStream(InputStream in) {
            this.in = in;
        }

        public void setOutputStream(OutputStream out) {
            this.out = out;
        }

        public void setErrorStream(OutputStream err) {
            this.err = err;
        }

        public void setExitCallback(ExitCallback callback) {
            this.callback = callback;
        }

        public void start(Environment env) throws IOException {
            environment = env;
            thread = new Thread(this, SHELL_THREAD_NAME);
            thread.start();
        }

        public void destroy() {
            if (reader != null)
                reader.shutdown();
            thread.interrupt();
        }

        public void run() {
            try {

                FilterInputStream fis = new FilterInputStream(in) {
                    @Override
                    public int read() throws IOException {

                        int i = in.read();

                        if (i == 63) {
                            //treat question mark as enter
                            return 10;
                        }
                        return i;
                    }

                };

                FilterOutputStream fos = new FilterOutputStream(out) {
                    @Override
                    public void write(final int i) throws IOException {
                        super.write(i);

                        // workaround for MacOSX!! reset line after CR..
                        /*
                        if (IS_MAC_OSX && i == ConsoleReader.CR.toCharArray()[0]) {
                            super.write(ConsoleReader.RESET_LINE);
                        }
                        */
                        if (i == ConsoleReader.CR.toCharArray()[0]) {
                            super.write(ConsoleReader.RESET_LINE);
                        }

                    }
                };

                reader = new ConsoleReader(fis, fos);

                reader.setPrompt(SHELL_PROMPT);
                //reader.addCompleter(new StringsCompleter(SHELL_CMD_QUIT,
                //        SHELL_CMD_EXIT, SHELL_CMD_VERSION, SHELL_CMD_HELP));

                //reader.setHistory(history);
                genCompletors();


                for (Completer c : completors) {
                    reader.addCompleter(c);
                }

                writer = new PrintWriter(reader.getOutput());

                // output welcome banner on ssh session startup
                writer.println("***************************************************************************");
                writer.println("*                                                                         *");
                writer.println("*      ________   _______      ________   ________   ________   ________  *");
                writer.println("*     /  _____/  /  ___  |    /  _____/  /  _____/  /  _____/  /  ___   / *");
                writer.println("*    /  /       /  /__/  /   /  /__     /  /___    /  /       /  /  /  /  *");
                writer.println("*   /  /       /  __   /    /  ___/    /____   /  /  /       /  /  /  /   *");
                writer.println("*  /  /____   /  /  |  |   /  /____   _____/  /  /  /____   /  /__/  /    *");
                writer.println("* /_______/  /__/   |__|  /_______/  /_______/  /_______/  /________/     *");
                writer.println("*                                                                         *");
                writer.println("*                             Shell Console                               *");
                writer.println("*                                                                         *");
                writer.println("***************************************************************************");
                writer.println("");
                writer.println("!!!WARNING!!!: This system is to be used by authorized users only for company");
                writer.println("work. Activities conducted on this system may be monitored and/or recorded");
                writer.println("with no expectation of privacy. All possible abuse and criminal activity may");
                writer.println("be handed over to the proper law enforcement officials for investigation and");
                writer.println("prosecution. Use implies consent to all of the conditions stated within this");
                writer.println("Warning Notification.");
                writer.println("");
                writer.flush();

                String line;

                while ((line = reader.readLine()) != null) {
                    handleUserInput(line.trim());
                }

            } catch (InterruptedIOException e) {
                // Ignore
            } catch (Exception e) {
                System.out.println("Error executing InAppShell..." + e);
                //log.error("Error executing InAppShell...", e);
            } finally {
                callback.onExit(0);
            }
        }

        private void handleUserInput(String line) throws Exception {

            String response;

            switch (line.toLowerCase()) {
                case SHELL_CMD_QUIT:
                    throw new InterruptedIOException();
                case SHELL_CMD_EXIT:
                    throw new InterruptedIOException();
                case SHELL_CMD_VERSION:
                    response = System.getProperty("os.name").toString() + " " + plugin.getVersion();
                    break;
                case SHELL_CMD_HELP:
                    response = getHelpString();
                    break;
                case SHELL_CMD_QUESTION:
                    response = getHelpString();
                    break;
                case SHELL_CMD_RESOURCES:
                    response = getResourceString();
                    break;
                default:
                    response = getHelpString();
            }

            writer.println(response);
            writer.flush();


        }

        private String getResourceString() {
            String response;
            Map<String,String> resourceTotal = plugin.getGDB().getResourceTotal();

            if(resourceTotal != null)
            {
                response = "Global Resources: \n";
                response = "------------- \n";
                for (Map.Entry<String, String> entry : resourceTotal.entrySet()) {
                    response += entry.getKey() + "\t\t:\t\t" + entry.getValue() + "\n";
                }
            }
            else
            {
                response = "Resource Query Failed !";
            }
            return response;
        }

        private String getHelpString() {

            String response = "Command Menu: \n";
            response += "------------- \n";

            response += "version\t\t:\t\tShow Version \n";
            response += "help\t\t:\t\tDisplay this menu \n";
            response += "?\t\t:\t\tDisplay this menu\n";
            response += "resources\t:\tPrint all resources \n";
            response += "quit\t\t:\t\tExit Shell \n";
            response += "exit\t\t:\t\tExit Shell \n";

            return response;
        }

        private void handleUserInput2(String line) throws Exception {
            line = line.replaceAll("\\s+", " ").trim().toLowerCase();
            String cmdString = line.replace(" ", "_");

            if (line.equalsIgnoreCase(SHELL_CMD_QUIT)
                    || line.equalsIgnoreCase(SHELL_CMD_EXIT))
                throw new InterruptedIOException();

            String response;
            if (line.equalsIgnoreCase(SHELL_CMD_VERSION))
                response = System.getProperty("os.name").toString() + " " + plugin.getVersion();
                //response = "unknown-static";
            //System.out.println("OS Name: " + System.getProperty("os.name"));


            else if (line.equalsIgnoreCase(SHELL_CMD_HELP)) {

                //response = "Help is not implemented yet...";
                response = "Command Menu: \n";
                response = "------------- \n";

                response += "quit : Exit \n";
                response += "exit : Exit \n";
                response += "version : Show Version \n";
                response += "help : Display this menu \n";
                response += "? : Display this menu\n";
                response += "resources : Print all resources \n";
            }


            else if(line.equalsIgnoreCase(SHELL_CMD_RESOURCES)) {
                Map<String,String> resourceTotal = plugin.getGDB().getResourceTotal();

                if(resourceTotal != null)
                {
                    response = "Global Resources: \n";
                    response = "------------- \n";
                    for (Map.Entry<String, String> entry : resourceTotal.entrySet()) {
                        response += entry.getKey() + " = " + entry.getValue() + "\n";
                    }
                }
                else
                {
                    response = "Resource Query Failed !";
                }
            }



            else {
                response = cmdExec(cmdString);

            }
            writer.println(response);
            writer.flush();


        }
    }

    public String cmdExec(String cmdString) throws Exception {

        String[] s_string = cmdString.split("_");
        List<String> cmdList = new ArrayList<>();
        StringBuilder sb = null;

        String cmd = new String();

        if (cmdMap.size() > 0) {
            Iterator it = cmdMap.entrySet().iterator();

            while (it.hasNext()) {
                Map.Entry pairs = (Map.Entry) it.next();

                String[] lines = pairs.getValue().toString().split(System.getProperty("line.separator"));


                for (String tmpStr : lines) {

                    String comStr = pairs.getKey().toString() + "_" + tmpStr;
                    //System.out.println("OK: " + comStr);


                    if (comStr.toLowerCase().startsWith(cmdString)) {
                        String[] s_comStr = comStr.split("_");
                        int cmdPosition = s_string.length - 1;

                        if (s_comStr.length >= s_string.length + 1) {
                            //System.out.println(s_comStr[cmdPosition + 1]);
                            //System.out.println(s_comStr[cmdPosition]);
                            //System.out.println("cmd: " + s_string.length + "com: " + s_comStr.length);
                            if (s_comStr[cmdPosition].equals(s_string[cmdPosition])) {
                                if (!cmdList.contains(s_comStr[cmdPosition + 1])) {
                                    cmdList.add(s_comStr[cmdPosition + 1]);
                                }
                            } else {
                                if (!cmdList.contains(s_comStr[cmdPosition])) {
                                    cmdList.add(s_comStr[cmdPosition]);

                                }
                            }

                        } else if (s_comStr.length == s_string.length) {
                            if (!s_comStr[cmdPosition].equals(s_string[cmdPosition])) {
                                if (!cmdList.contains(s_comStr[cmdPosition])) {
                                    cmdList.add(s_comStr[cmdPosition]);
                                }
                            }

                        }
                    }

                    if (comStr.toLowerCase().equals(cmdString.toLowerCase())) {
                        cmd = cmdString;
                    }


                }


                if ((cmd.length() > 0) && (cmdList.size() == 0)) {

                    //CmdEvent ce = cs.call(new CmdEvent("execute",cmd));
                    //return ce.getCmdResult();
                    //MsgEvent me = new MsgEvent(MsgEventType.EXEC,"test","controller2","plugin/0","shell command");
                    //me.setParam("cmd", cmd);
                    //me = PluginEngine.agentChannel.call(me);
                    //return me.getMsgBody();
                    //MsgEvent me = PluginEngine.agentChannel.call()
                }

                sb = new StringBuilder();
                for (String str : cmdList) {
                    sb.append(str + "\n");
                }

            }
            if ((cmd.length() == 0) && (cmdList.size() == 0)) {
                sb = new StringBuilder();
                sb.append("*Invalid Command*\n");
            }
        } else {
            sb = new StringBuilder();
            sb.append("NO AGENTS/COMMANDS FOUND\n");
        }
        return sb.toString().substring(0, sb.toString().length() - 1);
    }
}