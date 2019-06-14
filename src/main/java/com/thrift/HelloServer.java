package com.thrift;

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;

/**
 * Created with IDEA by ChouFy on 2019/5/30.
 *
 * @author Zhoufy
 */
public class HelloServer {
    private final static int SERVER_PORT = 7099;
    private static String SERVER_IP = "localhost";

    private void startServer() {
        try {
            System.out.println("HelloWorld Server start...");

            TServerSocket serverTransport = new TServerSocket(SERVER_PORT);
            TServer.Args args = new TServer.Args(serverTransport);
            TProcessor process = new RecSys.Processor(new RecSysImpl());
            TBinaryProtocol.Factory portFactory = new TBinaryProtocol.Factory(true, true);
            args.processor(process);
            args.protocolFactory(portFactory);

            TServer server = new TSimpleServer(args);
            server.serve();

        } catch (Exception e) {
            System.out.println("Server start error");
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        HelloServer server = new HelloServer();
        server.startServer();
    }
}
