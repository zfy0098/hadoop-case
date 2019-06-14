package com.thrift;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
/**
 * Created with IDEA by ChouFy on 2019/5/30.
 *
 * @author Zhoufy
 */
public class Client {

    private static final int SERVER_PORT = 9090;
    private static final String SERVER_IP = "172.16.85.140";

    private void startClient(String userName) {
        TTransport tTransport;
        try {
            tTransport = new TSocket(SERVER_IP, SERVER_PORT);
            //协议要和服务端一致
            TProtocol protocol = new TBinaryProtocol(tTransport);
            RecSys.Client client = new RecSys.Client(protocol);
            tTransport.open();

            String result = client.rec_data(userName);
            System.out.println("Thrift client result=" + result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Client client = new Client();
        String name = "zfy";
        client.startClient(name);
    }
}
