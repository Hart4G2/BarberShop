package org.example;

import javax.jmdns.JmDNS;
import javax.jmdns.ServiceInfo;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ServerMain {

    private static List<MonoThreadClientHandler> clientHandlers = new ArrayList<>();

    public static void main(String[] args) {
        ExecutorService executeIt = Executors.newFixedThreadPool(10);

        try (ServerSocket server = new ServerSocket(8080,0 , InetAddress.getLocalHost());
             BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {
            System.out.println("Server socket created, command console reader for listen to server commands");

            JmDNS jmdns = JmDNS.create(InetAddress.getLocalHost());
            jmdns.registerService(ServiceInfo.create("_http._tcp.local.", "myserver", 8080, "path=index.html"));

            while (!server.isClosed()) {
                if (br.ready()) {
                    System.out.println("ServerMain Server found any messages in channel, let's look at them.");

                    String serverCommand = br.readLine();

                    if (serverCommand.equalsIgnoreCase("quit")) {
                        System.out.println("ServerMain: Server initiated shutdown...");

                        for (MonoThreadClientHandler clientHandler : clientHandlers) {
                            clientHandler.close();
                        }

                        executeIt.shutdown();
                        while (!executeIt.isTerminated()) {
                            // Wait until all threads are terminated
                        }

                        server.close();
                        System.out.println("ServerMain: Server shutdown complete.");
                        break;
                    }
                }

                Socket client = server.accept();

                MonoThreadClientHandler clientHandler = new MonoThreadClientHandler(client);
                executeIt.execute(clientHandler);
                clientHandlers.add(clientHandler);
                System.out.print("Connection accepted.");
            }
            executeIt.shutdown();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}