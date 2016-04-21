import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class DHashTableServer {
    private int port;
    private Map<String, String> data = new Hashtable<String, String>();

    public DHashTableServer(int port) {
        this.port = port;
    }

    public static void main(String[] args) throws Exception {
        int port = 8000;
        DHashTableServer server = new DHashTableServer(port);
        new Doit(server, port, server.data).start();
        System.out.println("port: " + port);
        while (true) {
            Thread.sleep(10000);
        }
    }
}

class Doit extends Thread {
    private int port;
    private Map<String, String> data;

    public Doit(DHashTableServer server, int port, Map<String, String> data) {
        this.port = port;
        this.data = data;
    }

    @Override
    public void run() {
        Socket socket = null;
        try {
            ServerSocket serverSocket = new ServerSocket(port);
            while (true) {
                System.out.println("port " + port + " accept..");
                socket = serverSocket.accept();
                System.out.println("A client connected to ..." + port);
                new DealClient(socket, data).start();

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                socket.close();
            } catch (Exception e) {

            }
        }
    }
}

class DealClient extends Thread {
    private Socket socket;
    private Map<String, String> data;

    public DealClient(Socket socket, Map<String, String> data) {
        this.socket = socket;
        this.data = data;
    }

    @Override
    public void run() {
        try {
            InputStream inputStream = socket.getInputStream();
            OutputStream outputStream = socket.getOutputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] splits = line.split(" ");
                String order = splits[0];
                String key = splits[1];
                String value = null;
                if (order.equals("put")) {
                    value = splits[2];
                    if (value.equals("end")) {
                        System.out.println("continus test is over");
                        writer.write("end\r\n");
                        writer.flush();
                        break;
                    }
                    data.put(key, value);
                    //System.out.println("put successfully");
                    writer.write("1\r\n");
                    writer.flush();
                } else if (order.equals("get")) {
                    value = data.get(key);
                    if (value == null) {
                        writer.write("0\r\n");
                    } else {
                        writer.write(value + "\r\n");
                    }
                    writer.flush();
                } else if (order.equals("del")) {
                    value = data.remove(key);
                    if (value == null) {
                        writer.write("0\r\n");
                    } else {
                        writer.write(value + "\r\n");
                    }
                    writer.flush();
                } else if (order.equals("end")) {
                    System.out.println("contiunus test is over");
                    break;
                } else {
                    System.out.println("Wrong request from client");
                }
            }
            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
