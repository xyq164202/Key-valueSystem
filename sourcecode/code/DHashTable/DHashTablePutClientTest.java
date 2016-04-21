import java.io.*;
import java.net.Socket;
import java.util.*;

public class DHashTablePutClientTest {
    public static void main(String[] args) throws InterruptedException, FileNotFoundException {

        int clients = 2;
        int count = 10;
        if(args.length > 0) {
            clients = Integer.parseInt(args[0]);
            count = Integer.parseInt(args[1]);
        }
        for(int i = 0; i < clients; ++i) {
            new DHashTableClientDo(i, count).start();
        }
        while(true) {
            Thread.sleep(10000);
        }
    }
}

class DHashTableClientDo extends Thread{
    //means the number of the server
    private int flag;
    //count of operations, default = 10
    private int count;
    private BufferedReader reader;
    private BufferedWriter writer;
    private List<Integer> serverCount = new ArrayList<Integer>();
    private Map<Integer, Socket> sockets = new HashMap<>();
    private List<String> servers = new ArrayList<>();


    public DHashTableClientDo(int flag, int count) throws FileNotFoundException {
        this.count = count;
        this.flag = flag;
        //servers.add("127.0.0.1");
        servers.add("54.169.186.82");
        servers.add("54.169.131.8");
        /*
        File file = new File("servers.txt");
        Scanner in = new Scanner(new FileInputStream(file));
        for(int i = 0; i < 4; ++i) {
            String line = in.nextLine();
            servers.add(line);
        }
        */
        for(int i = 0; i < servers.size(); ++i) {
            serverCount.add(0);
        }
    }

    public static int abs(int val) {
        return val > 0 ? val : 0 - val;
    }

    public Socket connect(int port, int index) {
        Socket socket = null;
        try {
            socket = sockets.get(index);
            String serverAddress = servers.get(index);
            if(socket == null) {
                System.out.println("connect to " + serverAddress + ", " + port);
                socket = new Socket(serverAddress, port);
                sockets.put(index, socket);
            }
            serverCount.set(index, serverCount.get(index) + 1);
            return socket;
        } catch (Exception e) {
            //return connect(port, index);
            e.printStackTrace();
            return null;
        }
    }
    public boolean put(String key, String value) {
        if(key == null) {
            return false;
        }
        String res = null;
        Socket socket = null;
        try {
            //int port = 8000 + key.hashCode() % 8;
            int port = 8000;
            if(port < 8000 || port > 8007) {
                System.out.println("port wrong, port = " + port + ", key = " + key);
                return false;
            }
            int index1 = abs(key.hashCode()) % servers.size();
            //System.out.println("choose " + index1);
            socket = connect(port, index1);
            InputStream inputStream = socket.getInputStream();
            OutputStream outputStream = socket.getOutputStream();
            BufferedReader reader1 = new BufferedReader(new InputStreamReader(inputStream));
            BufferedWriter writer1 = new BufferedWriter(new OutputStreamWriter(outputStream));
            writer1.write("put " + key + " " + value + "\r\n");
            writer1.flush();
            res = reader1.readLine();
            //socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            try {
                //socket.close();
            } catch (Exception e) {
            }
        }
        if(res.equals("1")) {
            return true;
        }
        else {
            return false;
        }
    }
    public String get(String key) {
        if(key == null) {
            return null;
        }
        String res = null;
        try {
            //int port = 8000 + key.hashCode() % 8;
            int port = 8000;
            int index1 = abs(key.hashCode()) % servers.size();
            Socket socket = connect(port, index1);
            writer.write("get " + key + "\r\n");
            writer.flush();
            res = reader.readLine();
            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        if(res.equals("0")) {
            return null;
        }
        else {
            return res;
        }
    }
    public String del(String key) {
        if(key == null) {
            return null;
        }
        String res = null;
        try {
            int port = 8000;
            int index1 = abs(key.hashCode()) % servers.size();
            Socket socket = connect(port, index1);
            writer.write("del " + key + "\r\n");
            writer.flush();
            res = reader.readLine();
            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        if(res.equals("0")) {
            return  null;
        }
        else {
            return res;
        }
    }
    public void printCount() {
        System.out.println("Count of different servers: ");
        for(int i = 0; i < serverCount.size(); ++i) {
            System.out.print(serverCount.get(i) + " ");
        }
        System.out.println();
    }

    @Override
    public void run() {
        System.out.println(flag + " Begin: " + System.currentTimeMillis());
        for(int i = 0; i < count; ++i) {
            String key = randKey();
            String value = "123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890";
            put(key, value);
            System.out.println(flag + "put OK");
            if(i % 1000 == 0) {
                //System.out.println("finish 100");
                //System.out.println(new Date());
            }
        }
        put("end", "end");
        System.out.println(flag + " End: " + System.currentTimeMillis());
        //printCount();
    }
    public static String randKey() {
        Random random = new Random(System.currentTimeMillis());
        final String str = "abcdefghijklmnopqrstuvwxyz";
        StringBuilder builder = new StringBuilder();
        for(int i = 0; i < 9; ++i) {
            int subStrIndex = random.nextInt(26) + 1;
            builder.append(str.substring(subStrIndex - 1, subStrIndex));
        }
        return builder.toString();
    }

}
