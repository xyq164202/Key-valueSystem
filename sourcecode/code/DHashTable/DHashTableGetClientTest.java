import java.io.*;
import java.net.Socket;
import java.util.*;

public class DHashTableGetClientTest {
    public static void main(String[] args) throws InterruptedException, FileNotFoundException {

        int count = 10;
        int clients = 1;
        if(args.length > 0) {
            clients = Integer.parseInt(args[0]);
            count = Integer.parseInt(args[1]);
        }
        //put data to servers
        DHashTableClientDo putClient = new DHashTableClientDo(0, count);
        final String value = "123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890";
        int putCount = count * 1;
        for(int i = 0; i < putCount; ++i) {
            String key = DHashTableClientDo.randKey();
            putClient.put(key, value);
        }
        //putClient.put("end", "end");
        putClient.finshPut();
        System.out.println("finished put");

        for(int j = 0; j < clients; ++j) {
            new DHashTableClientDo(j, count).start();
        }
        while(true) {
            Thread.sleep(10000);
        }

        /*
        //test randKey() method
        for(int i = 0; i < 10; ++i) {
            System.out.println(DHashTableClientDo.randKey());
        }
        //confirm the size of the string
        String value = "123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890";
        byte[] bytes = value.getBytes();
        System.out.println("size: " + bytes.length);
        */
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
        /*
        servers.add("52.34.126.255");
        servers.add("52.33.158.25");
        */
        File file = new File("servers.txt");
        Scanner in = new Scanner(new FileInputStream(file));
        for(int i = 0; i < 4; ++i) {
            String line = in.nextLine();
            servers.add(line);
        }
        //servers.add("127.0.0.1");
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
                //System.out.println("connect to " + serverAddress + ", " + port);
                socket = new Socket(serverAddress, port);
                sockets.put(index, socket);
            }
            serverCount.set(index, serverCount.get(index) + 1);
            return socket;
        } catch (Exception e) {
            return connect(port, index);
        }
    }
    public void finshPut() {
        try {
            //int port = 8000 + key.hashCode() % 8;
            int port = 8000;
            //System.out.println("choose " + index1);
            for(int i = 0; i < sockets.size(); ++i) {
                Socket socket = sockets.get(i);
                InputStream inputStream = socket.getInputStream();
                OutputStream outputStream = socket.getOutputStream();
                BufferedReader reader1 = new BufferedReader(new InputStreamReader(inputStream));
                BufferedWriter writer1 = new BufferedWriter(new OutputStreamWriter(outputStream));
                writer1.write("put " + "end" + " " + "end" + "\r\n");
                writer1.flush();
                String res = reader1.readLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
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
            //System.out.println("choose " + index1 + ", " + port);
            Socket socket = connect(port, index1);
            InputStream inputStream = socket.getInputStream();
            OutputStream outputStream = socket.getOutputStream();
            BufferedReader reader1 = new BufferedReader(new InputStreamReader(inputStream));
            BufferedWriter writer1 = new BufferedWriter(new OutputStreamWriter(outputStream));
            writer1.write("get " + key + "\r\n");
            writer1.flush();
            res = reader1.readLine();
            //System.out.println("get: " + res);
            //socket.close();
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
        long begin = System.currentTimeMillis();
        System.out.println(flag + " Begin: " + begin);
        for(int i = 0; i < count; ++i) {
            String key = randKey();
            //String value = "123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890";
            String res = get(key);
            //System.out.println("get: " + res);
            if(i % 1000 == 0) {
                //System.out.println("finish 100");
                //System.out.println(new Date());
            }
        }
        System.out.println(flag + " End - begin : " + (System.currentTimeMillis() - begin));
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
