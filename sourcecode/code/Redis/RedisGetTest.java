import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.Socket;
import redis.clients.jedis.Jedis;

import java.util.*;

public class RedisGetTest {
    public static void main(String[] args) throws InterruptedException, FileNotFoundException {

        int clients = 1;
        int count = 1;
        if (args.length > 0) {
            clients = Integer.parseInt(args[0]);
            count = Integer.parseInt(args[1]);
        }

        //put datas
        DoIt doIt = new DoIt(0, 0);
        for (int i = 0; i < count; ++i) {
            doIt.put();
        }
        for(int i = 0; i < clients; ++i) {
            new DoIt(i, count).start();
        }
        System.out.println("OK");

        while (true) {
            Thread.sleep(10000);
        }
    }
}

class DoIt extends Thread {
    //Thread flag
    private int flag;
    //count of operations, default = 10
    private int count;
    private List<Integer> serverCount = new ArrayList<Integer>();
    private Map<Integer, Socket> sockets = new HashMap<>();
    private List<String> servers = new ArrayList<>();


    public DoIt(int flag, int count) throws FileNotFoundException {
        this.count = count;
        this.flag = flag;
        /*
        servers.add("52.34.126.255");
        servers.add("52.33.158.25");
        */
        //servers.add("127.0.0.1");
        File file = new File("servers.txt");
        Scanner in = new Scanner(new FileInputStream(file));
        for(int i = 0; i < 4; ++i) {
            String line = in.nextLine();
            servers.add(line);
        }
        for (int i = 0; i < servers.size(); ++i) {
            serverCount.add(0);
        }
    }

    public static int abs(int val) {
        return val > 0 ? val : 0 - val;
    }

    public void printCount() {
        System.out.println("Count of different servers: ");
        for (int i = 0; i < serverCount.size(); ++i) {
            System.out.print(serverCount.get(i) + " ");
        }
        System.out.println();
    }

    public void put() {
        //choose server
        String key = randKey();
        String value = "123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890";
        int index = abs(key.hashCode()) % servers.size();
        String serverAddresss = servers.get(index);
        Jedis jedis = new Jedis(serverAddresss);
        jedis.set(key, value);
        jedis.close();
    }

    @Override
    public void run() {
        System.out.println(flag + " Begin: " + System.currentTimeMillis());
        for (int i = 0; i < count; ++i) {
            String key = randKey();
            //String key = "keytest";
            String value = "123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890";
            // connect to Redis server
            int index = abs(key.hashCode()) % servers.size();
            String serverAddresss = servers.get(index);
            Jedis jedis = new Jedis(serverAddresss);
            jedis.set(key, value);
            jedis.close();
        }
        System.out.println(flag + " End: " + System.currentTimeMillis());
        //printCount();
    }

    public static String randKey() {
        Random random = new Random(System.currentTimeMillis());
        final String str = "abcdefghijklmnopqrstuvwxyz";
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 9; ++i) {
            int subStrIndex = random.nextInt(26) + 1;
            builder.append(str.substring(subStrIndex - 1, subStrIndex));
        }
        return builder.toString();
    }

}
