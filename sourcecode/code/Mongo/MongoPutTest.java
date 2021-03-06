import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.Socket;
import java.util.*;

public class MongoPutTest {
    public static void main(String[] args) throws InterruptedException, FileNotFoundException {

        int clients = 1;
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
    //Thread flag
    private int flag;
    //count of operations, default = 10
    private int count;
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
        //servers.add("127.0.0.1");
        File file = new File("servers.txt");
        Scanner in = new Scanner(new FileInputStream(file));
        for(int i = 0; i < 4; ++i) {
            String line = in.nextLine();
            servers.add(line);
        }
        for(int i = 0; i < servers.size(); ++i) {
            serverCount.add(0);
        }
    }

    public static int abs(int val) {
        return val > 0 ? val : 0 - val;
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
            // connect to mongodb server
            int index = abs(key.hashCode()) % servers.size();
            String serverAddresss = servers.get(index);
            MongoClient mongoClient = new MongoClient(serverAddresss, 27017);
            //connect to database
            MongoDatabase db = mongoClient.getDatabase("test");
            MongoCollection<Document> collection = db.getCollection("foo");

            Document document = new Document().append("key", key).append("value", value);
            collection.insertOne(document);
            mongoClient.close();
        }
        System.out.println(flag + " End: " + System.currentTimeMillis());
                printCount();
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
