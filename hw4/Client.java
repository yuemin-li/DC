/*
 * Author: Yuemin Li
 * Description: A 2-layer DNS server to store name and ip resolution, leveraging zookeeper APIs.
 */
import java.io.IOException;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.io.FileOutputStream;
import java.io.OutputStream;


import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs.Ids;

public class Client {
    private static final int SESSION_TIMEOUT = 10000;
    private ZooKeeper zk;
    private Watcher wh = new Watcher() {
        public void process (WatchedEvent evet) {
            //do nothing
        }
    };

    private void createZKInstance(String zkServer_ip, String zkServer_port) {
	try {
            zk = new ZooKeeper(zkServer_ip+":"+zkServer_port, this.SESSION_TIMEOUT, this.wh);
        } catch (IOException e) {
            System.out.println("Cannot create ZooKeeper Instance");
            System.out.println("Got an exception:" + e.getMessage());
        }
    }
    
    private void storeLayers(String layer1filename, String layer2filename){
        File layer1txt = new File(layer1filename);
        try{
            Scanner l1scanner = new Scanner(layer1txt);
            while (l1scanner.hasNextLine()){//layer1
                String line1 = l1scanner.nextLine();
                //mapping[0] = "domain name", mapping[1] = ip addr.
                String[] mapping1 = line1.split("\\s+");
                try{//create layer1 znode
                    zk.create("/"+mapping1[0], mapping1[1].getBytes(), 
                                    Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    System.out.println(new String(zk.getData("/"+mapping1[0], false, null)));
                    }catch (InterruptedException e) {
                        System.out.println("Cannot use ZooKeeper API");
                        System.out.println("Got an exception:" + e.getMessage());
                    } catch (KeeperException e) {
                        System.out.println("Cannot use ZooKeeper API");
                        System.out.println("Got an exception:" + e.getMessage());
                    }
                //each layer1 znode store a copy of layer2txt, 
                //here we only have a same copy for all layer2 resolution
                File layer2txt = new File(layer2filename);
                try{
                    Scanner l2scanner = new Scanner(layer2txt);
                    while(l2scanner.hasNextLine()){
                        String line2 = l2scanner.nextLine();
                        String[] mapping2 = line2.split("\\s+");
                        try{
                            zk.create("/"+mapping1[0]+"/"+mapping2[0], mapping2[1].getBytes(), 
                                                    Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                            System.out.println(new String(zk.getData("/"+mapping1[0]+"/"+mapping2[0], false, null)));
                        }catch (InterruptedException e) {
                            System.out.println("Cannot use ZooKeeper API");
                            System.out.println("Got an exception:" + e.getMessage());
                        } catch (KeeperException e) {
                            System.out.println("Cannot use ZooKeeper API");
                            System.out.println("Got an exception:" + e.getMessage());
                        }
                    }
                    l2scanner.close();
                }catch (FileNotFoundException e) {
                    e.printStackTrace();
                }

            }
            l1scanner.close();
        }catch(FileNotFoundException e) {
            e.printStackTrace();
        }
    }
/*
    private void useZKAPI() {
        try {
            zk.create("/DC_Homework", "all homework".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println(new String(zk.getData("/DC_Homework", false, null)));
            zk.setData("/DC_Homework", "no homework".getBytes(), -1);
            System.out.println(new String(zk.getData("/DC_Homework", false, null)));
       } catch (InterruptedException e) {
            System.out.println("Cannot use ZooKeeper API");
            System.out.println("Got an exception:" + e.getMessage());
       } catch (KeeperException e) {
            System.out.println("Cannot use ZooKeeper API");
            System.out.println("Got an exception:" + e.getMessage());
       }

    }
*/
    private void closeZKInstance() {
	try {
            zk.close();
        } catch (InterruptedException e) {
            System.out.println("Cannot close ZooKeeper Instance");
            System.out.println("Got an exception:" + e.getMessage());
        }
    }

    private void getDNS(String testfile){
        File inputfile = new File(testfile);
        File outputfile = new File("DNS_log.txt");
        try {
            OutputStream out = new FileOutputStream(outputfile, true);
            Scanner inputfile_scanner = new Scanner(inputfile);
            while (inputfile_scanner.hasNextLine()){
                //"www.swlaw.edu" --> {"www", "swlaw", "edu"} --> "/edu/www.swlaw" --> IP
                String website = inputfile_scanner.nextLine();
                String[] webdomains = website.split("\\.");
                String dirname_zk = "/"+webdomains[2]+"/"+webdomains[0]+"."+webdomains[1];
                String IP = new String(zk.getData(dirname_zk, false, null));

                out.write(website.getBytes());
                out.write("         ".getBytes());
                out.write(IP.getBytes());
                out.write('\r');
                out.write('\n');
                System.out.println("resoluted result: " + website + "         "+ IP);
            } 
            inputfile_scanner.close();
            out.close();       
        } catch (Exception e){
            System.err.println("Client exception: " + e.toString());
            e.printStackTrace();
        }
    }
    public static void main (String[] args) throws IOException {
        String first_layer_file = args[0];
        String second_layer_file = args[1];
        String zkServer_ip = args[2];
        String zkServer_port = args[3];
        String input_file = args[4];

        Client test = new Client();

        test.createZKInstance(zkServer_ip, zkServer_port);
        test.storeLayers(first_layer_file, second_layer_file);
        test.getDNS(input_file);
        test.closeZKInstance();
    }
}
