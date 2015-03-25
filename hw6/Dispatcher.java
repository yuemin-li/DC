import java.io.FileNotFoundException;
import java.io.DataOutputStream;
import java.util.List;
import java.lang.InterruptedException;
import java.lang.Integer;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

public class Dispatcher implements Runnable {
    
    //public String address;
    public ZooKeeper zk;
    
    public String processID;
    public Currency currency;
    public Clock dis_clock;
    public String root;    


    
    public void run(){
        
        //File update_log = new File("log"+processID+".txt");
        //FileOutputStream update_output = new FileOutputStream(update_log, true);

        try{
        int end_counter = 0;
        int update_counter = 0;
        while (true){
            if(zk.exists(root+"/"+"update"+update_counter, false)!=null){
                String update_value = new String(zk.getData(root+"/"+"update"+update_counter, false, null));
                update_counter++;
                if(update_value.equals("updateEnd")){
                    end_counter++;
                } else {
                    String[] deltas = update_value.split(",");
                    currency.setSellRate(Integer.parseInt(deltas[0]));
                    currency.setBuyRate(Integer.parseInt(deltas[1]));
                        //log
                    System.out.println("currency is set to "+currency.getSellRate()+","+currency.getBuyRate()
                                   +"by"+deltas[0]+","+deltas[1]);
                }
                if(end_counter == 2){
                    System.out.println("all finished.");
                    System.exit(0);
                }   
            }
        }
        } catch (KeeperException e){
            e.printStackTrace();
        } catch (InterruptedException e){
            e.printStackTrace();
        } /*catch (IOException e){
            e.printStackTrace();
        }*/
    }    
}
