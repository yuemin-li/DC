import java.io.IOException;
import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.net.Socket;



/** dispatcher thread that recv update msg, multicast ack
*   and forward acked update to application 
*   @author yuemin
*/

public class Dispatcher implements Runnable {
    
    //thread safe
    public static List<Msg> msgQ = Collections.synchronizedList(new ArrayList<Msg>());
    public static List<Msg> msgAck = Collections.synchronizedList(new ArrayList<Msg>());
    
    public int processID;
    public Currency currency;
    public Clock dis_clock;
    public ArrayList<Socket> clients;
    
    /* entrance of the thread */
    public void run(){

        int op = 1;//update operation counter
        int end = 0;//end msg counter

        //set in and out stream for each client socket
        ArrayList<DataOutputStream> out_stream = new ArrayList<DataOutputStream>();
        ArrayList<DataInputStream> in_stream = new ArrayList<DataInputStream>();
        for(int i=0; i<clients.size(); i++){
            try{
                DataOutputStream out = new DataOutputStream(clients.get(i).getOutputStream());
                DataInputStream in = new DataInputStream(clients.get(i).getInputStream());
                out_stream.add(out);
                in_stream.add(in);
            } catch (IOException e){
                e.printStackTrace();
            }
        }
        
        //processing msg
        while(true){
            for(int i=0; i<clients.size(); i++){
                Msg msg_coming = new Msg();
                try{
                if (in_stream.get(i).available() != 0){//available returns an estimate of bytes that can read
                    //parsing msg
                    dis_clock.increaseStamp();//recv msg, increase local clock by 1
                    String msg_coming_str = in_stream.get(i).readUTF();
                    msg_coming.parsingMsg(msg_coming_str);
                   
                    if (msg_coming.type == 1 || msg_coming.type == 0){//an update msg
                        synchronized (msgQ){
                            msgQ.add(msg_coming);
                        }
                        msg_coming.ack_num++;//the destination ack
                        
                        //if there is already some acks for this update msg
                        for(int m=0; m<msgAck.size(); m++){
                            if(msg_coming.msgId.equals(msgAck.get(m).msgId)){
                                msg_coming.ack_num++;
                                msgAck.remove(m);//save memory space of ack queue
                                m--;
                            }
                        }
                        
                        //sending ack msg
                        Msg msg_ack = new Msg();
                        msg_ack.type = 2; //ending:0; update:1; ack:2
                        msg_ack.processID = processID;
                        msg_ack.msgId = msg_coming.msgId;
                        msg_ack.deltaBuy = 0;//no need for ack msg
                        msg_ack.deltaSell = 0;//no need for ack msg
                        msg_ack.timestamp = dis_clock.counter+0.1*processID;//vetor timestamp     
                        
                        String msg_ack_str = msg_ack.sendingMsg();
                        dis_clock.increaseStamp();//every time we send msg, increase local clock by 1
                       
                        //sending ack msg through output stream
                        for(int j=0; j<clients.size(); j++){
                            out_stream.get(j).writeUTF(msg_ack_str);
                        } 
                    } else if (msg_coming.type == 2){//an ack msg
                        if(msgQ.isEmpty()){// there is a update msg on the way
                            msgAck.add(msg_coming);
                        }else{//locate the target update msg in msgQ
                            boolean no_match = true;
                            for (int k=0; k<msgQ.size(); k++){
                                if(msg_coming.msgId.equals(msgQ.get(k).msgId)){
                                    msgQ.get(k).ack_num++;
                                    no_match = false;
                                    break;
                                }
                            }
                            if (no_match){
                                msgAck.add(msg_coming);
                            }
                        
                        }
                    } else {//an ending msg
                    
                    }
                    synchronized (msgQ){
                        Collections.sort(msgQ);
                    }
        
                    /* processing the update msg */
                    //open log
                    File update_log = new File("log"+processID+".txt");
                    FileOutputStream output = new FileOutputStream(update_log, true);

                    while (!msgQ.isEmpty() && msgQ.get(0).ack_num == clients.size()){
                        if(msgQ.get(0).type == 1){
                            this.currency.setBuyRate(this.currency.getBuyRate()+msgQ.get(0).deltaBuy);
                            this.currency.setSellRate(this.currency.getSellRate()+msgQ.get(0).deltaSell);
                            String update_str = Lamport.realTime() + " [OP" + op
                                                + ":C" + dis_clock.counter
                                                + "] Currency value is set to ("
                                                + this.currency.getSellRate() + ", "
                                                + this.currency.getBuyRate() + ") by ("
                                                + msgQ.get(0).deltaSell + ", "
                                                + msgQ.get(0).deltaBuy + ").\n";
                            update_output.write(update_str.getBytes());
                            System.out.println(update_str);
                            msgQ.remove(0);
                            op++;
                        }else{//end msg
                                String ending = "P" + msgQ.get(0).processID + " finished.\n";
                                update_output.write(ending.getBytes());
                                System.out.println(ending);
                                msgQ.remove(0);
                                end++;
                        }
                    }
                    //stop this process
                    if (end == clients.size() + 1 && msgQ.isEmpty()) {
                        String all_end = "All finished, P" + this.processID + " is terminating...\n";
                        update_output.write(all_end.getBytes());
                        System.out.println(all_end);

                        System.exit(0);
                    } 
                    update_output.close();           
                }
                }catch(IOException e){
                    e.printStackTrace();
                }
            }
        }
        

 
    }

}
