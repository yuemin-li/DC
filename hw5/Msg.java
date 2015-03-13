
/** payload object to send/recv in communication 
*   @author yuemin
*/
public class Msg implements Comparable<Msg>{

    public String msgId;
    public int type;
    public int processID;
    public int deltaBuy;
    public int deltaSell;
    public double timestamp;
    public int ack_num;
    
    //construct the msg payload
    public String sendingMsg(){
        String sending_msg = String.valueOf(this.type) + "//" + String.valueOf(this.processID) + "//" + msgId +
                            "//" + String.valueOf(this.deltaBuy) + "//" + String.valueOf(this.deltaSell) + "//" + 
                            String.valueOf(timestamp) + "//" + String.valueOf(ack_num);
        return sending_msg;
    } 

    //parsing the msg payload
    public void parsingMsg(String msg){
        //System.out.println(msg);
        String[] parsingMsg = msg.split("//");
        this.type = Integer.parseInt(parsingMsg[0]);
        this.processID = Integer.parseInt(parsingMsg[1]);
        this.msgId = parsingMsg[2];
        this.deltaBuy = Integer.parseInt(parsingMsg[3]);
        this.deltaSell = Integer.parseInt(parsingMsg[4]);
        this.timestamp = Double.parseDouble(parsingMsg[5]);
        this.ack_num = Integer.parseInt(parsingMsg[6]);
    }
    
    //override default compareTo method
    public int compareTo(Msg msg){
        if(this.timestamp == msg.timestamp) return 0;
        else if (this.timestamp > msg.timestamp) return 1;
        else return -1;
    }


    
}
