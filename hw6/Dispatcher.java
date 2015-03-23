
public class Dispatcher implements Runnable {
    
    public String processID;
    public Currency currency;
    public Clock dis_clock;
    public ZooKeeper zk;
    public String root;    
    public Queue pcQ;
    public Barrier b;
    public Watcher wh;


    /* the producer: whenever watcher receive a datachanged event, pull update into the queue */
    public void pull(Queue pcQ, ZooKeeper zk){
        String update_value = new String(zk.getData(root+"/"+"update", wh, null));//leave a new watch
        pcQ.produce(update_value);
    
    }
    
    public void run(){
        int end_counter = 0;
        while (true){
            String update_value = pcQ.consume();
            if(update_value.equals("updateEnd")){
                end_counter++;
            }else if (!update_value.empty()){
                String[] deltas = update_value.split("/");
                currency.sellRate += Integers.toInt(deltas[0]);
                currency.buyRate += Integers.toInt(deltas[1]);
                //log
                System.out.println("currency is set to "+currency.sellRate+","+currency.buyRate
                                    +"by"+deltas[0]+","+deltas[1]);
            }
            if(end_conter == 3){
                System.out.println("P"+processID+"finished.");
                System.exit(0);
            }
            
        }
    }
    public boolean pull(Wathcher wh, Barrier b, Queue pcQ){
        if (b.enter()){
            String update_value = zk.getData(wh);
            pcQ.produce(update_value);
        }
        return b.leave();

    }
    public 

        
}
