

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs.Ids;

public class TotalOrderZK {

    private static final int SESSION_TIMEOUT = 10000;
    public ZooKeeper zk;
    public Worker w;
    public Dispatcher dp;
    public Barrier b;
    public Watcher wh = new Watcher() {
        public void process (WatchedEvent event) {
            switch (event.getType()){
                case NodeChildrenChanged:
                case NodeCreated:
                case NodeDataChanged:
                    if(b.enter()){
                        dp.pull();
                    }
                    b.leave();
                    break;
                case NodeDeleted:
                case None:
                    log.info("Got unexpected zookeeper event: " + event.getType());
                    break;
            }
        }
    };
    

    //connect to zookeeper    
    private void createZKInstance(String address) {
    try {
            zk = new ZooKeeper(address, this.SESSION_TIMEOUT, this.wh);
        } catch (IOException e) {
            System.out.println("Cannot create ZooKeeper Instance");
            System.out.println("Got an exception:" + e.getMessage());
        }
    }
    

    
    //disconnect with zookeeper    
    private void closeZKInstance() {
    try {
            zk.close();
        } catch (InterruptedException e) {
            System.out.println("Cannot close ZooKeeper Instance");
            System.out.println("Got an exception:" + e.getMessage());
        }
    }

    
    public static void main(String args[]) throws IOException {
        //parsing args
        int processID = Integer.parseInt(args[0]);
        int operation_num = Integer.parseInt(args[1]);
        int clockRate = Integer.parseInt(args[2]);
        int zkServer_ip = Integer.parseInt(args[3]);
        int zkServer_port = Integer.parseInt(args[4]);

        String address = zkServer_ip + ":" + zkServer_port;
        String root = "totalOrder";   
        
        //init currency, initial value is (100,100)
        Currency currency = new Currency();
        currency.setBuyRate(100);
        currency.setSellRate(100);

        //init clock
        Clock clock = new Clock();
        clock.setClockRate(clockRate);

        //start local clock
        LocalClock lc = new LocalClock();
        lc.localClock = clock;
        Thread lcthread = new Thread(lc);
        lcthread.start();

     
        Queue pcQ = new Queue();
        Barrier barrier = new Barrier();

        TotalOrderZK to = new TotalOrderZK();
        to.createZKInstance(address); 
        to.b = b;

        Worker worker = new Worker();
        worker.zk = to.zk;
        worker.root = root;
        worker.operation_num = operation_num;
        worker.worker_clock = clock;

        to.w = worker;

        Dispatcher dis = new Dispatcher();
        dis.processID = processID;
        dis.currency = currency;
        dis.dis_clock = clock;
        dis.zk = zk;
        dis.root = root;
        dis.pcQ = pcQ;
        dis.b = barrier;
        dis.wh = to.wh;

        to.dp = dis;

        Thread worker_thread = new Thread(worker);
        worker_thread.start();
        Thread dp_thread = new Thread(dp);
        dp_thread.start();

        
        to.closeZKInstance();
    }

}

