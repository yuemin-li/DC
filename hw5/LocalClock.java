

/** thread: local logical clock 
*   @author yuemin
*/

public class LocalClock implements Runnable {
    public Clock localClock;

    public void run(){
        while(true){
            try {
                Thread.sleep(1000);//realtime clock tick
                int currentCounter = localClock.counter;
                currentCounter += localClock.clockRate;
                localClock.setCounter(currentCounter);
            } catch (InterruptedException e){
                e.printStackTrace();
            }
        }
    }

}
