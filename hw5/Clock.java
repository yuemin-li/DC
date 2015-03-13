
/** one logical clock for each process 
*   @author yuemin
*/

public class Clock{
    public int clockRate;
    public int counter;

    public void setClockRate(int i){
        this.clockRate = i;
    }

    // all paticipants might visit this sometime, 
    // need to be synchronized
    public synchronized void setCounter(int i){
        this.counter = i;
    }
    public synchronized void increaseStamp(){
        this.counter++;
    }
}
