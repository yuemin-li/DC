

/** currency object storing currency sell rate and buy rate in pair 
*   @author yuemin
*/

public class Currency{

    private int buyRate;
    private int sellRate;
    
    //no need to be synchronized methods, 
    //only visit when msg is hand to process.
    public void setBuyRate(int i){
        this.buyRate = i;
    }
    public void setSellRate(int i){
        this.sellRate = i;
    }
    public int getBuyRate(){
        return this.buyRate;
    }
    public int getSellRate(){
        return this.sellRate;
    }
    
}
