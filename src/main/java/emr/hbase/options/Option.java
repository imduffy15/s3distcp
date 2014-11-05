package emr.hbase.options;

public interface Option
{
    int matches(String[] p0, int p1);
    
    String helpLine();
    
    void require();
    
    boolean defined();
}
