package emr.hbase.options;

public abstract class OptionBase implements Option
{
    protected String arg;
    protected String desc;
    
    public OptionBase(final String arg, final String desc) {
        super();
        this.arg = arg;
        this.desc = desc;
    }
    
    @Override
    public void require() {
        if (!this.defined()) {
            throw new RuntimeException("expected argument " + this.arg);
        }
    }
}
