package emr.hbase.options;

public class SimpleOption extends OptionBase implements Option
{
    public boolean value;
    
    SimpleOption(final String arg, final String desc) {
        super(arg, desc);
        this.value = false;
    }
    
    @Override
    public int matches(final String[] arguments, final int matchIndex) {
        final String argument = arguments[matchIndex];
        if (argument.equals(this.arg)) {
            this.value = true;
            return matchIndex + 1;
        }
        return matchIndex;
    }
    
    @Override
    public String helpLine() {
        return this.arg + "   -   " + this.desc;
    }
    
    @Override
    public void require() {
        if (!this.value) {
            throw new RuntimeException("expected argument " + this.arg);
        }
    }
    
    @Override
    public boolean defined() {
        return this.value;
    }
}
