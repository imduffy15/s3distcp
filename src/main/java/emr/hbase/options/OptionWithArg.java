package emr.hbase.options;

public class OptionWithArg extends OptionBase implements Option
{
    public String value;
    
    public OptionWithArg(final String arg, final String desc) {
        super(arg, desc);
    }
    
    @Override
    public int matches(final String[] arguments, final int matchIndex) {
        final String argument = arguments[matchIndex];
        if (argument.equals(this.arg)) {
            if (matchIndex + 1 < arguments.length) {
                this.value = arguments[matchIndex + 1];
                return matchIndex + 2;
            }
            throw new RuntimeException("expected argument for " + this.arg + " but no argument was given");
        }
        else {
            if (argument.length() >= this.arg.length() + 1 && argument.substring(0, this.arg.length() + 1).equals(this.arg + "=")) {
                this.value = argument.substring(this.arg.length() + 1);
                return matchIndex + 1;
            }
            if (argument.length() >= this.arg.length() + 2 && argument.substring(0, this.arg.length() + 2).equals(this.arg + "==")) {
                this.value = argument.substring(this.arg.length() + 2);
                return matchIndex + 1;
            }
            return matchIndex;
        }
    }
    
    @Override
    public String helpLine() {
        return this.arg + "=VALUE   -   " + this.desc;
    }
    
    @Override
    public boolean defined() {
        return this.value != null;
    }
    
    public String getValue(final String defaultValue) {
        if (this.value != null) {
            return this.value;
        }
        return defaultValue;
    }
}
