package emr.hbase.options;

import com.google.common.collect.*;
import java.util.*;

public class Options
{
    List<Option> options;
    List<String> extrArgs;
    
    public Options() {
        super();
        this.options = Lists.newArrayList();
        this.extrArgs = Lists.newArrayList();
    }
    
    public OptionWithArg withArg(final String arg, final String description) {
        final OptionWithArg option = new OptionWithArg(arg, description);
        this.options.add(option);
        return option;
    }
    
    public SimpleOption noArg(final String arg, final String description) {
        final SimpleOption option = new SimpleOption(arg, description);
        this.options.add(option);
        return option;
    }
    
    public Option add(final Option option) {
        this.options.add(option);
        return option;
    }
    
    public String helpText() {
        final StringBuffer result = new StringBuffer();
        result.append("Options:\n");
        for (final Option option : this.options) {
            result.append("     ");
            result.append(option.helpLine());
            result.append("\n");
        }
        result.append("\n");
        return result.toString();
    }
    
    public void parseArguments(final String[] args) {
        this.parseArguments(args, false);
    }
    
    public void parseArguments(final String[] args, final boolean allowExtraArguments) {
        int matchIndex = 0;
        int prevMatchIndex = 0;
        while (matchIndex < args.length) {
            prevMatchIndex = matchIndex;
            for (final Option option : this.options) {
                matchIndex = option.matches(args, matchIndex);
                if (matchIndex >= args.length) {
                    break;
                }
            }
            if (prevMatchIndex == matchIndex) {
                if (!allowExtraArguments) {
                    throw new RuntimeException("Argument " + args[matchIndex] + " doesn't match.");
                }
                this.extrArgs.add(args[matchIndex]);
                ++matchIndex;
            }
        }
    }
    
    public static void require(final Option... options) {
        try {
            for (final Option option : options) {
                option.require();
            }
        }
        catch (RuntimeException e) {
            System.out.println("Error: " + e.getMessage());
            System.exit(1);
        }
    }
}
