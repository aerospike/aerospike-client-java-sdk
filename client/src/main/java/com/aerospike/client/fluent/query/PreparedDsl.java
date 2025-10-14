package com.aerospike.client.fluent.query;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PreparedDsl {
    private final String statement;
    public PreparedDsl(String statement) {
        this.statement = statement;
    }
    
    protected String formValue(Object ...params) {
        Pattern pattern = Pattern.compile("\\$(\\d+)");
        Matcher matcher = pattern.matcher(statement);
        StringBuffer sb = new StringBuffer();

        while (matcher.find()) {
            int index = Integer.parseInt(matcher.group(1)) - 1;
            String replacement = (index >= 0 && index < params.length && params[index] != null)
                    ? params[index].toString()
                    : matcher.group(); // leave unchanged if no param

            // quote to avoid treating $ or \ in replacement specially
            matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }

        matcher.appendTail(sb);
        return sb.toString();
    }
}