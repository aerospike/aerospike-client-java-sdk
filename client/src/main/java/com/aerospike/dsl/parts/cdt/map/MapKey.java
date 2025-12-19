package com.aerospike.dsl.parts.cdt.map;

import static com.aerospike.dsl.util.ParsingUtils.unquote;

import com.aerospike.client.fluent.Value;
import com.aerospike.client.fluent.cdt.CTX;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.exp.MapExp;
import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.DslParseException;
import com.aerospike.dsl.parts.path.BasePath;

public class MapKey extends MapPart {
    private final String key;

    public MapKey(String key) {
        super(MapPartType.KEY);
        this.key = key;
    }

    public static MapKey from(ConditionParser.MapKeyContext ctx) {
        if (ctx.QUOTED_STRING() != null) {
            return new MapKey(unquote(ctx.QUOTED_STRING().getText()));
        }
        if (ctx.NAME_IDENTIFIER() != null) {
            return new MapKey(ctx.NAME_IDENTIFIER().getText());
        }
        throw new DslParseException("Could not translate MapKey from ctx: %s".formatted(ctx));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        return MapExp.getByKey(cdtReturnType, valueType,
                Exp.val(key), Exp.bin(basePath.getBinPart().getBinName(), basePath.getBinType()), context);
    }

    @Override
    public CTX getContext() {
        return CTX.mapKey(Value.get(key));
    }
}
