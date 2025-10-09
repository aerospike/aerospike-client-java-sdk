package com.aerospike.client.fluent.query;

import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.info.classes.IndexState;
import com.aerospike.client.fluent.info.classes.IndexType;

public class IndexQueryHelper {

    public static class Index {
        private String ns;
        private String indexname;
        private String set;
        private String bin;
        private IndexType type;
        private String indextype;
        private String context;
        private String exp;
        private IndexState state;
        private int entriesPerBval;

        public void set(String key, String value) {
            switch (key) {
            case "ns":
                this.ns = value;
                break;
            case "indexname":
                this.indexname = value;
                break;
            case "set":
                this.set = value;
                break;
            case "bin":
                this.bin = value;
                break;
            case "type":
                this.type = IndexType.fromString(value);
                break;
            case "indextype":
                this.indextype = value;
                break;
            case "context":
                this.context = value;
                break;
            case "exp":
                this.exp = value;
                break;
            case "state":
                this.state = IndexState.fromString(value);
                break;
            case "entriesPerBval":
                this.entriesPerBval = Integer.parseInt(value);
                break;
            default:
                throw new IllegalArgumentException("Unknown key: " + key);
            }
        }


        public String getNs() {
            return ns;
        }

        public String getIndexname() {
            return indexname;
        }

        public String getSet() {
            return set;
        }

        public String getBin() {
            return bin;
        }

        public IndexType getType() {
            return type;
        }

        public String getIndextype() {
            return indextype;
        }

        public String getContext() {
            return context;
        }

        public String getExp() {
            return exp;
        }

        public IndexState getState() {
            return state;
        }

        public int getEntriesPerBval() {
            return entriesPerBval;
        }

        @Override
        public String toString() {
            return "Index [ns=" + ns + ", indexname=" + indexname + ", set=" + set + ", bin=" + bin + ", type=" + type
                    + ", indextype=" + indextype + ", context=" + context + ", exp=" + exp + ", state=" + state + "]";
        }
    }

    private final Session session;
    public IndexQueryHelper(Session session) {
        this.session = session;
    }

//    public List<Index> parseIndexes(String input) {
//        return Arrays.stream(input.split(";"))
//                .map(String::trim)
//                .filter(s -> !s.isEmpty())
//                .map(IndexQueryHelper::parseIndex)
//                .collect(Collectors.toList());
//    }

//    private static Map<String, String> parseIndex(String indexStr) {
//        Map<String, String> map = Arrays.stream(indexStr.split(":"))
//            .map(s -> s.split("=", 2))
//            .filter(kv -> kv.length == 2)
//            .collect(Collectors.toMap(kv -> kv[0].trim(), kv -> kv[1].trim()));
//
//        Index index = new Index();
//        map.forEach(index::set);
//        return index;
//    }
//
//    private Index populateEntriesPerBval(Index index) {
//        Node[] nodex = session.getClient().getNodes();
//
//
//    }
//    private static Map<String, String> parseInfoResponse(String indexStr) {
//        Arrays.stream(indexStr.split(":"))
//              .map(pair -> pair.split("=", 2)) // split only on the first '='
//              .filter(kv -> kv.length == 2)
//              .forEach(kv -> index.set(kv[0].trim(), kv[1].trim()));
//
//        return index;
//    }
//
//    public static void main(String[] args) {
//        String test = "ns=test:indexname=user_state_idx:set=users:bin=profile:type=string:indextype=mapvalues:context=[map_key(<string#7>)]:exp=null:state=RW;ns=test:indexname=name_idx:set=person:bin=name:type=string:indextype=default:context=null:exp=null:state=RW;ns=test:indexname=age_idx:set=person:bin=age:type=numeric:indextype=default:context=null:exp=null:state=RW";
//        IndexQueryHelper.parseIndexes(test).forEach(System.out::println);
//    }

}
