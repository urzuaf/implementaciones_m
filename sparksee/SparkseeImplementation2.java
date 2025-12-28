import com.sparsity.sparksee.gdb.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

public class SparkseeImplementation2 {

    private static final int NODE_BATCH = 50_000;
    private static final int EDGE_BATCH = 100_000;

    private final Map<String, Integer> nodeTypeIds = new HashMap<>();
    private final Map<String, Integer> edgeTypeIds = new HashMap<>();
    private final Map<Integer, Map<String, Integer>> attrsByType = new HashMap<>();
    
    private final Map<String, Long> oidByExtId = new HashMap<>(200_000);

    private int extIdAttr = Attribute.InvalidAttribute;

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            usage();
        }

        Map<String, String> params = new HashMap<>();
        List<String> benchFiles = new ArrayList<>();
        boolean isBench = false;

        for (int i = 0; i < args.length; i++) {
            if ("-bench".equals(args[i])) {
                isBench = true;
                if (i + 3 < args.length) {
                    benchFiles.add(args[i+1]);
                    benchFiles.add(args[i+2]);
                    benchFiles.add(args[i+3]);
                    i += 3;
                }
            } else if (args[i].startsWith("-") && i + 1 < args.length) {
                params.put(args[i], args[i + 1]);
                i++;
            }
        }

        SparkseeImplementation2 app = new SparkseeImplementation2();

        if (params.containsKey("-n") && params.containsKey("-e") && params.containsKey("-d")) {
            System.out.println("Starting Ingest...");
            app.runIngest(params.get("-n"), params.get("-e"), params.get("-d"));
            return;
        }

        if (isBench && params.containsKey("-d")) {
            if (benchFiles.size() < 3) usage();
            Path idsPath = Paths.get(benchFiles.get(0));
            Path labelsPath = Paths.get(benchFiles.get(1));
            Path propsPath = Paths.get(benchFiles.get(2));
            app.benchmarking(params.get("-d"), idsPath, labelsPath, propsPath);
            return;
        }

        if (params.containsKey("-d")) {
            String dbPath = params.get("-d");

            if (params.containsKey("-g")) {
                app.runGetNode(dbPath, params.get("-g"));
                return;
            }
            if (params.containsKey("-gel")) {
                app.runGetEdgeIdsByLabel(dbPath, params.get("-gel"));
                return;
            }
            if (params.containsKey("-nv")) {
                app.runFindNodesByAttrAcrossTypes(dbPath, params.get("-nv"));
                return;
            }
        }

        usage();
    }

    private static void usage() {
        System.err.println("""
            Usage:
              Ingest:    
                java ... -n Nodes.pgdf -e Edges.pgdf -d graph.gdb
              
              Benchmark: 
                java ... -d graph.gdb -bench <ids_file> <labels_file> <props_file>

              Queries:
                java ... -d graph.gdb -g <node_id>
                java ... -d graph.gdb -gel <label>
                java ... -d graph.gdb -nv <attr=val>
            """);
        System.exit(1);
    }

    public void benchmarking(String dbPath, Path idsPath, Path labelsPath, Path propsPath) throws Exception {
        List<String> ids = Files.readAllLines(idsPath);
        List<String> labels = Files.readAllLines(labelsPath);
        List<String> props = Files.readAllLines(propsPath);

        System.out.println("Benchmark...");

        SparkseeConfig cfg = new SparkseeConfig("sparksee.cfg");
        Sparksee sparksee = new Sparksee(cfg);
        Database db = null;
        Session sess = null;

        try {
            db = sparksee.open(dbPath, true); 
            sess = db.newSession();
            Graph g = sess.getGraph();

            int targetAttr = g.findAttribute(Type.NodesType, "ext_id");
            if (targetAttr == Attribute.InvalidAttribute) {
                System.err.println("Error: 'ext_id' not found.");
                return;
            }

            Value vArg = new Value();
            Value vRes = new Value();

            long totalGetNode = 0;
            System.out.println("\ngetNode");
            for (String id : ids) {
                long t0 = System.nanoTime();
                
                long oid = g.findObject(targetAttr, vArg.setString(id));
                if (oid != com.sparsity.sparksee.gdb.Objects.InvalidOID) {
                    AttributeList attrs = g.getAttributes(oid);
                    for (Integer attrId : attrs) {
                        g.getAttribute(oid, attrId, vRes);
                    }
                }
                
                long t1 = System.nanoTime();
                long diff = t1 - t0;
                totalGetNode += diff;
                System.out.printf(Locale.ROOT, "ID %s: %.4f ms%n", id, diff / 1e6);
            }

            long totalLabels = 0;
            System.out.println("\ngetEdgesByLabel");
            for (String label : labels) {
                AtomicLong count = new AtomicLong();
                long t0 = System.nanoTime();
                
                int edgeType = g.findType(label);
                if (edgeType != Type.InvalidType) {
                    int eidAttr = g.findAttribute(edgeType, "eid");
                    com.sparsity.sparksee.gdb.Objects objs = g.select(edgeType);
                    ObjectsIterator it = objs.iterator();
                    while (it.hasNext()) {
                        long eoid = it.next();
                        if (eidAttr != Attribute.InvalidAttribute) {
                            g.getAttribute(eoid, eidAttr, vRes);
                        }
                        count.incrementAndGet();
                    }
                    it.close();
                    objs.close();
                }
                
                long t1 = System.nanoTime();
                long diff = t1 - t0;
                totalLabels += diff;
                System.out.printf(Locale.ROOT, "Label %s (found %d): %.4f ms%n", label, count.get(), diff / 1e6);
            }

            long totalProps = 0;
            System.out.println("\nfindNodesByAttr");
            
            TypeList tList = g.findNodeTypes();
            List<Integer> allNodeTypes = new ArrayList<>();
            TypeListIterator tIt = tList.iterator();
            while(tIt.hasNext()) allNodeTypes.add(tIt.next());
            
            for (String line : props) {
                String[] kv = line.split("=", 2);
                if (kv.length < 2) continue;
                String key = kv[0];
                String val = kv[1];
                
                AtomicLong count = new AtomicLong();
                long t0 = System.nanoTime();

                for (int typeId : allNodeTypes) {
                    int attrId = g.findAttribute(typeId, key);
                    if (attrId == Attribute.InvalidAttribute) continue;

                    DataType dt = g.getAttribute(attrId).getDataType();
                    boolean typeOk = false;
                    try {
                        switch (dt) {
                            case Integer: vArg.setInteger(Integer.parseInt(val)); typeOk = true; break;
                            case Long:    vArg.setLong(Long.parseLong(val));      typeOk = true; break;
                            case Double:  vArg.setDouble(Double.parseDouble(val));typeOk = true; break;
                            case Boolean: vArg.setBoolean(Boolean.parseBoolean(val)); typeOk = true; break;
                            default:      vArg.setString(val); typeOk = true; break;
                        }
                    } catch (Exception ignore) {}

                    if (!typeOk) continue;

                    com.sparsity.sparksee.gdb.Objects objs = g.select(attrId, Condition.Equal, vArg);
                    ObjectsIterator it = objs.iterator();
                    while(it.hasNext()) {
                        long oid = it.next();
                        count.incrementAndGet();
                    }
                    it.close();
                    objs.close();
                }

                long t1 = System.nanoTime();
                long diff = t1 - t0;
                totalProps += diff;
                System.out.printf(Locale.ROOT, "Prop %s=%s (found %d): %.4f ms%n", key, val, count.get(), diff / 1e6);
            }

            System.out.println("\n==========================================");
            System.out.println("BENCHMARK (Sparksee)");
            System.out.println("==========================================");
            printStats("getNode", ids.size(), totalGetNode);
            printStats("getEdgesByLabel", labels.size(), totalLabels);
            printStats("findNodesByAttr", props.size(), totalProps);

        } finally {
            if (sess != null) sess.close();
            if (db != null) db.close();
            sparksee.close();
        }
    }

    private void printStats(String op, int count, long totalNs) {
        double totalMs = totalNs / 1e6;
        double avgMs = totalMs / count;
        System.out.printf(Locale.ROOT, "%-28s | Total: %8.3f ms | Avg: %8.4f ms | Count: %d%n", op, totalMs, avgMs, count);
    }

    private void runIngest(String nodesPath, String edgesPath, String dbPath) throws Exception {
        long startTotal = System.nanoTime();
        
        SparkseeConfig cfg = new SparkseeConfig("sparksee.cfg");
        Sparksee sparksee = new Sparksee(cfg);
        Database db = null;
        Session sess = null;
        try {
            Files.deleteIfExists(Paths.get(dbPath));
            db = sparksee.create(dbPath, "PGDF");
            db.disableRollback();
            sess = db.newSession();
            Graph g = sess.getGraph();

            extIdAttr = ensureAttribute(g, Type.NodesType, "ext_id", DataType.String, AttributeKind.Unique);

            System.out.println("Loading nodes...");
            loadNodes(sess, g, Paths.get(nodesPath));

            System.out.println("Loading edges...");
            loadEdges(sess, g, Paths.get(edgesPath));

            System.out.println("Committing database...");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (sess != null) sess.close();
            if (db != null) db.close();
            sparksee.close();
        }
        
        long endTotal = System.nanoTime();
        System.out.printf(Locale.ROOT, "Ingest Completed. Total Time: %.3f ms%n", (endTotal - startTotal) / 1e6);
    }

    private void loadNodes(Session sess, Graph g, Path nodesFile) throws IOException {
        try (BufferedReader br = Files.newBufferedReader(nodesFile, StandardCharsets.UTF_8)) {
            String line;
            String[] header = null;
            int count = 0;
            Value v = new Value();

            sess.beginUpdate();

            while ((line = br.readLine()) != null) {
                if (line.isBlank()) continue;
                if (line.startsWith("@")) {
                    header = line.split("\\|", -1);
                    continue;
                }
                if (header == null) continue;

                String[] cols = line.split("\\|", -1);
                
                String label = null, extId = null;
                for (int i = 0; i < header.length && i < cols.length; i++) {
                    if ("@label".equals(header[i])) label = cols[i];
                    else if ("@id".equals(header[i])) extId = cols[i];
                }

                if (label == null || extId == null) continue;

                int typeId = ensureNodeType(g, label);
                long oid = g.newNode(typeId);
                
                oidByExtId.put(extId, oid);
                g.setAttribute(oid, extIdAttr, v.setString(extId));

                for (int i = 0; i < header.length && i < cols.length; i++) {
                    String name = header[i];
                    if (name.equals("@id") || name.equals("@label")) continue;
                    
                    String val = cols[i];
                    if (val == null || val.isEmpty()) continue;

                    DataType dt = guessType(name);
                    int attrId = ensureAttribute(g, typeId, name, dt, AttributeKind.Basic);
                    setAttributeValue(g, oid, attrId, dt, val, v);
                }

                count++;
                if (count % NODE_BATCH == 0) {
                    sess.commit();
                    sess.beginUpdate();
                }
            }
            sess.commit();
            System.out.printf("  Nodes loaded: %,d%n", count);
        }
    }

    private void loadEdges(Session sess, Graph g, Path edgesFile) throws IOException {
        try (BufferedReader br = Files.newBufferedReader(edgesFile, StandardCharsets.UTF_8)) {
            String line = br.readLine();
            if (line == null) return;

            String[] header = line.split("\\|", -1);
            Map<String, Integer> idx = new HashMap<>();
            for (int i = 0; i < header.length; i++) idx.put(header[i], i);

            int count = 0;
            Value v = new Value();
            sess.beginUpdate();

            while ((line = br.readLine()) != null) {
                if (line.isBlank() || line.startsWith("@")) continue;

                String[] cols = line.split("\\|", -1);

                String label = safeGet(cols, idx.get("@label"));
                String dir   = safeGet(cols, idx.get("@dir"));
                String out   = safeGet(cols, idx.get("@out"));
                String in    = safeGet(cols, idx.get("@in"));

                if (label == null || out == null || in == null) continue;

                boolean directed = !"F".equalsIgnoreCase(dir);
                int edgeType = ensureEdgeType(g, label, directed);

                Long outOid = oidByExtId.get(out);
                Long inOid  = oidByExtId.get(in);

                if (outOid == null || inOid == null) continue;

                long eoid = g.newEdge(edgeType, outOid, inOid);

                String eid = safeGet(cols, idx.get("@id"));
                if (eid == null) eid = out + "|" + label + "|" + in;
                
                int eidAttr = ensureAttribute(g, edgeType, "eid", DataType.String, AttributeKind.Basic);
                g.setAttribute(eoid, eidAttr, v.setString(eid));

                count++;
                if (count % EDGE_BATCH == 0) {
                    sess.commit();
                    sess.beginUpdate();
                }
            }
            sess.commit();
            System.out.printf("  Edges loaded: %,d%n", count);
        }
    }

    // -g <id>
    private void runGetNode(String dbPath, String wantedId) throws Exception {
        runWithSession(dbPath, (sess, g) -> {
            int targetAttr = g.findAttribute(Type.NodesType, "ext_id");
            if (targetAttr == Attribute.InvalidAttribute) return;

            System.out.println("Searching node ext_id='" + wantedId + "' ...");
            long t0 = System.nanoTime();
            
            Value v = new Value();
            long oid = g.findObject(targetAttr, v.setString(wantedId));
            
            if (oid == com.sparsity.sparksee.gdb.Objects.InvalidOID) {
                System.out.println("Not found.");
            } else {
                int typeId = g.getObjectType(oid);
                System.out.println("Found OID=" + oid + " Type=" + g.getType(typeId).getName());
                printAttributes(g, oid);
            }
            long t1 = System.nanoTime();
            System.out.printf(Locale.ROOT, "Query time: %.3f ms%n", (t1 - t0) / 1e6);
        });
    }

    // -gel <label>
    private void runGetEdgeIdsByLabel(String dbPath, String label) throws Exception {
        runWithSession(dbPath, (sess, g) -> {
            System.out.println("Searching edges with label='" + label + "' ...");
            long t0 = System.nanoTime();
            
            int edgeType = g.findType(label);
            if (edgeType == Type.InvalidType) {
                System.out.println("Edge label not found: " + label);
                return;
            }
            int eidAttr = g.findAttribute(edgeType, "eid");
            
            com.sparsity.sparksee.gdb.Objects objs = g.select(edgeType);
            ObjectsIterator it = objs.iterator();
            long count = 0;
            Value v = new Value();
            try {
                while (it.hasNext()) {
                    long eoid = it.next();
                    count++;
                    if (count <= 10 && eidAttr != Attribute.InvalidAttribute) {
                        g.getAttribute(eoid, eidAttr, v);
                        System.out.println(v.getString());
                    }
                }
            } finally {
                it.close();
                objs.close();
            }
            long t1 = System.nanoTime();
            if (count > 10) System.out.println("... (total " + count + ")");
            System.out.printf(Locale.ROOT, "Total edges: %,d. Query time: %.3f ms%n", count, (t1 - t0) / 1e6);
        });
    }

    // -nv <attr=val>
    private void runFindNodesByAttrAcrossTypes(String dbPath, String spec) throws Exception {
        String[] parts = spec.split("=");
        if (parts.length != 2) return;
        String key = parts[0];
        String val = parts[1];

        runWithSession(dbPath, (sess, g) -> {
            System.out.println("Searching nodes where " + key + "='" + val + "' ...");
            long t0 = System.nanoTime();
            long total = 0;
            
            TypeList tList = g.findNodeTypes();
            TypeListIterator tIt = tList.iterator();
            Value vArg = new Value();
            
            while(tIt.hasNext()) {
                int typeId = tIt.next();
                int attrId = g.findAttribute(typeId, key);
                if (attrId == Attribute.InvalidAttribute) continue;
                
                DataType dt = g.getAttribute(attrId).getDataType();
                if (!trySetValue(vArg, dt, val)) continue;

                com.sparsity.sparksee.gdb.Objects objs = g.select(attrId, Condition.Equal, vArg);
                ObjectsIterator it = objs.iterator();
                while(it.hasNext()) {
                    long oid = it.next();
                    total++;
                    if (total <= 10) System.out.println("Found ID=" + oid);
                }
                it.close();
                objs.close();
            }
            long t1 = System.nanoTime();
            if (total > 10) System.out.println("... (total " + total + ")");
            System.out.printf(Locale.ROOT, "Total nodes: %,d. Query time: %.3f ms%n", total, (t1 - t0) / 1e6);
        });
    }

    // utils

    @FunctionalInterface
    interface GraphAction {
        void accept(Session s, Graph g) throws Exception;
    }

    private void runWithSession(String dbPath, GraphAction action) throws Exception {
        SparkseeConfig cfg = new SparkseeConfig("sparksee.cfg");
        Sparksee sparksee = new Sparksee(cfg);
        Database db = null;
        Session sess = null;
        try {
            db = sparksee.open(dbPath, true);
            sess = db.newSession();
            action.accept(sess, sess.getGraph());
        } finally {
            if (sess != null) sess.close();
            if (db != null) db.close();
            sparksee.close();
        }
    }

    private void printAttributes(Graph g, long oid) {
        AttributeList attrs = g.getAttributes(oid);
        Value val = new Value();
        for (Integer attrId : attrs) {
            Attribute meta = g.getAttribute(attrId);
            g.getAttribute(oid, attrId, val);
            System.out.println("  " + meta.getName() + ": " + valToString(val, meta.getDataType()));
        }
    }

    private String valToString(Value v, DataType dt) {
        if (v.isNull()) return "null";
        switch (dt) {
            case Boolean: return String.valueOf(v.getBoolean());
            case Integer: return String.valueOf(v.getInteger());
            case Long: return String.valueOf(v.getLong());
            case Double: return String.valueOf(v.getDouble());
            case String: return v.getString();
            default: return v.getString();
        }
    }

    private void setAttributeValue(Graph g, long oid, int attrId, DataType dt, String sVal, Value v) {
        if (trySetValue(v, dt, sVal)) {
            g.setAttribute(oid, attrId, v);
        }
    }

    private boolean trySetValue(Value v, DataType dt, String sVal) {
        try {
            switch (dt) {
                case Integer: v.setInteger(Integer.parseInt(sVal)); break;
                case Long: v.setLong(Long.parseLong(sVal)); break;
                case Double: v.setDouble(Double.parseDouble(sVal)); break;
                case Boolean: v.setBoolean(Boolean.parseBoolean(sVal)); break;
                default: v.setString(sVal);
            }
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private int ensureNodeType(Graph g, String label) {
        if (nodeTypeIds.containsKey(label)) return nodeTypeIds.get(label);
        int type = g.findType(label);
        if (type == Type.InvalidType) type = g.newNodeType(label);
        nodeTypeIds.put(label, type);
        return type;
    }

    private int ensureEdgeType(Graph g, String label, boolean directed) {
        if (edgeTypeIds.containsKey(label)) return edgeTypeIds.get(label);
        int type = g.findType(label);
        if (type == Type.InvalidType) type = g.newEdgeType(label, directed, true);
        edgeTypeIds.put(label, type);
        return type;
    }

    private int ensureAttribute(Graph g, int parentType, String name, DataType dt, AttributeKind kind) {
        attrsByType.computeIfAbsent(parentType, k -> new HashMap<>());
        if (attrsByType.get(parentType).containsKey(name)) return attrsByType.get(parentType).get(name);

        int attr = g.findAttribute(parentType, name);
        if (attr == Attribute.InvalidAttribute) attr = g.newAttribute(parentType, name, dt, kind);
        
        attrsByType.get(parentType).put(name, attr);
        return attr;
    }

    private DataType guessType(String name) {
        String lc = name.toLowerCase(Locale.ROOT);
        if (lc.equals("age") || lc.equals("year")) return DataType.Integer;
        return DataType.String;
    }

    private String safeGet(String[] arr, Integer idx) {
        if (idx == null || idx < 0 || idx >= arr.length) return null;
        return arr[idx];
    }
}