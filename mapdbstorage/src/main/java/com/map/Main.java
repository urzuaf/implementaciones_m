package com.map;

import org.mapdb.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class Main implements AutoCloseable {

    private static final String DB_FILE_NAME = "graph.db";
    
    // Batch sizes
    private static final int NODE_BATCH = 50_000;
    private static final int EDGE_BATCH = 100_000;

    private static final String MAP_NODES = "map_nodes";
    private static final String MAP_EDGES = "map_edges";

    private static final String IDX_EDGEIDS_BY_LABEL = "idx_edgeids_by_label";
    private static final String IDX_NODES_BY_PROP    = "idx_nodes_by_prop";

    private static final String HI_SENTINEL = "\uFFFF";

    private final DB db;
    private final HTreeMap<String, NodeBlob> nodes;
    private final HTreeMap<String, EdgeBlob> edges;
    private final NavigableSet<KeyTuple> idxEdgeIdsByLabel;
    private final NavigableSet<KeyTuple> idxNodesByProp;

    public static void main(String[] args) throws Exception {
        if (args.length < 1) usage();

        if (Objects.equals(args[0], "ingest")) {
            if (args.length < 4) usage();
            Path nodes = Path.of(args[1]);
            Path edges = Path.of(args[2]);
            Path dbPath = Path.of(args[3]);

            try (Main store = Main.open(dbPath)) {
                System.out.println("Starting Ingest...");
                long t0 = System.nanoTime();
                
                store.ingestNodes(nodes);
                store.ingestEdges(edges);
                
                long t1 = System.nanoTime();
                System.out.printf(Locale.ROOT, "Ingest completed: %.3f ms%n", (t1 - t0) / 1e6);
            }
            return;
        }

        if (args.length < 2) usage();
        Path dbPath = Path.of(args[0]);
        String flag = args[1];

        try (Main store = Main.open(dbPath)) {
            switch (flag) {
                case "bench" -> {
                    if (args.length < 5) usage();
                    Path idsPath = Path.of(args[2]);
                    Path labelsPath = Path.of(args[3]);
                    Path propsPath = Path.of(args[4]);
                    store.benchmarking(idsPath, labelsPath, propsPath);
                }

                case "-g" -> { 
                    if (args.length < 3) usage();
                    String nodeId = args[2];
                    System.out.println("Querying Node ID: " + nodeId);
                    long start = System.nanoTime();
                    NodeBlob n = store.getNode(nodeId);
                    long end = System.nanoTime();
                    if (n == null) {
                        System.out.println("Node not found");
                    } else {
                        System.out.println("label=" + n.label);
                        System.out.println("props=" + n.props);
                    }
                    System.out.printf(Locale.ROOT, "Query time: %.3f ms%n", (end - start) / 1e6);
                }

                case "-gel" -> { 
                    if (args.length < 3) usage();
                    String label = args[2];
                    System.out.println("Querying edges with Label: " + label);
                    long start = System.nanoTime();
                    AtomicLong c = new AtomicLong();
                    store.forEachEdgeIdByLabel(label, eid -> {
                        long idx = c.incrementAndGet();
                        if (idx <= 10) System.out.println("  Found Edge ID: " + eid);
                    });
                    long end = System.nanoTime();
                    if (c.get() > 10) System.out.println("  ... (total " + c.get() + ")");
                    System.out.printf(Locale.ROOT, "Total edges: %,d. Query time: %.3f ms%n", c.get(), (end - start) / 1e6);
                }

                case "-nv" -> { 
                    if (args.length < 3 || !args[2].contains("=")) usage();
                    String[] kv = args[2].split("=", 2);
                    String key = kv[0];
                    String val = kv[1];
                    System.out.println("Querying nodes where " + key + " = " + val);
                    long start = System.nanoTime();
                    AtomicLong c = new AtomicLong();
                    store.forEachNodeByPropertyEquals(key, val, nid -> {
                        long idx = c.incrementAndGet();
                        if (idx <= 10) System.out.println("  Found Node ID: " + nid);
                    });
                    long end = System.nanoTime();
                    if (c.get() > 10) System.out.println("  ... (total " + c.get() + ")");
                    System.out.printf(Locale.ROOT, "Total nodes: %,d. Query time: %.3f ms%n", c.get(), (end - start) / 1e6);
                }
                default -> usage();
            }
        }
    }

    private static void usage() {
        System.err.println("""
          Usage:
            Ingest:
              java ... ingest <nodes.pgdf> <edges.pgdf> <db_path>

            Benchmark:
              java ... <db_path> bench <ids_file> <labels_file> <props_file>

            Queries:
              java ... <db_path> -g  <nodeId>
              java ... <db_path> -gel <label>
              java ... <db_path> -nv <key=value>
        """);
        System.exit(2);
    }

    public void benchmarking(Path idsPath, Path labelsPath, Path propsPath) throws IOException {
        List<String> ids = Files.readAllLines(idsPath);
        List<String> labels = Files.readAllLines(labelsPath);
        List<String> props = Files.readAllLines(propsPath);

        System.out.println("Benchmark ...");

        long totalGetNode = 0;
        System.out.println("\ngetNode");
        for (String id : ids) {
            long t0 = System.nanoTime();
            getNode(id);
            long t1 = System.nanoTime();
            long diff = t1 - t0;
            totalGetNode += diff;
            System.out.printf(Locale.ROOT, "ID %s: %.4f ms%n", id, diff / 1e6);
        }

        long totalLabels = 0;
        System.out.println("\nforEachEdgeIdByLabel");
        for (String label : labels) {
            AtomicLong count = new AtomicLong();
            long t0 = System.nanoTime();
            forEachEdgeIdByLabel(label, eid -> count.incrementAndGet());
            long t1 = System.nanoTime();
            long diff = t1 - t0;
            totalLabels += diff;
            System.out.printf(Locale.ROOT, "Label %s (found %d): %.4f ms%n", label, count.get(), diff / 1e6);
        }

        long totalProps = 0;
        System.out.println("\nforEachNodeByPropertyEquals");
        for (String line : props) {
            String[] kv = line.split("=", 2);
            if (kv.length < 2) continue;
            String k = kv[0];
            String v = kv[1];
            AtomicLong count = new AtomicLong();
            long t0 = System.nanoTime();
            forEachNodeByPropertyEquals(k, v, nid -> count.incrementAndGet());
            long t1 = System.nanoTime();
            long diff = t1 - t0;
            totalProps += diff;
            System.out.printf(Locale.ROOT, "Prop %s=%s (found %d): %.4f ms%n", k, v, count.get(), diff / 1e6);
        }

        System.out.println("\n==========================================");
        System.out.println("BENCHMARK (MapDB)");
        System.out.println("==========================================");
        printStats("getNode", ids.size(), totalGetNode);
        printStats("forEachEdgeIdByLabel", labels.size(), totalLabels);
        printStats("forEachNodeByPropertyEquals", props.size(), totalProps);
    }

    private void printStats(String op, int count, long totalNs) {
        double totalMs = totalNs / 1e6;
        double avgMs = totalMs / count;
        System.out.printf(Locale.ROOT, "%-28s | Total: %8.3f ms | Avg: %8.4f ms | Count: %d%n", op, totalMs, avgMs, count);
    }

    private Main(DB db,
                 HTreeMap<String, NodeBlob> nodes,
                 HTreeMap<String, EdgeBlob> edges,
                 NavigableSet<KeyTuple> idxEdgeIdsByLabel,
                 NavigableSet<KeyTuple> idxNodesByProp) {
        this.db = db;
        this.nodes = nodes;
        this.edges = edges;
        this.idxEdgeIdsByLabel = idxEdgeIdsByLabel;
        this.idxNodesByProp = idxNodesByProp;
    }

    @SuppressWarnings("unchecked")
    public static Main open(Path folder) {
        folder.toFile().mkdirs();
        File dbFile = folder.resolve(DB_FILE_NAME).toFile();

        DB db = DBMaker.fileDB(dbFile)
                .fileMmapEnableIfSupported()
                .closeOnJvmShutdown()
                .concurrencyScale(16)
                .make();

        HTreeMap<String, NodeBlob> nodes = db.hashMap(MAP_NODES, Serializer.STRING, Serializer.JAVA).createOrOpen();
        HTreeMap<String, EdgeBlob> edges = db.hashMap(MAP_EDGES, Serializer.STRING, Serializer.JAVA).createOrOpen();

        NavigableSet<KeyTuple> idxEdgeIdsByLabel = (NavigableSet<KeyTuple>) db.treeSet(IDX_EDGEIDS_BY_LABEL, Serializer.JAVA).createOrOpen();
        NavigableSet<KeyTuple> idxNodesByProp    = (NavigableSet<KeyTuple>) db.treeSet(IDX_NODES_BY_PROP,    Serializer.JAVA).createOrOpen();

        return new Main(db, nodes, edges, idxEdgeIdsByLabel, idxNodesByProp);
    }

    @Override
    public void close() {
        if (db != null && !db.isClosed()) {
            db.commit();
            db.close();
        }
    }

    public static final class NodeBlob implements Serializable {
        @Serial private static final long serialVersionUID = 1L;
        public final String label;
        public final Map<String, String> props;
        public NodeBlob(String label, Map<String, String> props) {
            this.label = label;
            this.props = new LinkedHashMap<>(props);
        }
        @Override public String toString() { return "Node{label="+label+", props="+props+"}"; }
    }

    public static final class EdgeBlob implements Serializable {
        @Serial private static final long serialVersionUID = 1L;
        public final String label;
        public final String src;
        public final String dst;
        public EdgeBlob(String label, String src, String dst) {
            this.label = label; this.src = src; this.dst = dst;
        }
        @Override public String toString() { return "Edge{label="+label+", src="+src+", dst="+dst+"}"; }
    }

    public static final class KeyTuple implements Serializable, Comparable<KeyTuple> {
        @Serial private static final long serialVersionUID = 1L;
        
        final int type;
        final String p1;
        final String p2;
        final String p3;

        public KeyTuple(String label, String edgeId) {
            this.type = 1;
            this.p1 = label;
            this.p2 = edgeId;
            this.p3 = "";
        }

        public KeyTuple(String key, String val, String nodeId) {
            this.type = 2;
            this.p1 = key;
            this.p2 = val;
            this.p3 = nodeId;
        }

        @Override
        public int compareTo(KeyTuple o) {
            if (this.type != o.type) return Integer.compare(this.type, o.type);
            int cmp = this.p1.compareTo(o.p1);
            if (cmp != 0) return cmp;
            cmp = this.p2.compareTo(o.p2);
            if (cmp != 0) return cmp;
            return this.p3.compareTo(o.p3);
        }
    }

    public void ingestNodes(Path nodesPgdf) throws IOException {
        try (BufferedReader br = Files.newBufferedReader(nodesPgdf, StandardCharsets.UTF_8)) {
            String line; String[] header = null;
            long written = 0;
            long nCount = 0;

            while ((line = br.readLine()) != null) {
                if (line.isBlank()) continue;
                if (line.startsWith("@")) {
                    header = Arrays.stream(line.split("\\|")).map(String::trim).toArray(String[]::new);
                    continue;
                }
                if (header == null) continue;

                String[] cols = line.split("\\|", -1);
                Map<String,String> row = new LinkedHashMap<>();
                for (int i=0;i<header.length && i<cols.length;i++) row.put(header[i], cols[i]);

                String nodeId = row.getOrDefault("@id","").trim();
                String label  = row.getOrDefault("@label","").trim();
                if (nodeId.isEmpty() || label.isEmpty()) continue;

                Map<String,String> props = new LinkedHashMap<>();
                for (var e : row.entrySet()){
                    String k = e.getKey();
                    if ("@id".equals(k) || "@label".equals(k)) continue;
                    String v = e.getValue() == null ? "" : e.getValue();
                    if (!v.isEmpty()) props.put(k, v);
                }

                nodes.put(nodeId, new NodeBlob(label, props));

                for (var e : props.entrySet()) {
                    String val = e.getValue();
                    if (val == null || val.isEmpty()) continue;
                    idxNodesByProp.add(new KeyTuple(e.getKey(), val, nodeId));
                }

                nCount++;
                if ((++written % NODE_BATCH) == 0) db.commit();
            }
            db.commit();
            System.out.printf(Locale.ROOT, "  Nodes loaded: %,d%n", nCount);
        }
    }

    public void ingestEdges(Path edgesPgdf) throws IOException {
        try (BufferedReader br = Files.newBufferedReader(edgesPgdf, StandardCharsets.UTF_8)) {
            String line = br.readLine();
            if (line == null) return;
            
            String[] headerLine = line.split("\\|", -1);
            Map<String,Integer> idx = new HashMap<>();
            for(int i=0; i<headerLine.length; i++) idx.put(headerLine[i], i);

            long written = 0;
            long eCount = 0;

            while ((line = br.readLine()) != null) {
                if (line.isBlank() || line.startsWith("@")) continue;

                String[] cols = line.split("\\|", -1);
                
                String label = safeGet(cols, idx.get("@label"));
                String src   = safeGet(cols, idx.get("@out"));
                String dst   = safeGet(cols, idx.get("@in"));
                String dir   = safeGet(cols, idx.get("@dir"));
                
                if (label.isEmpty() || src.isEmpty() || dst.isEmpty()) continue;
                if (dir != null && !"T".equalsIgnoreCase(dir) && !dir.isEmpty()) continue; 

                String edgeId = safeGet(cols, idx.get("@id"));
                if (edgeId.isEmpty()) edgeId = makeEdgeId(src, label, dst);

                edges.put(edgeId, new EdgeBlob(label, src, dst));

                idxEdgeIdsByLabel.add(new KeyTuple(label, edgeId));

                eCount++;
                if ((++written % EDGE_BATCH) == 0) db.commit();
            }
            db.commit();
            System.out.printf(Locale.ROOT, "  Edges loaded: %,d%n", eCount);
        }
    }

    public NodeBlob getNode(String nodeId) {
        return nodes.get(nodeId);
    }

    public void forEachEdgeIdByLabel(String label, Consumer<String> edgeIdConsumer) {
        KeyTuple min = new KeyTuple(label, "");
        KeyTuple max = new KeyTuple(label, HI_SENTINEL);

        for (KeyTuple t : idxEdgeIdsByLabel.subSet(min, true, max, true)) {
            edgeIdConsumer.accept(t.p2); 
        }
    }

    public void forEachNodeByPropertyEquals(String propName, String propValue, Consumer<String> nodeIdConsumer) {
        KeyTuple min = new KeyTuple(propName, propValue, "");
        KeyTuple max = new KeyTuple(propName, propValue, HI_SENTINEL);

        for (KeyTuple t : idxNodesByProp.subSet(min, true, max, true)) {
            nodeIdConsumer.accept(t.p3); 
        }
    }

    private static String makeEdgeId(String src, String label, String dst) {
        String s = src + "|" + label + "|" + dst;
        long x = 1125899906842597L;
        for (int i=0;i<s.length();i++) x = (x * 1315423911L) ^ s.charAt(i);
        return Long.toUnsignedString(x);
    }
    
    private static String safeGet(String[] arr, Integer i) {
        if (i == null || i < 0 || i >= arr.length) return "";
        String s = arr[i];
        return s == null ? "" : s.trim();
    }
}