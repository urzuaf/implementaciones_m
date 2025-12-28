package com.rocks;

import org.rocksdb.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class Main implements AutoCloseable {

    // Batch sizes
    private static final int NODE_BATCH = 50_000;
    private static final int EDGE_BATCH = 100_000;

    // Column Families
    private static final String CF_NODES = "cf_nodes";
    private static final String CF_EDGES = "cf_edges";
    private static final String CF_INDEX = "cf_index";

    private static final byte SEP = 0;
    private static final byte[] EMPTY_VAL = new byte[0];

    private final RocksDB db;
    private final ColumnFamilyHandle cfNodes;
    private final ColumnFamilyHandle cfEdges;
    private final ColumnFamilyHandle cfIndex;
    private final List<ColumnFamilyHandle> allHandles;

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

    public void benchmarking(Path idsPath, Path labelsPath, Path propsPath) throws IOException, RocksDBException {
        //Load files to RAM
        List<String> ids = Files.readAllLines(idsPath);
        List<String> labels = Files.readAllLines(labelsPath);
        List<String> props = Files.readAllLines(propsPath);

        System.out.println("Benchmark ...");

        long totalGetNode = 0;
        System.out.println("\ngetNode...");
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
        System.out.println("\nTforEachNodeByPropertyEquals");
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
        System.out.println("BENCHMARK (RocksDB)");
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

    private Main(RocksDB db, List<ColumnFamilyHandle> handles) {
        this.db = db;
        this.allHandles = handles;
        this.cfNodes = handles.get(1);
        this.cfEdges = handles.get(2);
        this.cfIndex = handles.get(3);
    }

    public static Main open(Path folder) throws RocksDBException, IOException {
        RocksDB.loadLibrary();
        Files.createDirectories(folder);

        final BlockBasedTableConfig tableConfig = new BlockBasedTableConfig()
                .setCacheIndexAndFilterBlocks(true)
                .setPinL0FilterAndIndexBlocksInCache(true)
                .setEnableIndexCompression(true);

        final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()
                .setCompressionType(CompressionType.LZ4_COMPRESSION)
                .setBottommostCompressionType(CompressionType.LZ4_COMPRESSION)
                .setTableFormatConfig(tableConfig)
                .setWriteBufferSize(128 * 1024 * 1024) 
                .setMaxWriteBufferNumber(4)
                .setMinWriteBufferNumberToMerge(2);

        List<ColumnFamilyDescriptor> cfds = Arrays.asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()),
                new ColumnFamilyDescriptor(CF_NODES.getBytes(StandardCharsets.UTF_8), cfOpts),
                new ColumnFamilyDescriptor(CF_EDGES.getBytes(StandardCharsets.UTF_8), cfOpts),
                new ColumnFamilyDescriptor(CF_INDEX.getBytes(StandardCharsets.UTF_8), cfOpts)
        );

        List<ColumnFamilyHandle> handles = new ArrayList<>();
        
        try (DBOptions dbo = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setMaxBackgroundJobs(Runtime.getRuntime().availableProcessors())) {
            
            RocksDB db = RocksDB.open(dbo, folder.toString(), cfds, handles);
            return new Main(db, handles);
        }
    }

    @Override
    public void close() {
        for (ColumnFamilyHandle h : allHandles) h.close();
        if (db != null) db.close();
    }

    private static byte[] keyNode(String nodeId) { return ("node:" + nodeId).getBytes(StandardCharsets.UTF_8); }
    private static byte[] keyEdge(String edgeId) { return ("edge:" + edgeId).getBytes(StandardCharsets.UTF_8); }

    private static byte[] idxKey(String... parts) {
        int len = 3;
        for (String p : parts) len += 1 + p.getBytes(StandardCharsets.UTF_8).length;
        byte[] out = new byte[len];
        int i = 0;
        byte[] idx = "idx".getBytes(StandardCharsets.UTF_8);
        System.arraycopy(idx, 0, out, i, idx.length); i += idx.length;
        for (String p : parts) {
            out[i++] = SEP;
            byte[] b = p.getBytes(StandardCharsets.UTF_8);
            System.arraycopy(b, 0, out, i, b.length);
            i += b.length;
        }
        return out;
    }

    private static byte[] idxPrefix(String... partsWithoutLast) {
        return idxKey(partsWithoutLast);
    }

    private static boolean startsWith(byte[] a, byte[] p) {
        if (a.length < p.length) return false;
        for (int i=0;i<p.length;i++) if (a[i]!=p[i]) return false;
        return true;
    }

    private static String suffixAfterPrefix(byte[] key, byte[] prefix) {
        int start = prefix.length;
        if (start < key.length && key[start] == SEP) start++;
        return new String(key, start, key.length - start, StandardCharsets.UTF_8);
    }

    public static final class NodeBlob {
        public final String label; public final Map<String,String> props;
        NodeBlob(String label, Map<String,String> props){ this.label=label; this.props=props; }
    }

    private static byte[] encodeNodeBlob(String label, Map<String,String> props){
        byte[] lb = label.getBytes(StandardCharsets.UTF_8);
        int size = 2 + lb.length + 2;
        for (var e: props.entrySet()){
            size += 2 + e.getKey().getBytes(StandardCharsets.UTF_8).length
                    + 4 + e.getValue().getBytes(StandardCharsets.UTF_8).length;
        }
        ByteBuffer bb = ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN);
        bb.putShort((short)lb.length).put(lb);
        bb.putShort((short)props.size());
        for (var e: props.entrySet()){
            byte[] k = e.getKey().getBytes(StandardCharsets.UTF_8);
            byte[] v = e.getValue().getBytes(StandardCharsets.UTF_8);
            bb.putShort((short)k.length).put(k);
            bb.putInt(v.length).put(v);
        }
        return bb.array();
    }

    private static NodeBlob decodeNodeBlob(byte[] b){
        ByteBuffer bb = ByteBuffer.wrap(b).order(ByteOrder.BIG_ENDIAN);
        int ll = bb.getShort() & 0xFFFF; byte[] lb = new byte[ll]; bb.get(lb);
        String label = new String(lb, StandardCharsets.UTF_8);
        int pc = bb.getShort() & 0xFFFF;
        Map<String,String> props = new LinkedHashMap<>(pc);
        for (int i=0;i<pc;i++){
            int kl = bb.getShort() & 0xFFFF; byte[] kb = new byte[kl]; bb.get(kb);
            String k = new String(kb, StandardCharsets.UTF_8);
            int vl = bb.getInt(); byte[] vb = new byte[vl]; bb.get(vb);
            String v = new String(vb, StandardCharsets.UTF_8);
            props.put(k, v);
        }
        return new NodeBlob(label, props);
    }

    public static final class EdgeBlob {
        public final String label, src, dst;
        EdgeBlob(String label, String src, String dst){ this.label=label; this.src=src; this.dst=dst; }
    }

    private static byte[] encodeEdgeBlob(String label, String src, String dst){
        byte[] lb = label.getBytes(StandardCharsets.UTF_8);
        byte[] sb = src.getBytes(StandardCharsets.UTF_8);
        byte[] db = dst.getBytes(StandardCharsets.UTF_8);
        ByteBuffer bb = ByteBuffer.allocate(2+lb.length + 2+sb.length + 2+db.length).order(ByteOrder.BIG_ENDIAN);
        bb.putShort((short)lb.length).put(lb);
        bb.putShort((short)sb.length).put(sb);
        bb.putShort((short)db.length).put(db);
        return bb.array();
    }

    private static EdgeBlob decodeEdgeBlob(byte[] b){
        ByteBuffer bb = ByteBuffer.wrap(b).order(ByteOrder.BIG_ENDIAN);
        int ll=bb.getShort()&0xFFFF; byte[] lb=new byte[ll]; bb.get(lb);
        int sl=bb.getShort()&0xFFFF; byte[] sb=new byte[sl]; bb.get(sb);
        int dl=bb.getShort()&0xFFFF; byte[] db=new byte[dl]; bb.get(db);
        return new EdgeBlob(new String(lb, StandardCharsets.UTF_8),
                            new String(sb, StandardCharsets.UTF_8),
                            new String(db, StandardCharsets.UTF_8));
    }

    public void ingestNodes(Path nodesPgdf) throws IOException, RocksDBException {
        try (final WriteOptions wo = new WriteOptions().setDisableWAL(true).setSync(false);
             final BufferedReader br = Files.newBufferedReader(nodesPgdf, StandardCharsets.UTF_8)) {

            WriteBatch batch = new WriteBatch();
            String line;
            String[] header = null;
            long count = 0;
            long totalNodes = 0;

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

                batch.put(cfNodes, keyNode(nodeId), encodeNodeBlob(label, props));

                for (var e : props.entrySet()){
                    byte[] k = idxKey("prop", e.getKey(), e.getValue(), nodeId);
                    batch.put(cfIndex, k, EMPTY_VAL);
                }

                count++;
                totalNodes++;

                if (count >= NODE_BATCH) {
                    db.write(wo, batch);
                    batch.close();
                    batch = new WriteBatch();
                    count = 0;
                }
            }
            if (count > 0) {
                db.write(wo, batch);
            }
            batch.close();
            System.out.printf(Locale.ROOT, "  Nodes loaded: %,d%n", totalNodes);
        }
    }

    public void ingestEdges(Path edgesPgdf) throws IOException, RocksDBException {
        try (final WriteOptions wo = new WriteOptions().setDisableWAL(true).setSync(false);
             final BufferedReader br = Files.newBufferedReader(edgesPgdf, StandardCharsets.UTF_8)) {

            WriteBatch batch = new WriteBatch();
            String line;
            String[] header = null;
            long count = 0;
            long totalEdges = 0;
            
            Map<String,Integer> idxMap = new HashMap<>();

            while ((line = br.readLine()) != null) {
                if (line.isBlank()) continue;
                if (line.startsWith("@")) {
                    header = Arrays.stream(line.split("\\|")).map(String::trim).toArray(String[]::new);
                    for(int i=0; i<header.length; i++) idxMap.put(header[i], i);
                    continue;
                }
                if (header == null) continue;

                String[] cols = line.split("\\|", -1);
                
                String label = safeGet(cols, idxMap.get("@label"));
                String src   = safeGet(cols, idxMap.get("@out"));
                String dst   = safeGet(cols, idxMap.get("@in"));
                String dir   = safeGet(cols, idxMap.get("@dir"));

                if (label.isEmpty() || src.isEmpty() || dst.isEmpty()) continue;
                if (dir != null && !"T".equalsIgnoreCase(dir) && !dir.isEmpty()) continue;

                String edgeId = safeGet(cols, idxMap.get("@id"));
                if (edgeId.isEmpty()) edgeId = makeEdgeId(src, label, dst);

                batch.put(cfEdges, keyEdge(edgeId), encodeEdgeBlob(label, src, dst));

                batch.put(cfIndex, idxKey("label", "edge", label, edgeId), EMPTY_VAL);

                count++;
                totalEdges++;

                if (count >= EDGE_BATCH) {
                    db.write(wo, batch);
                    batch.close();
                    batch = new WriteBatch();
                    count = 0;
                }
            }
            if (count > 0) {
                db.write(wo, batch);
            }
            batch.close();
            System.out.printf(Locale.ROOT, "  Edges loaded: %,d%n", totalEdges);
        }
    }

    public NodeBlob getNode(String nodeId) throws RocksDBException {
        byte[] v = db.get(cfNodes, keyNode(nodeId));
        return v==null ? null : decodeNodeBlob(v);
    }

    public void forEachEdgeIdByLabel(String label, Consumer<String> consumer){
        byte[] prefix = idxPrefix("label", "edge", label);
        try (RocksIterator it = db.newIterator(cfIndex)) {
            it.seek(prefix);
            while (it.isValid() && startsWith(it.key(), prefix)) {
                consumer.accept(suffixAfterPrefix(it.key(), prefix));
                it.next();
            }
        }
    }

    public void forEachNodeByPropertyEquals(String propName, String propValue, Consumer<String> consumer){
        byte[] prefix = idxPrefix("prop", propName, propValue);
        try (RocksIterator it = db.newIterator(cfIndex)) {
            it.seek(prefix);
            while (it.isValid() && startsWith(it.key(), prefix)) {
                consumer.accept(suffixAfterPrefix(it.key(), prefix));
                it.next();
            }
        }
    }

    private static String safeGet(String[] arr, Integer idx) {
        if (idx == null || idx < 0 || idx >= arr.length) return "";
        return arr[idx].trim();
    }

    private static String makeEdgeId(String src, String label, String dst) {
        String s = src + "|" + label + "|" + dst;
        long x = 1125899906842597L;
        for (int i=0;i<s.length();i++) x = (x * 1315423911L) ^ s.charAt(i);
        return Long.toUnsignedString(x);
    }
}