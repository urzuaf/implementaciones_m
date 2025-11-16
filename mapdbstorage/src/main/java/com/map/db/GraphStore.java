package com.map.db;

import org.mapdb.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Consumer;

public class GraphStore implements AutoCloseable {

    // Colecciones principales
    private static final String CF_NODES = "cf_nodes"; 
    private static final String CF_EDGES = "cf_edges"; 

    // Índices separados
    private static final String IDX_EDGEIDS_BY_LABEL = "idx_edgeids_by_label";      
    private static final String IDX_SRC_BY_LABEL     = "idx_src_by_label";          
    private static final String IDX_DST_BY_LABEL     = "idx_dst_by_label";         
    private static final String IDX_NODES_BY_PROP    = "idx_nodes_by_prop";         

    private static final char SEP = '\0';
    private static final String UPPER = "\uFFFF"; // cota superior para subSet

    private final DB db;
    private final HTreeMap<String, NodeBlob> nodes;
    private final HTreeMap<String, EdgeBlob> edges;

    private final NavigableSet<String> idxEdgeIdsByLabel;
    private final NavigableSet<String> idxSrcByLabel;
    private final NavigableSet<String> idxDstByLabel;
    private final NavigableSet<String> idxNodesByProp;

    private GraphStore(DB db,
                       HTreeMap<String, NodeBlob> nodes,
                       HTreeMap<String, EdgeBlob> edges,
                       NavigableSet<String> idxEdgeIdsByLabel,
                       NavigableSet<String> idxSrcByLabel,
                       NavigableSet<String> idxDstByLabel,
                       NavigableSet<String> idxNodesByProp) {
        this.db = db;
        this.nodes = nodes;
        this.edges = edges;
        this.idxEdgeIdsByLabel = idxEdgeIdsByLabel;
        this.idxSrcByLabel = idxSrcByLabel;
        this.idxDstByLabel = idxDstByLabel;
        this.idxNodesByProp = idxNodesByProp;
    }

    public static GraphStore open(Path file) {
        file.toFile().getParentFile().mkdirs();

        DB db = DBMaker.fileDB(file.toFile())
                .fileMmapEnableIfSupported()
                .closeOnJvmShutdown()
                .concurrencyScale(16)
                // .transactionEnable() 
                .make();

        HTreeMap<String, NodeBlob> nodes = db.hashMap(CF_NODES, Serializer.STRING, Serializer.JAVA).createOrOpen();
        HTreeMap<String, EdgeBlob> edges = db.hashMap(CF_EDGES, Serializer.STRING, Serializer.JAVA).createOrOpen();

        NavigableSet<String> idxEdgeIdsByLabel = db.treeSet(IDX_EDGEIDS_BY_LABEL, Serializer.STRING).createOrOpen();
        NavigableSet<String> idxSrcByLabel     = db.treeSet(IDX_SRC_BY_LABEL,     Serializer.STRING).createOrOpen();
        NavigableSet<String> idxDstByLabel     = db.treeSet(IDX_DST_BY_LABEL,     Serializer.STRING).createOrOpen();
        NavigableSet<String> idxNodesByProp    = db.treeSet(IDX_NODES_BY_PROP,    Serializer.STRING).createOrOpen();

        return new GraphStore(db, nodes, edges, idxEdgeIdsByLabel, idxSrcByLabel, idxDstByLabel, idxNodesByProp);
    }

    @Override
    public void close() {
        try { db.commit(); } catch (Exception ignore) {}
        try { db.close(); } catch (Exception ignore) {}
    }

    // ===== blobs =====
    public static final class NodeBlob implements Serializable {
        @Serial private static final long serialVersionUID = 1L;
        public final String label;
        public final Map<String, String> props;
        public NodeBlob(String label, Map<String, String> props) {
            this.label = label;
            this.props = new LinkedHashMap<>(props);
        }
        @Override public String toString() { return "NodeBlob{label="+label+", props="+props+"}"; }
    }

    public static final class EdgeBlob implements Serializable {
        @Serial private static final long serialVersionUID = 1L;
        public final String label;
        public final String src;
        public final String dst;
        public EdgeBlob(String label, String src, String dst) {
            this.label = label; this.src = src; this.dst = dst;
        }
        @Override public String toString() { return "EdgeBlob{label="+label+", src="+src+", dst="+dst+"}"; }
    }

    // ===== helpers =====
    private static String kEdgeIdsByLabel(String label, String edgeId) {
        return label + SEP + edgeId;
    }
    private static String kSrcByLabel(String label, String src) {
        return label + SEP + src;
    }
    private static String kDstByLabel(String label, String dst) {
        return label + SEP + dst;
    }
    private static String kNodesByProp(String prop, String valLower, String nodeId) {
        return prop + SEP + valLower + SEP + nodeId;
    }
    private static String norm(String s) { return s.toLowerCase(Locale.ROOT); }

    // ===== ingest =====
    public void ingestNodes(Path nodesPgdf) throws IOException {
        try (BufferedReader br = Files.newBufferedReader(nodesPgdf, StandardCharsets.UTF_8)) {
            String line; String[] header = null;
            long written = 0;
            final long BATCH = 50_000; 

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
                    String v = e.getValue()==null? "" : e.getValue();
                    if (!v.isEmpty()) props.put(k, v);
                }

                nodes.put(nodeId, new NodeBlob(label, props));

                for (var e : props.entrySet()) {
                    String val = e.getValue();
                    if (val == null || val.isEmpty()) continue;
                    idxNodesByProp.add(kNodesByProp(e.getKey(), norm(val), nodeId));
                }

                if ((++written % BATCH) == 0) db.commit();
            }
            db.commit();
        }
    }

    public void ingestEdges(Path edgesPgdf) throws IOException {
        try (BufferedReader br = Files.newBufferedReader(edgesPgdf, StandardCharsets.UTF_8)) {
            String line; String[] header = null;
            long written = 0;
            final long BATCH = 100_000;

            while ((line = br.readLine()) != null) {
                if (line.isBlank()) continue;
                if (line.startsWith("@")) {
                    header = Arrays.stream(line.split("\\|")).map(String::trim).toArray(String[]::new);
                    continue;
                }
                if (header == null) continue;

                String[] cols = line.split("\\|", -1);
                Map<String,String> row = new HashMap<>();
                for (int i=0;i<header.length && i<cols.length;i++) row.put(header[i], cols[i]);

                String edgeId = row.getOrDefault("@id","").trim();
                String label  = row.getOrDefault("@label","").trim();
                String dir    = row.getOrDefault("@dir","T").trim();
                String src    = row.getOrDefault("@out","").trim();
                String dst    = row.getOrDefault("@in","").trim();
                if (label.isEmpty() || src.isEmpty() || dst.isEmpty()) continue;
                if (!"T".equalsIgnoreCase(dir)) continue;

                if (edgeId.isEmpty()) edgeId = makeEdgeId(src, label, dst);

                edges.put(edgeId, new EdgeBlob(label, src, dst));

                idxEdgeIdsByLabel.add(kEdgeIdsByLabel(label, edgeId));
                idxSrcByLabel.add(kSrcByLabel(label, src));
                idxDstByLabel.add(kDstByLabel(label, dst));

                if ((++written % BATCH) == 0) db.commit();
            }
            db.commit();
        }
    }

    private static String makeEdgeId(String src, String label, String dst) {
        String s = src + "|" + label + "|" + dst;
        long x = 1125899906842597L;
        for (int i=0;i<s.length();i++) x = (x * 1315423911L) ^ s.charAt(i);
        return Long.toUnsignedString(x);
    }

    // ===== gets =====
    public NodeBlob getNode(String nodeId) { return nodes.get(nodeId); }
    public EdgeBlob getEdge(String edgeId) { return edges.get(edgeId); }

    public void forEachEdgeIdByLabel(String label, Consumer<String> edgeIdConsumer) {
        final String prefix = label + SEP;
        var range = idxEdgeIdsByLabel.subSet(prefix, true, prefix + UPPER, true);
        for (String k : range) {
            int p = k.indexOf(SEP);
            if (p < 0 || p+1 >= k.length()) continue;
            String edgeId = k.substring(p+1);
            edgeIdConsumer.accept(edgeId);
        }
    }

    public void forEachSourceNodeByLabel(String label, Consumer<String> nodeIdConsumer) {
        final String prefix = label + SEP;
        var range = idxSrcByLabel.subSet(prefix, true, prefix + UPPER, true);
        for (String k : range) {
            int p = k.indexOf(SEP);
            if (p < 0 || p+1 >= k.length()) continue;
            String nodeId = k.substring(p+1);
            nodeIdConsumer.accept(nodeId);
        }
    }

    public void forEachDestinationNodeByLabel(String label, Consumer<String> nodeIdConsumer) {
        final String prefix = label + SEP;
        var range = idxDstByLabel.subSet(prefix, true, prefix + UPPER, true);
        for (String k : range) {
            int p = k.indexOf(SEP);
            if (p < 0 || p+1 >= k.length()) continue;
            String nodeId = k.substring(p+1);
            nodeIdConsumer.accept(nodeId);
        }
    }

    public void forEachNodeByPropertyEquals(String propName, String propValue, Consumer<String> nodeIdConsumer) {
        final String prefix = propName + SEP + norm(propValue) + SEP;
        var range = idxNodesByProp.subSet(prefix, true, prefix + UPPER, true);
        for (String k : range) {
            int p1 = k.indexOf(SEP);
            if (p1 < 0) continue;
            int p2 = k.indexOf(SEP, p1+1);
            if (p2 < 0 || p2+1 >= k.length()) continue;
            String nodeId = k.substring(p2+1);
            nodeIdConsumer.accept(nodeId);
        }
    }
}
