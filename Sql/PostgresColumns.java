import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class PostgresColumns {

    // Tamaño de lotes para inserción masiva
    private static final int NODE_BATCH = 50_000;
    private static final int EDGE_BATCH = 100_000;
    private static final int PROP_BATCH = 100_000;

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            usage();
        }

        String url  = args[0];
        String user = args[1];
        String pass = args[2];

        boolean queryMode = args[3].startsWith("-");

        try (Connection cx = DriverManager.getConnection(url, user, pass)) {
            cx.setAutoCommit(false);

            if (!queryMode) {
                // Ingesta: <jdbc> <user> <pass> <Nodes.pgdf> <Edges.pgdf>
                if (args.length < 5) usage();
                Path nodesFile = Paths.get(args[3]);
                Path edgesFile = Paths.get(args[4]);

                ensureTables(cx);

                long t0 = System.nanoTime();
                ingestNodes(cx, nodesFile);
                ingestEdges(cx, edgesFile);
                cx.commit();
                createIndexes(cx);
                long t2 = System.nanoTime();
                System.out.printf(Locale.ROOT,
                        "Ingesta completada: %.3f ms%n",
                        (t2 - t0)/1e6);
                return;
            }

            if (args.length < 5) usage();

            String flag = args[3];
            switch (flag) {
                case "-g" -> {
                    String nodeId = args[4];
                    queryNodeWithAllProps(cx, nodeId);
                }
                case "-gl" -> {
                    String label = args[4];
                    queryEdgeIdsByLabel(cx, label);
                }
                case "-nv" -> {
                    String spec = args[4]; // key=valor
                    int p = spec.indexOf('=');
                    if (p <= 0 || p == spec.length()-1) {
                        System.err.println("Formato inválido para -nv. Usa atributo=valor");
                        System.exit(2);
                    }
                    String key = spec.substring(0, p);
                    String val = spec.substring(p+1);
                    queryNodesByPropEquals(cx, key, val);
                }
                case "-e" ->{
                    String filepath = args[4];
                    medirTiempos(cx, filepath);
                }
                default -> usage();
            }
            cx.commit();
        }
    }

    private static void usage() {
        System.err.println("""
            Uso:

              Ingesta:
                java -cp postgresql.jar:. PgdfToPostgres <jdbc_url> <user> <pass> <Nodes.pgdf> <Edges.pgdf>

              Consultas:
                1) Nodo + todas sus propiedades
                   java -cp postgresql.jar:. PgdfToPostgres <jdbc_url> <user> <pass> -g <node_id>

                2) IDs de aristas por etiqueta
                   java -cp postgresql.jar:. PgdfToPostgres <jdbc_url> <user> <pass> -gl <label>

                3) Nodos con propiedad=valor (case-insensitive)
                   java -cp postgresql.jar:. PgdfToPostgres <jdbc_url> <user> <pass> -nv <atributo=valor>
            """);
        System.exit(2);
    }

        public static void medirTiempos(Connection cx, String rutaArchivo) {
        List<Long> tiempos = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(rutaArchivo))) {
            String linea;
            int contador = 0;
            while ((linea = br.readLine()) != null) {
                try {
                 long inicio = System.nanoTime(); // tiempo en nanosegundos
                queryNodeWithAllProps(cx, linea);
                long fin = System.nanoTime();

                long duracion = fin - inicio;
                tiempos.add(duracion);
                System.out.printf("Línea %d procesada en %.3f ms%n", ++contador, duracion / 1_000_000.0);                   
                } catch (Exception e) {
                    System.out.printf("Línea %d produjo error: %s%n", ++contador, e.getMessage());
                }

            }

            if (!tiempos.isEmpty()) {
                double promedio = tiempos.stream()
                                         .mapToLong(Long::longValue)
                                         .average()
                                         .orElse(0.0);
                System.out.printf("%nTiempo promedio: %.3f ms%n", promedio / 1_000_000.0);
            } else {
                System.out.println("El archivo está vacío.");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // ===== DDL =====
    private static void ensureTables(Connection cx) throws SQLException {
    String ddl =
        "CREATE TABLE IF NOT EXISTS nodes (" +
        "  id    TEXT PRIMARY KEY," +
        "  label TEXT NOT NULL," +
        "  props JSONB" +
        ");" +
        "CREATE TABLE IF NOT EXISTS node_properties (" +
        "  node_id  TEXT NOT NULL REFERENCES nodes(id)," +
        "  label    TEXT NOT NULL," +
        "  key      TEXT NOT NULL," +
        "  value    TEXT NOT NULL," +
        "  value_lc TEXT NOT NULL" +
        ");" +
        "CREATE TABLE IF NOT EXISTS edges (" +
        "  id       TEXT PRIMARY KEY," +
        "  label    TEXT NOT NULL," +
        "  src      TEXT NOT NULL REFERENCES nodes(id)," +
        "  dst      TEXT NOT NULL REFERENCES nodes(id)," +
        "  directed BOOLEAN NOT NULL" +
        ");" +
        "CREATE TABLE IF NOT EXISTS edge_properties (" +
        "  edge_id  TEXT NOT NULL REFERENCES edges(id)," +
        "  key      TEXT NOT NULL," +
        "  value    TEXT NOT NULL," +
        "  value_lc TEXT NOT NULL" +
        ");";
    try (Statement st = cx.createStatement()) {
        st.execute(ddl);
    }
    cx.commit();
}
private static void createIndexes(Connection cx) throws SQLException {
    String ddl =
        "CREATE INDEX IF NOT EXISTS idx_nodes_label           ON nodes(label);" +
        "CREATE INDEX IF NOT EXISTS idx_nodeprops_key_vlc     ON node_properties(key, value_lc);" +
        "CREATE INDEX IF NOT EXISTS idx_edges_label           ON edges(label);" +
        "CREATE INDEX IF NOT EXISTS idx_edges_label_src       ON edges(label, src);" +
        "CREATE INDEX IF NOT EXISTS idx_edges_label_dst       ON edges(label, dst);" +
        "CREATE INDEX IF NOT EXISTS idx_nodeprops_node        ON node_properties(node_id);";
    try (Statement st = cx.createStatement()) {
        st.execute(ddl);
    }
    cx.commit();
}

   private static void ensureSchema(Connection cx) throws SQLException {
    String ddl =
        "CREATE TABLE IF NOT EXISTS nodes (" +
        "  id    TEXT PRIMARY KEY," +
        "  label TEXT NOT NULL," +
        "  props JSONB" +
        ");" +
        "CREATE TABLE IF NOT EXISTS node_properties (" +
        "  node_id  TEXT NOT NULL REFERENCES nodes(id)," +
        "  label    TEXT NOT NULL," +
        "  key      TEXT NOT NULL," +
        "  value    TEXT NOT NULL," +
        "  value_lc TEXT NOT NULL" +
        ");" +
        "CREATE TABLE IF NOT EXISTS edges (" +
        "  id       TEXT PRIMARY KEY," +
        "  label    TEXT NOT NULL," +
        "  src      TEXT NOT NULL REFERENCES nodes(id)," +
        "  dst      TEXT NOT NULL REFERENCES nodes(id)," +
        "  directed BOOLEAN NOT NULL" +
        ");" +
        "CREATE TABLE IF NOT EXISTS edge_properties (" +
        "  edge_id  TEXT NOT NULL REFERENCES edges(id)," +
        "  key      TEXT NOT NULL," +
        "  value    TEXT NOT NULL," +
        "  value_lc TEXT NOT NULL" +
        ");" +
        "CREATE INDEX IF NOT EXISTS idx_nodes_label           ON nodes(label);" +
        "CREATE INDEX IF NOT EXISTS idx_nodeprops_key_vlc     ON node_properties(key, value_lc);" +
        "CREATE INDEX IF NOT EXISTS idx_edges_label           ON edges(label);" +
        "CREATE INDEX IF NOT EXISTS idx_edges_label_src       ON edges(label, src);" +
        "CREATE INDEX IF NOT EXISTS idx_edges_label_dst       ON edges(label, dst);" +
        "CREATE INDEX IF NOT EXISTS idx_nodeprops_node        ON node_properties(node_id);";
    try (Statement st = cx.createStatement()) {
        st.execute(ddl);
    }
    cx.commit();
}

// Ingesta Node
private static void ingestNodes(Connection cx, Path nodesPgdf) throws IOException, SQLException {
    // Esta consulta ya está correcta, solo inserta id y label
    final String upsertNode =
        "INSERT INTO nodes(id,label) VALUES(?,?) " +
        "ON CONFLICT (id) DO UPDATE SET label=EXCLUDED.label";

    final String insertProp =
        "INSERT INTO node_properties(node_id,label,key,value,value_lc) VALUES(?,?,?,?,?) " +
        "ON CONFLICT DO NOTHING";

    try (BufferedReader br = Files.newBufferedReader(nodesPgdf, StandardCharsets.UTF_8);
         PreparedStatement psNode = cx.prepareStatement(upsertNode);
         PreparedStatement psProp = cx.prepareStatement(insertProp)) {

        String line;
        String[] header = null;

        int nBatch = 0, pBatch = 0;
        long nCount = 0, pCount = 0;

        while ((line = br.readLine()) != null) {
            if (line.isBlank()) continue;

            if (line.startsWith("@")) {
                header = line.split("\\|", -1);
                continue;
            }
            if (header == null) continue;

            String[] cols = line.split("\\|", -1);
            Map<String,String> row = new LinkedHashMap<>();
            for (int i = 0; i < header.length && i < cols.length; i++) {
                row.put(header[i], cols[i]);
            }

            String id    = nonNull(row.get("@id")).trim();
            String label = nonNull(row.get("@label")).trim();
            if (id.isEmpty() || label.isEmpty()) continue;

            // --- BLOQUE DE CONSTRUCCIÓN JSON ELIMINADO ---
            // Ya no se necesita el StringBuilder sb

            // Nodo (solo id y label)
            psNode.setString(1, id);
            psNode.setString(2, label);
            // --- LÍNEA ELIMINADA: psNode.setString(3, sb.toString()); ---
            psNode.addBatch();
            nBatch++; nCount++;

            // Propiedades individuales (esta lógica se mantiene intacta)
            for (var e : row.entrySet()) {
                String k = e.getKey();
                if ("@id".equals(k) || "@label".equals(k)) continue;

                String v = nonNull(e.getValue());
                if (v.isEmpty()) continue;

                psProp.setString(1, id);
                psProp.setString(2, label);
                psProp.setString(3, k);
                psProp.setString(4, v);
                psProp.setString(5, v.toLowerCase(Locale.ROOT));
                psProp.addBatch();
                pBatch++; pCount++;

                if (pBatch >= PROP_BATCH) {
                    if (nBatch > 0) {
                        psNode.executeBatch();
                        nBatch = 0;
                    }
                    psProp.executeBatch();
                    pBatch = 0;
                    cx.commit();
                }
            }

            if (nBatch >= NODE_BATCH) {
                psNode.executeBatch();
                nBatch = 0;
                cx.commit();
            }
        }

        if (nBatch > 0) psNode.executeBatch();
        if (pBatch > 0) psProp.executeBatch();
        cx.commit();

        System.out.printf(Locale.ROOT, "  Nodes: %,d  NodeProps: %,d%n", nCount, pCount);
    }
}


// Ingesta Edges
private static void ingestEdges(Connection cx, Path edgesPgdf) throws IOException, SQLException {
    // Insertar SOLO si existen src y dst en nodes
    final String insertEdgeIfNodesExist =
        "INSERT INTO edges(id,label,src,dst,directed) " + "VALUES(?,?,?,?,?) " + "ON CONFLICT (id) DO NOTHING " ;

    try (BufferedReader br = Files.newBufferedReader(edgesPgdf, StandardCharsets.UTF_8);
         PreparedStatement psEdge = cx.prepareStatement(insertEdgeIfNodesExist)) {

        // Leer el  header
        String line = br.readLine();
        if (line == null) return;
        String[] header = line.split("\\|", -1);
        Map<String,Integer> idx = new HashMap<>();
        for (int i=0;i<header.length;i++) idx.put(header[i], i);

        int eBatch = 0;
        long eCount = 0;

        while ((line = br.readLine()) != null) {
            if (line.isBlank()) continue;

            // de momento si aparece otro header en medio lo saltamos 
            if (line.startsWith("@")) {
                continue;
            }

            String[] cols = line.split("\\|", -1);

            String eid  = safe(cols, idx.get("@id"));
            String lab  = safe(cols, idx.get("@label"));
            String dir  = safe(cols, idx.get("@dir"));
            String src  = safe(cols, idx.get("@out"));
            String dst  = safe(cols, idx.get("@in"));

            if (lab.isEmpty() || src.isEmpty() || dst.isEmpty()) continue;
            boolean directed = !"F".equalsIgnoreCase(dir.isEmpty() ? "T" : dir);
            if (eid.isEmpty()) eid = makeEdgeId(src, lab, dst);

           
            psEdge.setString(1, eid);
            psEdge.setString(2, lab);
            psEdge.setString(3, src);
            psEdge.setString(4, dst);
            psEdge.setBoolean(5, directed);
            psEdge.addBatch();
            eBatch++; eCount++;

            if (eBatch >= EDGE_BATCH) {
                psEdge.executeBatch();
                eBatch = 0;
                cx.commit();
            }
        }

        if (eBatch > 0) psEdge.executeBatch();
        cx.commit();
        System.out.printf(Locale.ROOT, "  Edges:  %,d%n", eCount);
    }
}



    // -g <node_id>
  // -g <node_id>
private static void queryNodeWithAllProps(Connection cx, String nodeId) throws SQLException {
    long t0 = System.nanoTime();

    String sql = "SELECT label, key, value FROM node_properties WHERE node_id = ?";

    try (PreparedStatement ps = cx.prepareStatement(sql)) {
        ps.setString(1, nodeId);
        ps.setFetchSize(1000);

        String label = null;
        long propcount = 0;

        try (ResultSet rs = ps.executeQuery()) {
           while(rs.next()) {
                //proceso de obtener los datos 
               if (label == null) {
                   label = rs.getString(1);
                   System.out.println("Node ID: " + nodeId);
                   System.out.println("Label:   " + label);
               }
               String key = rs.getString(2);
               String val = rs.getString(3);
               System.out.println("  " + key + ": " + val);
               propcount++;

           }
        }
        if (label == null) {
            System.out.println("No se encontró el nodo con ID: " + nodeId);
        } 
        System.out.println("Total propiedades: " + propcount);
        long t1 = System.nanoTime();
        System.out.printf(Locale.ROOT, "Lookup node time: %.3f ms%n", (t1 - t0)/1e6);
        
    }

}


    // -gl <label>
   // -gl <label>
private static void queryEdgeIdsByLabel(Connection cx, String label) throws SQLException {
    long t0 = System.nanoTime();
    String sql = "SELECT id FROM edges WHERE label = ?";

    long count = 0L;
    final long LIMIT_PRINT = 10;

    try (PreparedStatement ps = cx.prepareStatement(sql)) {
        ps.setFetchSize(10_000);
        ps.setString(1, label);
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                count++;
                if (count <= LIMIT_PRINT) {
                    String id = rs.getString(1);
                    System.out.println(id);
                }
            }
        }
    }

    long t1 = System.nanoTime();
    if (count > LIMIT_PRINT) {
        System.out.println("… (mostrando slos primeros " + LIMIT_PRINT + " resultados)");
    }
    System.err.printf(Locale.ROOT, "Total edgeIds: %,d  (%.3f ms)%n", count, (t1 - t0)/1e6);
}



private static void queryNodesByPropEquals(Connection cx, String key, String value) throws SQLException {
    long t0 = System.nanoTime();
    String sql = "SELECT node_id FROM node_properties WHERE key = ? AND value = ? ";

    long count = 0L;
    final long LIMIT_PRINT = 10;

    try (PreparedStatement ps = cx.prepareStatement(sql)) {
        ps.setFetchSize(10_000);
        ps.setString(1, key);
        ps.setString(2, value);
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                count++;
                if (count <= LIMIT_PRINT) {
                    String id = rs.getString(1);
                    System.out.println(id);
                }
            }
        }
    }

    long t1 = System.nanoTime();
    if (count > LIMIT_PRINT) {
        System.out.println("… (mostrando solo los primeros " + LIMIT_PRINT + " resultados)");
    }
    System.err.printf(Locale.ROOT, "Total nodes: %,d  (%.3f ms)%n", count, (t1 - t0)/1e6);
}


    // ===== Helpers =====
    private static String nonNull(String s){ return (s == null) ? "" : s; }
    private static String safe(String[] a, Integer i){
        if (i == null || i < 0 || i >= a.length) return "";
        String s = a[i];
        return s == null ? "" : s;
    }
    private static String makeEdgeId(String src, String label, String dst) {
        String s = src + "|" + label + "|" + dst;
        long x = 1125899906842597L;
        for (int i=0;i<s.length();i++) x = (x * 1315423911L) ^ s.charAt(i);
        return Long.toUnsignedString(x);
    }
}
