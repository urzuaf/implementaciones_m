package main

import (
	"bufio"
	"log"
	"os"
	"path/filepath"
	"strings"
)

type Queries struct {
	neo4j    map[string]string
	postgres map[string]string
	rocksdb  map[string]string
	mapdb    map[string]string
	sparksee map[string]string
}

type Content struct {
	neo4j    string
	postgres string
	rocksdb  string
	mapdb    string
	sparksee string
}

// Placeholders, replace with real ones
const (
	NEO4J_USER        = "neo4j"
	NEO4J_PASSWORD    = "Fernando"
	POSTGRES_USER     = "furzua"
	POSTGRES_PASSWORD = "Fernando"
	POSTGRES_DATABASE = "gdb"
	MAPDB_DATABASE    = "gdb"
	ROCKSDB_DATABASE  = "gdb"
	SPARKSEE_DATABASE = "gdb"
)

func main() {

	Queries := Queries{neo4j: make(map[string]string), postgres: make(map[string]string), rocksdb: make(map[string]string), mapdb: make(map[string]string), sparksee: make(map[string]string)}

	Queries.neo4j["ids"] = "mvn compile -q exec:java -Dexec.mainClass=\"com.neo4j.Main\" -Dexec.args=\"bolt://localhost:7687 <db_user> <db_pass> -g <node_id>\""
	Queries.neo4j["labels"] = "mvn compile -q exec:java -Dexec.mainClass=\"com.neo4j.Main\" -Dexec.args=\"bolt://localhost:7687 <db_user> <db_pass> -gel <edge_label>\""
	Queries.neo4j["properties"] = "mvn compile -q exec:java -Dexec.mainClass=\"com.neo4j.Main\" -Dexec.args=\"bolt://localhost:7687 <db_user> <db_pass> -nv <property=value>\""

	Queries.postgres["ids"] = "java -cp postgresql-42.7.8.jar:. PostgresColumns.java jdbc:postgresql://localhost:5432/<db_name> <db_user> <db_pass> -g <node_id>"
	Queries.postgres["labels"] = "java -cp postgresql-42.7.8.jar:. PostgresColumns.java jdbc:postgresql://localhost:5432/<db_name> <db_user> <db_pass> -gel <edge_label>"
	Queries.postgres["properties"] = "java -cp postgresql-42.7.8.jar:. PostgresColumns.java jdbc:postgresql://localhost:5432/<db_name> <db_user> <db_pass> -nv <property=value>"

	Queries.rocksdb["ids"] = "mvn compile -q exec:java -Dexec.mainClass=\"com.rocks.Main\" -Dexec.args=\"<db_name> -g <node_id>\""
	Queries.rocksdb["labels"] = " mvn compile -q exec:java -Dexec.mainClass=\"com.rocks.Main\" -Dexec.args=\"<db_name> -gel <edge_label>\""
	Queries.rocksdb["properties"] = " mvn compile -q exec:java -Dexec.mainClass=\"com.rocks.Main\" -Dexec.args=\"<db_name> -nv <property=value>\""

	Queries.mapdb["ids"] = "mvn compile -q exec:java -Dexec.mainClass=\"com.map.Main\" -Dexec.args=\"<db_name> -g <node_id>\""
	Queries.mapdb["labels"] = "mvn compile -q exec:java -Dexec.mainClass=\"com.map.Main\" -Dexec.args=\"<db_name> -gel <edge_label>\""
	Queries.mapdb["properties"] = "mvn compile -q exec:java -Dexec.mainClass=\"com.map.Main\" -Dexec.args=\"<db_name> -nv <property=value>\""

	Queries.sparksee["ids"] = "java  -cp sparkseejava.jar:.  SparkseeImplementation2 -d <db_name> -g <node_id>"
	Queries.sparksee["labels"] = "java  -cp sparkseejava.jar:.  SparkseeImplementation2 -d <db_name> -gel <edge_label>"
	Queries.sparksee["properties"] = "java  -cp sparkseejava.jar:.  SparkseeImplementation2 -d <db_name> -nv <property=value>"

	EnginesLocations := make(map[string]string)
	EnginesLocations["neo4j"] = "./neo4jc"
	EnginesLocations["rocksdb"] = "./rocksstorage"
	EnginesLocations["mapdb"] = "./mapdbstorage"
	EnginesLocations["sparksee"] = "./sparksee"
	EnginesLocations["postgres"] = "./sql"

	process(&Queries, EnginesLocations)
}

func processFile(filename string) []string {

	lines := []string{}
	file, err := os.Open(filename)

	if err != nil {
		log.Panic("Error opening file")
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		lines = append(lines, line)
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error while reading %s", err)
	}

	return lines
}

func writeFileInFolder(folderPath string, filename string, fileContent string) {
	fullpath := filepath.Join(folderPath, filename)

	content := []byte(fileContent)

	err := os.WriteFile(fullpath, content, 0777)

	if err != nil {
		log.Fatal("error: ", err)
	}
}

func process(q *Queries, EnginesLocations map[string]string) {
	var content Content
	var tail string

	data := make(map[string]string)

	data["ids"] = "<node_id>"
	data["labels"] = "<edge_label>"
	data["properties"] = "<property=value>"

	for key, value := range data {
		tail = " >> resultados" + key + "\n"
		content = Content{}
		lines := processFile(key)
		for _, line := range lines {
			neos := strings.ReplaceAll(q.neo4j[key], "<db_user>", NEO4J_USER)
			neos = strings.ReplaceAll(neos, "<db_pass>", NEO4J_PASSWORD)
			neos = strings.ReplaceAll(neos, value, line)
			content.neo4j += (neos + tail)

			post := strings.ReplaceAll(q.postgres[key], "<db_user>", POSTGRES_USER)
			post = strings.ReplaceAll(post, "<db_pass>", POSTGRES_PASSWORD)
			post = strings.ReplaceAll(post, "<db_name>", POSTGRES_DATABASE)
			post = strings.ReplaceAll(post, value, line)
			content.postgres += (post + tail)

			rocks := strings.ReplaceAll(q.rocksdb[key], "<db_name>", ROCKSDB_DATABASE)
			rocks = strings.ReplaceAll(rocks, value, line)
			content.rocksdb += (rocks + tail)

			mapdb := strings.ReplaceAll(q.mapdb[key], "<db_name>", MAPDB_DATABASE)
			mapdb = strings.ReplaceAll(mapdb, value, line)
			content.mapdb += (mapdb + tail)

			sparks := strings.ReplaceAll(q.sparksee[key], "<db_name>", SPARKSEE_DATABASE)
			sparks = strings.ReplaceAll(sparks, value, line)
			content.sparksee += (sparks + tail)
		}

		writeManager(content.neo4j, key, "neo4j", EnginesLocations)
		writeManager(content.rocksdb, key, "rocksdb", EnginesLocations)
		writeManager(content.mapdb, key, "mapdb", EnginesLocations)
		writeManager(content.postgres, key, "postgres", EnginesLocations)
		writeManager(content.sparksee, key, "sparksee", EnginesLocations)

	}

}

func writeManager(content, queryType, database string, EngineLocations map[string]string) {
	switch queryType {
	case "ids":
		writeFileInFolder(EngineLocations[database], "IdQueries.sh", content)
	case "labels":
		writeFileInFolder(EngineLocations[database], "LabelQueries.sh", content)
	case "properties":
		writeFileInFolder(EngineLocations[database], "PropertyQueries.sh", content)
	}
}
