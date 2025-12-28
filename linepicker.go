package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"os"
	"strings"
)

func main() {
	filenamePtr := flag.String("file", "", "filename to process")
	countPtr := flag.Int("n", 100, "amount of random lines to get")

	flag.Parse()

	filename := *filenamePtr
	targetCount := *countPtr

	if filename == "" {
		fmt.Println("Error: Must specify a file.")
		fmt.Println("Use: -file <filename> -n <lines>")
		os.Exit(1)
	}

	lines := make(map[string]bool)
	var results []string

	cycleCount := 0

	file, err := os.Open(filename)

	if err != nil {
		log.Fatal("Error opening file")
	}

	defer file.Close()

	fileinfo, err := file.Stat()
	if err != nil {
		log.Fatal("Error getting file information")
	}

	byteSize := fileinfo.Size()

	for len(results) < targetCount && (cycleCount < targetCount*5) {

		pos := rand.Int64N(byteSize)

		//Jump to the random byte in the file
		_, err := file.Seek(pos, io.SeekStart)
		if err != nil {
			continue
		}

		// open a buffer in the random position
		reader := bufio.NewReader(file)

		if pos != 0 {
			// read useless bytes to find the next line, (we dont do this if its the byte 0, cause its possible to get the first line)
			_, err := reader.ReadString('\n')
			if err != nil {
				// EOF
				continue
			}
		}

		// read the valid line
		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			// EOF too, but it should give us the line that was read before the EOF
			continue
		}

		line = strings.TrimSpace(line)

		if line == "" {
			continue
		}

		if !lines[line] {
			lines[line] = true
			results = append(results, line)
		}

		cycleCount++
		if cycleCount >= targetCount*5 {
			fmt.Println("The program will finish now, make sure that the target count is lower than the file lines")
			fmt.Println("Total amount of results: ", len(results))
			fmt.Println()
		}
	}

	for _, l := range results {
		fmt.Println(l)
	}

}
