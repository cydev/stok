package main

import (
	"fmt"
	"github.com/klauspost/crc32"
	log "github.com/Sirupsen/logrus"

	"os"
	"bufio"
	"encoding/hex"
)
func main() {
	f, err := os.Open("hashes.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	count := 0
	collisions := 0
	c := make(map[uint32]string)
	s := make(map[string]bool)
	for scanner.Scan() {
		t := scanner.Text()
		if s[t] {
			continue
		}
		s[t] = true
		data, err := hex.DecodeString(t)
		if err != nil {
			log.Error(err)
		}
		slice := data[:]
		sH := hex.EncodeToString(slice)
		d := crc32.ChecksumIEEE(slice)
		if len(c[d]) > 0 {
			collisions++
			log.Errorf("%010d for %s and %s", d, sH, c[d])
		}
		c[d] = sH
		count++
	}
	fmt.Println("total:", count)
	fmt.Printf("collisions: %d (%.3f)\n", collisions, float64(collisions)/float64(count))
}
