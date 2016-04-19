package main

import (
	"encoding/binary"
	"flag"
	"os"
	"path/filepath"
	"runtime"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/dustin/go-humanize"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

var (
	dbDir      string
	batchSize  int64
	total      int64
	batchCount int64
)

func init() {
	flag.Int64Var(&batchSize, "batch.size", 10*1000, "Keys count in batch")
	flag.Int64Var(&batchCount, "batch.count", 1000, "Batch count")
	flag.StringVar(&dbDir, "db", "db.lvl", "Path to database directory")
}

func DirSize() string {
	defer func() {
		if p := recover(); p != nil {
			log.Error(p)
		}
	}()
	start := time.Now()
	var size uint64
	if err := filepath.Walk(dbDir, func(_ string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			size += uint64(info.Size())
		}
		return err
	}); err != nil {
		log.Fatal(err)
	}
	elapsed := time.Now().Sub(start)
	log.Debugln("DirSize timing:", elapsed)
	return humanize.Bytes(size)
}

func main() {
	flag.Parse()
	total = batchCount * batchSize
	opts := &opt.Options{
		NoSync: false,
	}
	db, err := leveldb.OpenFile("db.lvl", opts)
	if err != nil {
		log.Fatal(err)
	}
	log.Infoln("writing", total, "in", batchCount)
	count := int64(0)
	totalBytes := 0
	start := time.Now()
	var mem runtime.MemStats
	for d := int64(0); d < batchCount; d++ {
		b := new(leveldb.Batch)
		f := log.WithField("count", 40*1000)
		var (
			k, v int64
		)
		for i := int64(0); i < batchSize; i++ {
			k = count
			v = count + 10000
			count++
			kBuf := make([]byte, 8)
			vBuf := make([]byte, 8)
			binary.PutVarint(kBuf, k)
			binary.PutVarint(vBuf, v)
			b.Put(kBuf, vBuf)
			totalBytes += len(kBuf) + len(vBuf)
		}
		log.WithFields(log.Fields{
			"id":  d,
			"len": b.Len(),
		}).Println("batch")
		if err := db.Write(b, nil); err != nil {
			f.Fatal(err)
		} else {
			elapsed := time.Now().Sub(start)
			speed := float64(count) / elapsed.Seconds()
			runtime.ReadMemStats(&mem)
			log.WithFields(log.Fields{
				"n":  count,
				"v":  humanize.Bytes(uint64(totalBytes)),
				"m":  humanize.Bytes(mem.Alloc),
				"db": DirSize(),
			}).Printf("%04.1f%% %08.2f op/sec", float64(count)/float64(total)*100.0, speed)
		}
	}
	if err := db.Close(); err != nil {
		log.Error(err)
	}
	truncate := func(toTruncate time.Time) time.Time {
		return toTruncate.Truncate(time.Millisecond * 100)
	}
	start = truncate(start)
	log.WithFields(log.Fields{
		"t": truncate(time.Now()).Sub(start),
		"n": total,
	}).Info("OK")
}
