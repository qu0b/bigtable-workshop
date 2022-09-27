package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/sirupsen/logrus"
)

// SETUP
// docker run -d -p 8086:8086 --rm google/cloud-sdk gcloud beta emulators bigtable start --host-port 0.0.0.0:8086
// alias bt="docker run --rm --network host -e BIGTABLE_EMULATOR_HOST="localhost:8086" google/cloud-sdk cbt -project test -instance localhost:8086"
// use bt when the example states cbt
// export BIGTABLE_EMULATOR_HOST=localhost:8086

func main() {
	// $(gcloud beta emulators bigtable env-init) sets BIGTABLE_EMULATOR_HOST=localhost:8086
	// setup with cbt
	// cbt --project test --instance test createtable tbl
	// check wether the table has been created
	// cbt --project test --instance test ls
	// create a column family named fam
	// cbt --project test --instance test createfamily tbl fam
	table := "tbl"
	project := "test"
	instance := "test"

	logrus.Infof("env BIGTABLE_EMULATOR_HOST: %v", os.Getenv("BIGTABLE_EMULATOR_HOST"))

	if len(os.Getenv("BIGTABLE_EMULATOR_HOST")) == 0 {
		logrus.Fatalf("BIGTABLE_EMULATOR_HOST env variable not set", os.Getenv("BIGTABLE_EMULATOR_HOST"))
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	// Set up Bigtable data operations client.
	// There is also an admin client to manage bigtable
	client, err := bigtable.NewClient(ctx, project, instance)
	if err != nil {
		log.Fatalf("Could not create data operations client: %v", err)
	}

	tbl := client.Open(table)

	// Bonus Exercise

	// Exercise 5.1
	// Given the keys
	// token:1
	// token:101
	// token:2000
	// token:456
	// token:5
	// token:54
	// token:92
	// token:8
	// First manually sort them into their lexicographical ascending order.
	// Next write a sort function to compare your order with the results of the sort function.
	keys := []string{
		"token:1",
		"token:101",
		"token:2000",
		"token:5",
		"token:54",
		"token:92",
		"token:8",
		"token:456",
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	logrus.Infof("keys %+v", keys)

	// Exercise 5.2
	// Insert the given rows into bigtable such that you can query them in descending order.

	max := int64(10000)

	// write padded keys
	for _, key := range keys {
		numStr := strings.Split(key, ":")
		num, err := strconv.ParseInt(numStr[1], 10, 64)
		if err != nil {
			logrus.WithError(err).Errorf("error parsing string to number, key: %v", key)
		}

		keyPadded := fmt.Sprintf("token:%05d", max-num)

		mut := bigtable.NewMutation()
		mut.Set("fam", "qualifier", bigtable.Now(), []byte("test"))

		err = tbl.Apply(ctx, keyPadded, mut)
		if err != nil {
			logrus.WithError(err).Error("error applying mutation")
		}
	}

	// read keys in decending order
	rowRange := bigtable.InfiniteRange("token:")
	err = tbl.ReadRows(ctx, rowRange, func(row bigtable.Row) bool {
		key := row.Key()
		numStr := strings.Split(key, ":")
		num, _ := strconv.ParseInt(numStr[1], 10, 64)

		unpadded := fmt.Sprintf("token:%d", max-num)

		logrus.Infof("reading row: %v, unpadded: %v", row.Key(), unpadded)
		return true
	})
	if err != nil {
		logrus.WithError(err).Errorf("error reading rows")
	}

	// Exercise 5.3
	// Write a function that returns a key which is lexicographical one value higher than the given input key
	currKey := keys[0]
	logrus.Infof("incrementing key: %v, bytes: %b", currKey, []byte(currKey))
	logrus.Infof("incremented  key: %v, bytes: %b", incrementKey(currKey), []byte(incrementKey(currKey)))

	// Exercise 5.4
	// Write a paging query function such that you can use the last returned row to query successive (N + 1) rows where N is the last row of the previous query.
	start := keys[len(keys)-1] // some start key
	logrus.Infof("starting to read %v", start)
	rr := bigtable.NewRange(incrementKey(paddKey(start)), "") // increment so the result does not include the start key
	err = tbl.ReadRows(ctx, rr, func(row bigtable.Row) bool {
		key := row.Key()
		numStr := strings.Split(key, ":")
		num, _ := strconv.ParseInt(numStr[1], 10, 64)

		unpadded := fmt.Sprintf("token:%d", max-num)

		logrus.Infof("reading row: %v, unpadded: %v", row.Key(), unpadded)
		return true
	})
	if err != nil {
		logrus.WithError(err).Errorf("error reading rows")
	}

}

// adds a single byte to the key ex 5.3
func incrementKey(key string) string {
	return key + "\x00"
}
func paddKey(key string) string {
	max := int64(10000)
	numStr := strings.Split(key, ":")
	num, err := strconv.ParseInt(numStr[1], 10, 64)
	if err != nil {
		logrus.WithError(err).Errorf("error parsing string to number, key: %v", key)
	}

	return fmt.Sprintf("token:%05d", max-num)
}

// if something goes wrong you can always restart the emulator, since the emulator is in-memory you will have a fresh start
