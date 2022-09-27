package main

import (
	"context"
	"log"
	"os"
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
	// token:5
	// token:54
	// token:92
	// token:8
	// token:456
	// First manually sort them into their lexicographical ascending order.
	// Next write a sort function to compare your order with the results of the sort function.

	// Exercise 5.2
	// Insert the given rows into bigtable such that you can query them in descending order.

	// Exercise 5.3
	// Write a function that returns a key which is lexicographical one value higher, or lower than the given input key

	// Exercise 5.4
	// Write a paging query function such that you can use the last returned row to query successive (N + 1) rows where N is the last row of the previous query.

}

// if something goes wrong you can always restart the emulator, since the emulator is in-memory you will have a fresh start
