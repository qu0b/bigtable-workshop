// Important Links
// https://cloud.google.com/bigtable/docs/overview
// https://cloud.google.com/bigtable/docs/schema-design
// https://cloud.google.com/bigtable/docs/performance
// https://cloud.google.com/bigtable/docs/garbage-collection

// https://cloud.google.com/bigtable/docs/cbt-reference

package main

import (
	"context"
	"log"
	"os"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/sirupsen/logrus"
)

// Before you begin please scan through
// https://cloud.google.com/bigtable/docs/overview

//

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

	logrus.Infof("BIGTABLE_EMULATOR_HOST: %v", os.Getenv("BIGTABLE_EMULATOR_HOST"))

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

	// Read data in a row using a row key
	// sets the value so that we can query it
	// cbt --project test --instance test set tbl row1 fam:qualifier=hello
	// to check the value has been written
	// cbt --project test --instance test lookup tbl row1
	rowKey := "row1"
	columnFamilyName := "fam"

	log.Printf("Getting a single row by row key (returns all of the rows column families and their respective columns):")
	row, err := tbl.ReadRow(ctx, rowKey)
	if err != nil {
		log.Fatalf("Could not read row with key %s: %v", rowKey, err)
	}
	log.Printf("Row key: %s\n", rowKey)
	log.Printf("Data: %s\n", string(row[columnFamilyName][0].Value))

	if err = client.Close(); err != nil {
		log.Fatalf("Could not close data operations client: %v", err)
	}
}

// if something goes wrong you can always restart the emulator, since the emulator is in-memory you will have a fresh start
