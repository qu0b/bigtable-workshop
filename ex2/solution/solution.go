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

	// Exercise 2.1
	// read from row2 and read modify write (append) it to row1
	// hint: check the row value with
	// cbt --project test --instance test lookup tbl row1
	row2, err := tbl.ReadRow(ctx, "row2")
	if err != nil {
		logrus.Fatalf("error: %v", err)
	}
	if len(row2) == 0 {
		logrus.Fatalf("result has no families / columns")
	}

	if len(row2["fam"]) == 0 {
		logrus.Fatalf("result has no columns")
	}

	rowKey := "row1"
	m := bigtable.NewReadModifyWrite()
	m.AppendValue("fam", "qualifier", row2["fam"][0].Value)

	row, err := tbl.ApplyReadModifyWrite(ctx, rowKey, m)
	if err != nil {
		logrus.Fatalf("error: %v", err)
	}
	logrus.Infof("read: %s", row["fam"][0].Value)

	// using the cbt (alias bt) utility
	// create a new column family called meme with cbt and set its garbage collection policy to maxversions 1
	// insert three different values with in the meme column family and the column pepe (such as pepe_noted)
	// family: meme, column: pepe, value: e.g. pepe_noted
	// do a lookup after each insert to see that there is never more than one version

	// Exercise 2.2
	// cbt --project test --instance test createfamily tbl pepe
	// cbt --project test --instance test setgcpolicy tbl pepe maxversions=1
	// cbt --project test --instance test set tbl row1 meme:pepe=pepe_noted
	// cbt --project test --instance test lookup tbl row1 columns=meme
	// cbt --project test --instance test set tbl row1 meme:pepe=peepee_hands
	// cbt --project test --instance test lookup tbl row1 columns=meme
	// cbt --project test --instance test set tbl row1 meme:pepe=monka_christ
	// cbt --project test --instance test lookup tbl row1 columns=meme

	// Exercise 2.3
	// cbt --project test --instance test createfamily tbl ico
	// cbt --project test --instance test setgcpolicy tbl ico maxage=60s
	// cbt --project test --instance test set tbl row1 ico:symbol=USDT
	// cbt --project test --instance test lookup tbl row1 columns=symbol
	// cbt --project test --instance test set tbl row1 ico:symbol=DAI
	// cbt --project test --instance test lookup tbl row1 columns=symbol
	// cbt --project test --instance test set tbl row1 ico:symbol=yDAI
	// cbt --project test --instance test lookup tbl row1 columns=symbol
}

// if something goes wrong you can always restart the emulator, since the emulator is in-memory you will have a fresh start
