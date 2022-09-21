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

	// delete from row1 the cells in the column qualifier and the column pepe
	mutQualifier := bigtable.NewMutation()
	mutQualifier.DeleteCellsInColumn("fam", "qualifier")
	err = tbl.Apply(ctx, "row1", mutQualifier)
	if err != nil {
		logrus.WithError(err).Error("error deleting row")
	}

	// delete
	mutPepe := bigtable.NewMutation()
	mutPepe.DeleteCellsInColumn("meme", "pepe")
	err = tbl.Apply(ctx, "row1", mutPepe)
	if err != nil {
		logrus.WithError(err).Error("error deleting row")
	}

	// lookup row1 with the cbt tool to see the results

	// delete row1 and row2 that were created in the previous exercises
	// hint: use a mutation
	// do a lookup after you've executed the delete
	mut := bigtable.NewMutation()
	mut.DeleteRow()

	err = tbl.Apply(ctx, "row1", mut)
	if err != nil {
		logrus.WithError(err).Error("error deleting row")
	}

	err = tbl.Apply(ctx, "row2", mut)
	if err != nil {
		logrus.WithError(err).Error("error deleting row")
	}

	// lookup row1 and row2 with the cbt tool to see the results

}

// if something goes wrong you can always restart the emulator, since the emulator is in-memory you will have a fresh start
