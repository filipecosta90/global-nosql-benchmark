package cmd

import (
	"context"
	"fmt"
	"github.com/magiconair/properties"
)

// readPreference int mapping
const (
	ReadPrimary   int64 = 0
	ReadSecondary       = 1
)

// DBCreator creates a database layer.
type DBCreator interface {
	Create(p *properties.Properties) (DB, error)
}

// DB is the layer to access the database to be benchmarked.
type DB interface {

	// Read reads a record from the database and returns a map of each field/value pair.
	// table: The name of the table.
	// key: The record key of the record to read.
	// readPreference:  route read operations to the primaries (0) or to the secondaries/replicas (1)
	// fields: The list of fields to read, nil|empty for reading all.
	Read(ctx context.Context, table string, key string, readPreference int64, fields []string) (map[string][]byte, error)

	// Insert inserts a record in the database. Any field/value pairs will be written into the
	// database.
	// table: The name of the table.
	// key: The record key of the record to insert.
	// numReplicas: number of replicas that acknowledged the insert.
	// numReplicasTimeout: timeout in milliseconds to ack.
	// values: A map of field/value pairs to insert in the record.
	Insert(ctx context.Context, table string, key string, numReplicas int64, numReplicasTimeout int64, values map[string][]byte) error
}

var dbCreators = map[string]DBCreator{}

// RegisterDBCreator registers a creator for the database
func RegisterDBCreator(name string, creator DBCreator) {
	_, ok := dbCreators[name]
	if ok {
		panic(fmt.Sprintf("duplicate register database %s", name))
	}

	dbCreators[name] = creator
}

// GetDBCreator gets the DBCreator for the database
func GetDBCreator(name string) DBCreator {
	return dbCreators[name]
}
