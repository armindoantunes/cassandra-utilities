package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/gocql/gocql"
)

func showUsageAndExit() {

	fmt.Println("Missing mandatory parameter...")
	flag.PrintDefaults()
	os.Exit(1)
}

func main() {
	// Parse command-line parameters
	host := flag.String("host", "localhost", "Cassandra host address")
	port := flag.Int("port", 9042, "Cassandra port")
	keyspace := flag.String("keyspace", "", "Cassandra keyspace name (mandatory)")
	username := flag.String("username", "", "Cassandra username")
	password := flag.String("password", "", "Cassandra password")
	table := flag.String("table", "", "Table to query (mandatory)")
	columnParam := flag.String("column", "", "Column to count (mandatory)")
	groupByParam := flag.String("groupby", "", "Column to group by")
	whereParam := flag.String("where", "", "'where' statment to filter")
	interval := flag.Int("interval", 1000, "Row interval to log")

	flag.Parse()

	if len(*table) == 0 || len(*columnParam) == 0 || len(*keyspace) == 0 {
		showUsageAndExit()
	}

	var where string
	if *whereParam != "" {
		where = " where " + *whereParam
	}

	var groupColumn string
	var groupBy string
	var groupByExpression string
	if *groupByParam != "" {
		groupBy = ", cast(" + *groupByParam + " as text)"
		groupColumn = ", " + *groupByParam
		groupByExpression = " group by " + *groupByParam
	}
	fmt.Printf("Going to count %s nulls using query: select %s%s from %s%s%s\n", *columnParam, *columnParam, *&groupColumn, *table, where, groupByExpression)

	m := make(map[string]int)

	// Create a new Cassandra session
	cluster := gocql.NewCluster(*host)

	//cluster.PoolConfig.HostSelectionPolicy = gocql.DCAwareRoundRobinPolicy("dc1")
	cluster.Port = *port
	cluster.Keyspace = *keyspace
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: *username,
		Password: *password,
	}
	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	defer session.Close()

	// Define a CQL query
	query := "SELECT cast(" + *columnParam + " as text)" + groupBy + "  FROM " + *table + where

	// Execute the query and process the results
	iter := session.Query(query).Iter()
	var (
		field      string
		fieldGroup string
	)
	var i int
	if *groupByParam != "" {
		for iter.Scan(&field, &fieldGroup) {
			i++
			if field == "" {
				m[fieldGroup] += 1
			} else {
				m[fieldGroup] += 0
			}
			if i%*interval == 0 {
				fmt.Printf(" rows: %d\n", i)
			}
		}
	} else {
		for iter.Scan(&field) {
			i++
			if field == "" {
				m["SUM"] += 1
			}
			if i%*interval == 0 {
				fmt.Printf(" rows: %d", i)
			}
		}
	}
	if err := iter.Close(); err != nil {
		panic(err)
	}
	fmt.Println("result:", m)
}
