// Command rotten-worker watches an observed database's pg_stat_statements and
// records what it sees in the rotten DB.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/benchub/rotten/internal/identity"
	"github.com/benchub/rotten/internal/worker"
)

var configFileFlag = flag.String("config", "", "the config file")
var noIdleHandsFlag = flag.Bool("noIdleHands", false, "when set to true, kill us (ungracefully) if we seem to be doing nothing")
var debugFlag = flag.Bool("debug", false, "when set to true, turn on debugging")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var memprofile = flag.String("memprofile", "", "write mem profile to file")

type Configuration struct {
	ObservedDBConn      []string
	RottenDBConn        []string
	StatusInterval      uint32
	ObservationInterval uint32
	SanityCheck         string
	FQDN                string
	Project             string
	Environment         string
	Cluster             string
	Role                string
	ContextController   string
	ContextAction       string
	ContextJob          string
}

func remakeSSLCertConfig(connectionString string, host string) (*tls.Config, error) {
	// hacky hack solution to get the rootca files, as well as the client certs, so that we can build up a cert chain with all the intermediate certs.
	connectionStringSettings := make(map[string]string)

	// Split the string by spaces to get each key-value pair
	pairs := strings.Split(connectionString, " ")

	for _, pair := range pairs {
		// Split each pair by the equals sign to separate the key from the value
		kv := strings.Split(pair, "=")
		if len(kv) == 2 {
			// Insert the key and value into the map
			connectionStringSettings[kv[0]] = kv[1]
		}
	}

	// If we didn't pass in a host we want to explicitly use, just
	// use the first host in our list of hosts (i.e. host=host1[,host2[,host3]])
	if host == "" {
		host = strings.Split(connectionStringSettings["host"], ",")[0]
	}

	// Load root CA cert
	rootCertPool := x509.NewCertPool()
	rootCert, err := os.ReadFile(connectionStringSettings["sslrootcert"])
	if err != nil {
		return nil, fmt.Errorf("error loading root certificate: %w", err)
	}

	// Load client cert & key
	clientCert, err := os.ReadFile(connectionStringSettings["sslcert"])
	if err != nil {
		return nil, fmt.Errorf("failed to read client certificate file: %w", err)
	}
	clientKey, err := os.ReadFile(connectionStringSettings["sslkey"])
	if err != nil {
		return nil, fmt.Errorf("failed to read client key file: %w", err)
	}

	ok := rootCertPool.AppendCertsFromPEM(rootCert)
	if !ok {
		return nil, fmt.Errorf("failed to append root certificate to pool")
	}

	if *debugFlag {
		var block *pem.Block
		log.Println("Loaded Root CA Certificates:")
		rootsPEM := rootCert
		block, rootsPEM = pem.Decode(rootsPEM)
		if block != nil {
			if block.Type == "CERTIFICATE" {
				caCert, err := x509.ParseCertificate(block.Bytes)
				if err != nil {
					return nil, fmt.Errorf("error parsing certificate: %w", err)
				}
				log.Printf("\tSubject: %s\n", caCert.Subject)
			}
		}
	}

	// Append the client cert and CA chain to get a full certificate chain
	clientChain := append(clientCert, []byte("\n")...)
	clientChain = append(clientChain, rootCert...)
	clientCerts, err := tls.X509KeyPair(clientChain, clientKey)
	if err != nil {
		return nil, fmt.Errorf("error loading client key pair: %w", err)
	}

	if *debugFlag {
		log.Println("Client Certificate and Chain:")
		for _, cert := range clientCerts.Certificate {
			parsedCert, err := x509.ParseCertificate(cert)
			if err != nil {
				return nil, fmt.Errorf("error parsing client certificate: %w", err)
			}
			log.Printf("\tSubject: %s\n", parsedCert.Subject)
		}
	}

	if *debugFlag {
		log.Println("Making tls config for", host)
	}
	// Create a custom TLS config with specific versions and cipher suites
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{clientCerts},
		ClientCAs:    rootCertPool,
		RootCAs:      rootCertPool,
		ServerName:   host, // Set the ServerName to the host you are connecting to
		MinVersion:   tls.VersionTLS12,
		MaxVersion:   tls.VersionTLS13,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		},
	}

	return tlsConfig, nil
}

func main() {
	var cfg worker.Config
	var status_interval uint32

	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
	}

	if len(os.Args) == 1 {
		flag.PrintDefaults()
		os.Exit(0)
	}

	sigs := make(chan os.Signal, 1)
	// catch all signals since not explicitly listing
	signal.Notify(sigs, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	// method invoked upon seeing signal
	go func() {
		s := <-sigs
		log.Printf("RECEIVED SIGNAL: %s", s)
		AppCleanup()
		os.Exit(1)
	}()

	if *configFileFlag == "" {
		log.Fatalln("I need a config file!")
		// will now exit because Fatal
	} else {
		configFile, err := os.Open(*configFileFlag)
		if err != nil {
			log.Fatalln("opening config file:", err)
			// will now exit because Fatal
		}

		decoder := json.NewDecoder(configFile)
		configuration := &Configuration{}
		decoder.Decode(&configuration)

		// build up our connection to the rotten DB
		rottenDBConfig, err := pgxpool.ParseConfig(configuration.RottenDBConn[0])
		if err != nil {
			log.Fatalln("couldn't create rottenDBConfig", err)
			// will now exit because Fatal
		}
		rottenDBConfig.MaxConnLifetime = time.Second * 10
		rottenDBConfig.MaxConns = 5
		rottenDBConfig.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec

		if rottenDBConfig.ConnConfig.TLSConfig != nil {
			if rottenDBConfig.ConnConfig.TLSConfig.RootCAs != nil {
				// golang libraries don't seem to send intermediate certs, so we have to manually make sure that happens.
				if *debugFlag {
					log.Println("We seem to have a root CA for rotten DB; remaking the chain to be sure to capture any intermediate certs.", configuration.RottenDBConn[0])
				}

				rottenDBConfig.ConnConfig.TLSConfig, err = remakeSSLCertConfig(configuration.RottenDBConn[0], "")
				if err != nil {
					log.Fatalln("couldn't remake rotten db TLS config:", err)
				}

				for i := 0; i < len(rottenDBConfig.ConnConfig.Fallbacks); i++ {
					rottenDBConfig.ConnConfig.Fallbacks[i].TLSConfig, err = remakeSSLCertConfig(configuration.RottenDBConn[0], rottenDBConfig.ConnConfig.Fallbacks[i].Host)
					if err != nil {
						log.Fatalln("couldn't remake rotten db fallback TLS config:", err)
					}
				}
			}
		}

		rottenDB, err := pgxpool.NewWithConfig(context.Background(), rottenDBConfig)
		if err != nil {
			log.Fatalln("couldn't connect to rotten db", err)
			// will now exit because Fatal
		}
		cfg.RottenDB = rottenDB

		// Now build up the two connections we're going to use for the observed db
		// First, make the config, then reuse it twice (2 connections to the same db)
		observedDBConfig, err := pgx.ParseConfig(configuration.ObservedDBConn[0])
		if err != nil {
			log.Fatalln("couldn't create observedDBConfig", err)
			// will now exit because Fatal
		}

		// Don't get in the way of pgBouncer transaction pooling
		observedDBConfig.DefaultQueryExecMode = pgx.QueryExecModeExec

		if observedDBConfig.TLSConfig != nil {
			if observedDBConfig.TLSConfig.RootCAs != nil {
				// golang libraries don't seem to send intermediate certs, so we have to manually make sure that happens.
				if *debugFlag {
					log.Printf("We seem to have a root CA for observed DB; remaking the chain to be sure to capture any intermediate certs.")
				}

				observedDBConfig.TLSConfig, err = remakeSSLCertConfig(configuration.ObservedDBConn[0], "")
				if err != nil {
					log.Fatalln("couldn't remake observed db TLS config:", err)
				}
			}
		}

		observedDB, err := pgx.ConnectConfig(context.Background(), observedDBConfig)
		if err != nil {
			log.Fatalln("couldn't connect to observed db", err)
			// will now exit because Fatal
		}
		defer observedDB.Close(context.Background())
		cfg.ObservedDB = observedDB

		observedDBReset, err := pgx.ConnectConfig(context.Background(), observedDBConfig)
		if err != nil {
			log.Fatalln("couldn't connect to observed db for resets", err)
			// will now exit because Fatal
		}
		defer observedDBReset.Close(context.Background())
		cfg.ObservedDBReset = observedDBReset

		status_interval = configuration.StatusInterval
		cfg.ObservationInterval = configuration.ObservationInterval
		cfg.SanityCheck = configuration.SanityCheck
		fqdn := configuration.FQDN
		project := configuration.Project
		environment := configuration.Environment
		cluster := configuration.Cluster
		role := configuration.Role
		cfg.ReController, cfg.ReAction, cfg.ReJobTag, err = identity.CompileRegexes(configuration.ContextController, configuration.ContextAction, configuration.ContextJob)
		if err != nil {
			log.Fatalln(err)
			// will now exit because Fatal
		}

		// find out the logical source ID we will be using
		if err := rottenDB.QueryRow(context.Background(), `select id from logical_sources where project=$1 and environment=$2 and cluster=$3 and role=$4`, project, environment, cluster, role).Scan(&cfg.LogicalID); err == nil {
			// yay, we have our ID
		} else if err == pgx.ErrNoRows {
			if err := rottenDB.QueryRow(context.Background(), `insert into logical_sources(project,environment,cluster,role) values ($1,$2,$3,$4) returning id`, project, environment, cluster, role).Scan(&cfg.LogicalID); err == nil {
				// yay, we have our ID
			} else {
				log.Fatalln("couldn't insert into logical_sources", err)
				// will now exit because Fatal
			}
		} else {
			log.Fatalln("couldn't select from logical_sources", err)
			// will now exit because Fatal
		}

		// find out the physical source ID we will be using
		if err := rottenDB.QueryRow(context.Background(), `select id from physical_sources where fqdn=$1`, fqdn).Scan(&cfg.PhysicalID); err == nil {
			// yay, we have our ID
		} else if err == pgx.ErrNoRows {
			if err := rottenDB.QueryRow(context.Background(), `insert into physical_sources(fqdn) values ($1) returning id`, fqdn).Scan(&cfg.PhysicalID); err == nil {
				// yay, we have our ID
			} else {
				log.Fatalln("couldn't insert into physical_sources", err)
				// will now exit because Fatal
			}
		} else {
			log.Fatalln("couldn't select from physical_sources", err)
			// will now exit because Fatal
		}
	}

	w := worker.New(cfg, worker.RealClock{})

	// We like stats
	go w.ReportProgress(context.Background(), *noIdleHandsFlag, status_interval)

	w.Run(context.Background())

	// until we implement graceful exiting, we'll never get here
	// AppCleanup()
}

func AppCleanup() {
	log.Println("...and that's all folks!")
	pprof.StopCPUProfile()
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
		f.Close()
	}
}
