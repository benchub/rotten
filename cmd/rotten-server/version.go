package main

import "fmt"

var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

func versionText(command string) string {
	return fmt.Sprintf("%s %s (%s, %s)", command, version, commit, date)
}
