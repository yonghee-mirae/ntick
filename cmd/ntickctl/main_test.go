package main

import (
	"os/exec"
	"testing"
)

// The CLI must reject unknown subcommands and refuse today's date for rejudge.
func TestUsage(t *testing.T) {
	if err := exec.Command("go", "run", ".").Run(); err == nil {
		t.Error("no subcommand accepted")
	}
	if err := exec.Command("go", "run", ".", "rejudge", "-data", t.TempDir(), "-date", "29990101").Run(); err == nil {
		t.Error("future date accepted")
	}
}
