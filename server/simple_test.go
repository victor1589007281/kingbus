package server

import (
	"testing"

	"github.com/flike/kingbus/log"
	"github.com/stretchr/testify/require"
)

// TestLoggerAdapter tests the logger adapter functionality
func TestLoggerAdapter(t *testing.T) {
	// Initialize logger first
	log.InitLoggers("/tmp", "DEBUG")
	
	// Test that our logger adapter works
	adapter := &LoggerAdapter{log.Log}
	
	// This should not panic
	adapter.Warning("test warning")
	adapter.Warningf("test warning with format: %s", "test")
}

// TestBasicStructCreation tests basic struct creation
func TestBasicStructCreation(t *testing.T) {
	// Test creating a syncer without starting it
	syncer := &Syncer{}
	require.NotNil(t, syncer)
	
	// Test creating a binlog server without starting it
	server := &BinlogServer{}
	require.NotNil(t, server)
} 