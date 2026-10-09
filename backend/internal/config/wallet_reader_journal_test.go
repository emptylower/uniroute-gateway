//go:build unit

package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWalletReaderJournalDirectoryDefaultsToEmptyAndLoadsExplicitEnv(t *testing.T) {
	resetViperWithJWTSecret(t)
	cfg, err := Load()
	require.NoError(t, err)
	require.Empty(t, cfg.CanonicalWallet.ReaderJournalDirectory)
	directory := filepath.Join(t.TempDir(), "not-created-by-validation")
	t.Setenv("GATEWAY_WALLET_READER_JOURNAL_DIR", directory)
	cfg, err = Load()
	require.NoError(t, err)
	require.Equal(t, directory, cfg.CanonicalWallet.ReaderJournalDirectory)
	_, err = os.Stat(directory)
	require.True(t, os.IsNotExist(err), "configuration loading must not create journal storage")
}

func TestWalletReaderJournalDirectoryValidationHasNoFilesystemSideEffects(t *testing.T) {
	resetViperWithJWTSecret(t)
	cfg, err := Load()
	require.NoError(t, err)
	for _, directory := range []string{"relative/journal", "/var/lib/../journal", "/var/lib/journal/", " /var/lib/journal", "/var/lib/journal\x00", "/var/lib/journal\n"} {
		cfg.CanonicalWallet.ReaderJournalDirectory = directory
		require.ErrorContains(t, cfg.Validate(), "reader_journal_directory")
	}
	cfg.CanonicalWallet.ReaderJournalDirectory = ""
	require.NoError(t, cfg.Validate())
	cfg.CanonicalWallet.ReaderJournalDirectory = filepath.Join(t.TempDir(), "journal")
	require.NoError(t, cfg.Validate(), "a sane path does not require pre-existing storage at config-validation time")
}
