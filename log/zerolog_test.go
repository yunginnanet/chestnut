package log

import (
	"os"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/stretchr/testify/assert"
)

func TestWrapper(t *testing.T) {
	const (
		testName  = "test"
		emptyName = ""
	)
	tests := []struct {
		logger    interface{}
		name      string
		assertNil assert.ValueAssertionFunc
	}{
		{nil, emptyName, assert.Nil},
		{log.Logger, emptyName, assert.NotNil},
		{log.Logger, testName, assert.NotNil},
		{NewZerologLoggerWithLevel(ErrorLevel), emptyName, assert.NotNil},
		{NewZerologLoggerWithLevel(ErrorLevel), testName, assert.NotNil},
		{zerolog.New(os.Stderr), emptyName, assert.NotNil},
		{zerolog.New(os.Stderr), testName, assert.NotNil},
	}

	for _, test := range tests {
		logger := Named(test.logger, "name")
		test.assertNil(t, logger)
		if logger != nil {
			_, ok := logger.(Logger)
			assert.True(t, ok)
			// error
			logger.Error(testName)
			logger.Errorf("%s", testName)
		}
	}
}
