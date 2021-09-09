package bitcask

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"git.tcp.direct/tcp.direct/bitcask-mirror"
	jsoniter "github.com/json-iterator/go"

	"git.tcp.direct/kayos/chestnut-bitcask/log"
	"git.tcp.direct/kayos/chestnut-bitcask/storage"
)

const (
	logName   = "bitcask"
	storeName = "bitcask"
)

// bitcaskStore is an implementation the Storage interface for bitcask.
type bitcaskStore struct {
	opts storage.StoreOptions
	path string
	db   *bitcask.Bitcask
	log  log.Logger
}

var _ storage.Storage = (*bitcaskStore)(nil)

// NewStore is used to instantiate a datastore backed by bitcask.
func NewStore(path string, opt ...storage.StoreOption) storage.Storage {
	opts := storage.ApplyOptions(storage.DefaultStoreOptions, opt...)
	logger := log.Named(opts.Logger(), logName)
	if path == "" {
		logger.Panic("store path required")
	}
	return &bitcaskStore{path: path, opts: opts, log: logger}
}

// Options returns the configuration options for the store.
func (s *bitcaskStore) Options() storage.StoreOptions {
	return s.opts
}
// Open opens the store.

func (s *bitcaskStore) Open() (err error) {
	s.log.Debugf("opening store at path: %s", s.path)
	var path string
	path, err = ensureDBPath(s.path)
	if err != nil {
		err = s.logError("open", err)
		return
	}
	s.db, err = bitcask.Open(path)
	if err != nil {
		err = s.logError("open", err)
		return
	}
	if s.db == nil {
		err = errors.New("unable to open backing store")
		err = s.logError("open", err)
		return
	}
	s.log.Infof("opened store at path: %s", s.path)
	return
}

// Put an entry in the store.
func (s *bitcaskStore) Put(name string, key []byte, value []byte) error {
	s.log.Warnf("bitcask doesn't use name (%s)", name)
	s.log.Debugf("put: %d value bytes to key: %s", len(value), key)
	return s.logError("put", s.db.Put(key, value))
}

// Get a value from the store.
func (s *bitcaskStore) Get(name string, key []byte) ([]byte, error) {
	s.log.Warnf("bitcask doesn't use name (%s)", name)
	var value []byte
	var err error
	if value, err = s.db.Get(key); err != nil {
		return value, s.logError("load", err)
	}
	return value, nil
}

// Save the value in v and store the result at key.
func (s *bitcaskStore) Save(name string, key []byte, v interface{}) error {
	s.log.Warnf("bitcask doesn't use name (%s)", name)
	b, err := jsoniter.Marshal(v)
	if err != nil {
		return s.logError("save", err)
	}
	return s.db.Put(key, b)
}

// Load the value at key and stores the result in v.
func (s *bitcaskStore) Load(name string, key []byte, v interface{}) error {
	s.log.Warnf("bitcask doesn't use name (%s)", name)
	b, err := s.db.Get(key)
	if err != nil {
		return s.logError("load", err)
	}
	return s.logError("load", jsoniter.Unmarshal(b, v))
}

// Has checks for a key in the store.
func (s *bitcaskStore) Has(name string, key []byte) (bool, error) {
	s.log.Warnf("bitcask doesn't use name (%s)", name)
	s.log.Debugf("has: key: %s", key)
	return s.db.Has(key), nil
}

// Delete removes a key from the store.
func (s *bitcaskStore) Delete(name string, key []byte) error {
	s.log.Warnf("bitcask doesn't use name (%s)", name)
	s.log.Debugf("delete: key: %s", key)
		return s.db.Delete(key)
}

// List returns a list of all keys in the namespace.
func (s *bitcaskStore) List(name string) (keys [][]byte, err error) {
	s.log.Warnf("bitcask doesn't use name (%s)", name)
	s.log.Debugf("list: keys in bitcask storage")
	bkeys := s.db.Keys()
	select {
		case key := <- bkeys:
			keys = append(keys, key)
		default:
	}

	s.log.Debugf("list: found %d keys: %s", s.db.Len(), keys)
	return
}

// ListAll returns a mapped list of all keys in the store.
func (s *bitcaskStore) ListAll() (map[string][][]byte, error) {
	s.log.Debugf("list: keys in bitcask storage")
	keymap := make(map[string][][]byte)
	var err error
	keymap["bitcask"], err = s.List("")
	return keymap, err
}

// Export copies the datastore to directory at path.
func (s *bitcaskStore) Export(path string) error {
/*	s.log.Debugf("export: to path: %s", path)
	if path == "" {
		err := fmt.Errorf("invalid path: %s", path)
		return s.logError("export", err)
	} else if s.path == path {
		err := fmt.Errorf("path cannot be store path: %s", path)
		return s.logError("export", err)
	}
	var err error
	path, err = ensureDBPath(path)*/


	return errors.New("Export is not yet implemented for bitcask stores")
}

// Close closes the datastore and releases all db resources.
func (s *bitcaskStore) Close() error {
	s.log.Debugf("closing store at path: %s", s.path)
	err := s.db.Close()
	s.db = nil
	s.log.Info("store closed")
	return s.logError("close", err)
}

func (s *bitcaskStore) logError(name string, err error) error {
	if err == nil {
		return nil
	}
	if name != "" {
		err = fmt.Errorf("%s: %w", name, err)
	}
	s.log.Error(err)
	return err
}

func ensureDBPath(path string) (string, error) {
	if path == "" {
		return "", errors.New("path not found")
	}
	// does the path exist?
	info, err := os.Stat(path)
	exists := !os.IsNotExist(err)
	// this is some kind of actual error
	if err != nil && exists {
		return "", err
	}
	if exists && info.Mode().IsDir() {
		// if we have a directory, then append our default name
		path = filepath.Join(path, storeName)
	}
	return path, nil
}
