package bitcask

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"git.tcp.direct/tcp.direct/database/bitcask"
	jsoniter "github.com/json-iterator/go"

	"github.com/mholt/archiver/v4"

	"git.tcp.direct/kayos/chestnut/log"
	"git.tcp.direct/kayos/chestnut/storage"
)

const (
	logName   = "bitcask"
	storeName = "bitcask"
)

// bitcaskStore is an implementation the Storage interface for bitcask.
type bitcaskStore struct {
	opts storage.StoreOptions
	path string
	db   *bitcask.DB
	log  log.Logger
}

var _ storage.Storage = (*bitcaskStore)(nil)

var exportFormat = archiver.CompressedArchive{
	Compression: archiver.Gz{},
	Archival:    archiver.Tar{},
}

// NewStore is used to instantiate a datastore backed by bitcask.
func NewStore(path string, opt ...storage.StoreOption) storage.Storage {
	opts := storage.ApplyOptions(storage.DefaultStoreOptions, opt...)
	logger := log.Named(opts.Logger(), logName)
	if path == "" {
		logger.Panic("NewStore: store path required")
	}
	st := &bitcaskStore{path: path, opts: opts, log: logger}
	return st
}

// Options returns the configuration options for the store.
func (st *bitcaskStore) Options() storage.StoreOptions {
	return st.opts
}

// Open opens the store.
func (st *bitcaskStore) Open() (err error) {
	st.log.Debugf("opening store at path: %s", st.path)
	var path string
	path, err = ensureDBPath(st.path)
	if err != nil {
		err = st.logError("open", err)
		return
	}
	pathDir, pathFile := filepath.Split(path)
	finfo, err1 := os.Stat(filepath.Join(pathDir, pathFile+".tar.gz"))
	finfo2, err2 := os.Stat(filepath.Join(path, "bitcask.tar.gz"))
	if err1 == nil {
		if finfo.IsDir() {
			st.log.Panic("NewStore: export file is a directory")
		}
		path = filepath.Join(pathDir, pathFile+".tar.gz")
	}
	if err2 == nil {
		if finfo2.IsDir() {
			st.log.Panic("NewStore: export file is a directory")
		}
		path = filepath.Join(path, "bitcask.tar.gz")
	}
	var storeNames []string
	_, fileName := filepath.Split(path)
	if strings.HasSuffix(fileName, ".tar.gz") {
		st.log.Infof("NewStore: preparing to extract tar.gz file: %s", path)
		path = filepath.Dir(path)
		if strings.HasSuffix(path, storeName) {
			path = filepath.Join(path, "..")
		}
		importErr := os.MkdirAll(path, 0755)
		if importErr != nil {
			st.log.Fatalf("NewStore: failed to create directory to restore export: %s", importErr)
			return nil
		}
		var f *os.File
		f, importErr = os.Open(filepath.Join(path, fileName))
		if importErr != nil {
			st.log.Fatalf("NewStore: failed to open arcive to restore export: %s", importErr)
			return nil
		}
		defer func() {
			_ = f.Close()
		}()
		st.log.Debugf("NewStore: extracting archive: %s", f.Name())
		importErr = exportFormat.Extract(
			context.Background(), f, nil,
			func(ctx context.Context, af archiver.File) error {
				var afErr error
				if af.IsDir() {
					st.log.Debugf("NewStore: creating directory: %s", af.Name())
					afErr = os.MkdirAll(filepath.Join(path, af.NameInArchive), 0755)
					if afErr != nil {
						return fmt.Errorf("MkdirAll: %w", afErr)
					}
					return nil
				}
				var afile io.Reader
				afile, afErr = af.Open()
				if afErr != nil {
					return fmt.Errorf("archive.Open: %w", afErr)
				}
				var tf *os.File
				extractTo := filepath.Join(path, af.NameInArchive)
				targetDir, targetFile := filepath.Split(extractTo)
				if targetFile == "lock" {
					return nil
				}
				if targetFile == "stores.json" {
					storesFile, err := af.Open()
					if err != nil {
						return fmt.Errorf("open stores.json: %w", err)
					}
					storesData, err := io.ReadAll(storesFile)
					if err != nil {
						return fmt.Errorf("io err stores.json: %w", err)
					}
					err = jsoniter.Unmarshal(storesData, &storeNames)
					if err != nil {
						return fmt.Errorf("unmarshal stores.json: %w", err)
					}
				}
				err := os.MkdirAll(targetDir, 0755)
				if err != nil {
					return fmt.Errorf("MkdirAll: %w", err)
				}
				st.log.Debugf("NewStore: extracting %s to %s", af.NameInArchive, extractTo)
				tf, afErr = os.Create(extractTo)
				if afErr != nil {
					return fmt.Errorf("os.Create: %w", afErr)
				}
				_, afErr = io.Copy(tf, afile)
				if afErr != nil {
					return fmt.Errorf("io.Copy: %w", afErr)
				}
				return nil
			})
		if importErr != nil {
			st.log.Fatalf("NewStore: failed to restore export: %s", importErr)
			return nil
		}
		if len(storeNames) == 0 {
			st.log.Fatalf("NewStore: failed to restore export: no stores found")
			return nil
		}
	}
	st.db = bitcask.OpenDB(path)
	if st.db == nil {
		err = errors.New("unable to open backing store")
		err = st.logError("open", err)
		return
	}
	if len(storeNames) > 0 {
		for _, sto := range storeNames {
			st.log.Infof("NewStore: restoring store: %s", sto)
			err = st.db.Init(sto)
			if err != nil {
				st.log.Errorf("NewStore: failed to restore store: %s: %s", sto, err)
			}
		}
		restored, restoreErr := st.ListAll()
		if restoreErr != nil {
			st.log.Errorf("NewStore: failed to list restored: %s", restoreErr)
			return nil
		}
		if len(restored) != len(storeNames) {
			st.log.Errorf("NewStore: failed to restore all stores: %d stores restored", len(restored))
			return nil
		}
		var count int
		for _, sto := range restored {
			count = count + len(sto)
		}
		st.log.Infof("NewStore: restored %d keys", count)
	}
	st.log.Infof("opened store at path: %s", st.path)
	return
}

// Put an entry in the store.
func (st *bitcaskStore) Put(name string, key []byte, value []byte) error {
	if len(key) < 1 {
		return st.logError("put", errors.New("key cannot be empty"))
	}
	if len(value) < 1 {
		return st.logError("put", errors.New("value cannot be empty"))
	}
	st.log.Debugf("put: %d value bytes to key: %s", len(value), key)
	return st.logError("put", st.db.WithNew(name).Put(key, value))
}

// Get a value from the store.
func (st *bitcaskStore) Get(name string, key []byte) ([]byte, error) {
	if len(key) < 1 {
		return nil, st.logError("put", errors.New("key cannot be empty"))
	}
	var value []byte
	var err error
	if value, err = st.db.WithNew(name).Get(key); err != nil {
		return value, st.logError("load", err)
	}
	return value, nil
}

// Save the value in v and store the result at key.
func (st *bitcaskStore) Save(name string, key []byte, v interface{}) error {
	if len(key) < 1 {
		return st.logError("save", errors.New("key cannot be empty"))
	}
	b, err := jsoniter.Marshal(v)
	if err != nil {
		return st.logError("save", err)
	}
	return st.db.WithNew(name).Put(key, b)
}

// Load the value at key and stores the result in v.
func (st *bitcaskStore) Load(name string, key []byte, v interface{}) error {
	if len(key) < 1 {
		return st.logError("load", errors.New("key cannot be empty"))
	}
	b, err := st.db.WithNew(name).Get(key)
	if err != nil {
		return st.logError("load", err)
	}
	return st.logError("load", jsoniter.Unmarshal(b, v))
}

// Has checks for a key in the store.
func (st *bitcaskStore) Has(name string, key []byte) (bool, error) {
	if len(key) < 1 {
		return false, st.logError("has", errors.New("key cannot be empty"))
	}
	st.log.Debugf("has: key: %s", key)
	return st.db.WithNew(name).Has(key), nil
}

// Delete removes a key from the store.
func (st *bitcaskStore) Delete(name string, key []byte) error {
	if len(key) < 1 {
		return st.logError("delete", errors.New("key cannot be empty"))
	}
	st.log.Debugf("delete: key: %s", key)
	return st.db.WithNew(name).Delete(key)
}

// List returns a list of all keys in the namespace.
func (st *bitcaskStore) List(name string) (keys [][]byte, err error) {
	st.log.Debugf("list: keys in bitcask store named: %s", name)
	keys = st.db.WithNew(name).Keys()
	st.log.Debugf("list: found %d keys: %s", st.db.WithNew(name).Len(), keys)
	return
}

// ListAll returns a mapped list of all keys in the store.
func (st *bitcaskStore) ListAll() (map[string][][]byte, error) {
	st.log.Debugf("list: all keys in bitcask storage")
	keymap := make(map[string][][]byte)
	var err error
	for n, s := range st.db.AllStores() {
		for _, k := range s.Keys() {
			keymap[n] = append(keymap[n], k)
		}
	}
	return keymap, err
}

func (st *bitcaskStore) writeAllStoreNames() error {
	all, err := st.ListAll()
	if err != nil {
		return st.logError("export", err)
	}
	var storeNames []string
	for n := range all {
		storeNames = append(storeNames, n)
	}
	writeNamesTo, err := os.OpenFile(filepath.Join(st.path, "stores.json"), os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return st.logError("export", err)
	}
	var namesJSON []byte
	namesJSON, err = jsoniter.Marshal(storeNames)
	if err != nil {
		return st.logError("export", err)
	}
	if len(namesJSON) < 1 {
		return st.logError("export", errors.New("no store names found"))
	}
	var written int
	written, err = writeNamesTo.Write(namesJSON)
	if written != len(namesJSON) {
		return st.logError("export", errors.New("unable to write all store names"))
	}
	return writeNamesTo.Close()
}

// Export copies the datastore to directory at path.
func (st *bitcaskStore) Export(path string) error {
	st.log.Debugf("export: to path: %s", path)
	if path == "" {
		err := fmt.Errorf("invalid path: %s", path)
		return st.logError("export", err)
	} else if st.path == path {
		err := fmt.Errorf("path cannot be store path: %s", path)
		return st.logError("export", err)
	}
	var err error
	path, err = ensureDBPath(path)
	if err != nil {
		return st.logError("export", err)
	}
	if err = st.writeAllStoreNames(); err != nil {
		return st.logError("export", err)
	}
	err = st.Close()
	if err != nil {
		return st.logError("export", err)
	}
	defer func() {
		if openErr := st.Open(); openErr != nil {
			err = st.logError("export",
				fmt.Errorf("unable to reopen bitcask store: %s", openErr))
		}
	}()
	filesystem := os.DirFS(st.path)
	var files = make(map[string]string)
	err = fs.WalkDir(filesystem, ".", func(walkPath string, d fs.DirEntry, err error) error {
		if walkPath == "." {
			return nil
		}
		if _, fileName := filepath.Split(walkPath); fileName == "lock" {
			return nil
		}
		st.log.Debugf("export: found file: %s", walkPath)
		if err == nil {
			files[filepath.Join(st.path, walkPath)] = strings.TrimPrefix(walkPath, storeName)
		}
		return err
	})
	if err != nil {
		return st.logError("export", err)
	}
	var archiveFiles []archiver.File
	var out *os.File
	defer func() {
		_ = out.Close()
	}()
	archiveFiles, err = archiver.FilesFromDisk(nil, files)
	st.log.Debugf("export: found %d files", len(archiveFiles))
	targetDir, targetName := filepath.Split(path)
	target := filepath.Join(targetDir, targetName+".tar.gz")
	st.log.Debugf("export: creating archive: %s", target)
	out, err = os.Create(target)
	err = exportFormat.Archive(context.Background(), out, archiveFiles)
	if err != nil {
		return st.logError("export", err)
	}
	return nil
}

// Close closes the datastore and releases all db resources.
func (st *bitcaskStore) Close() error {
	st.log.Debugf("closing store at path: %s", st.path)
	err := st.db.CloseAll()
	st.db = nil
	st.log.Info("store closed")
	if errors.Is(err, bitcask.ErrNoStores) {
		return nil
	}
	return st.logError("close", err)
}

func (st *bitcaskStore) logError(name string, err error) error {
	if err == nil {
		return nil
	}
	if name != "" {
		err = fmt.Errorf("%s: %w", name, err)
	}
	st.log.Error(err)
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
