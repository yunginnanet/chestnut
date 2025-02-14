package chestnut

import (
	"errors"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"git.tcp.direct/kayos/chestnut/encoding/compress"
	"git.tcp.direct/kayos/chestnut/encoding/compress/zstd"
	"git.tcp.direct/kayos/chestnut/encryptor"
	"git.tcp.direct/kayos/chestnut/encryptor/aes"
	"git.tcp.direct/kayos/chestnut/encryptor/crypto"
	"git.tcp.direct/kayos/chestnut/log"
	"git.tcp.direct/kayos/chestnut/storage"
	"git.tcp.direct/kayos/chestnut/storage/bolt"
	"git.tcp.direct/kayos/chestnut/storage/nuts"
	"git.tcp.direct/kayos/chestnut/value"
)

type TObject struct {
	ValueA string `json:"value_a"`
	ValueB int    `json:"value_b"`
}

type THash struct {
	TObject
	HashValueA string `json:"hash_value_a,hash"`
	HashValueB int    `json:"hash_value_b,hash"`
}

type TSecure struct {
	TObject
	SecureValueA string `json:"sparse_value_a,secure"`
	SecureValueB int    `json:"sparse_value_b,secure"`
}

type TAll struct {
	TObject
	Hash      THash
	Secure    TSecure
	AllValueA string `json:"all_value_a,secure,hash"`
	AllValueB int    `json:"all_value_b,secure,hash"`
}

type testCase struct {
	key       string
	value     string
	err       assert.ErrorAssertionFunc
	assertHas assert.BoolAssertionFunc
}

var (
	testName     = "test-namespace"
	testValue    = "i-am-plaintext"
	textSecret   = crypto.TextSecret("i-am-a-good-secret")
	encryptorOpt = WithAES(crypto.Key256, aes.CFB, textSecret)
)

var lorumIpsum = `Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed
do eiusmod tempor incididunt ut labore et dolore magna aliqua. Eu consequat ac
felis donec et odio pellentesqu diam. Hac habitasse platea dictumst quisque
sagittis purus. Risus at ultrices mi tempus imperdiet nulla malesuada
pellentesque. Vitae justo eget magna fermentum iaculis eu non diam phasellus.
Cursus risus at ultrices mi tempus imperdiet. Ante metus dictum at tempor
commodo. Accumsan lacus vel facilisis volutpat est velit egestas. Dolor sed
viverra ipsum nunc aliquet bibendum enim facilisis. Tristique risus nec feugiat
in. Feugiat nisl pretium fusce id velit ut tortor pretium. Eget magna fermentum
iaculis eu. Velit laoreet id donec ultrices tincidunt. Tristique senectus et
netus et malesuada fames ac turpis egestas. Diam phasellus vestibulum lorem sed
risus ultricies tristique. Cursus mattis molestie a iaculis. Sem nulla pharetra
diam sit amet nisl suscipit adipiscing bibendum. Viverra justo nec ultrices dui
sapien eget. Ornare arcu dui vivamus arcu felis. Egestas integer eget aliquet
nibh praesent. Risus feugiat in ante metus dictum at tempor commodo. Id ornare
arcu odio ut sem. Tincidunt dui ut ornare lectus. Sagittis orci a scelerisque
purus. Suscipit adipiscing bibendum est ultricies integer quis. Libero nunc
consequat interdum varius sit amet mattis vulputate. Euismod lacinia at quis
risus sed vulputate. Molestie at elementum eu facilisis sed odio morbi quis.
Nunc sed augue lacus viverra vitae congue eu consequat ac. Interdum velit
euismod in pellentesque. Mi sit amet mauris commodo quis imperdiet massa
tincidunt. Eget magna fermentum iaculis eu. Metus aliquam eleifend mi in nulla
posuere sollicitudin. Nisi est sit amet facilisis magna. Tellus in hac habitasse
platea dictumst. Venenatis tellus in metus vulputate eu scelerisque. Feugiat sed
lectus vestibulum mattis. Sit amet nisl suscipit adipiscing bibendum est
ultricies. Ipsum nunc aliquet bibendum enim facilisis gravida neque. Duis
tristique sollicitudin nibh sit amet commodo. Purus in massa tempor nec. Eget
aliquet nibh praesent tristique magna sit amet. Mauris in aliquam semf ringilla`

var valOut = testValue

var objectSrc = TObject{
	ValueA: testValue,
	ValueB: 42,
}

var objOut = objectSrc

var hashSrc = THash{
	TObject:    objectSrc,
	HashValueA: testValue,
	HashValueB: 1600,
}

var hashOut = THash{
	TObject:    objOut,
	HashValueA: "sha256:0fdabf2262ab284503a700b876994fc95ee4690133db96acfb5f9ea526d71e94",
	HashValueB: 1600,
}

var secureSrc = TSecure{
	TObject:      objectSrc,
	SecureValueA: testValue,
	SecureValueB: 1337,
}

var secureOut = TSecure{
	TObject:      objOut,
	SecureValueA: "i-am-plaintext",
	SecureValueB: 1337,
}

var secureSparse = TSecure{
	TObject:      objOut,
	SecureValueA: "",
	SecureValueB: 0,
}

var allSrc = TAll{
	TObject:   objectSrc,
	Hash:      hashSrc,
	Secure:    secureSrc,
	AllValueA: "i-am-a-random-string",
	AllValueB: 0xbeef,
}

var allOut = TAll{
	TObject:   objOut,
	Hash:      hashOut,
	Secure:    secureOut,
	AllValueA: "sha256:50d5a31ee8353543fe8d6c0de2c9d5e5e2cdb7b973c4f9c25f99fcdf41bd5eec",
	AllValueB: 0xbeef,
}

var allSparse = TAll{
	TObject:   objOut,
	Hash:      hashOut,
	Secure:    secureSparse,
	AllValueA: "",
	AllValueB: 0,
}

type objTest struct {
	key string
	src interface{}
	dst interface{}
	out interface{}
	spr interface{}
	err assert.ErrorAssertionFunc
}

var putTests = []testCase{
	{"", "", assert.Error, assert.False},
	{"a", "", assert.Error, assert.False},
	{"b", testValue, assert.NoError, assert.True},
	{"c/c", testValue, assert.NoError, assert.True},
	{".d", testValue, assert.NoError, assert.True},
	{newKey(), testValue, assert.NoError, assert.True},
}

var tests = append(putTests,
	testCase{"not-found", "", assert.Error, assert.False},
)

var objTests = []objTest{
	{"", nil, nil, nil, nil, assert.Error},
	{"a", nil, nil, nil, nil, assert.Error},
	{"b", testValue, new(string), &valOut, &valOut, assert.NoError},
	{newKey(), testValue, new(string), &valOut, &valOut, assert.NoError},
	{newKey(), objectSrc, &TObject{}, &objOut, &objOut, assert.NoError},
	{newKey(), hashSrc, &THash{}, &hashOut, &hashOut, assert.NoError},
	{newKey(), secureSrc, &TSecure{}, &secureOut, &secureSparse, assert.NoError},
	{newKey(), allSrc, &TAll{}, &allOut, &allSparse, assert.NoError},
}

func newKey() string {
	return uuid.New().String()
}

func nutsStore(t *testing.T, path string) storage.Storage {
	store := nuts.NewStore(path)
	assert.NotNil(t, store)
	return store
}

func boltStore(t *testing.T, path string) storage.Storage {
	store := bolt.NewStore(path)
	assert.NotNil(t, store)
	return store
}

type StoreFunc = func(t *testing.T, path string) storage.Storage

type ChestnutTestSuite struct {
	suite.Suite
	storeFunc StoreFunc
	cn        *Chestnut
}

func TestChestnut(t *testing.T) {
	testStores := []StoreFunc{nutsStore, boltStore}
	for _, test := range testStores {
		ts := new(ChestnutTestSuite)
		ts.storeFunc = test
		suite.Run(t, ts)
	}
}

func (ts *ChestnutTestSuite) SetupTest() {
	store := ts.storeFunc(ts.T(), ts.T().TempDir())
	ts.NotNil(store)
	ts.cn = NewChestnut(store, encryptorOpt)
	ts.NotNil(ts.cn)
	err := ts.cn.Open()
	ts.NoError(err)
}

func (ts *ChestnutTestSuite) TearDownTest() {
	err := ts.cn.Close()
	ts.NoError(err)
}

func (ts *ChestnutTestSuite) BeforeTest(_, testName string) {
	switch testName {
	case "TestChestnut_Put",
		"TestChestnut_List",
		"TestChestnut_Save":
		break
	case "TestChestnut_Load",
		"TestChestnut_Sparse":
		ts.TestChestnut_Save()
		break
	case "TestChestnut_LoadKeyed",
		"TestChestnut_SparseKeyed":
		ts.TestChestnut_SaveKeyed()
		break
	default:
		ts.TestChestnut_Put()
	}
}

func (ts *ChestnutTestSuite) TestChestnut_Put() {
	for i, test := range putTests {
		err := ts.cn.Put(testName, []byte(test.key), []byte(test.value))
		test.err(ts.T(), err, "%d test key: %s", i, test.key)
	}
}

func (ts *ChestnutTestSuite) TestChestnut_Get() {
	for i, test := range tests {
		value, err := ts.cn.Get(testName, []byte(test.key))
		test.err(ts.T(), err, "%d test key: %s", i, test.key)
		ts.Equal(test.value, string(value),
			"%d test key: %s", i, test.key)
	}
}

func (ts *ChestnutTestSuite) TestChestnut_Save() {
	for i, test := range objTests {
		err := ts.cn.Save(testName, []byte(test.key), test.src)
		test.err(ts.T(), err, "%d test key: %s", i, test.key)
	}
}

func (ts *ChestnutTestSuite) TestChestnut_Load() {
	for i, test := range objTests {
		if test.dst == nil {
			continue
		}
		typ := reflect.ValueOf(test.dst).Elem().Type()
		ptr := reflect.New(typ).Interface()
		err := ts.cn.Load(testName, []byte(test.key), ptr)
		test.err(ts.T(), err, "%d test key: %s", i, test.key)
		ts.Equal(test.out, ptr)
	}
}

func (ts *ChestnutTestSuite) TestChestnut_Sparse() {
	for i, test := range objTests {
		if test.dst == nil {
			continue
		}
		typ := reflect.ValueOf(test.dst).Elem().Type()
		ptr := reflect.New(typ).Interface()
		err := ts.cn.Sparse(testName, []byte(test.key), ptr)
		test.err(ts.T(), err, "%d test key: %s", i, test.key)
		ts.Equal(test.spr, ptr)
	}
}

var keyedObj = value.NewSecureValue(newKey(), []byte(lorumIpsum))

func (ts *ChestnutTestSuite) TestChestnut_SaveKeyed() {
	objs := []struct {
		in  *value.Secure
		err assert.ErrorAssertionFunc
	}{
		{nil, assert.Error},
		{&value.Secure{}, assert.Error},
		{keyedObj, assert.NoError},
	}
	for i, test := range objs {
		err := ts.cn.SaveKeyed(test.in)
		test.err(ts.T(), err, "%d test", i)
	}
}

func (ts *ChestnutTestSuite) TestChestnut_LoadKeyed() {
	objs := []struct {
		in  *value.Secure
		out *value.Secure
		err assert.ErrorAssertionFunc
	}{
		{nil, nil, assert.Error},
		{&value.Secure{}, nil, assert.Error},
		{&value.Secure{ID: value.ID{ID: "not-found"}}, nil, assert.Error},
		{&value.Secure{ID: keyedObj.ID}, keyedObj, assert.NoError},
	}
	for i, test := range objs {
		err := ts.cn.LoadKeyed(test.in)
		test.err(ts.T(), err, "%d test", i)
		if err == nil {
			ts.Equal(test.out, test.in)
		}
	}
}

func (ts *ChestnutTestSuite) TestChestnut_SparseKeyed() {
	sparse := &value.Secure{
		ID:       keyedObj.ID,
		Metadata: map[string]interface{}{},
	}
	objs := []struct {
		in  *value.Secure
		out *value.Secure
		err assert.ErrorAssertionFunc
	}{
		{nil, nil, assert.Error},
		{&value.Secure{}, nil, assert.Error},
		{in: &value.Secure{ID: value.ID{ID: "not-found"}}, err: assert.Error},
		{&value.Secure{ID: keyedObj.ID}, sparse, assert.NoError},
	}
	for i, test := range objs {
		err := ts.cn.SparseKeyed(test.in)
		test.err(ts.T(), err, "%d test", i)
		if err == nil {
			ts.Equal(test.out, test.in)
		}
	}
}

func (ts *ChestnutTestSuite) TestChestnut_Has() {
	for i, test := range tests {
		has, _ := ts.cn.Has(testName, []byte(test.key))
		test.assertHas(ts.T(), has, "%d test key: %s", i, test.key)
	}
}

func (ts *ChestnutTestSuite) TestChestnut_List() {
	const listLen = 100
	list := make([]string, listLen)
	for i := 0; i < listLen; i++ {
		list[i] = uuid.New().String()
		err := ts.cn.Put(testName, []byte(list[i]), []byte(testValue))
		ts.NoError(err)
	}
	keys, err := ts.cn.List(testName)
	ts.NoError(err)
	ts.Len(keys, listLen)
	// put both lists in the same order so we can compare them
	strKeys := make([]string, len(keys))
	for i, k := range keys {
		strKeys[i] = string(k)
	}
	sort.Strings(list)
	sort.Strings(strKeys)
	ts.Equal(list, strKeys)
}

func (ts *ChestnutTestSuite) TestChestnut_Delete() {
	var deleteTests = []struct {
		key string
		err assert.ErrorAssertionFunc
	}{
		//		{"", assert.Error},
		{"a", assert.NoError},
		{"b", assert.NoError},
		{"c/c", assert.NoError},
		{".d", assert.NoError},
		{"eee", assert.NoError},
		{"not-found", assert.NoError},
	}
	for i, test := range deleteTests {
		err := ts.cn.Delete(testName, []byte(test.key))
		test.err(ts.T(), err, "%d test key: %s", i, test.key)
	}
}

func (ts *ChestnutTestSuite) TestStore_Export() {
	err := ts.cn.Export(ts.T().TempDir())
	ts.NoError(err)
}

func (ts *ChestnutTestSuite) TestStore_SecureEntry() {
	const (
		testKey   = "hello"
		testValue = "world"
		testData  = "foobar"
	)
	entries := make([]*value.Secure, 20)
	for i := range entries {
		e := value.NewSecureValue(uuid.New().String(), []byte(testData))
		e.SetMetadata(testKey, testValue)
		entries[i] = e
	}
	for _, e := range entries {
		err := ts.cn.Save(testName, e.Key(), e)
		ts.NoError(err)
	}
	for _, e := range entries {
		spr := &value.Secure{}
		err := ts.cn.Sparse(testName, e.Key(), &spr)
		ts.NoError(err)
		ts.Empty(spr.Data)
		ts.Equal(testValue, spr.GetMetadata(testKey))
	}
	for _, e := range entries {
		spr := &value.Secure{}
		err := ts.cn.Load(testName, e.Key(), &spr)
		ts.NoError(err)
		ts.Equal(testData, string(spr.Data))
		ts.Equal(testValue, spr.GetMetadata(testKey))
	}
}

func (ts *ChestnutTestSuite) TestChestnut_OverwritesDisabled() {
	ts.testOptionDisableOverwrites(false)
}

func (ts *ChestnutTestSuite) TestChestnut_OverwritesEnabled() {
	ts.testOptionDisableOverwrites(true)
}

func (ts *ChestnutTestSuite) testOptionDisableOverwrites(enabled bool) {
	key := newKey()
	path := filepath.Join(ts.T().TempDir())
	store := ts.storeFunc(ts.T(), path)
	ts.NotNil(store)
	opts := []ChestOption{
		encryptorOpt,
	}
	assertErr := assert.NoError
	if !enabled {
		assertErr = assert.Error
		opts = append(opts, OverwritesForbidden())
	}
	cn := NewChestnut(store, opts...)
	ts.NotNil(cn)
	ts.Equal(enabled, cn.opts.overwrites)
	defer func() {
		err := cn.Close()
		ts.NoError(err)
	}()
	err := cn.Open()
	ts.NoError(err)
	err = cn.Put(testName, []byte(key), []byte(testValue))
	ts.NoError(err)
	// this should fail with an error if overwrites are disabled
	err = cn.Put(testName, []byte(key), []byte(testValue))
	assertErr(ts.T(), err)
}

func (ts *ChestnutTestSuite) TestChestnut_ChainedEncryptor() {
	var operation = "encrypting"
	// initialize a keystore with a chained encryptor
	openSecret := func(s crypto.Secret) []byte {
		ts.T().Logf("%s with secret %s", operation, s.ID())
		return []byte(s.ID())
	}
	managedSecret := crypto.NewManagedSecret(uuid.New().String(), "i-am-a-managed-secret")
	secureSecret1 := crypto.NewSecureSecret(uuid.New().String(), openSecret)
	secureSecret2 := crypto.NewSecureSecret(uuid.New().String(), openSecret)
	encryptorChainOpt := WithEncryptorChain(
		encryptor.NewAESEncryptor(crypto.Key128, aes.CFB, secureSecret1),
		encryptor.NewAESEncryptor(crypto.Key192, aes.CTR, managedSecret),
		encryptor.NewAESEncryptor(crypto.Key256, aes.GCM, secureSecret2),
	)
	path := ts.T().TempDir()
	store := ts.storeFunc(ts.T(), path)
	ts.NotNil(store)
	cn := NewChestnut(store, encryptorChainOpt)
	ts.NotNil(cn)
	defer func() {
		err := cn.Close()
		ts.NoError(err)
	}()
	err := cn.Open()
	ts.NoError(err)
	key := newKey()
	err = cn.Put(testName, []byte(key), []byte(testValue))
	ts.NoError(err)
	operation = "decrypting"
	v, err := cn.Get(testName, []byte(key))
	ts.NotEmpty(v)
	ts.NoError(err)
	ts.Equal([]byte(testValue), v)
	err = cn.Delete(testName, []byte(key))
	ts.NoError(err)
	e := value.NewSecureValue(uuid.New().String(), []byte(testValue))
	err = cn.Save(testName, []byte(key), e)
	ts.NoError(err)
	se1 := &value.Secure{}
	err = cn.Sparse(testName, []byte(key), se1)
	ts.NoError(err)
	se2 := &value.Secure{}
	err = cn.Load(testName, []byte(key), se2)
	ts.NoError(err)
}

func (ts *ChestnutTestSuite) TestChestnut_Compression() {
	compOpt := WithCompression(compress.Zstd)
	key := newKey()
	path := filepath.Join(ts.T().TempDir())
	store := ts.storeFunc(ts.T(), path)
	ts.NotNil(store)
	cn := NewChestnut(store, encryptorOpt, compOpt)
	ts.NotNil(cn)
	defer func() {
		err := cn.Close()
		ts.NoError(err)
	}()
	err := cn.Open()
	ts.NoError(err)
	err = cn.Put(testName, []byte(key), []byte(lorumIpsum))
	ts.NoError(err)
	val, err := cn.Get(testName, []byte(key))
	ts.NoError(err)
	ts.Equal(lorumIpsum, string(val))
}

func (ts *ChestnutTestSuite) TestChestnut_Compressors() {
	compOpt := WithCompressors(zstd.Compress, zstd.Decompress)
	key := newKey()
	path := filepath.Join(ts.T().TempDir())
	store := ts.storeFunc(ts.T(), path)
	ts.NotNil(store)
	cn := NewChestnut(store, encryptorOpt, compOpt)
	ts.NotNil(cn)
	defer func() {
		err := cn.Close()
		ts.NoError(err)
	}()
	err := cn.Open()
	ts.NoError(err)
	err = cn.Put(testName, []byte(key), []byte(lorumIpsum))
	ts.NoError(err)
	val, err := cn.Get(testName, []byte(key))
	ts.NoError(err)
	ts.Equal(lorumIpsum, string(val))
}

func (ts *ChestnutTestSuite) TestChestnut_OpenErr() {
	cn := &Chestnut{}
	err := cn.Open()
	ts.Error(err)
}

func (ts *ChestnutTestSuite) TestChestnut_SetLogger() {
	path := ts.T().TempDir()
	store := ts.storeFunc(ts.T(), path)
	ts.NotNil(store)
	cn := NewChestnut(store, encryptorOpt)
	logTests := []log.Logger{
		nil,
		log.NewZerologLoggerWithLevel(log.DebugLevel),
	}
	for _, test := range logTests {
		cn.SetLogger(test)
		err := cn.Open()
		ts.NoError(err)
		err = cn.Close()
		ts.NoError(err)
	}
}

func (ts *ChestnutTestSuite) TestChestnut_WithLogger() {
	levels := []log.Level{
		log.DebugLevel,
		log.InfoLevel,
		log.WarnLevel,
		log.ErrorLevel,
		log.PanicLevel,
	}
	type LoggerOpt func(log.Level) ChestOption
	logOpts := []LoggerOpt{
		WithZerologLogger,
		WithStdLogger,
	}
	path := ts.T().TempDir()
	store := ts.storeFunc(ts.T(), path)
	ts.NotNil(store)
	for _, level := range levels {
		for _, logOpt := range logOpts {
			opt := logOpt(level)
			cn := NewChestnut(store, encryptorOpt, opt)
			err := cn.Open()
			ts.NoError(err)
			err = cn.Close()
			ts.NoError(err)
		}
	}
}

func (ts *ChestnutTestSuite) TestChestnut_BadConfig() {
	store := ts.storeFunc(ts.T(), ts.T().TempDir())
	ts.Panics(func() {
		_ = NewChestnut(nil, encryptorOpt)
	})
	ts.Panics(func() {
		_ = NewChestnut(store)
	})
	ts.Panics(func() {
		_ = NewChestnut(store, encryptorOpt, WithCompression("X"))
	})
	ts.Panics(func() {
		_ = NewChestnut(store, encryptorOpt, WithCompressors(nil, nil))
	})
	ts.Panics(func() {
		_ = NewChestnut(store, encryptorOpt, WithCompressors(compress.PassthroughCompressor, nil))
	})
	ts.Panics(func() {
		_ = NewChestnut(store, encryptorOpt, WithCompressors(nil, compress.PassthroughDecompressor))
	})
}

type badEncryptor struct{}

func (b badEncryptor) ID() string {
	return "a"
}

func (b badEncryptor) Name() string {
	return "a"
}

func (b badEncryptor) Encrypt([]byte) ([]byte, error) {
	return nil, errors.New("an error")
}

func (b badEncryptor) Decrypt([]byte) ([]byte, error) {
	return nil, errors.New("an error")
}

var _ crypto.Encryptor = (*badEncryptor)(nil)

func (ts *ChestnutTestSuite) TestChestnut_BadEncryptor() {
	var testGood = []byte("test-good")
	var testBad = []byte("test-bad")
	badCompress := func(data []byte) (compressed []byte, err error) {
		return nil, errors.New("error")
	}
	store := ts.storeFunc(ts.T(), ts.T().TempDir())
	ts.Panics(func() {
		_ = NewChestnut(store, WithEncryptor(nil))
	})
	cn := NewChestnut(store, encryptorOpt)
	err := cn.Open()
	ts.NoError(err)
	err = cn.Put(testName, testGood, testGood)
	ts.NoError(err)
	err = cn.Close()
	ts.NoError(err)

	cn = NewChestnut(store, WithEncryptor(&badEncryptor{}))
	err = cn.Open()
	ts.NoError(err)
	err = cn.Put(testName, testBad, testBad)
	ts.Error(err)
	_, err = cn.Get(testName, testGood)
	ts.Error(err)
	err = cn.Close()
	ts.NoError(err)

	compOpt := WithCompressors(compress.PassthroughCompressor, compress.PassthroughDecompressor)
	cn = NewChestnut(store, encryptorOpt, compOpt)
	err = cn.Open()
	ts.NoError(err)
	err = cn.Put(testName, testGood, testGood)
	ts.NoError(err)
	err = cn.Close()
	ts.NoError(err)

	cn = NewChestnut(store, encryptorOpt, WithCompressors(badCompress, badCompress))
	err = cn.Open()
	ts.NoError(err)
	err = cn.Put(testName, testBad, testBad)
	ts.Error(err)
	ts.Error(err)
	_, err = cn.Get(testName, testGood)
	ts.Error(err)
	err = cn.Close()
	ts.NoError(err)
}
