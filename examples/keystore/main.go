package main

import (
	"bytes"
	"log"
	"os"
	"path/filepath"

	"git.tcp.direct/kayos/chestnut"
	"git.tcp.direct/kayos/chestnut/encryptor/aes"
	"git.tcp.direct/kayos/chestnut/encryptor/crypto"
	"git.tcp.direct/kayos/chestnut/keystore"
	"git.tcp.direct/kayos/chestnut/storage/nuts"
)

func main() {
	path := filepath.Join(os.TempDir(), "keystore")
	defer os.RemoveAll(path)
	// use nutsdb
	store := nuts.NewStore(path)
	// use a simple text secret
	textSecret := crypto.TextSecret("i-am-a-good-secret")
	opts := []chestnut.ChestOption{
		// use AES256-CFB encryption
		chestnut.WithAES(crypto.Key256, aes.CFB, textSecret),
	}
	// open the keystore with nutsdb and the aes encryptor
	ks := keystore.NewKeystore(store, opts...)
	if err := ks.Open(); err != nil {
		log.Panic(err)
	}
	cs := chestnut.NewChestnut(store, opts...)
	if cs == nil {
		log.Panic("unable to create chestnut")
	}
	if err := cs.Put("hello", textSecret.Open(), []byte("world")); err != nil {
		log.Panic(err)
	}
	res, err := cs.Get("hello", []byte("yeet"))
	if err == nil {
		log.Panicf("expected error, got nil and result: %s", res)
	}
	res, err = cs.Get("hello", textSecret.Open())
	if err != nil {
		log.Panic(err)
	}
	if !bytes.Equal(res, []byte("world")) {
		log.Panicf("expected world, got %s", res)
	}
	log.Printf("Success! got result: %s", res)
}
