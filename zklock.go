package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

func main() {
	hostname, _ := os.Hostname()
	zookeepers := flag.String("zk", "localhost", "zookeeper endpoints joined by ','")
	prefix := flag.String("p", "/zklock", "prefix node")
	nowait := flag.Bool("n", true, "Fail rather than wait")
	sessionTimeout := flag.Int("t", 4000, "session timeout in miliseconds")
	sleepAfter := flag.Int("a", 0, "sleep after command was executed in seconds")
	sleepBefore := flag.Int("b", 0, "sleep before lock in seconds")
	dontKill := flag.Bool("k", false, "don't kill subprocess if something wrong")
	debug := flag.Bool("d", false, "debug")
	flag.Parse()

	if !*debug {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}

	args := flag.Args()
	log.Printf("Args: %+v\n", args)

	if len(args) < 2 {
		flag.Usage()
		return
	}

	if *sleepBefore > 0 {
		log.Printf("Sleep %d seconds\n", *sleepBefore)
		time.Sleep(time.Duration(*sleepBefore) * time.Second)
	}

	c, _, err := zk.Connect(strings.Split(*zookeepers, ","), time.Millisecond*time.Duration(*sessionTimeout))
	if err != nil {
		panic(err)
	}

	acl := zk.WorldACL(zk.PermAll)

	exists, _, err := c.Exists(*prefix)
	if err != nil {
		panic(err)
	}
	if !exists {
		_, err = c.Create(*prefix, []byte{}, 0, acl)
		if err != nil && err != zk.ErrNodeExists {
			panic(err)
		}
	}

	lockname := args[0]
	lockpath, err := c.Create(*prefix+"/"+lockname, []byte(hostname), zk.FlagEphemeral, acl)
	if err != nil {
		if err == zk.ErrNodeExists {
			if *nowait { //TODO
			}
			log.Printf("Lock %s exists. Exit 1\n", lockpath)
			os.Exit(1)
		}
		panic(err)
	}
	log.Printf("Lock %s aquired\n", lockpath)

	log.Printf("Run: %+v\n", args[1:])
	cmd := exec.Command(args[1], args[2:]...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin

	cmdKill := func() {
		if !*dontKill {
			cmd.Process.Kill()
		}
	}

	exitFatal := func(str string) {
		cmdKill()
		log.Fatal(str)
	}

	signalChannel := make(chan os.Signal, 2)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)

	go func() {
		for {
			sig := <-signalChannel
			switch sig {
			case os.Interrupt:
				log.Println("SIGINT received")
				cmdKill()
			case syscall.SIGTERM:
				log.Println("SIGTERM received")
				cmdKill()
				os.Exit(1)
			}
		}
	}()

	go func() {
		for {
			time.Sleep(1 * time.Second)
			data, _, err := c.Get(lockpath)
			if err == zk.ErrNodeExists || bytes.Equal(data, []byte("")) {
				log.Println("Lock disappeared. Try to re-aquire")
				_, err = c.Create(lockpath, []byte(hostname), zk.FlagEphemeral, acl)
				if err != nil {
					exitFatal(err.Error())
				}
				log.Printf("Lock %s re-aquired\n", lockpath)
				continue
			}
			if err != nil {
				log.Println(err)
			}
			if !bytes.Equal(data, []byte(hostname)) {
				exitFatal(fmt.Sprintf("Lock %s re-aquired by another host %s\n", lockpath, data))
			}
		}
	}()

	cmd.Run()

	if *sleepAfter > 0 {
		log.Printf("Sleep %d seconds\n", *sleepAfter)
		time.Sleep(time.Duration(*sleepAfter) * time.Second)
	}

	log.Println("done")
}
