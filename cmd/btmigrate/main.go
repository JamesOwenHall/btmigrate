package main

import (
	"log"
	"os"
)

func main() {
	err := NewApp(os.Stdout).Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
