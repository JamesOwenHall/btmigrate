package main

import (
	"log"
	"os"
)

func main() {
	err := NewCLI(os.Stdout).Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
