package main

import (
	"log"
	"os"
)

func main() {
	err := NewApp().Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
