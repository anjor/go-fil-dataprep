package main

import (
	"data-prep/fil-data-prep"
	"data-prep/split-and-commp"
	"fmt"
	"github.com/urfave/cli/v2"
	"os"
)

func main() {
	app := cli.NewApp()
	app.Name = "fil-dataprep"
	app.Usage = "Chunking for CAR files + calculating commP. Splits a CAR file into smaller CAR files and at the same time also calculates commP for the smaller CAR files."
	app.Commands = []*cli.Command{
		split_and_commp.Cmd,
		fil_data_prep.Cmd,
	}
	err := app.Run(os.Args)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
}
