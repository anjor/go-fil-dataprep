package main

import (
	"fmt"
	"github.com/anjor/go-fil-dataprep/cmd/data-prep/fil-data-prep"
	"github.com/anjor/go-fil-dataprep/cmd/data-prep/split-and-commp"
	"github.com/urfave/cli/v2"
	"os"
)

var (
	Version  = ""
	Revision = ""
	Build    = ""
	Branch   = ""
)

func main() {
	cli.VersionPrinter = func(cCtx *cli.Context) {
		fmt.Printf("version=%s revision=%s build=%s branch=%s\n", cCtx.App.Version, Revision, Build, Branch)
	}

	app := cli.NewApp()
	app.Name = "fil-dataprep"
	app.Version = Version
	app.Usage = "Chunking for CAR files + calculating commP. Splits a CAR file into smaller CAR files and at the same time also calculates commP for the smaller CAR files."
	app.Commands = []*cli.Command{
		split_and_commp.Cmd,
		fil_data_prep.Cmd,
	}
	err := app.Run(os.Args)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}
