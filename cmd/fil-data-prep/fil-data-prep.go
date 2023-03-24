package fil_data_prep

import (
	split_and_commp "data-prep/split-and-commp"
	"data-prep/utils"
	"fmt"
	"github.com/anjor/anelace"
	"github.com/urfave/cli/v2"
	"io"
	"sync"
)

var Cmd = &cli.Command{
	Name:    "fil-data-prep",
	Usage:   "end to end data prep",
	Aliases: []string{"dp"},
	Action:  filDataPrep,
	Flags: []cli.Flag{
		&cli.IntFlag{
			Name:     "size",
			Aliases:  []string{"s"},
			Required: true,
			Value:    2 << 20,
			Usage:    "Target size in bytes to chunk CARs to.",
		},
		&cli.StringFlag{
			Name:     "output",
			Aliases:  []string{"o"},
			Required: false,
			Usage:    "optional output name for car files. Defaults to filename (stdin if streamed in from stdin).",
		},
		&cli.StringFlag{
			Name:     "metadata",
			Aliases:  []string{"m"},
			Required: false,
			Usage:    "optional metadata file name. Defaults to __metadata.csv",
		},
	},
}

func filDataPrep(c *cli.Context) error {
	dir, name, fi, err := utils.GetReader(c)
	if err != nil {
		return err
	}

	output := c.String("output")
	if output != "" {
		name = output
	}

	meta := c.String("metadata")
	if meta == "" {
		meta = "__metadata.csv"
	}

	size := c.Int("size")
	args := []string{
		"dolphin-songs",
		"--emit-stderr=roots-jsonl",
		"--emit-stdout=car-v1-stream",
	}
	pr, pw := io.Pipe()
	wg := sync.WaitGroup{}
	wg.Add(2)

	anl := anelace.NewAnelaceFromArgv(args)

	go func() {
		defer wg.Done()
		defer pw.Close()
		anl.SetCarWriter(pw)

		err := anl.ProcessReader(fi, nil)
		if err != nil {
			fmt.Printf("process reader error: %s", err)
		}
	}()

	go func() {
		defer wg.Done()
		err = split_and_commp.SplitAndCommp(pr, size, meta, dir, name)
		if err != nil {
			fmt.Printf("errored in split and commp: %s", err)
		}
	}()

	wg.Wait()
	return nil
}
