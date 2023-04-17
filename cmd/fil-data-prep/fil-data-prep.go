package fil_data_prep

import (
	split_and_commp "data-prep/split-and-commp"
	"fmt"
	"github.com/anjor/anelace"
	"github.com/urfave/cli/v2"
	"io"
	"os"
	"path/filepath"
	"sync"
)

var Cmd = &cli.Command{
	Name:    "fil-data-prep",
	Usage:   "end to end data prep",
	Aliases: []string{"dp"},
	Action:  filDataPrep,
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "output",
			Aliases:  []string{"o"},
			Required: false,
			Usage:    "optional output name for car files. Defaults to filename (stdin if streamed in from stdin).",
		},
		&cli.IntFlag{
			Name:     "size",
			Aliases:  []string{"s"},
			Required: false,
			Value:    2 << 20,
			Usage:    "Target size in bytes to chunk CARs to.",
		},
		&cli.StringFlag{
			Name:     "metadata",
			Aliases:  []string{"m"},
			Required: false,
			Value:    "__metadata.csv",
			Usage:    "metadata file name. ",
		},
	},
}

func filDataPrep(c *cli.Context) error {
	output := c.String("output")
	meta := c.String("metadata")
	size := c.Int("size")
	if !c.Args().Present() {
		return fmt.Errorf("expected some data to be processed, found none.\n")
	}

	//rerr, werr := io.Pipe()
	rout, wout := io.Pipe()

	anl, errs := anelace.NewAnelaceWithWriters(os.Stderr, wout)
	anl.SetMultipart(true)
	if errs != nil {
		return fmt.Errorf("unexpected error: %s\n", errs)
	}

	var fileReaders []io.Reader
	paths := c.Args().Slice()
	for _, path := range paths {
		pathInfo, err := os.Stat(path)
		if err != nil {
			return err
		}

		if !pathInfo.IsDir() {
			fr, err := getFileReader(path, pathInfo)
			if err != nil {
				return err
			}
			fileReaders = append(fileReaders, fr)
		} else {
			filepath.WalkDir(path, processDir)
			// get entry one by one
			// if entry is a file -> get a fileReader
			// if entry is a directory -> recurse with this directory as root.
			// return a list of file readers
		}

	}

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer wout.Close()
		if err := anl.ProcessReader(io.MultiReader(fileReaders...), nil); err != nil {
			fmt.Printf("process reader error: %s\n", err)
		}
	}()

	go func() {
		defer wg.Done()
		err := split_and_commp.SplitAndCommp(rout, size, meta, ".", output)
		if err != nil {
			fmt.Printf("errored in split and commp: %s", err)
		}
	}()

	wg.Wait()

	return nil
}
