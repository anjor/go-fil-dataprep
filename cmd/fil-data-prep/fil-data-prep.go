package fil_data_prep

import (
	split_and_commp "data-prep/split-and-commp"
	"fmt"
	"github.com/anjor/anelace"
	"github.com/urfave/cli/v2"
	"io"
	"io/fs"
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
	if !c.Args().Present() {
		return fmt.Errorf("expected some data to be processed, found none.\n")
	}

	//rerr, werr := io.Pipe()
	rout, wout := io.Pipe()

	anl, errs := anelace.NewAnelaceWithWriters(os.Stderr, wout)
	if errs != nil {
		return fmt.Errorf("unexpected error: %s\n", errs)
	}
	anl.SetMultipart(true)

	var fileReaders []io.Reader
	var files []string
	paths := c.Args().Slice()
	for _, path := range paths {
		pathInfo, err := os.Stat(path)
		if err != nil {
			return err
		}
		if !pathInfo.IsDir() {
			r, err := getFileReader(path, pathInfo)
			if err != nil {
				return err
			}
			fileReaders = append(fileReaders, r)
		} else {
			var frs []io.Reader
			err := filepath.WalkDir(path, func(p string, d fs.DirEntry, e error) error {
				if e != nil {
					return e
				}

				if d.IsDir() {
					return nil
				}
				files = append(files, p)
				di, err := d.Info()
				if err != nil {
					return err
				}
				r, err := getFileReader(p, di)
				if err != nil {
					return err
				}
				frs = append(frs, r)
				return nil
			})
			if err != nil {
				return err
			}

			fileReaders = append(fileReaders, frs...)

		}
	}

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer wout.Close()
		if err := anl.ProcessReader(io.MultiReader(fileReaders...), nil); err != nil {
			fmt.Printf("process reader error: %s", err)
		}
	}()

	o := c.String("output")
	m := c.String("metadata")
	s := c.Int("size")

	go func() {
		defer wg.Done()

		if err := split_and_commp.SplitAndCommp(rout, s, m, ".", o); err != nil {
			fmt.Printf("split and commp failed: %s", err)
		}
	}()

	wg.Wait()
	fmt.Printf("dirs = %s\n", files)
	return nil
}
