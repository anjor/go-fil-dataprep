package fil_data_prep

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/anjor/anelace"
	"github.com/urfave/cli/v2"
	"io"
	"os"
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
	//output := c.String("output")
	//meta := c.String("metadata")
	//size := c.Int("size")
	if !c.Args().Present() {
		return fmt.Errorf("expected some data to be processed, found none.\n")
	}

	//rerr, werr := io.Pipe()
	//rout, wout := io.Pipe()

	anl, errs := anelace.NewAnelaceWithWriters(os.Stderr, os.Stdout)
	fmt.Printf("Anelace = %s\n", anl)
	if errs != nil {
		return fmt.Errorf("unexpected error: %s\n", errs)
	}

	paths := c.Args().Slice()
	for _, path := range paths {
		pathInfo, err := os.Stat(path)
		if err != nil {
			return err
		}
		if !pathInfo.IsDir() {
			fileSize := pathInfo.Size()

			sizeBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(sizeBytes, uint64(fileSize))

			fi, err := os.Open(path)
			if err != nil {
				return err
			}

			r := io.MultiReader(bytes.NewReader(sizeBytes), fi)

			//go func() {
			//defer wg.Done()
			//defer pw.Close()

			err = anl.ProcessReader(r, nil)
			if err != nil {
				fmt.Printf("process reader error: %s", err)
			}
			//}()

			//go func() {
			//	defer wg.Done()
			//	err = split_and_commp.SplitAndCommp(pr, size, meta, dir, name)
			//	if err != nil {
			//		fmt.Printf("errored in split and commp: %s", err)
			//	}
			//}()

		}

	}

	//if err := wout.Close(); err != nil {
	//	return err
	//}
	//if err := werr.Close(); err != nil {
	//	return err
	//}
	//
	//stderrs, err := io.ReadAll(rerr)
	//if err != nil {
	//	return err
	//}
	//fmt.Printf("stderr = %s\n", string(stderrs))
	//stdouts, err := io.ReadAll(rout)
	//if err != nil {
	//	return err
	//}
	//fmt.Printf("stdout = %s\n", string(stdouts))
	//
	//// wg.Wait()
	return nil
}
