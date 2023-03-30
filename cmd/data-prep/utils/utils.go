package utils

import (
	"github.com/urfave/cli/v2"
	"io"
	"os"
)

func GetReader(c *cli.Context) (io.Reader, error) {
	if c.Args().Present() {
		path := c.Args().First()
		fi, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		return fi, nil

	}
	return os.Stdin, nil
}
