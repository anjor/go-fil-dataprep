package utils

import (
	"github.com/urfave/cli/v2"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func GetReader(c *cli.Context) (string, string, io.Reader, error) {
	if c.Args().Present() {
		path := c.Args().First()
		dir := filepath.Dir(path)
		name := strings.TrimSuffix(filepath.Base(path), ".car")

		fi, err := os.Open(path)
		if err != nil {
			return "", "", nil, err
		}

		return dir, name, fi, nil

	}
	return ".", "stdin", os.Stdin, nil
}
