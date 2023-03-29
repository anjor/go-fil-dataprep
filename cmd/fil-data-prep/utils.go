package fil_data_prep

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs"
	unixfspb "github.com/ipfs/go-unixfs/pb"
	"github.com/multiformats/go-multihash"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

type roots struct {
	Event    string `json:"event"`
	Payload  int    `json:"payload"`
	Stream   int    `json:"stream"`
	Cid      string `json:"cid"`
	Wiresize int    `json:"wiresize"`
}

func getFileReader(path string, pathInfo os.FileInfo) (io.Reader, error) {
	if pathInfo.IsDir() {
		return nil, fmt.Errorf("expect file got directory: %s", path)
	}
	fileSize := pathInfo.Size()

	sizeBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(sizeBytes, uint64(fileSize))

	fi, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return io.MultiReader(bytes.NewReader(sizeBytes), fi), nil
}

func recursivelyGetFileReaders(path string) (files []string, frs []io.Reader, err error) {
	err = filepath.WalkDir(path, func(p string, d fs.DirEntry, e error) error {
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
		return nil, nil, err
	}

	return
}

type node struct {
	name     string
	children []*node
	cid      cid.Cid
	pbn      *merkledag.ProtoNode
}

func newNode(name string) *node {
	return &node{name: name}
}

func (n *node) addChild(child *node) {
	n.children = append(n.children, child)
}

func (n *node) setCid(c cid.Cid) {
	n.cid = c
}

func (n *node) getCid() cid.Cid {
	return n.cid
}

func (n *node) constructNode() {
	if len(n.children) == 0 {
		return
	}

	nd := merkledag.NodeWithData(unixfs.NewFSNode(unixfspb.Data_Directory).Data())
	nd.SetCidBuilder(cid.V1Builder{Codec: cid.DagCBOR, MhType: multihash.SHA2_256})

	for _, child := range n.children {
		child.constructNode()
		err := nd.AddRawLink(child.name, &format.Link{
			Cid: child.cid,
		})
		if err != nil {
			return
		}

	}

	n.pbn = nd
	n.cid = nd.Cid()
}

func constructTree(paths []string, rs []roots) []*merkledag.ProtoNode {
	root := newNode("root")

	for i, path := range paths {
		parts := strings.Split(path, "/")
		currentNode := root

		for _, part := range parts {
			var foundChild *node
			for _, child := range currentNode.children {
				if child.name == part {
					foundChild = child
					break
				}
			}

			if foundChild == nil {
				foundChild = newNode(part)
				currentNode.addChild(foundChild)
			}

			currentNode = foundChild
		}

		currentNode.setCid(cid.MustParse(rs[i].Cid)) // or any other value you want to associate with the leaf nodes
	}

	root.constructNode()

	return getDirectoryNodes(root)[1:]
}

func getDirectoryNodes(node *node) []*merkledag.ProtoNode {
	var nodes []*merkledag.ProtoNode
	nodes = append(nodes, node.pbn)
	for _, child := range node.children {
		if len(child.children) != 0 {
			nodes = append(nodes, getDirectoryNodes(child)...)
		}
	}
	return nodes
}
func appendVarint(tgt []byte, v uint64) []byte {
	for v > 127 {
		tgt = append(tgt, byte(v|128))
		v >>= 7
	}
	return append(tgt, byte(v))
}
