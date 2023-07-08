package fil_data_prep

import (
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs"
	unixfspb "github.com/ipfs/go-unixfs/pb"
	"github.com/multiformats/go-multihash"
	"strings"
)

type roots struct {
	Event    string `json:"event"`
	Payload  int    `json:"payload"`
	Stream   int    `json:"stream"`
	Cid      string `json:"cid"`
	Wiresize uint64 `json:"wiresize"`
}

type node struct {
	name     string
	children []*node
	cid      cid.Cid
	pbn      *merkledag.ProtoNode
	size     uint64
}

func newNode(name string) *node {
	return &node{name: name}
}

func (n *node) addChild(child *node) {
	n.children = append(n.children, child)
}

func (n *node) constructNode() {
	if len(n.children) == 0 {
		return
	}

	ndbs, err := unixfs.NewFSNode(unixfspb.Data_Directory).GetBytes()
	if err != nil {
		return
	}
	nd := merkledag.NodeWithData(ndbs)
	nd.SetCidBuilder(cid.V1Builder{Codec: cid.DagProtobuf, MhType: multihash.SHA2_256})

	var size uint64
	for _, child := range n.children {
		child.constructNode()
		err := nd.AddRawLink(child.name, &format.Link{
			Cid:  child.cid,
			Size: child.size,
		})
		if err != nil {
			return
		}
		size += child.size

	}
	n.pbn = nd
	n.cid = nd.Cid()
	n.size = size
}

func constructTree(files []string, rs []roots) *node {
	root := newNode("root")

	for i, file := range files {
		parts := strings.Split(file, "/")
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

		currentNode.cid = cid.MustParse(rs[i].Cid)
		currentNode.size = rs[i].Wiresize
	}

	root.constructNode()

	return root
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
