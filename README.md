# go-fil-dataprep

And end to end data preparation tool to onboard data to filecoin. 

## Installation

```
go install github.com/anjor/go-fil-dataprep/cmd/data-prep@latest
```

## Usage

The cli supports 2 commands -- `fil-data-prep` and `split-and-commp`.

### fil-data-prep

This command transforms data into a bunch of car files sized "correctly" (target size provided as an input), calculates commP and saves all of this data in a metadata file.

```
$data-prep dp --size 1000000 --output a --metadata test_meta.csv subdir2
root cid = bafybeig3mkjrgyde33grqwyano74pq2x5vcdj4twii5khotua4k4kedpha
```

```
$cat test_meta.csv
timestamp,original data,car file,root_cid,piece cid,padded piece size,unpadded piece size
2023-03-30 14:07:44.228804 +0100 BST m=+0.005866668,a,bafybeig3mkjrgyde33grqwyano74pq2x5vcdj4twii5khotua4k4kedpha,a-0.car,baga6ea4seaqcjzz5iztdwawdakw3yel3nkhppyhsxidc3fhkpgq462iednah6na,1024,1016

```
