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
2023-03-30T20:15:09Z,out,bafybeig3mkjrgyde33grqwyano74pq2x5vcdj4twii5khotua4k4kedpha,out-0.car,baga6ea4seaqcjzz5iztdwawdakw3yel3nkhppyhsxidc3fhkpgq462iednah6na,1024,1016
```
### split-and-commp

This command takes in a car file and splits it into smaller car files of the provided size (roughly). It also calculates commp at the same time and writes it out to a metadata file.

```
$ ~/repos/anjor/go-fil-dataprep/cmd/data-prep/data-prep sac --size 100000 --metadata m.csv data.car
Writing CAR chunk to ./data-0.car
```

```
$ cat m.csv
name,car,commp,padded_piece_size,unpadded_piece_size
data,data-0.car,baga6ea4seaqj6boqxzkorusdm5z2aw3t256lsus6lgucwgxcdedejailyvgd2ny,1024,1016
```
