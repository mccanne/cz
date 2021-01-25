# cz - convert csv to zng

cz takes CSV data on stdin and converts to to ZNG on stdout.

## Install

```
git clone https://github.com/mccanne/cz
cd cz
go install
```

## Example

```
printf "n,s,b\n1,foo,true\n" | cz | zq -f zson -
```
gives
```
{
    n: 1e+00,
    s: "foo",
    b: true
} (=0)
```
