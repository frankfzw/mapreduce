package main

import "os"
import "fmt"
import "../mapreduce"
import "container/list"
import "unicode"
import "strconv"
import "strings"

// our simplified version of MapReduce does not supply a
// key to the Map function, as in the paper; only a value,
// which is a part of the input file contents
func filter(input string) string {
	var output string
	r := []rune(input)
	for i := 0; i < len(r); i ++ {
		if (!unicode.IsLetter(r[i])) && (!unicode.IsDigit(r[i])) {
			r[i] = ' '
		}
	}
	output = string(r)
	return output
}
func Map(value string) *list.List {
	l := list.New()
	
	components := strings.Fields(filter(value))
	for i := 0; i < len(components); i ++ {
		var kv mapreduce.KeyValue
		kv.Value = "1"
		kv.Key = components[i]
		l.PushBack(kv)
			 
	}
	return l
}

// iterate over list and add values
func Reduce(key string, values *list.List) string {
	sum := 0
	for e := values.Front(); e != nil; e = e.Next() {
		num, err := strconv.Atoi(e.Value.(string))
		if err != nil {
			fmt.Println("Map: parse int error\n")
		}
		sum += num
	}
	s := strconv.Itoa(sum)
	return s
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master x.txt sequential)
// 2) Master (e.g., go run wc.go master x.txt localhost:7777)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
  if len(os.Args) != 4 {
    fmt.Printf("%s: see usage comments in file\n", os.Args[0])
  } else if os.Args[1] == "master" {
    if os.Args[3] == "sequential" {
      mapreduce.RunSingle(5, 3, os.Args[2], Map, Reduce)
    } else {
      mr := mapreduce.MakeMapReduce(5, 3, os.Args[2], os.Args[3])    
      // Wait until MR is done
      <- mr.DoneChannel
    }
  } else {
    mapreduce.RunWorker(os.Args[2], os.Args[3], Map, Reduce, 100)
  }
}
