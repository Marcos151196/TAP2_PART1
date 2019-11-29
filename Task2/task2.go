package main

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/spf13/pflag"
	"github.com/vistarmedia/gossamr"
)

type Task2 struct{}

var outTask1 string
var mapTask1 = map[int64]string{}

func init() {
	var flags pflag.FlagSet
	flags.ParseErrorsWhitelist.UnknownFlags = true
	flags.StringVarP(&outTask1, "outtask1", "o", "nada", "year||onegram_year||onegram_...year||onegram Given by task 1.")
	flags.Parse(os.Args[5:])
	os.Args = os.Args[0:5]
}

func main() {
	// CONVERT outputTask1 ARRAY TO MAP
	outTask1array := strings.Split(outTask1, "_")
	for _, val := range outTask1array {
		tokens := strings.Split(val, "||")
		decade, _ := strconv.Atoi(tokens[0])
		onegram := tokens[1]
		mapTask1[int64(decade)] = onegram
	}

	Task2 := gossamr.NewTask(&Task2{})
	err := gossamr.Run(Task2)
	if err != nil {
		log.Fatal(err)
	}
}

// CONVERTS FILE TO [DECADE,ARRAY_WITH_ALL_THE_NGRAM+COUNT_PAIRS_FOR_THAT_DECADE] JUST FROM 1900 TO 1999
func (wc *Task2) Map(p int64, line string, c gossamr.Collector) error {
	tokens := strings.Fields(line)
	if len(tokens) == 5 {
		firstword := tokens[0]
		secondword := tokens[1]
		year, _ := strconv.Atoi(tokens[2])
		match_count := tokens[3]
		decade := (int64(year) / 10) * 10

		if year >= 1900 && year < 2000 {
			if strings.HasPrefix(firstword, mapTask1[decade]) {
				c.Collect(decade, firstword+"\t"+secondword+"||"+match_count)
			}
		}
	}
	return nil
}

// THIS IS RUN 1 TIME FOR EACH KEY AND RECEIVES [DECADE,ARRAY_WITH_ALL_THE_WORD+COUNT_PAIRS_FOR_THAT_DECADE]
func (wc *Task2) Reduce(key int64, values chan string, c gossamr.Collector) error {
	mapa := map[string]int64{}
	for value := range values {
		tokens := strings.Split(value, "||")
		ngram := tokens[0]
		count, _ := strconv.Atoi(tokens[1])
		mapa[ngram] = mapa[ngram] + int64(count) // Computes total count for a word (in case the same word is the most repeated one for different years within the decade)
	}
	word, count := mapMax(mapa)
	c.Collect(key, fmt.Sprintf("%s\t%d", word, count))
	return nil
}

// GETS [KEY,VALUE] FOR THE MAXIMUM VALUE OF A MAP[KEY,VALUE]
// IF THERE IS MULTIPLE KEYS THAT HAVE THE MAXIMUM VALUE, RETURNS ONLY THE FIRST ALPHABETICALLY ORDERED KEY
func mapMax(mapa map[string]int64) (string, int64) {
	var maxvalue int64 = 0
	var keySlice []string
	for key, value := range mapa {
		if value > maxvalue {
			maxvalue = value
			keySlice = []string{key}
		} else if value == maxvalue {
			keySlice = append(keySlice, key)
			maxvalue = value
		}
	}
	sort.Strings(keySlice)
	maxKey := keySlice[0]
	return maxKey, maxvalue
}
