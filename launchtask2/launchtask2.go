package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"unicode"

	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"

	log "github.com/sirupsen/logrus"
	viper "github.com/theherk/viper"
)

func main() {
	var args []string
	var err error
	var output string

	// CONFIG FILE
	cfgFile := "config/config.toml"
	viper.SetConfigFile(cfgFile)
	if err := viper.ReadInConfig(); err != nil {
		log.Errorf("Unable to read config from file %s: %v", cfgFile, err)
		os.Exit(1)
	} else {
		log.Infof("Read configuration from file %s", cfgFile)
	}

	// REMOVE TASK1 OUTPUT FROM LOCAL IF IT ALREADY EXISTS
	args = []string{"-rf", viper.GetString("localoutputdirectory") + "output.task1"}
	output, err = RunCMD("rm", args, true)
	if err != nil {
		log.Warnf("Could not remove task1 output from local: %v", err)
	} else {
		log.Debugf("Result: %s", output)
	}

	// REMOVE TASK2 OUTPUT FROM LOCAL IF IT ALREADY EXISTS
	args = []string{"-rf", viper.GetString("localoutputdirectory") + "output.task2"}
	output, err = RunCMD("rm", args, true)
	if err != nil {
		log.Warnf("Could not remove task2 output from local: %v", err)
	} else {
		log.Debugf("Result: %s", output)
	}

	// REMOVE TASK1 OUTPUT FROM DFS IF IT ALREADY EXISTS
	args = []string{"dfs", "-rm", "-r", viper.GetString("dfsoutputdirectory") + "output.task1"}
	output, err = RunCMD("/opt/hadoop-3.2.1/bin/hdfs", args, true)
	if err != nil {
		log.Warnf("Could not remove task1 output from dfs: %v", err)
	} else {
		log.Debugf("Result: %s", output)
	}

	// REMOVE TASK2 OUTPUT FROM DFS IF IT ALREADY EXISTS
	args = []string{"dfs", "-rm", "-r", viper.GetString("dfsoutputdirectory") + "output.task2"}
	output, err = RunCMD("/opt/hadoop-3.2.1/bin/hdfs", args, true)
	if err != nil {
		log.Warnf("Could not remove task2 output from dfs: %v", err)
	} else {
		log.Debugf("Result: %s", output)
	}

	// RUN TASK 1
	args = []string{"streaming", "-input", viper.GetString("dfstask1input"), "-output", "output.task1", "-mapper", "\"" + viper.GetString("task1binary") + " -task 0 -phase map" + "\"", "-reducer", "\"" + viper.GetString("task1binary") + " -task 0 -phase reduce" + "\"", "-io", "typedbytes"}
	output, err = RunCMD("/opt/hadoop-3.2.1/bin/mapred", args, true)
	if err != nil {
		log.Errorf("Could not run task1 mapred: %v", err)
		return
	} else {
		log.Debugf("Result: %s", output)
	}

	// COPY TASK1 OUTPUT TO LOCAL
	args = []string{"fs", "-copyToLocal", viper.GetString("dfsoutputdirectory") + "output.task1", viper.GetString("localoutputdirectory")}
	output, err = RunCMD("/opt/hadoop-3.2.1/bin/hadoop", args, true)
	if err != nil {
		log.Errorf("Could not copy task1 output to local directory: %v", err)
		return
	} else {
		log.Debugf("Result: %s", output)
	}

	// GET INPUT FILE LIST
	args = []string{"streaming"}
	inputFileMap := map[string]string{}
	var arrayTask1 []string
	outTask1, err := os.Open(viper.GetString("localoutputdirectory") + "output.task1/part-00000")
	if err != nil {
		log.Errorf("Could not open task1 output file for reading: %v", err)
		return
	}
	defer outTask1.Close()

	scanner := bufio.NewScanner(outTask1)
	for scanner.Scan() {
		line := scanner.Text()
		tokens := strings.Fields(line)
		year := tokens[0]
		t := transform.Chain(norm.NFD, transform.RemoveFunc(isMn), norm.NFC)
		onegram := tokens[1]
		onegramsintilde, _, _ := transform.String(t, tokens[1])
		arrayTask1 = append(arrayTask1, year+"||"+onegram)
		var firstletters string
		if len(onegramsintilde) < 2 {
			firstletters = fmt.Sprintf("%v_", onegramsintilde)
		} else {
			firstletters = onegramsintilde[0:2]
		}
		inputFileMap[firstletters] = viper.GetString("dfsbigramfiles") + strings.ToLower(firstletters)
	}

	for _, value := range inputFileMap {
		args = append(args, "-input")
		args = append(args, value)
	}

	// RUN TASK 2
	args = append(args, "-output", "output.task2", "-mapper", "\""+viper.GetString("task2binary")+" -task 0 -phase map --outtask1 "+strings.Join(arrayTask1, "_")+"\"", "-reducer", "\""+viper.GetString("task2binary")+" -task 0 -phase reduce --outtask1 "+strings.Join(arrayTask1, "_")+"\"", "-io", "typedbytes")
	output, err = RunCMD("/opt/hadoop-3.2.1/bin/mapred", args, true)
	if err != nil {
		fmt.Println("Error: ", err)
	} else {
		fmt.Println("Result: ", output)
	}

	// COPY TASK2 OUTPUT TO LOCAL
	args = []string{"fs", "-copyToLocal", viper.GetString("dfsoutputdirectory") + "output.task2", viper.GetString("localoutputdirectory")}
	output, err = RunCMD("/opt/hadoop-3.2.1/bin/hadoop", args, true)
	if err != nil {
		log.Errorf("Could not copy task2 output to local directory: %v", err)
		return
	} else {
		log.Debugf("Result: %s", output)
	}
}

// RunCMD is a simple wrapper around terminal commands
func RunCMD(path string, args []string, debug bool) (out string, err error) {
	cmd := exec.Command(path, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	_ = cmd.Run()
	return
}

func isMn(r rune) bool {
	return unicode.Is(unicode.Mn, r)
}
