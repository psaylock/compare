package main

import (
	"bufio"
	"compare/anagram"
	"compare/jsonUtils"
	"compare/restclientV1"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

const StatusError = "error"

type configJSON struct {
	Token     string            `json:"token" binding:"required"`
	Scopes    []string          `json:"scopes" binding:"required"`
	Headers   map[string]string `json:"headers" binding:"required"`
	SkipLines int               `json:"skip_lines" binding:"required"`
	Filename  string            `json:"filename" binding:"required"`
}

func main() {
	configFile := "config_file.json"
	if len(os.Args) == 2 {
		configFile = os.Args[1]
	}
	runningConfig, errConfig := loadConfigFile(configFile)
	if errConfig != nil {
		log.Fatal(errConfig)
	}

	numThreads := runtime.NumCPU()
	runtime.GOMAXPROCS(numThreads)
	filename := runningConfig.Filename
	skipLines := runningConfig.SkipLines
	workersCount := numThreads
	scopes := runningConfig.Scopes
	headers := map[string]string{"X-Auth-Token": runningConfig.Token}

	for key, value := range runningConfig.Headers {
		headers[key] = value
	}

	urls := make(chan string)
	workerOutput := make(chan string)
	done := make(chan bool)
	doneWriter := make(chan bool)
	go reader(urls, filename, skipLines)
	go writer(workerOutput, time.Now().Format("2006_01_02_15_04_05")+".tsv", doneWriter)
	log.Printf("starting with %d workers\n", workersCount)
	for i := 0; i < workersCount; i++ {
		go workers(workerOutput, urls, scopes, headers, done)
	}
	for i := 0; i < workersCount; i++ {
		<-done
		log.Println("worker done")
	}
	close(workerOutput)
	<-doneWriter
	log.Println("writer done")
}

func reader(urls chan<- string, filename string, skipLines int) {
	defer recoverGoRoutine()
	defer close(urls)
	log.Printf("Openning file: %s\n", filename)
	file, errOpen := os.Open(filename)
	if errOpen != nil {
		log.Fatal(errOpen)
	}
	scanner := bufio.NewScanner(file)
	lineNumber := 0
	for ; lineNumber < skipLines && scanner.Scan(); lineNumber++ {
		log.Printf("Skipping: %s\n", scanner.Text())
	}
	lineNumber = 0
	for ; scanner.Scan(); lineNumber++ {
		if lineNumber%1000 == 0 {
			log.Printf("Line: %d\n", lineNumber)
		}
		fields := strings.Split(scanner.Text(), ";")
		if len(fields) > 1 {
			if fields[1] != StatusError {
				continue
			}
		}
		// esto no me acuerdo si hace falta.
		urls <- strings.ReplaceAll(strings.ReplaceAll(fields[0], "\"", ""), "access_token", "saya")
	}
	if err := scanner.Err(); err != nil {
		log.Println(err.Error())
	}
	if errClose := file.Close(); errClose != nil {
		log.Printf("Error closing file '%s': %s", filename, errClose.Error())
	}
	log.Printf("Done %d", lineNumber)
}

func writer(result <-chan string, outputFilename string, done chan<- bool) {
	defer recoverGoRoutine()
	defer func(finish chan<- bool) {
		finish <- true
	}(done)
	tsvFile, errTSV := os.Create(outputFilename)
	if errTSV != nil {
		log.Fatalf("failed creating file: %s", errTSV)
	}

	if _, errWrite := tsvFile.WriteString("status\turl\tmsg\n"); errWrite != nil {
		log.Printf("failed writing to file: %s\n", errWrite.Error())
	}
	for line := range result {
		if _, errWrite := tsvFile.WriteString(line + "\n"); errWrite != nil {
			log.Printf("failed writing to file: %s\n", errWrite.Error())
		}
	}

	if errClose := tsvFile.Close(); errClose != nil {
		log.Printf("Error closing file '%s': %s", outputFilename, errClose.Error())
	}
}

func workers(result chan<- string, urls <-chan string, scopes []string, headers map[string]string, done chan<- bool) {
	defer recoverGoRoutine()
	defer func(finish chan<- bool) {
		finish <- true
	}(done)

	scopeCount := len(scopes)
	countOk := 0
	countTotal := 0
	for url := range urls {
		wg := sync.WaitGroup{}
		countTotal++
		wg.Add(scopeCount)
		response := make([]restclientV1.RequestResult, len(scopes))
		for i := 0; i < scopeCount; i++ {
			go restclientV1.GetData(&response[i], scopes[i], url, headers, &wg)
		}
		wg.Wait()
		compareOK := true
		i := 0
		for i < scopeCount-1 && compareOK {
			if response[i].Err != nil {
				compareOK = false
				result <- fmt.Sprintf("%s\t%s\t%s", StatusError, url, response[i].Err.Error())
				break
			}
			if response[i+1].Err != nil {
				compareOK = false
				result <- fmt.Sprintf("%s\t%s\t%s", StatusError, url, response[i+1].Err.Error())
				break
			}
			if response[i].Status != response[i+1].Status {
				compareOK = false
				errMsg := fmt.Sprintf("StatusCodes %d!=%d", response[i].Status, response[i+1].Status)
				result <- fmt.Sprintf("%s\t%s\t%s", StatusError, url, errMsg)
				break
			}
			if len(response[i].Response) != len(response[i+1].Response) {
				compareOK = false
				result <- fmt.Sprintf("%s\t%s\t%s", StatusError, url, "invalid length")
				break
			}
			if ok, errComp := jsonUtils.JSONBytesEqual(response[i].Response, response[i+1].Response); !ok {
				var errMsg string
				if anagram.AreAnagram(string(response[i].Response), string(response[i+1].Response)) {
					errMsg = "Anagram!"
				} else {
					errMsg = "Different!"
				}
				compareOK = false
				if errComp != nil {
					errMsg += " Err:" + errComp.Error()
				}
				result <- fmt.Sprintf("%s\t%s\t%s", StatusError, url, errMsg)
				break
			}
			i++
		}
		if compareOK {
			countOk++
		}
		if countTotal%1000 == 0 {
			log.Printf("%d\t%d\t%3.2f%%", countOk, countTotal, float32(countOk*100)/float32(countTotal))
		}
	}
	if countTotal > 0 {
		log.Printf("%d\t%d\t%3.2f%%", countOk, countTotal, float32(countOk*100)/float32(countTotal))
	}
}

func recoverGoRoutine() {
	if err := recover(); err != nil {
		fmt.Printf("[Custom Recovery] panic recovered: %s", fmt.Errorf("%+v", err))
		debug.PrintStack()
	}
}

func loadConfigFile(filename string) (*configJSON, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	configuration := configJSON{}
	err = decoder.Decode(&configuration)
	if err != nil {
		return nil, err
	}
	return &configuration, nil
}
