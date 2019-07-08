package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/valyala/fasthttp"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
)

import _ "net/http/pprof"

type cmdData struct {
	targetUrl             string
	numWorkers            int
	numAccums             int
	accumQueueSize        int
	outputQueueSize       int
	flushThresholdInBytes int
	template              map[string]interface{}
}

func main() {
	if len(os.Args) < 2 {
		_, _ = fmt.Fprintln(os.Stderr, "Usage: scan <url>")
		return
	}
	cmd := cmdData{
		targetUrl:  os.Args[1],
		numWorkers: 64,
		numAccums:  12,
	}
	var err error
	numWorkersStr := os.Getenv("POSTTO_NUM_WORKERS")
	if numWorkersStr != "" {
		cmd.numWorkers, err = strconv.Atoi(numWorkersStr)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			return
		}
	}
	numAccumsStr := os.Getenv("POSTTO_NUM_ACCUMS")
	if numAccumsStr != "" {
		cmd.numAccums, err = strconv.Atoi(numAccumsStr)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			return
		}
	}
	cmd.accumQueueSize = 64
	accumQueueSizeStr := os.Getenv("POSTTO_ACCUMULATOR_QUEUE_SIZE")
	if accumQueueSizeStr != "" {
		cmd.accumQueueSize, err = strconv.Atoi(accumQueueSizeStr)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			return
		}
	}
	cmd.outputQueueSize = cmd.numWorkers * 64
	workerQueueSizeStr := os.Getenv("POSTTO_OUTPUT_QUEUE_SIZE")
	if workerQueueSizeStr != "" {
		cmd.outputQueueSize, err = strconv.Atoi(workerQueueSizeStr)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			return
		}
	}
	cmd.flushThresholdInBytes = 64 * 1024 // 64 kB
	flushThresholdInBytesStr := os.Getenv("POSTTO_FLUSH_THRESHOLD_BYTES")
	if flushThresholdInBytesStr != "" {
		cmd.flushThresholdInBytes, err = strconv.Atoi(flushThresholdInBytesStr)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			return
		}
	}
	templateStr := os.Getenv("POSTTO_GET_ITEMS_TEMPLATE")
	cmd.template = make(map[string]interface{})
	if templateStr != "" {
		err = json.Unmarshal([]byte(templateStr), &cmd.template)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			return
		}
	}
	cmd.template["TotalSegment"] = cmd.numWorkers
	go func() { // for pprof
		_, _ = fmt.Fprintln(os.Stderr, http.ListenAndServe("localhost:6060", nil))
	}()
	err = do(cmd)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
	}
}

var password = func() string {
	p := os.Getenv("V3IO_PASSWORD")
	if p == "" {
		return "datal@ke!"
	}
	return p
}()

var authorization = "Basic " + base64.StdEncoding.EncodeToString([]byte("iguazio:"+password))

func do(cmd cmdData) error {

	lineOutChan := make(chan []byte, cmd.outputQueueSize)
	terminationChannel := make(chan error, cmd.numWorkers)
	itemChannels := make([]chan map[string]map[string]string, cmd.numAccums)
	for i := 0; i < cmd.numAccums; i++ {
		itemChannels[i] = make(chan map[string]map[string]string, cmd.outputQueueSize)
	}

	for i := 0; i < cmd.numWorkers; i++ {
		template := make(map[string]interface{}, len(cmd.template)+1)
		for k, v := range cmd.template {
			template[k] = v
		}
		template["Segment"] = i
		go worker(cmd, template, itemChannels, terminationChannel)
	}

	var accumCounter = int64(cmd.numAccums)

	for _, itemChan := range itemChannels {
		go accumulate(int64(cmd.flushThresholdInBytes), itemChan, lineOutChan, &accumCounter)
	}

	var terminationCount int
	for {
		select {
		case err := <-terminationChannel:
			if err != nil {
				return err
			}
			terminationCount++
			if terminationCount == cmd.numWorkers {
				terminationChannel = nil
				for _, itemOutChannel := range itemChannels {
					close(itemOutChannel)
				}
			}
		case line, ok := <-lineOutChan:
			if !ok {
				return nil
			}
			fmt.Println(string(line))
		}
	}
}

type Response struct {
	LastItemIncluded string
	NumItems         int
	NextMarker       string
	Items            []map[string]map[string]string
}

func worker(cmd cmdData, template map[string]interface{}, itemChannels []chan map[string]map[string]string, termination chan<- error) {
	var response Response
	for {
		templateStr, err := json.Marshal(template)
		if err != nil {
			termination <- err
			return
		}
		err = getitems(cmd, templateStr, &response)
		if err != nil {
			termination <- err
			return
		}

		for _, item := range response.Items {
			parentDir, _ := strconv.Atoi(item["parent_dir"]["N"])
			ch := itemChannels[parentDir%len(itemChannels)]
			ch <- item
		}
		if response.LastItemIncluded == "TRUE" {
			break
		}
		template["Marker"] = response.NextMarker
	}
	termination <- nil
}

type accumulator struct {
	size  int64
	slice [][]byte
}

func accumulate(flushThresholdInBytes int64, itemChannel <-chan map[string]map[string]string, lineOutChan chan<- []byte, accumCounter *int64) {
	m := make(map[string]accumulator)

	for item := range itemChannel {
		key := item["parent_dir"]["N"]
		accum := m[key]
		j, _ := json.Marshal(item) // Error not expected because we got the item from Unmarshal.
		accum.slice = append(accum.slice, j)
		accum.size += int64(len(j))
		m[key] = accum
		if accum.size >= flushThresholdInBytes {
			//for _, accumItem := range accum.slice {
			//	lineOutChan <- accumItem
			//}
			lineOutChan <- []byte(fmt.Sprintf("%s: %d items, %d bytes", key, len(accum.slice), accum.size))
			delete(m, key)
		}
	}

	atomic.AddInt64(accumCounter, -1)
	if *accumCounter == 0 {
		close(lineOutChan)
	}
}

func getitems(cmd cmdData, body []byte, response *Response) error {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.SetRequestURI(cmd.targetUrl)
	req.Header.SetMethod("PUT")
	req.Header.Set("Authorization", authorization)
	req.Header.Set("X-v3io-function", "GetItems")
	req.SetBody(body)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	err := fasthttp.Do(req, resp)
	if err != nil {
		return errors.Wrap(err, "http error")
	} else if resp.StatusCode() >= 300 {
		return errors.Errorf("status code not OK: Request:\n%v\nResponse:\n%v\n", req, resp)
	}
	//var response Response
	respBody := resp.Body()
	err = json.Unmarshal(respBody, response)
	if err != nil {
		return errors.Wrapf(err, "Failed to unmarshal json: %s", string(respBody))
	}
	return nil
}
