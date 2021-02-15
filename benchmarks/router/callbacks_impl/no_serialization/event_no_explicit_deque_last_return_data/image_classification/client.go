package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"time"
)

func unixMilli(t time.Time) float64 {
	return float64(t.Round(time.Millisecond).UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond)))
}

func MakeRequest(url string, values []byte, ch chan<- string,
	deadline_ms float64) {
	// values := map[string]byte{"data": username}

	start := time.Now()
	current_time := unixMilli(start)
	current_time += deadline_ms
	current_time_str := strconv.FormatFloat(current_time, 'E', -1, 64)
	// values["absolute_slo_ms"] = current_time_str
	// jsonValue, _ := json.Marshal(values)
	resp, _ := http.Post(url, "application/json", bytes.NewBuffer(values))
	secs := time.Since(start).Seconds()
	body, _ := ioutil.ReadAll(resp.Body)
	ch <- fmt.Sprintf("%.2f elapsed with response length: %s %s deadline_ms %s", secs, body, url, current_time_str)
}
func main() {
	ch := make(chan string)
	deadline_ms, err := strconv.ParseFloat(os.Args[1], 64)
	img_path := os.Args[2]
	arrival_curve := os.Args[3:]

	imgFile, err := os.Open(img_path)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer imgFile.Close()
	fInfo, _ := imgFile.Stat()
	var size int64 = fInfo.Size()
	buf := make([]byte, size)
	fReader := bufio.NewReader(imgFile)
	fReader.Read(buf)
	// imgBase64Str := base64.StdEncoding.EncodeToString(buf)

	// values := map[string]string{"image": imgBase64Str}
	time.Sleep(10 * time.Millisecond)
	start := time.Now()
	for i := 0; i < len(arrival_curve); i++ {
		// time.Sleep(12195 * time.Microsecond)
		// time.Sleep(10 * time.Millisecond)
		time_ms, err_time := strconv.ParseFloat(arrival_curve[i], 64)
		if err_time != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		time.Sleep(time.Duration(time_ms) * time.Millisecond)
		// values := map[string]string{"data": imgBase64Str}
		go MakeRequest("http://127.0.0.1:8000/resnet50", buf, ch, deadline_ms)
	}
	for i := 0; i < len(arrival_curve); i++ {
		<-ch
	}
	fmt.Printf("%.2fs elapsed\n", time.Since(start).Seconds())
}
