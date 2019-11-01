package main
import (
     "fmt"
     "net/http"
     "time"
     "io/ioutil"
)
func MakeRequest(url string, ch chan<-string) {
  start := time.Now()
  resp, _ := http.Get(url)
  secs := time.Since(start).Seconds()
  body, _ := ioutil.ReadAll(resp.Body)
  ch <- fmt.Sprintf("%.2f elapsed with response length: %s %s", secs, body, url)
}
func main() {
  start := time.Now()
  ch := make(chan string)
  for i := 0; i <= 1000; i++ {
  	  time.Sleep(10 * time.Millisecond)
      go MakeRequest("http://127.0.0.1:8000/echo", ch)
  }
  for i := 0; i <= 1000; i++ {
    fmt.Println(<-ch)
  }
  fmt.Printf("%.2fs elapsed\n", time.Since(start).Seconds())
}