package restclientV1

import (
	"io/ioutil"
	"log"
	"net/http"
	"sync"
)

type RequestResult struct {
	Status   int
	Response []byte
	Err      error
}

func GetData(resp *RequestResult, scope string, url string, headers map[string]string, wg *sync.WaitGroup) {
	defer wg.Done()
	var request *http.Request
	request, resp.Err = http.NewRequest(http.MethodGet, scope+url, nil)
	log.Printf("URL: %s%s", scope, url)
	if resp.Err != nil {
		log.Printf("%s\n", resp.Err.Error())
		return
	}
	for hName, hValue := range headers {
		request.Header.Add(hName, hValue)
	}
	var response *http.Response
	client := &http.Client{}
	response, resp.Err = client.Do(request)
	if resp.Err != nil {
		log.Printf("%s\n", resp.Err.Error())
		return
	}
	resp.Status = response.StatusCode
	//if response.StatusCode != http.StatusOK {
	//resp.Err = fmt.Errorf("NOT 200 - StatusCode %d", response.StatusCode)
	//return
	//}
	resp.Response, resp.Err = ioutil.ReadAll(response.Body)
	if response.Body != nil {
		if errBody := response.Body.Close(); errBody != nil {
			log.Printf("Error closing body: %s", errBody.Error())
		}
	}
}
