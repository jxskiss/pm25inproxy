// pm25.in data crawler and proxy.
//
// The API provided by pm25.in has a request count limit rather small, this
// program download the data from pm25.in and serves requests within company
// as a proxy service. The inside air data crawlers should all request from
// this service instead of pm25.in.
//
// This proxy service should run on a server with large bandwidth to avoid
// failed requests, which consume the request limits but without results.
//
// Features:
//     1. proxy service cache support
//     2. update data from pm25.in periodically for cached APIs
//     3. saving data to local disk with index stored in database
//     4. simple history data query support
//     5. cached APIs data update time querying support
//     6. compatible APIs with the python version proxy service
//
// Changes:
// 2016-09-24
//     1. Make history data file gzipped to decrease disk space occupation.
//        Uncompressed history data file is supported for reading to keep
//        compatible with old history data files.
//     2. New scheduled tasks to clean expired history data files.
//     3. Improvement: remove invalid records when query history data.
//     4. New scheduled tasks to archive history data files by month.
//     5. Minor bug fixes.
//
// NOTE: the history database (bolt) is different with the database used with
// the python version (sqlite3), also the history filename pattern is different,
// so the service cannot be switched from one to another one. You either use
// the python version or the go version, however a manual migration can be
// applied (with some dirty work) if you really want.
package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/golang/glog"
)

type ApiInfo struct {
	Id    int
	Limit int
}

type CachedData struct {
	sync.RWMutex
	UpdateTime int64
	DataTime   time.Time
	Data       []byte
}

func (cd *CachedData) GetUpdateTime() int64 {
	cd.RLock()
	defer cd.RUnlock()
	return cd.UpdateTime
}

func (cd *CachedData) GetDataTime() string {
	cd.RLock()
	defer cd.RUnlock()
	return cd.DataTime.Format("2006-01-02T15:04:05")
}

func (cd *CachedData) GetCachedData() []byte {
	cd.RLock()
	defer cd.RUnlock()
	return cd.Data
}

type ProxyCount struct {
	sync.RWMutex
	UpdateTime int64
	Count      int
}

func (pc *ProxyCount) GetUpdateTime() int64 {
	pc.RLock()
	defer pc.RUnlock()
	return pc.UpdateTime
}

func (pc *ProxyCount) AddCount(delta int) {
	pc.Lock()
	defer pc.Unlock()
	pc.UpdateTime = time.Now().Unix()
	pc.Count += delta
}

var CachedAPIs = map[string]ApiInfo{
	// 1.10、获取一个城市的检测点列表，限15次/小时
	"/api/querys/station_names.json": {10, 1},
	// 1.11、获取实施了新《环境空气质量标准》的城市列表，即有PM2.5数据的城市列表，限15次小时
	"/api/querys.json": {11, 1},
	// 1.12、获取所有城市的空气质量详细数据，限5次/小时
	"/api/querys/all_cities.json": {12, 3},
	// 1.13、获取全部城市的空气质量指数(AQI)排行榜，限15次/小时
	"/api/querys/aqi_ranking.json": {12, 3},
}

var ProxyAPIs = map[string]ApiInfo{
	// 1.1、获取一个城市所有监测点的PM2.5数据，限500次/小时
	"/api/querys/pm2_5.json": {1, 400},
	// 1.2、获取一个城市所有监测点的PM10数据，限500次/小时
	"/api/querys/pm10.json": {2, 400},
	// 1.3、获取一个城市所有监测点的CO数据，限500次/小时
	"/api/querys/co.json": {3, 400},
	// 1.4、获取一个城市所有监测点的NO2数据，限500次/小时
	"/api/querys/no2.json": {4, 400},
	// 1.5、获取一个城市所有监测点的SO2数据，限500次/小时
	"/api/querys/so2.json": {5, 400},
	// 1.6、获取一个城市所有监测点的O3数据，限500次/小时
	"/api/querys/o3.json": {6, 400},
	// 1.7、获取一个城市所有监测点的AQI数据（含详情），限500次/小时
	"/api/querys/aqi_details.json": {7, 400},
	// 1.8、获取一个城市所有监测点的AQI数据（不含详情，仅AQI），限500次/小时
	"/api/querys/only_aqi.json": {8, 400},
	// 1.9、获取一个监测点的AQI数据（含详情），限500次/小时
	"/api/querys/aqis_by_station.json": {9, 400},
}

func fullAPIPath(basePath string) string {
	if basePath == "querys.json" {
		return "/api/querys.json"
	} else {
		return "/api/querys/" + basePath
	}
}

var ( // global states shared by all requests
	historyDb   *bolt.DB
	apiCached   = make(map[string]*CachedData)
	apiCountsMu sync.Mutex
	apiCounts   = make(map[string]map[string]*ProxyCount)
)

var ( // program options
	pm25inUrl    = flag.String("pm25url", "http://www.pm25.in", "base pm25in api url")
	pm25inKey    = flag.String("pm25key", "", "AppKey for pm25.in service")
	dataStoreDir = flag.String("datadir", "", "data store directory")
	keptDays     = flag.Int("kept", 365, "kept days for history file before removed")
	port         = flag.Int("port", 5059, "listening port")
)

func main() {
	// parse command line options and flush logs before exit
	flag.Parse()
	defer glog.Flush()

	var err error
	if len(*pm25inUrl) == 0 || *port <= 0 {
		glog.Fatalln("Invalid options")
	}
	if *pm25inKey == "" {
		glog.Fatalln("AppKey for pm25.in must be provided")
	}
	if *dataStoreDir == "" {
		*dataStoreDir, err = os.Getwd()
		if err != nil {
			glog.Fatalln("Cannot get data store directory")
		}
		glog.Warningln("the data directory is set to:", *dataStoreDir)
	}
	if (*pm25inUrl)[len(*pm25inUrl)-1] == '/' {
		*pm25inUrl = (*pm25inUrl)[:len(*pm25inUrl)-1]
	}
	// prepare data directory and history database
	dbPath := filepath.Join(*dataStoreDir, "history.bolt")
	historyDb, err = bolt.Open(dbPath, 0644, nil)
	if err != nil {
		glog.Fatalln("Cannot open history database:", err)
	}
	defer historyDb.Close()
	// create data directory if not exists
	latestDataDir := filepath.Join(*dataStoreDir, "latest")
	if _, err := os.Stat(latestDataDir); os.IsNotExist(err) {
		err = os.Mkdir(filepath.Join(*dataStoreDir, "latest"), 0755)
		if err != nil {
			glog.Fatalln("Cannot create data directory:", err)
		}
	}

	// initialize apiCached and apiCounts global states
	// and load cached history from database
	cAPIs := make([]string, 0, len(CachedAPIs))
	for api := range CachedAPIs {
		cAPIs = append(cAPIs, api)
		apiCached[api] = new(CachedData)
	}
	initHistory(cAPIs)
	for api := range ProxyAPIs {
		apiCounts[api] = make(map[string]*ProxyCount)
	}

	// schedule periodically housekeeping tasks
	go schedHouseKeep(24, *keptDays)

	// schedule periodically refresh tasks
	schedAPIs := []string{
		"/api/querys/all_cities.json",
		"/api/querys/aqi_ranking.json"}
	go schedRefresh(schedAPIs, 10)

	// register handlers and start serving requests
	http.HandleFunc("/", route)
	glog.Infoln("Starting http server, listening on port", *port)
	glog.Fatalln(http.ListenAndServe(fmt.Sprintf(":%v", *port), nil))
}

func route(w http.ResponseWriter, r *http.Request) {
	api := r.URL.Path
	if _, ok := CachedAPIs[api]; ok {
		handleCached(w, r)
	} else if _, ok = ProxyAPIs[api]; ok {
		handleProxy(w, r)
	} else if strings.HasPrefix(api, "/status") {
		handleStatus(w, r)
	} else {
		http.NotFound(w, r)
	}
}

func handleCached(w http.ResponseWriter, r *http.Request) {
	api := r.URL.Path
	if _, ok := CachedAPIs[api]; !ok {
		w.Write([]byte(`{"error": "Unsupported API for cache service"}`))
		return
	}

	// query for history data
	r.ParseForm()
	if len(r.Form["history"]) > 0 {
		ymdh := r.Form["history"][0]
		if len(ymdh) != 10 {
			w.Write([]byte(`{"error": "Error history argument"}`))
			return
		}
		res, err := getHistory(api, ymdh)
		if err != nil {
			glog.Errorln("error query history:", err)
			w.Write([]byte(`{"error": "Server internal error"}`))
			return
		}
		io.Copy(w, res) // ignores potential errors
		return
	}

	// get the previous cached data if exist
	if pre := apiCached[api].GetUpdateTime(); pre > 0 {
		w.Write(apiCached[api].GetCachedData())
		return
	}

	// hasn't been cached, get it from pm25.in
	err := refreshCache(api)
	if err != nil {
		w.Write([]byte(`{"error": "Error proxy to pm25.in"}`))
		return
	}
	w.Write(apiCached[api].GetCachedData())
}

func refreshCache(api string) error {
	// lock the dataCache when refreshing, preventing other requests
	// triggering simultaneous requests for this api to pm25.in
	apiCached[api].Lock()
	defer apiCached[api].Unlock()

	start := time.Now().UnixNano()
	// one second deviation allowed
	if int(start/1e9-apiCached[api].UpdateTime)-3600/CachedAPIs[api].Limit < -1 {
		glog.Warningf("%s: %s", api, "already refreshed")
		return nil
	}

	apiUrl := *pm25inUrl + api + "?token" + *pm25inKey
	// give pm25.in service 5 minutes timeout tolerance
	body, err := fetchGet(apiUrl, 5*time.Minute)
	if err != nil {
		// retry after 5 seconds on error
		time.Sleep(time.Second * 5)
		body, err = fetchGet(apiUrl, 5*time.Minute)
		if err != nil {
			glog.Errorf("%v: %v", api, err)
			return err
		}
	}

	// check if the response is an error message
	if bytes.HasPrefix(body, []byte("{")) && bytes.Contains(body, []byte(`"error"`)) {
		err = fmt.Errorf("unexpected response: %s", body)
		glog.Errorf("%v: %v\n", api, err)
		return err
	}

	end := time.Now().UnixNano()
	apiCached[api].UpdateTime = end / 1e9
	apiCached[api].Data = body
	oldTime := apiCached[api].DataTime
	apiCached[api].DataTime, err = parseDataTime(body)
	if err != nil {
		apiCached[api].DataTime = time.Unix(apiCached[api].UpdateTime, 0)
	}
	if apiCached[api].DataTime != oldTime {
		err = saveHistory(api, end/1e9, body)
		if err != nil {
			glog.Errorln("failed to save history:", err)
		}
	}
	glog.Infof("api %s refreshed in %.4fms", api, float32(end-start)/1e6)
	return nil
}

func fetchGet(url string, timeout time.Duration) ([]byte, error) {
	client := http.Client{Timeout: timeout}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

// helper sturct to get data time point
type apiDataRec struct {
	TimePoint string `json:"time_point"`
}

func parseDataTime(data []byte) (time.Time, error) {
	var res []apiDataRec
	if err := json.Unmarshal(data, &res); err != nil {
		return time.Time{}, err
	}
	tsCounts := make(map[string]int)
	for _, dr := range res {
		tsCounts[dr.TimePoint]++
	}
	var tsMostCommon string
	var tsMaxCount int
	for ts, cnt := range tsCounts {
		if cnt > tsMaxCount {
			tsMostCommon, tsMaxCount = ts, cnt
		}
	}
	return time.ParseInLocation("2006-01-02T15:04:05Z", tsMostCommon, time.Local)
}

func initHistory(apis []string) {
	// time.Parse uses UTC time, use ParseInLocation instead
	for _, api := range apis {
		historyDb.Update(func(tx *bolt.Tx) error {
			bucket, err := tx.CreateBucketIfNotExists([]byte(api))
			if err != nil {
				glog.Fatalln("Failed to create bucket:", err)
			}
			cursor := bucket.Cursor()
			ts, fn := cursor.Last()
			if ts == nil {
				return nil
			}
			updateTime, _ := time.ParseInLocation("20060102150405", string(ts), time.Local)
			file, err := os.Open(filepath.Join(*dataStoreDir, string(fn)))
			if err != nil {
				glog.Errorln("Failed to load history:", err)
				return nil
			}
			defer file.Close()
			// read gzipped data, keep compatible with plain text history file
			var reader io.Reader
			if bytes.HasSuffix(fn, []byte(".gz")) {
				reader, err = gzip.NewReader(file)
				if err != nil {
					return nil
				}
				defer reader.(*gzip.Reader).Close()
			} else {
				reader = file
			}
			// read cached data
			cache, err := ioutil.ReadAll(reader)
			if err != nil {
				glog.Errorln("Failed to read history:", err)
				return nil
			}
			apiCached[api].Lock()
			defer apiCached[api].Unlock()
			apiCached[api].Data = cache
			apiCached[api].DataTime, err = parseDataTime(cache)
			if err != nil {
				apiCached[api].DataTime = updateTime
			}
			apiCached[api].UpdateTime = updateTime.Unix()
			glog.Infoln("History loaded:", string(ts), api)
			return nil
		})
	}
}

func saveHistory(api string, updateTime int64, data []byte) error {
	ts := time.Unix(updateTime, 0).Format("20060102150405")
	fn := filepath.Base(api) + "@" + ts + ".gz"
	file, err := os.OpenFile(filepath.Join(
		*dataStoreDir, "latest", fn), os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	gzWriter := gzip.NewWriter(file)
	defer gzWriter.Close()
	_, err = gzWriter.Write(data)
	if err != nil {
		return err
	}
	err = historyDb.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(api))
		err := b.Put([]byte(ts), []byte(filepath.Join("latest", fn)))
		return err
	})
	return err
}

func getHistory(api, history string) (io.Reader, error) {
	var res = new(bytes.Buffer)
	// if some files not exist, the invalid records should be removed
	// to avoid more requests trying to read the files
	var notExistHistory [][]byte
	err := historyDb.View(func(tx *bolt.Tx) error {
		ymdh := []byte(history)
		c := tx.Bucket([]byte(api)).Cursor()

		fmt.Fprint(res, "{\n")
		first := true
		for ts, fn := c.Seek(ymdh); bytes.HasPrefix(ts, ymdh); ts, fn = c.Next() {
			if !first {
				fmt.Fprint(res, "\n, ")
			}
			file, err := os.Open(filepath.Join(*dataStoreDir, string(fn)))
			if err != nil {
				glog.Errorln("error open file:", string(fn))
				if os.IsNotExist(err) {
					notExistHistory = append(notExistHistory, ts)
				}
				continue
			}
			defer file.Close()
			// read compressed data if is gzipped
			// also keep compatible with plain text history file
			var reader io.Reader
			if bytes.HasSuffix(fn, []byte(".gz")) {
				reader, err = gzip.NewReader(file)
				if err != nil {
					glog.Errorln("error initialize gzip reader:", string(fn))
					continue
				}
				defer reader.(*gzip.Reader).Close()
			} else {
				reader = file
			}
			// read cached data
			data, err := ioutil.ReadAll(reader)
			if err != nil {
				glog.Errorln("error load history file:", string(fn))
				continue
			}
			fmt.Fprintf(res, "%q: %s", ts, data)
			first = false
		}
		fmt.Fprint(res, "\n}\n")
		return nil
	})
	if len(notExistHistory) > 0 {
		errUpdate := historyDb.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(api))
			for _, ts := range notExistHistory {
				b.Delete(ts)
			}
			return nil
		})
		if errUpdate != nil {
			glog.Errorln("failed to remove invalid record:", errUpdate)
		}
	}
	return res, err
}

func schedRefresh(apis []string, sleep int) {
	timeout := make(chan string, len(apis))
	for idx, api := range apis {
		// the variables during iteration must be copied to the inner lexical scope,
		// or all the goroutines will get the values of the last iteration.
		// see gopl "5.6.1. Caveat: Capturing Iteration Variables" for instructions.
		idx, api := idx, api
		go func() {
			wait := 3600/CachedAPIs[api].Limit - int(
				time.Now().Unix()-apiCached[api].GetUpdateTime())
			// force sleeping 20 seconds between requests when startup
			if wait < 20+idx*20 {
				wait = 20 + idx*20
			}
			time.Sleep(time.Second * time.Duration(wait))
			timeout <- api
			ticker := time.Tick(time.Minute * time.Duration(60/CachedAPIs[api].Limit))
			for range ticker {
				timeout <- api
			}
		}()
	}

	go func() {
		for api := range timeout {
			// NOTE: ignore the returned error
			// the error has already been logged in refreshCache function
			refreshCache(api)
			// force sleeping a while between two cached api requests
			time.Sleep(time.Second * time.Duration(sleep))
		}
	}()
}

func cleanHistory(deadline time.Time) error {
	// empty directories will be removed
	var fromDirs []string
	// The keys are stored in byte-sorted order within a bucket.
	// So we can iterate the keys and remove the expired history files.
	err := historyDb.Update(func(tx *bolt.Tx) error {
		stop := deadline.Format("20060102150405")
		for api := range CachedAPIs {
			b := tx.Bucket([]byte(api))
			if b == nil { // the bucket may not exist
				continue
			}
			c := b.Cursor()
			for ts, fn := c.First(); ts != nil && string(ts) < stop; ts, fn = c.Next() {
				// TODO: do we need to lock the file through syscall or something else?
				err := os.Remove(filepath.Join(*dataStoreDir, string(fn)))
				if err != nil {
					glog.Errorln("failed to remove file:", err)
					if !os.IsNotExist(err) {
						continue
					}
				} else {
					glog.Infoln("history file removed:", string(fn))
				}
				fromDirs = append(fromDirs, string(ts)[:6])
				c.Delete()
			}
		}
		return nil
	})
	for _, dir := range fromDirs {
		// the directory may be not exist or not empty
		// ignore any potential errors
		os.Remove(filepath.Join(*dataStoreDir, dir))
	}
	return err
}

func archiveHistory() {
	stop := time.Now().Add(-time.Hour * time.Duration(24*30)).Format("20060102150405")
	// parse api and timestamp from filename
	parse := func(fn string) (api, ts string) {
		atIdx := strings.Index(fn, "@")
		if atIdx == -1 || len(fn[:atIdx]) < 15 {
			return "", ""
		}
		return fn[:atIdx], fn[atIdx+1 : atIdx+15]
	}
	// get old data files list
	files, _ := ioutil.ReadDir(filepath.Join(*dataStoreDir, "latest"))
	var oldFiles = make(map[string][]string)
	var ymDirs = make(map[string]bool)
	for _, fi := range files {
		if fi.IsDir() {
			continue
		}
		api, ts := parse(fi.Name())
		if api == "" || ts == "" {
			continue
		}
		if ts < stop {
			oldFiles[api] = append(oldFiles[api], fi.Name())
		}
		ymDirs[ts[:6]] = true
	}
	// create directory which not exists
	for dir := range ymDirs {
		err := os.Mkdir(filepath.Join(*dataStoreDir, dir), 0644)
		if err != nil && !os.IsExist(err) {
			glog.Errorln("failed to create directory:", err)
			return
		}
	}
	// move history data files
	for api, files := range oldFiles {
		for _, fn := range files {
			_, ts := parse(fn)
			if ts == "" {
				continue
			}
			oldName := filepath.Join(*dataStoreDir, "latest", fn)
			newName := filepath.Join(*dataStoreDir, ts[:6], fn)
			err := historyDb.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte(fullAPIPath(api)))
				err := os.Rename(oldName, newName)
				b.Put([]byte(ts), []byte(filepath.Join(ts[:6], fn)))
				return err
			})
			if err != nil {
				glog.Errorln("failed to rename file:", err)
			}
		}
	}
}

func schedHouseKeep(everyHours, keptDays int) {
	deadline := time.Now().Add(-time.Duration(time.Hour * time.Duration(24*keptDays)))
	// do cleaning when the the program startup
	cleanHistory(deadline)
	archiveHistory()
	// do cleaning periodically
	ticker := time.Tick(time.Hour * time.Duration(everyHours))
	for range ticker {
		cleanHistory(deadline)
		archiveHistory()
	}
}

func handleProxy(w http.ResponseWriter, r *http.Request) {
	api := r.URL.Path
	apiInfo, ok := ProxyAPIs[api]
	if !ok {
		w.Write([]byte(`{"error": "Unsupported API for proxy service"}`))
		return
	}
	params, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		w.Write([]byte(`{"error": "Wrong query parameters"}`))
		return
	}

	key := params.Get("token")
	if key == "" {
		key = *pm25inKey
		params.Set("token", key)
	}

	// ensure the ProxyCount for this api and this key is initialized
	if _, ok := apiCounts[api][key]; !ok {
		apiCountsMu.Lock()
		apiCounts[api][key] = new(ProxyCount)
		apiCountsMu.Unlock()
	}

	now := time.Now().Unix()
	// get the previous request time
	if pre := apiCounts[api][key].GetUpdateTime(); int(now-pre) < 3600/apiInfo.Limit {
		w.Write([]byte(`{"error": "API called too frequently"}`))
		return
	}

	// update the apiCount first to prevent other requests sending
	// simultaneous requests for this api to pm25.in using this key,
	// then proxy the request to pm25.in
	apiCounts[api][key].AddCount(1)
	resp, err := http.Get(*pm25inUrl + api + "?" + params.Encode())
	if err != nil {
		w.Write([]byte(`{"error": "Error proxy to pm25.in"}`))
		return
	}

	defer resp.Body.Close()
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body) // ignore potential errors
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
	api := r.URL.Path[len("/status"):]
	if api == "" || api == "/" {
		w.Write([]byte("ok"))
		return
	}
	if _, exist := CachedAPIs[api]; !exist {
		http.NotFound(w, r)
		return
	}
	w.Write([]byte(apiCached[api].GetDataTime()))
}
