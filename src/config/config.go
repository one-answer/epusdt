package config

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/assimon/luuu/util/http_client"
	"github.com/spf13/viper"
	"github.com/tidwall/gjson"
)

var (
	AppDebug          bool
	MysqlDns          string
	RuntimePath       string
	LogSavePath       string
	StaticPath        string
	TgBotToken        string
	TgProxy           string
	TgManage          int64
	UsdtRate          float64
	RateApiUrl        string
	TRON_GRID_API_KEY string
	BuildVersion      = "0.0.0-dev"
	BuildCommit       = "none"
	BuildDate         = "unknown"
)

func Init() {
	viper.AddConfigPath("./")
	viper.SetConfigFile(".env")
	err := viper.ReadInConfig()
	if err != nil {
		panic(err)
	}
	gwd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	AppDebug = viper.GetBool("app_debug")
	StaticPath = viper.GetString("static_path")
	RuntimePath = fmt.Sprintf("%s%s", gwd, viper.GetString("runtime_root_path"))
	LogSavePath = fmt.Sprintf("%s%s", RuntimePath, viper.GetString("log_save_path"))
	mustMkdir(RuntimePath)
	mustMkdir(LogSavePath)
	MysqlDns = fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		url.QueryEscape(viper.GetString("mysql_user")),
		url.QueryEscape(viper.GetString("mysql_passwd")),
		fmt.Sprintf("%s:%s", viper.GetString("mysql_host"), viper.GetString("mysql_port")),
		viper.GetString("mysql_database"))
	TgBotToken = viper.GetString("tg_bot_token")
	TgProxy = viper.GetString("tg_proxy")
	TgManage = viper.GetInt64("tg_manage")

	RateApiUrl = GetRateApiUrl()
	TRON_GRID_API_KEY = viper.GetString("tron_grid_api_key")
}

func mustMkdir(path string) {
	if err := os.MkdirAll(path, 0o755); err != nil {
		panic(err)
	}
}

func GetAppVersion() string {
	return BuildVersion
}

func GetBuildCommit() string {
	return BuildCommit
}

func GetBuildDate() string {
	return BuildDate
}

func GetAppName() string {
	appName := viper.GetString("app_name")
	if appName == "" {
		return "epusdt"
	}
	return appName
}

func GetAppUri() string {
	return viper.GetString("app_uri")
}

func GetApiAuthToken() string {
	return viper.GetString("api_auth_token")
}

func GetRateApiUrl() string {
	rateURL := viper.GetString("api_rate_url")
	if rateURL == "" {
		rateURL = os.Getenv("API_RATE_URL")
	}
	if rateURL == "" {
		log.Println("api_rate_url is empty")
	}
	RateApiUrl = rateURL
	return rateURL
}

func GetRateForCoin(coin string, base string) float64 {
	coin = strings.ToLower(strings.TrimSpace(coin))
	base = strings.ToLower(strings.TrimSpace(base))
	if coin == "" || base == "" {
		return 0
	}
	if coin == base {
		return 1
	}
	if coin == "usdt" {
		switch base {
		case "usd":
			return 1
		case "cny":
			usdtRate := GetUsdtRate()
			if usdtRate > 0 {
				return 1 / usdtRate
			}
		}
	}

	baseURL := RateApiUrl
	if baseURL == "" {
		baseURL = GetRateApiUrl()
	}
	if baseURL == "" {
		log.Printf("rate api url is empty")
		return 0.0
	}
	if baseURL[len(baseURL)-1] != '/' {
		baseURL += "/"
	}

	client := http_client.GetHttpClient()
	resp, err := client.R().Get(baseURL + fmt.Sprintf("%s.json", base))
	if err != nil {
		log.Printf("call rate api error: %s", err.Error())
		return 0.0
	}
	if resp.StatusCode() < 200 || resp.StatusCode() >= 300 {
		log.Printf("call rate api unexpected status: %s", resp.Status())
		return 0.0
	}

	targetRate := 0.0
	gjson.GetBytes(resp.Body(), base).ForEach(func(key, value gjson.Result) bool {
		if key.String() == coin {
			targetRate = value.Float()
			return false
		}
		return true
	})
	return targetRate
}

func GetUsdtRate() float64 {
	forcedUsdtRate := viper.GetFloat64("forced_usdt_rate")
	if forcedUsdtRate > 0 {
		return forcedUsdtRate
	}
	if UsdtRate <= 0 {
		return 6.4
	}
	return UsdtRate
}

func GetOrderExpirationTime() int {
	timer := viper.GetInt("order_expiration_time")
	if timer <= 0 {
		return 10
	}
	return timer
}

func GetOrderExpirationTimeDuration() time.Duration {
	timer := GetOrderExpirationTime()
	return time.Minute * time.Duration(timer)
}

func GetRuntimeSqlitePath() string {
	filename := viper.GetString("runtime_sqlite_filename")
	if filename == "" {
		filename = "epusdt-runtime.db"
	}
	if filepath.IsAbs(filename) {
		return filename
	}
	return filepath.Join(RuntimePath, filename)
}

func GetQueueConcurrency() int {
	concurrency := viper.GetInt("queue_concurrency")
	if concurrency <= 0 {
		return 10
	}
	return concurrency
}

func GetQueuePollInterval() time.Duration {
	interval := viper.GetInt("queue_poll_interval_ms")
	if interval <= 0 {
		interval = 1000
	}
	return time.Duration(interval) * time.Millisecond
}

func GetOrderNoticeMaxRetry() int {
	retry := viper.GetInt("order_notice_max_retry")
	if retry < 0 {
		return 0
	}
	return retry
}

func GetCallbackRetryBaseDuration() time.Duration {
	seconds := viper.GetInt("callback_retry_base_seconds")
	if seconds <= 0 {
		seconds = 5
	}
	return time.Duration(seconds) * time.Second
}
