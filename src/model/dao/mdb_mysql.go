package dao

import (
	"crypto/tls"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/assimon/luuu/config"
	"github.com/assimon/luuu/util/log"
	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/gookit/color"
	"github.com/spf13/viper"
	"gorm.io/driver/mysql"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

var (
	tidbTLSRegisterOnce sync.Once
	tidbTLSRegisterErr  error
)

// MysqlInit 数据库初始化
func MysqlInit() error {
	if err := ensureTiDBTLSConfig(); err != nil {
		return err
	}

	var err error
	Mdb, err = gorm.Open(buildMySQLDialector(), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			TablePrefix:   viper.GetString("mysql_table_prefix"),
			SingularTable: true,
		},
		Logger: logger.Default.LogMode(logger.Error),
	})
	if err != nil {
		color.Red.Printf("[store_db] mysql open DB,err=%s\n", err)
		// panic(err)
		return err
	}
	if config.SQLDebug {
		Mdb = Mdb.Debug()
	}
	sqlDB, err := Mdb.DB()
	if err != nil {
		color.Red.Printf("[store_db] mysql get DB,err=%s\n", err)
		// panic(err)
		time.Sleep(10 * time.Second)
		MysqlInit()
		return err
	}
	sqlDB.SetMaxIdleConns(viper.GetInt("mysql_max_idle_conns"))
	sqlDB.SetMaxOpenConns(viper.GetInt("mysql_max_open_conns"))
	sqlDB.SetConnMaxLifetime(time.Hour * time.Duration(viper.GetInt("mysql_max_life_time")))
	err = sqlDB.Ping()
	if err != nil {
		color.Red.Printf("[store_db] mysql connDB err:%s", err.Error())
		// panic(err)
		time.Sleep(10 * time.Second)
		MysqlInit()
		return err
	}
	log.Sugar.Debug("[store_db] mysql connDB success")
	return nil
}

func buildMySQLDialector() gorm.Dialector {
	if strings.EqualFold(strings.TrimSpace(viper.GetString("db_type")), "tidb") {
		return mysql.New(mysql.Config{
			DSN:               config.MysqlDns,
			DefaultStringSize: 191,
		})
	}
	return mysql.Open(config.MysqlDns)
}

func ensureTiDBTLSConfig() error {
	if !strings.EqualFold(strings.TrimSpace(viper.GetString("db_type")), "tidb") {
		return nil
	}

	tidbTLSRegisterOnce.Do(func() {
		serverName := strings.TrimSpace(viper.GetString("tidb_tls_server_name"))
		if serverName == "" {
			serverName = strings.TrimSpace(viper.GetString("mysql_host"))
		}
		if serverName == "" {
			tidbTLSRegisterErr = fmt.Errorf("tidb_tls_server_name or mysql_host is required when db_type=tidb")
			return
		}

		tlsConfigName := config.MySQLTLSConfigName
		if tlsConfigName == "" {
			tlsConfigName = "tidb"
		}

		tidbTLSRegisterErr = mysqlDriver.RegisterTLSConfig(tlsConfigName, &tls.Config{
			MinVersion: tls.VersionTLS12,
			ServerName: serverName,
		})
	})

	return tidbTLSRegisterErr
}
