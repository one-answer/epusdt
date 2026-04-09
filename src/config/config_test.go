package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/viper"
)

func TestNormalizeConfiguredPathUsesExplicitFile(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	configPath := filepath.Join(root, "custom.env")
	if err := os.WriteFile(configPath, []byte("app_name=test\n"), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	got, err := normalizeConfiguredPath(configPath)
	if err != nil {
		t.Fatalf("normalize explicit file: %v", err)
	}
	if got != configPath {
		t.Fatalf("config path = %s, want %s", got, configPath)
	}
}

func TestNormalizeConfiguredPathUsesExplicitDirectory(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	configPath := filepath.Join(root, ".env")
	if err := os.WriteFile(configPath, []byte("app_name=test\n"), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	got, err := normalizeConfiguredPath(root)
	if err != nil {
		t.Fatalf("normalize explicit directory: %v", err)
	}
	if got != configPath {
		t.Fatalf("config path = %s, want %s", got, configPath)
	}
}

func TestResolveConfigFilePathUsesCurrentDirectoryByDefault(t *testing.T) {
	t.Helper()

	oldCwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("get cwd: %v", err)
	}
	defer func() { _ = os.Chdir(oldCwd) }()

	root := t.TempDir()
	configPath := filepath.Join(root, ".env")
	if err := os.WriteFile(configPath, []byte("app_name=test\n"), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := os.Chdir(root); err != nil {
		t.Fatalf("chdir: %v", err)
	}

	t.Setenv("EPUSDT_CONFIG", "")
	SetConfigPath("")

	got, err := resolveConfigFilePath()
	if err != nil {
		t.Fatalf("resolve config path: %v", err)
	}

	gotAbs, err := filepath.Abs(got)
	if err != nil {
		t.Fatalf("abs got: %v", err)
	}
	wantAbs, err := filepath.Abs(configPath)
	if err != nil {
		t.Fatalf("abs want: %v", err)
	}

	gotReal, err := filepath.EvalSymlinks(gotAbs)
	if err != nil {
		t.Fatalf("eval symlinks got: %v", err)
	}
	wantReal, err := filepath.EvalSymlinks(wantAbs)
	if err != nil {
		t.Fatalf("eval symlinks want: %v", err)
	}

	if gotReal != wantReal {
		t.Fatalf("config path = %s, want %s", gotReal, wantReal)
	}
}

func TestResolveConfigFilePathPrefersExplicitOverEnv(t *testing.T) {
	t.Helper()

	oldCwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("get cwd: %v", err)
	}
	defer func() { _ = os.Chdir(oldCwd) }()

	root := t.TempDir()
	if err := os.Chdir(root); err != nil {
		t.Fatalf("chdir: %v", err)
	}

	envDir := filepath.Join(root, "from-env")
	if err := os.MkdirAll(envDir, 0o755); err != nil {
		t.Fatalf("mkdir env dir: %v", err)
	}
	envPath := filepath.Join(envDir, ".env")
	if err := os.WriteFile(envPath, []byte("app_name=env\n"), 0o644); err != nil {
		t.Fatalf("write env config: %v", err)
	}

	flagDir := filepath.Join(root, "from-flag")
	if err := os.MkdirAll(flagDir, 0o755); err != nil {
		t.Fatalf("mkdir flag dir: %v", err)
	}
	flagPath := filepath.Join(flagDir, ".env")
	if err := os.WriteFile(flagPath, []byte("app_name=flag\n"), 0o644); err != nil {
		t.Fatalf("write flag config: %v", err)
	}

	t.Setenv("EPUSDT_CONFIG", envDir)
	SetConfigPath(flagDir)
	defer SetConfigPath("")

	got, err := resolveConfigFilePath()
	if err != nil {
		t.Fatalf("resolve config path: %v", err)
	}
	if got != flagPath {
		t.Fatalf("config path = %s, want %s", got, flagPath)
	}
}

func TestBuildMySQLDSNDefaultsToTLSTrue(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("db_type", "mysql")
	viper.Set("mysql_user", "root")
	viper.Set("mysql_passwd", "p@ss")
	viper.Set("mysql_host", "127.0.0.1")
	viper.Set("mysql_port", "3306")
	viper.Set("mysql_database", "epusdt")

	dsn := buildMySQLDSN()
	if !strings.Contains(dsn, "tls=true") {
		t.Fatalf("dsn = %s, want tls=true", dsn)
	}
	if MySQLTLSConfigName != "true" {
		t.Fatalf("MySQLTLSConfigName = %s, want true", MySQLTLSConfigName)
	}
}

func TestBuildMySQLDSNUsesTiDBTLSConfigName(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("db_type", "tidb")
	viper.Set("tidb_tls_config_name", "tidb-cloud")
	viper.Set("mysql_user", "root")
	viper.Set("mysql_passwd", "pass")
	viper.Set("mysql_host", "gateway01.ap-northeast-1.prod.aws.tidbcloud.com")
	viper.Set("mysql_port", "4000")
	viper.Set("mysql_database", "epusdt")

	dsn := buildMySQLDSN()
	if !strings.Contains(dsn, "tls=tidb-cloud") {
		t.Fatalf("dsn = %s, want tls=tidb-cloud", dsn)
	}
	if MySQLTLSConfigName != "tidb-cloud" {
		t.Fatalf("MySQLTLSConfigName = %s, want tidb-cloud", MySQLTLSConfigName)
	}
}
