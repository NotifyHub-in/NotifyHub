package config

import (
	"fmt"
	"os"
	"strconv"
)

type HTTPServiceConfig struct {
	ServiceName string
	Port        int
	Version     string
	Environment string
}

func LoadHTTPServiceConfig(serviceName string, defaultPort int) (HTTPServiceConfig, error) {
	portValue := GetEnv("PORT", strconv.Itoa(defaultPort))
	port, err := strconv.Atoi(portValue)
	if err != nil {
		return HTTPServiceConfig{}, fmt.Errorf("parse PORT: %w", err)
	}

	return HTTPServiceConfig{
		ServiceName: serviceName,
		Port:        port,
		Version:     GetEnv("APP_VERSION", "dev"),
		Environment: GetEnv("APP_ENV", "local"),
	}, nil
}

func MustGetEnv(key string) string {
	value, ok := os.LookupEnv(key)
	if !ok || value == "" {
		panic(fmt.Sprintf("missing required environment variable %s", key))
	}
	return value
}

func GetEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && value != "" {
		return value
	}
	return fallback
}
