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
	portValue := getEnv("PORT", strconv.Itoa(defaultPort))
	port, err := strconv.Atoi(portValue)
	if err != nil {
		return HTTPServiceConfig{}, fmt.Errorf("parse PORT: %w", err)
	}

	return HTTPServiceConfig{
		ServiceName: serviceName,
		Port:        port,
		Version:     getEnv("APP_VERSION", "dev"),
		Environment: getEnv("APP_ENV", "local"),
	}, nil
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && value != "" {
		return value
	}
	return fallback
}
