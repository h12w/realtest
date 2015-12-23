package container

import (
	"bytes"
	"errors"
	"log"
	"os"
	"strings"

	"h12.me/realtest/util"
)

func (c Container) ip() (string, error) {
	out := util.Command("docker-machine", "ip", "default").Output()
	return string(bytes.TrimSpace(out)), nil
}

func initDocker() error {
	if !(util.CmdExists("docker-machine") && util.CmdExists("docker")) {
		return errors.New("docker not installed")
	}
	if status := string(util.Command("docker-machine", "status", "default").Output()); status != "Running" {
		log.Println("docker-machine start ...")
		if err := util.Command("docker-machine", "start", "default").Run(); err != nil {
			return err
		}
	}
	if os.Getenv("DOCKER_HOST") == "" || os.Getenv("DOCKER_CERT_PATH") == "" || os.Getenv("DOCKER_TLS_VERIFY") == "" {
		os.Setenv("SHELL", "/bin/bash")
		envSettings := strings.Split(string(util.Command("docker-machine", "env", "default").Output()), "\n")
		for _, line := range envSettings {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			line = strings.TrimPrefix(line, "export ")
			keyVal := strings.Split(line, "=")
			os.Setenv(keyVal[0], keyVal[1])
		}
	}
	return nil
}
