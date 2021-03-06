package container

import (
	"errors"

	"h12.io/realtest/util"
)

func (c *Container) ip() (string, error) {
	ip, err := util.LocalIP()
	return ip.String(), err
	/*
		type networkSettings struct {
			IPAddress string
		}
		type container struct {
			NetworkSettings networkSettings
		}
		var cs []container
		out := util.Command("docker", "inspect", c.ID).Output()
		if err := json.NewDecoder(bytes.NewReader(out)).Decode(&cs); err != nil {
			return "", err
		}
		if len(cs) == 0 {
			return "", errors.New("no output from docker inspect")
		}
		if ip := cs[0].NetworkSettings.IPAddress; ip != "" {
			return ip, nil
		}
		return "", errors.New("could not find an IP. Not running?")
	*/
}

func initDocker() error {
	if !util.CmdExists("docker") {
		return errors.New("docker not installed")
	}
	return nil
}
