package bridge

import (
	"strconv"
	"strings"

	"github.com/cenkalti/backoff"
	dockerapi "github.com/fsouza/go-dockerclient"
)

func retry(fn func() error) error {
	return backoff.Retry(fn, backoff.NewExponentialBackOff())
}

func mapDefault(m map[string]string, key, default_ string) string {
	v, ok := m[key]
	if !ok || v == "" {
		return default_
	}
	return v
}

// Golang regexp module does not support /(?!\\),/ syntax for spliting by not escaped comma
// Then this function is reproducing it
func recParseEscapedComma(str string) []string {
	if len(str) == 0 {
		return []string{}
	} else if str[0] == ',' {
		return recParseEscapedComma(str[1:])
	}

	offset := 0
	for len(str[offset:]) > 0 {
		index := strings.Index(str[offset:], ",")

		if index == -1 {
			break
		} else if str[offset+index-1:offset+index] != "\\" {
			return append(recParseEscapedComma(str[offset+index+1:]), str[:offset+index])
		}

		str = str[:offset+index-1] + str[offset+index:]
		offset += index
	}

	return []string{str}
}

func combineTags(tagParts ...string) []string {
	tags := make([]string, 0)
	for _, element := range tagParts {
		tags = append(tags, recParseEscapedComma(element)...)
	}
	return tags
}

func serviceMetaData(config *dockerapi.Config, port string) (map[string]string, map[string]bool) {
	meta := config.Env
	for k, v := range config.Labels {
		meta = append(meta, k+"="+v)
	}
	metadata := make(map[string]string)
	metadataFromPort := make(map[string]bool)
	for _, kv := range meta {
		kvp := strings.SplitN(kv, "=", 2)
		if strings.HasPrefix(kvp[0], "SERVICE_") && len(kvp) > 1 {
			key := strings.ToLower(strings.TrimPrefix(kvp[0], "SERVICE_"))
			if metadataFromPort[key] {
				continue
			}
			portkey := strings.SplitN(key, "_", 2)
			_, err := strconv.Atoi(portkey[0])
			if err == nil && len(portkey) > 1 {
				if portkey[0] != port {
					continue
				}
				metadata[portkey[1]] = kvp[1]
				metadataFromPort[portkey[1]] = true
			} else {
				metadata[key] = kvp[1]
			}
		}
	}
	return metadata, metadataFromPort
}

func servicePort(container *dockerapi.Container, port dockerapi.Port, published []dockerapi.PortBinding) ServicePort {

	return ServicePort{
		HostPort:          "",
		HostIP:            published[0].HostIP,
		ExposedPort:       "",
		ExposedIP:         "",
		PortType:          "",
		ContainerID:       container.ID,
		ContainerHostname: container.Config.Hostname,
		container:         container,
	}
}
