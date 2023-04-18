package main

import (
	"fmt"
	"os"
	"sync"
)

type ProviderSecrets map[SecretType]string

type SecretStore struct {
	mu      sync.Mutex
	secrets map[int]map[SecretType]string
}

func (p *SecretStore) Secrets(id int, authType AuthType) (ProviderSecrets, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.secrets == nil {
		p.secrets = make(map[int]map[SecretType]string)
	}

	s, ok := p.secrets[id]
	if ok {
		return s, nil
	}

	vars, err := SecretEnvVarNames(id, authType)
	if err != nil {
		return nil, err
	}

	s = make(map[SecretType]string)
	for ty, name := range vars {
		val, ok := os.LookupEnv(name)
		if !ok {
			return nil, fmt.Errorf("missing environment variable: %q", name)
		}
		s[ty] = val
	}
	p.secrets[id] = s
	return s, nil
}

func SecretEnvVarNames(id int, authType AuthType) (map[SecretType]string, error) {
	vars := make(map[SecretType]string)
	switch authType {
	case AuthTypeBearerToken:
		vars[SecretTypeBearerToken] = fmt.Sprintf("%sPROVIDER%d_BEARER_TOKEN", envPrefix, id)
	case AuthTypeBasicAuth:
		vars[SecretTypeUsername] = fmt.Sprintf("%sPROVIDER%d_USERNAME", envPrefix, id)
		vars[SecretTypePassword] = fmt.Sprintf("%sPROVIDER%d_PASSWORD", envPrefix, id)
	default:
		return nil, fmt.Errorf("unsupported auth type: %q", authType)
	}
	return vars, nil
}
