package knative

import (
	"context"

	docker "knative.dev/func/pkg/docker"
)

func NewCredentialsProvider(user, password string) docker.CredentialsProvider {
	return func(ctx context.Context, image string) (docker.Credentials, error) {
		return docker.Credentials{
			Username: user,
			Password: password,
		}, nil
	}
}
