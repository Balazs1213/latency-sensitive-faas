package knative

import (
	"context"

	docker "knative.dev/func/pkg/docker"
)

// NewCredentialsProvider returns a registry credentials provider that always returns the given user name and
// password.
func NewCredentialsProvider(user, password string) docker.CredentialsProvider {
	return func(ctx context.Context, image string) (docker.Credentials, error) {
		return docker.Credentials{
			Username: user,
			Password: password,
		}, nil
	}
}
