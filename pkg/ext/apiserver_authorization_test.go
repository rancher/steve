package ext

import (
	"context"
	"testing"

	"github.com/rancher/steve/pkg/accesscontrol"
	"github.com/rancher/steve/pkg/accesscontrol/fake"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
)

func TestAuthorization_NonResourceURLs(t *testing.T) {
	type input struct {
		ctx   context.Context
		attrs authorizer.Attributes
	}

	type expected struct {
		authorized authorizer.Decision
		reason     string
		err        error
	}

	sampleReadOnlyUser := &user.DefaultInfo{
		Name: "read-only-user",
	}

	sampleReadOnlyAccessSet := func() *accesscontrol.AccessSet {
		accessSet := &accesscontrol.AccessSet{}
		accessSet.AddNonResourceURLs([]string{
			"get",
		}, []string{
			"/metrics",
			"/healthz",
		})
		return accessSet
	}()

	sampleReadWriteUser := &user.DefaultInfo{
		Name: "read-write-user",
	}

	sampleReadWriteAccessSet := func() *accesscontrol.AccessSet {
		accessSet := &accesscontrol.AccessSet{}
		accessSet.AddNonResourceURLs([]string{
			"get", "post",
		}, []string{
			"/metrics",
			"/healthz",
		})
		return accessSet
	}()

	tests := []struct {
		name     string
		input    input
		expected expected

		mockUsername  *user.DefaultInfo
		mockAccessSet *accesscontrol.AccessSet
	}{
		{
			name: "authorized read-only user to read data",
			input: input{
				ctx: context.TODO(),
				attrs: authorizer.AttributesRecord{
					User:            sampleReadOnlyUser,
					ResourceRequest: false,
					Path:            "/healthz",
					Verb:            "get",
				},
			},
			expected: expected{
				authorized: authorizer.DecisionAllow,
				reason:     "",
				err:        nil,
			},
			mockUsername:  sampleReadOnlyUser,
			mockAccessSet: sampleReadOnlyAccessSet,
		},
		{
			name: "unauthorized read-only user to write data",
			input: input{
				ctx: context.TODO(),
				attrs: authorizer.AttributesRecord{
					User:            sampleReadOnlyUser,
					ResourceRequest: false,
					Path:            "/metrics",
					Verb:            "post",
				},
			},
			expected: expected{
				authorized: authorizer.DecisionDeny,
				reason:     "",
				err:        nil,
			},
			mockUsername:  sampleReadOnlyUser,
			mockAccessSet: sampleReadOnlyAccessSet,
		},
		{
			name: "authorized read-write user to read data",
			input: input{
				ctx: context.TODO(),
				attrs: authorizer.AttributesRecord{
					User:            sampleReadWriteUser,
					ResourceRequest: false,
					Path:            "/metrics",
					Verb:            "get",
				},
			},
			expected: expected{
				authorized: authorizer.DecisionAllow,
				reason:     "",
				err:        nil,
			},
			mockUsername:  sampleReadWriteUser,
			mockAccessSet: sampleReadWriteAccessSet,
		},
		{
			name: "authorized read-write user to write data",
			input: input{
				ctx: context.TODO(),
				attrs: authorizer.AttributesRecord{
					User:            sampleReadWriteUser,
					ResourceRequest: false,
					Path:            "/metrics",
					Verb:            "post",
				},
			},
			expected: expected{
				authorized: authorizer.DecisionAllow,
				reason:     "",
				err:        nil,
			},
			mockUsername:  sampleReadWriteUser,
			mockAccessSet: sampleReadWriteAccessSet,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			crtl := gomock.NewController(t)
			asl := fake.NewMockAccessSetLookup(crtl)
			asl.EXPECT().AccessFor(tt.mockUsername).Return(tt.mockAccessSet)

			auth := NewAccessSetAuthorizer(asl)
			authorized, reason, err := auth.Authorize(tt.input.ctx, tt.input.attrs)

			require.Equal(t, tt.expected.authorized, authorized)
			require.Equal(t, tt.expected.reason, reason)

			if tt.expected.err != nil {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

