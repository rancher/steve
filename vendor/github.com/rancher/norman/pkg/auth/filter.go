package auth

import (
	"net/http"
	"strings"

	v1 "k8s.io/api/authentication/v1"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/plugin/pkg/authenticator/token/webhook"
)

type Authenticator interface {
	Authenticate(req *http.Request) (user.Info, bool, error)
}

func NewWebhookAuthenticator(kubeConfigFile string) (Authenticator, error) {
	wh, err := webhook.New(kubeConfigFile, nil)
	if err != nil {
		return nil, err
	}

	return &webhookAuth{
		auth: wh,
	}, nil
}

func NewWebhookMiddleware(kubeConfigFile string) (func(http.ResponseWriter, *http.Request, http.Handler), error) {
	auth, err := NewWebhookAuthenticator(kubeConfigFile)
	if err != nil {
		return nil, err
	}
	return ToMiddleware(auth), nil
}

type webhookAuth struct {
	auth authenticator.Token
}

func (w *webhookAuth) Authenticate(req *http.Request) (user.Info, bool, error) {
	token := req.Header.Get("Authorization")
	if strings.HasPrefix(token, "Bearer ") {
		token = strings.TrimPrefix(token, "Bearer ")
	} else {
		token = ""
	}

	if token == "" {
		cookie, err := req.Cookie("R_SESS")
		if err != nil && err != http.ErrNoCookie {
			return nil, false, err
		} else if err != http.ErrNoCookie && len(cookie.Value) > 0 {
			token = "cookie://" + cookie.Value
		}
	}

	if token == "" {
		return nil, false, nil
	}

	resp, ok, err := w.auth.AuthenticateToken(req.Context(), token)
	if resp == nil {
		return nil, ok, err
	}
	return resp.User, ok, err
}

func ToMiddleware(auth Authenticator) func(rw http.ResponseWriter, req *http.Request, next http.Handler) {
	return func(rw http.ResponseWriter, req *http.Request, next http.Handler) {
		info, ok, err := auth.Authenticate(req)
		if err != nil {
			rw.WriteHeader(http.StatusServiceUnavailable)
			rw.Write([]byte(err.Error()))
			return
		}

		if !ok {
			rw.WriteHeader(http.StatusUnauthorized)
			return
		}

		ctx := request.WithUser(req.Context(), info)
		req = req.WithContext(ctx)

		req.Header.Set(v1.ImpersonateUserHeader, info.GetName())
		for _, group := range info.GetGroups() {
			req.Header.Set(v1.ImpersonateGroupHeader, group)
		}

		next.ServeHTTP(rw, req)
	}
}
