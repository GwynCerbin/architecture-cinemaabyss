package auth

import (
	"net/http"
	"time"

	"api-gateway/internal/controller/httpserver/handlers/v1/auth/adapters"
	"api-gateway/internal/domain/entities"
	authUC "api-gateway/internal/domain/usecases/auth"
	authInfra "api-gateway/internal/infra/auth"
	"api-gateway/pkg/ginbinder"

	"github.com/gin-gonic/gin"
)

type handlerSet struct {
	uc  authUC.UseCase
	cfg authInfra.Config
}

func NewHandlerRoute(router *gin.RouterGroup, cfg authInfra.Config, uc authUC.UseCase) {
	hs := &handlerSet{uc: uc, cfg: cfg}
	router.POST("/auth/login", hs.Login)
	router.POST("/auth/refresh", hs.Refresh)
	router.POST("/auth/logout", hs.Logout)
	router.GET("/session", hs.session)
}
func (h *handlerSet) authenticate(c *gin.Context) bool {
	cookie, err := c.Cookie(h.cfg.AccessCookie)
	if err == nil {
		var user *entities.UserInfo

		user, err = h.uc.Authenticate(cookie)
		if err == nil {
			c.Set(ginbinder.UserInfo, user)
			return true
		}
	}

	c.AbortWithStatusJSON(http.StatusUnauthorized, ginbinder.Response{Message: "unauthorized"})

	return false
}
func (h *handlerSet) session(c *gin.Context) {
	if !h.authenticate(c) {
		return
	}

	user := c.MustGet(ginbinder.UserInfo).(*entities.UserInfo)
	c.JSON(http.StatusOK, ginbinder.Response{Success: true, Message: "success", Result: adapters.SwitchEntity(user)})
}
func (h *handlerSet) cookie(c *gin.Context, name, value string, expires time.Time, remove bool) {
	cookie := &http.Cookie{Name: name, Value: value, Path: "/", HttpOnly: true, Secure: h.cfg.SecureCookies, SameSite: http.SameSiteStrictMode, Expires: expires}
	if remove {
		cookie.MaxAge = -1
	}

	http.SetCookie(c.Writer, cookie)
}
func (h *handlerSet) Login(c *gin.Context) {
	c.Header("Cache-Control", "no-store")

	var request adapters.LoginRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, ginbinder.Response{Message: "invalid login request"})
		return
	}

	session, err := h.uc.Login(c.Request.Context(), request.ToEntity())
	if err != nil {
		h.fail(c, err)
		return
	}

	h.setTokens(c, session, true)
	c.JSON(http.StatusOK, adapters.FromTokens(session.Tokens))
}

func (h *handlerSet) Refresh(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	token, _ := c.Cookie(h.cfg.RefreshCookie)
	session, err := h.uc.Refresh(c.Request.Context(), entities.RefreshToken(token))

	if err != nil {
		h.fail(c, err)
		return
	}

	h.setTokens(c, session, false)
	c.JSON(http.StatusOK, adapters.FromTokens(session.Tokens))
}

func (h *handlerSet) Logout(c *gin.Context) {
	c.Header("Cache-Control", "no-store")

	token, _ := c.Cookie(h.cfg.RefreshCookie)
	if err := h.uc.Logout(c.Request.Context(), entities.RefreshToken(token)); err != nil {
		h.fail(c, err)
		return
	}

	h.cookie(c, h.cfg.AccessCookie, "", time.Unix(0, 0), true)
	h.cookie(c, h.cfg.RefreshCookie, "", time.Unix(0, 0), true)
	c.Status(http.StatusNoContent)
}

func (h *handlerSet) setTokens(c *gin.Context, session *entities.Session, clearMissingRefresh bool) {
	h.cookie(c, h.cfg.AccessCookie, session.Tokens.AccessToken, time.UnixMilli(session.User.ExpiresAt()), false)

	if session.Tokens.RefreshToken != "" {
		h.cookie(c, h.cfg.RefreshCookie, session.Tokens.RefreshToken, time.Now().Add(h.cfg.RefreshTTL), false)
	} else if clearMissingRefresh {
		h.cookie(c, h.cfg.RefreshCookie, "", time.Unix(0, 0), true)
	}
}

func (h *handlerSet) fail(c *gin.Context, err error) {
	status, response := adapters.ErrorResponse(err)
	c.AbortWithStatusJSON(status, response)
}
