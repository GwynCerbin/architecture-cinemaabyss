package entities

import "slices"

type UserInfo struct {
	id        string
	basePower []string
	expiresAt int64
}

func (u *UserInfo) ID() string          { return u.id }
func (u *UserInfo) BasePower() []string { return u.basePower }

// ExpiresAt returns the session expiration as Unix milliseconds.
func (u *UserInfo) ExpiresAt() int64 { return u.expiresAt }

type UserInfoBuilder struct {
	ID        string
	BasePower []string
	ExpiresAt int64
}

func (u UserInfoBuilder) Build() *UserInfo {
	return &UserInfo{id: u.ID, basePower: u.BasePower, expiresAt: u.ExpiresAt}
}

func (u *UserInfo) HasPower(power string) bool {
	return u != nil && slices.Contains(u.basePower, power)
}

type Credentials struct {
	Login    string
	Password string
}

type RefreshToken string

// Tokens contains the access and refresh tokens issued by the authentication service.
type Tokens struct {
	AccessToken  string
	RefreshToken string
}

// Session combines issued tokens with the verified identity they represent.
type Session struct {
	Tokens Tokens
	User   *UserInfo
}
