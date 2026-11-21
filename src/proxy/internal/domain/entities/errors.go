package entities

type ForbiddenError struct{}

func (ForbiddenError) Error() string { return "forbidden" }
