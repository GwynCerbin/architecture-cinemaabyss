package adapters

type NoPathError struct {
}

func (e NoPathError) Error() string {
	return "path found not found"
}
