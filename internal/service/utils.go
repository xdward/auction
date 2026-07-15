package service

import "errors"

var (
	TypeCastingErr = errors.New("failed type cast")
)

func ToBytes(v any) ([]byte, error) {
	switch v := v.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		return nil, TypeCastingErr
	}
}
