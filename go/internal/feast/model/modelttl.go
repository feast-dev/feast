package model

import "time"

type ModelTTL[T any] struct {
	Model T
	ttl   time.Time
}

func (m *ModelTTL[T]) IsExpired() bool {
	if m.ttl.IsZero() {
		return false
	}
	return time.Now().After(m.ttl)
}

func (m *ModelTTL[T]) Copy() *ModelTTL[T] {
	return &ModelTTL[T]{Model: m.Model, ttl: m.ttl}
}

func NewModelTTLWithExpiration[T any](model T, ttl time.Duration) *ModelTTL[T] {
	return &ModelTTL[T]{Model: model, ttl: time.Now().Add(ttl)}
}
