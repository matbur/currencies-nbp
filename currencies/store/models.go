// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.16.0

package store

import (
	"time"
)

type Price struct {
	CreatedAt time.Time
	Date      time.Time
	Currency  string
	Price     float64
}
