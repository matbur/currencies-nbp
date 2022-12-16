package currencies

import (
	"context"
	"database/sql"
	"encore.app/currencies/store"
	"encore.dev/storage/sqldb"
	"errors"
	"log"
	"time"
)

const RFC3339Date = "2006-01-02"

var db = sqldb.Named("currencies").Stdlib()

// encore:service
type Service struct {
	repo *store.Queries
}

func initService() (*Service, error) {
	return &Service{
		repo: store.New(db),
	}, nil
}

type Params struct {
	Currency string
}

type Price struct {
	Date  string
	Price float64
}

type Response struct {
	Min    *Price `json:"min,omitempty"`
	Max    *Price `json:"max,omitempty"`
	Prices []Price
}

// encore:api private
func (s *Service) GetYear(ctx context.Context, p *Params) (*Response, error) {
	log.Println(p)
	return &Response{
		Prices: []Price{
			{
				Date:  time.Now().Format(RFC3339Date),
				Price: 3.4,
			},
		},
	}, nil
}

type GetMonthParams struct{}

// encore:api public
func (s *Service) GetMonth(ctx context.Context, p *GetMonthParams) (*Response, error) {
	_ = p

	rows, err := s.repo.GetPrices(ctx)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	firstDayOfMonth := time.Date(now.Year(), now.Month(), 1,
		0, 0, 0, 0, now.Location())

	prices := make([]Price, 0, len(rows))
	var max, min *Price
	for _, row := range rows {
		if row.Date.Before(firstDayOfMonth) {
			continue
		}

		p := Price{
			Date:  row.Date.Format(RFC3339Date),
			Price: row.Price,
		}
		if min == nil || p.Price < min.Price {
			min = &p
		}
		if max == nil || max.Price < p.Price {
			max = &p
		}
		prices = append(prices, p)
	}

	return &Response{
		Prices: prices,
		Min:    min,
		Max:    max,
	}, nil
}

// encore:api private
func (s *Service) saveYear(ctx context.Context) error {
	bb, err := getFile()
	if err != nil {
		return err
	}
	prices, err := parseFile(bb)
	if err != nil {
		return err
	}

	if err := s.savePrices(ctx, prices); err != nil {
		return err
	}

	return nil
}

func (s *Service) savePrices(ctx context.Context, prices []Price) error {
	return BeginTxFunc(ctx, db, func(tx *sql.Tx) error {
		for _, p := range prices {
			date, err := time.Parse(RFC3339Date, p.Date)
			if err != nil {
				return err
			}

			err = s.repo.WithTx(tx).SavePrice(ctx, store.SavePriceParams{
				Date:     date,
				Currency: "USD",
				Price:    p.Price,
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// https://github.com/jackc/pgx/blob/v4.17.2/tx.go#L108-L128
func BeginTxFunc(ctx context.Context, db *sql.DB, f func(*sql.Tx) error) (err error) {
	var tx *sql.Tx
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = rollbackErr
		}
	}()

	fErr := f(tx)
	if fErr != nil {
		_ = tx.Rollback() // ignore rollback error as there is already an error to return
		return fErr
	}

	return tx.Commit()
}
