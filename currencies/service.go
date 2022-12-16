package currencies

import (
	"context"
	"database/sql"
	"encore.app/currencies/store"
	"encore.dev/beta/errs"
	"encore.dev/storage/sqldb"
	"errors"
	"time"
)

const RFC3339Date = "2006-01-02"

var db = sqldb.Named("currencies").Stdlib()

var (
	ErrCurrencyNotSupported = errs.B().Code(errs.InvalidArgument).Msg("CURRENCY_NOT_SUPPORTED").Err()
)

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
	Date     string
	Currency string
	Price    float64
}

type PriceResponse struct {
	Date  string
	Price float64
}

type Response struct {
	Currency string
	Min      *PriceResponse `json:"min,omitempty"`
	Max      *PriceResponse `json:"max,omitempty"`
	Prices   []PriceResponse
}

// encore:api public method=GET path=/currencies/year
func (s *Service) GetYear(ctx context.Context, p *Params) (*Response, error) {
	if p.Currency != "" && p.Currency != "USD" {
		return nil, ErrCurrencyNotSupported
	}

	now := time.Now()
	firstDayOfYear := time.Date(now.Year(), 1, 1,
		0, 0, 0, 0, now.Location())

	prices, err := s.getPrices(ctx, &firstDayOfYear, nil)
	if err != nil {
		return nil, err
	}

	return pricesToResponse(prices), err
}

// encore:api public method=GET path=/currencies/month
func (s *Service) GetMonth(ctx context.Context, p *Params) (*Response, error) {
	if p.Currency != "" && p.Currency != "USD" {
		return nil, ErrCurrencyNotSupported
	}

	now := time.Now()
	firstDayOfMonth := time.Date(now.Year(), now.Month(), 1,
		0, 0, 0, 0, now.Location())

	prices, err := s.getPrices(ctx, &firstDayOfMonth, nil)
	if err != nil {
		return nil, err
	}

	return pricesToResponse(prices), err
}

func pricesToResponse(pp []Price) *Response {
	currency := ""

	priceResponses := make([]PriceResponse, 0, len(pp))
	var max, min *PriceResponse
	for _, p := range pp {
		if currency == "" {
			currency = p.Currency
		}

		pr := PriceResponse{
			Date:  p.Date,
			Price: p.Price,
		}
		if min == nil || p.Price < min.Price {
			min = &pr
		}
		if max == nil || max.Price < p.Price {
			max = &pr
		}
		priceResponses = append(priceResponses, pr)
	}

	return &Response{
		Currency: currency,
		Min:      min,
		Max:      max,
		Prices:   priceResponses,
	}
}

func (s *Service) getPrices(ctx context.Context, startDate, endDate *time.Time) ([]Price, error) {
	rows, err := s.repo.GetPrices(ctx)
	if err != nil {
		return nil, err
	}

	prices := make([]Price, 0, len(rows))
	for _, row := range rows {
		if startDate != nil && startDate.After(row.Date) {
			continue
		}
		if endDate != nil && endDate.Before(row.Date) {
			continue
		}

		prices = append(prices, Price{
			Date:     row.Date.Format(RFC3339Date),
			Currency: row.Currency,
			Price:    row.Price,
		})
	}
	return prices, err
}

// encore:api private method=POST path=/currencies/year
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
