package currencies

import (
	"context"
	"database/sql"
	"encore.app/currencies/store"
	"encore.dev/beta/errs"
	"encore.dev/storage/sqldb"
	"errors"
	"fmt"
	"github.com/pjaskulski/nbpapi"
	"strings"
	"time"
)

const (
	RFC3339Date  = "2006-01-02"
	RFC3339Month = "2006-01"
	RFC3339Year  = "2006"
)

var db = sqldb.Named("currencies").Stdlib()
var supportedCurrencies = map[string]struct{}{
	"USD": {},
	"EUR": {},
}

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
	// Currently supported currencies are: USD and EUR
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
	Currency string          `json:"currency"`
	Min      *PriceResponse  `json:"min,omitempty"`
	Max      *PriceResponse  `json:"max,omitempty"`
	Prices   []PriceResponse `json:"prices"`
}

type DateRange struct {
	From time.Time
	To   time.Time
}

func (r DateRange) String() string {
	return fmt.Sprintf("%s:%s", r.From.Format(RFC3339Date), r.To.Format(RFC3339Date))
}

// encore:api public method=GET path=/currencies/year
func (s Service) GetYear(ctx context.Context, p *Params) (*Response, error) {
	currency, err := parseCurrencyParam(p.Currency)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	firstDayOfYear := time.Date(now.Year(), 1, 1,
		0, 0, 0, 0, now.Location())

	prices, err := s.getPrices(ctx, currency, &firstDayOfYear, nil)
	if err != nil {
		return nil, err
	}

	return pricesToResponse(prices), err
}

// encore:api public method=GET path=/currencies/month
func (s Service) GetMonth(ctx context.Context, p *Params) (*Response, error) {
	currency, err := parseCurrencyParam(p.Currency)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	firstDayOfMonth := time.Date(now.Year(), now.Month(), 1,
		0, 0, 0, 0, now.Location())

	prices, err := s.getPrices(ctx, currency, &firstDayOfMonth, nil)
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

func (s Service) getPrices(ctx context.Context, currency string, startDate, endDate *time.Time) ([]Price, error) {
	rows, err := s.repo.GetPrices(ctx, currency)
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

// encore:api private method=POST path=/currencies/current
func (s Service) SaveCurrent(ctx context.Context) (*SaveDateResponse, error) {
	client := nbpapi.NewTable("A")
	tt, err := client.GetTableCurrent()
	if err != nil {
		return nil, fmt.Errorf("failed to get current table: %w", err)
	}

	if len(tt) == 0 {
		return nil, errors.New("no data returned")
	}

	prices := parseTable(tt[0])
	if err := s.savePrices(ctx, prices); err != nil {
		return nil, fmt.Errorf("failed to save prices: %w", err)
	}

	return &SaveDateResponse{
		Prices: prices,
	}, nil
}

type SaveMonthParams struct {
	Month string
}

// encore:api private method=POST path=/currencies/month
func (s Service) SaveMonth(ctx context.Context, params *SaveMonthParams) (*SaveDateResponse, error) {
	yearMonth := params.Month
	if yearMonth == "" {
		yearMonth = time.Now().Format("2006-01")
	}

	now := time.Now()
	dateRange, err := mkDateRange(yearMonth, &now)
	if err != nil {
		return nil, err
	}

	client := nbpapi.NewTable("A")
	tt, err := client.GetTableByDate(dateRange.String())
	if err != nil {
		return nil, fmt.Errorf("failed to get tables from %q: %w", dateRange, err)
	}

	var prices []Price
	for _, t := range tt {
		prices = append(prices, parseTable(t)...)
	}

	if err := s.savePrices(ctx, prices); err != nil {
		return nil, fmt.Errorf("failed to save prices: %w", err)
	}

	return &SaveDateResponse{
		Prices: prices,
	}, nil
}

func mkDateRange(yearMonth string, maxDate *time.Time) (DateRange, error) {
	rangeBegin, err := time.Parse(RFC3339Month, yearMonth)
	if err != nil {
		return DateRange{}, fmt.Errorf("failed to parse %q: %w", yearMonth, err)
	}

	rangeEnd := rangeBegin.AddDate(0, 1, -1)
	if maxDate != nil && rangeEnd.After(*maxDate) {
		rangeEnd = *maxDate
	}

	return DateRange{
		From: rangeBegin,
		To:   rangeEnd,
	}, nil
}

type SaveDateParams struct {
	Date string
}

type SaveDateResponse struct {
	Prices []Price
}

// encore:api private method=POST path=/currencies/date
func (s Service) SaveDate(ctx context.Context, params *SaveDateParams) (*SaveDateResponse, error) {
	client := nbpapi.NewTable("A")
	tt, err := client.GetTableByDate(params.Date)
	if err != nil {
		return nil, err
	}

	if len(tt) == 0 {
		return nil, errors.New("no data returned")
	}

	prices := parseTable(tt[0])
	if err := s.savePrices(ctx, prices); err != nil {
		return nil, err
	}

	return &SaveDateResponse{
		Prices: prices,
	}, nil
}

func parseTable(t nbpapi.ExchangeTable) []Price {
	prices := make([]Price, 0, len(t.Rates))
	for _, r := range t.Rates {
		if _, ok := supportedCurrencies[r.Code]; !ok {
			continue
		}

		prices = append(prices, Price{
			Date:     t.EffectiveDate,
			Currency: strings.ToUpper(r.Code),
			Price:    r.Mid,
		})
	}
	return prices
}

func (s Service) savePrices(ctx context.Context, prices []Price) error {
	return BeginTxFunc(ctx, db, func(tx *sql.Tx) error {
		for _, p := range prices {
			date, err := time.Parse(RFC3339Date, p.Date)
			if err != nil {
				return err
			}

			err = s.repo.WithTx(tx).SavePrice(ctx, store.SavePriceParams{
				Date:     date,
				Currency: p.Currency,
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

func parseCurrencyParam(c string) (string, error) {
	switch strings.ToLower(c) {
	case "", "usd":
		return "USD", nil
	case "eur":
		return "EUR", nil
	}
	return "", ErrCurrencyNotSupported
}
