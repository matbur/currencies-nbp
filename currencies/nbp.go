package currencies

import (
	"bytes"
	"encoding/csv"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func getFile() ([]byte, error) {
	url := "https://www.nbp.pl/kursy/Archiwum/archiwum_tab_a_2022.csv"
	//url = "http://localhost:8000/"
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}

func parseFile(bb []byte) ([]Price, error) {
	r := csv.NewReader(bytes.NewReader(bb))
	r.Comma = ';'
	r.FieldsPerRecord = -1

	ss, err := r.ReadAll()
	if err != nil {
		return nil, err
	}

	rows := make([]Price, 0, len(ss))
	for _, s := range ss {
		switch s[0] {
		case "", "data", "kod ISO", "nazwa waluty", "liczba jednostek":
			continue
		}

		date, err := time.Parse("20060102", s[0])
		if err != nil {
			return nil, err
		}

		usd, err := strconv.ParseFloat(
			strings.ReplaceAll(s[2], ",", "."),
			64)
		if err != nil {
			return nil, err
		}

		rows = append(rows, Price{
			Date:     date.Format(RFC3339Date),
			Currency: "USD",
			Price:    usd,
		})

		eur, err := strconv.ParseFloat(
			strings.ReplaceAll(s[8], ",", "."),
			64)
		if err != nil {
			return nil, err
		}

		rows = append(rows, Price{
			Date:     date.Format(RFC3339Date),
			Currency: "EUR",
			Price:    eur,
		})
	}
	return rows, nil
}
