package currencies

import "encore.dev/cron"

var _ = cron.NewJob("fetch-currencies", cron.JobConfig{
	Title:    "Fetch currencies",
	Endpoint: Service.SaveYear,
	Schedule: "5 12 * * 1-5",
})
