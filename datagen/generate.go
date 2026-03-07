package datagen

import (
	"math"
	"math/rand"

	"github.com/staf/simple-olap-db/storage"
)

var products = []string{
	"Widget", "Gadget", "Doohickey", "Thingamajig", "Whatchamacallit",
	"Gizmo", "Contraption", "Apparatus", "Doodad", "Sprocket",
}

var categories = []string{
	"Tools", "Electronics", "Home", "Garden", "Automotive",
}

var regions = []string{
	"North", "South", "East", "West", "Central",
}

// GenerateSalesRows creates n random sales rows.
// Use this to generate test data for benchmarking.
func GenerateSalesRows(n int, seed int64) []storage.Row {
	rng := rand.New(rand.NewSource(seed))
	rows := make([]storage.Row, n)

	for i := 0; i < n; i++ {
		price := math.Round(rng.Float64()*100*100) / 100 // 0.00 - 100.00
		rows[i] = storage.Row{
			"id":       int64(i + 1),
			"product":  products[rng.Intn(len(products))],
			"category": categories[rng.Intn(len(categories))],
			"price":    price,
			"quantity": int64(rng.Intn(200) + 1),
			"region":   regions[rng.Intn(len(regions))],
		}
	}

	return rows
}
