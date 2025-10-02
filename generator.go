package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/bytedance/gopkg/lang/fastrand"
	faker "github.com/go-faker/faker/v4"
	"github.com/google/uuid"
)

type Address struct {
	StreetAddress string `json:"STREET_ADDRESS"`
	City          string `json:"CITY"`
	State         string `json:"STATE"`
	PostalCode    string `json:"POSTALCODE"`
}

type EmergencyContact struct {
	Name  string `json:"NAME"`
	Phone string `json:"PHONE"`
}

type LiftTicket struct {
	TxID             string            `json:"TXID"`
	RFID             string            `json:"RFID"`
	Resort           string            `json:"RESORT"`
	PurchaseTime     string            `json:"PURCHASE_TIME"`
	ExpirationTime   string            `json:"EXPIRATION_TIME"`
	Days             int               `json:"DAYS"`
	Name             string            `json:"NAME"`
	Address          *Address          `json:"ADDRESS"`
	Phone            *string           `json:"PHONE"`
	Email            *string           `json:"EMAIL"`
	EmergencyContact *EmergencyContact `json:"EMERGENCY_CONTACT"`
	// Extras are additional top-level columns merged during JSON marshaling
	Extras map[string]any `json:"-"`
}

// MarshalJSON flattens core fields with Extras so additional columns become top-level keys.
func (lt LiftTicket) MarshalJSON() ([]byte, error) {
	m := map[string]any{
		"TXID":              lt.TxID,
		"RFID":              lt.RFID,
		"RESORT":            lt.Resort,
		"PURCHASE_TIME":     lt.PurchaseTime,
		"EXPIRATION_TIME":   lt.ExpirationTime,
		"DAYS":              lt.Days,
		"NAME":              lt.Name,
		"ADDRESS":           lt.Address,
		"PHONE":             lt.Phone,
		"EMAIL":             lt.Email,
		"EMERGENCY_CONTACT": lt.EmergencyContact,
	}
	if lt.Extras != nil {
		for k, v := range lt.Extras {
			m[k] = v
		}
	}
	return json.Marshal(m)
}

func generateLiftTicket() LiftTicket {
	resorts := []string{"Vail", "Beaver Creek", "Breckenridge", "Keystone", "Crested Butte", "Park City", "Heavenly", "Northstar",
		"Kirkwood", "Whistler Blackcomb", "Perisher", "Falls Creek", "Hotham", "Stowe", "Mount Snow", "Okemo",
		"Hunter Mountain", "Mount Sunapee", "Attitash", "Wildcat", "Crotched", "Stevens Pass", "Liberty", "Roundtop",
		"Whitetail", "Jack Frost", "Big Boulder", "Alpine Valley", "Boston Mills", "Brandywine", "Mad River",
		"Hidden Valley", "Snow Creek", "Wilmot", "Afton Alps", "Mt. Brighton", "Paoli Peaks"}

	type personData struct {
		First string `faker:"first_name"`
		Last  string `faker:"last_name"`
	}
	pd := personData{}
	_ = faker.FakeData(&pd)
	fullName := strings.TrimSpace(pd.First + " " + pd.Last)
	resort := resorts[fastIntn(len(resorts))]
	days := fastIntn(7) + 1

	now := time.Now().UTC()
	purchaseTime := now.Truncate(time.Microsecond).Format("2006-01-02T15:04:05.000000")
	expiration := now.AddDate(0, 0, days).Format("2006-01-02")

	var addr *Address
	if fastFloat64() < 0.4 {
		type addrData struct {
			Street string `faker:"street_address"`
			City   string `faker:"city"`
			State  string `faker:"state_abbr"`
			Zip    string `faker:"zip"`
		}
		ad := addrData{}
		_ = faker.FakeData(&ad)
		addr = &Address{StreetAddress: ad.Street, City: ad.City, State: ad.State, PostalCode: ad.Zip}
	}

	var phone *string
	if fastFloat64() < 0.7 {
		p := faker.Phonenumber()
		phone = &p
	}

	var email *string
	if fastFloat64() < 0.6 {
		parts := strings.Split(fullName, " ")
		first := parts[0]
		last := ""
		if len(parts) > 1 {
			last = parts[1]
		}
		e := strings.ToLower(strings.ReplaceAll(first, ".", "")) + strings.ToLower(strings.ReplaceAll(last, ".", "")) + "@example.com"
		email = &e
	}

	var emergency *EmergencyContact
	if fastFloat64() < 0.7 {
		ep := personData{}
		_ = faker.FakeData(&ep)
		emergency = &EmergencyContact{Name: strings.TrimSpace(ep.First + " " + ep.Last), Phone: faker.Phonenumber()}
	}

	// Generate large sets of additional columns
	extras := generateExtraColumns()

	return LiftTicket{
		TxID:             uuid.NewString(),
		RFID:             "0x" + randomHexFast(12),
		Resort:           resort,
		PurchaseTime:     purchaseTime,
		ExpirationTime:   expiration,
		Days:             days,
		Name:             fullName,
		Address:          addr,
		Phone:            phone,
		Email:            email,
		EmergencyContact: emergency,
		Extras:           extras,
	}
}

func randomHexFast(nBytes int) string {
	b := make([]byte, nBytes)
	i := 0
	for i+4 <= nBytes {
		v := fastrand.Uint32()
		b[i+0] = byte(v)
		b[i+1] = byte(v >> 8)
		b[i+2] = byte(v >> 16)
		b[i+3] = byte(v >> 24)
		i += 4
	}
	if i < nBytes {
		v := fastrand.Uint32()
		for j := 0; i < nBytes; i, j = i+1, j+8 {
			b[i] = byte(v >> j)
		}
	}
	return hex.EncodeToString(b)
}

func fastIntn(n int) int {
	if n <= 0 {
		return 0
	}
	return int(fastrand.Uint32() % uint32(n))
}

func fastFloat64() float64 {
	return float64(fastrand.Uint32()) / 4294967295.0
}

// generateExtraColumns creates:
// - 10 variant columns: VARIANT_01..VARIANT_10
// - 500 varchar columns: VCHAR_0001..VCHAR_0500
// - 500 numeric columns: NUM_0001..NUM_0500
func generateExtraColumns() map[string]any {
	extras := make(map[string]any, 10+500+500)

	// 10 VARIANT columns with small nested JSON objects
	for i := 1; i <= 10; i++ {
		key := fmt.Sprintf("VARIANT_%02d", i)
		// Create a small mixed-type object
		variant := map[string]any{
			"J": randomHexFast(6),
			"K": randomHexFast(6),
			"L": randomHexFast(6),
			"M": randomHexFast(6),
			"N": int(fastrand.Uint32() % 1_000),
			"O": int(fastrand.Uint32() % 500),
			"P": int(fastrand.Uint32() % 200),
			"Q": int(fastrand.Uint32() % 100),
			"B": (fastrand.Uint32()%2 == 0),
			"C": (fastrand.Uint32()%2 == 0),
			"D": (fastrand.Uint32()%2 == 0),
			"E": (fastrand.Uint32()%2 == 0),
		}
		// Occasionally add a nested object
		if fastFloat64() < 0.3 {
			variant["NESTED"] = map[string]any{
				"A": randomHexFast(4),
				"B": randomHexFast(4),
				"Y": int(fastrand.Uint32() % 1_000),
				"Z": int(fastrand.Uint32() % 500),
			}
		}
		extras[key] = variant
	}

	// 500 VARCHAR columns with random-length hex strings
	for i := 1; i <= 500; i++ {
		key := fmt.Sprintf("VCHAR_%04d", i)
		// Length between 6 and 24 bytes -> 12 to 48 hex chars
		n := 6 + fastIntn(19)
		extras[key] = randomHexFast(n)
	}

	// 500 NUMERIC columns with random integers
	for i := 1; i <= 500; i++ {
		key := fmt.Sprintf("NUM_%04d", i)
		// Random up to ~100k
		extras[key] = int(fastrand.Uint32() % 100_000)
	}

	return extras
}
