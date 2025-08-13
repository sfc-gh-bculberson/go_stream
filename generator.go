package main

import (
	"encoding/hex"
	"math/rand"
	"strings"
	"time"

	faker "github.com/go-faker/faker/v4"
	"github.com/google/uuid"
)

func generateLiftTicket(rng *rand.Rand) LiftTicket {
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
	resort := resorts[rng.Intn(len(resorts))]
	days := rng.Intn(7) + 1

	now := time.Now().UTC()
	purchaseTime := now.Truncate(time.Microsecond).Format("2006-01-02T15:04:05.000000")
	expiration := now.AddDate(0, 0, days).Format("2006-01-02")

	var addr *Address
	if rng.Float64() < 0.4 {
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
	if rng.Float64() < 0.7 {
		p := faker.Phonenumber()
		phone = &p
	}

	var email *string
	if rng.Float64() < 0.6 {
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
	if rng.Float64() < 0.7 {
		ep := personData{}
		_ = faker.FakeData(&ep)
		emergency = &EmergencyContact{Name: strings.TrimSpace(ep.First + " " + ep.Last), Phone: faker.Phonenumber()}
	}

	return LiftTicket{
		TxID:             uuid.NewString(),
		RFID:             "0x" + randomHex(rng, 12),
		Resort:           resort,
		PurchaseTime:     purchaseTime,
		ExpirationTime:   expiration,
		Days:             days,
		Name:             fullName,
		Address:          addr,
		Phone:            phone,
		Email:            email,
		EmergencyContact: emergency,
	}
}

func randomHex(rng *rand.Rand, nBytes int) string {
	b := make([]byte, nBytes)
	rng.Read(b)
	return hex.EncodeToString(b)
}
