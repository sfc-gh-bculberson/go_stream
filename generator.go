package main

import (
	"encoding/hex"
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
