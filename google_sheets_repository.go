package sharedinfrastructure

import (
	"context"
	"fmt"
	"os"

	"github.com/Kivio-Product/Kivio.Product.Auctions.Rules/internal/domain"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/sheets/v4"
)

type GoogleSheetsRepository interface {
	GetSheetData() (domain.SheetData, error)
}

type googleSheetsRepository struct {
	spreadsheetID string
	readRange     string
}

func NewGoogleSheetsRepository() GoogleSheetsRepository {
	return &googleSheetsRepository{
		spreadsheetID: "1W--Lxh6vk9Iw0OcdTKMl__KhJdkBn0Sbim38vyQ7co8",
		readRange:     "Sheet1!A1:V4",
	}
}

func (r *googleSheetsRepository) GetSheetData() (domain.SheetData, error) {
	ctx := context.Background()

	credentialsJSON, err := os.ReadFile("credentials.json")
	if err != nil {
		return domain.SheetData{}, err
	}

	config, err := google.JWTConfigFromJSON(credentialsJSON, sheets.SpreadsheetsReadonlyScope)
	if err != nil {
		return domain.SheetData{}, err
	}

	client := config.Client(ctx)
	srv, err := sheets.New(client)
	if err != nil {
		return domain.SheetData{}, err
	}

	resp, err := srv.Spreadsheets.Values.Get(r.spreadsheetID, r.readRange).Context(ctx).Do()

	fmt.Printf("Response: %v\n", resp)

	if err != nil {
		return domain.SheetData{}, err
	}

	return domain.SheetData{Values: resp.Values}, nil
}
