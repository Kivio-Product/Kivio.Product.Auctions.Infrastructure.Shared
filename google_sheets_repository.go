package sharedinfrastructure

import (
	"context"
	"fmt"
	"os"

	domain "github.com/Kivio-Product/Kivio.Product.Auctions.Domain.Shared/sheets"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/sheets/v4"
)

type GoogleSheetsRepository interface {
	ConnectToSheet(spreadsheetID string, readRange string) error
	GetSheetData(spreadsheetID string, readRange string) (domain.SheetData, error)
}

type googleSheetsRepository struct {
	spreadsheetID string
	readRange     string
	service       *sheets.Service
}

func NewGoogleSheetsRepository() GoogleSheetsRepository {
	return &googleSheetsRepository{}
}

func (r *googleSheetsRepository) ConnectToSheet(spreadsheetID string, readRange string) error {
	ctx := context.Background()

	credentialsJSON, err := os.ReadFile("credentials.json")
	if err != nil {
		return fmt.Errorf("unable to read client secret file: %v", err)
	}

	config, err := google.JWTConfigFromJSON(credentialsJSON, sheets.SpreadsheetsReadonlyScope)
	if err != nil {
		return fmt.Errorf("unable to parse client secret file to config: %v", err)
	}

	client := config.Client(ctx)
	srv, err := sheets.New(client)
	if err != nil {
		return fmt.Errorf("unable to create Sheets client: %v", err)
	}

	r.service = srv
	r.spreadsheetID = spreadsheetID
	r.readRange = readRange

	fmt.Println("Successfully connected to the Google Sheet.")
	return nil
}

func (r *googleSheetsRepository) GetSheetData(spreadsheetID string, readRange string) (domain.SheetData, error) {

	ctx := context.Background()

	credentialsJSON, err := os.ReadFile("credentials.json")
	if err != nil {
		return domain.SheetData{}, fmt.Errorf("unable to read client secret file: %v", err)
	}

	config, err := google.JWTConfigFromJSON(credentialsJSON, sheets.SpreadsheetsReadonlyScope)
	if err != nil {
		return domain.SheetData{}, fmt.Errorf("unable to parse client secret file to config: %v", err)
	}

	client := config.Client(ctx)
	srv, err := sheets.New(client)
	if err != nil {
		return domain.SheetData{}, fmt.Errorf("unable to create Sheets client: %v", err)
	}

	r.service = srv

	resp, err := r.service.Spreadsheets.Values.Get(spreadsheetID, readRange).Context(ctx).Do()
	if err != nil {
		return domain.SheetData{}, fmt.Errorf("unable to retrieve data from sheet: %v", err)
	}

	fmt.Printf("Successfully read data from sheet: %v\n", resp.Values)

	return domain.SheetData{Values: resp.Values}, nil
}
