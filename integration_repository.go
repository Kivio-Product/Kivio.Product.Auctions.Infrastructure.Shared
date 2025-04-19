package sharedinfrastructure

import (
	"fmt"
	"time"

	domain "github.com/Kivio-Product/Kivio.Product.Auctions.Users/internal/domain/integrations"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type IntegrationRepository interface {
	SaveIntegration(integration *domain.Integration) error
	GetIntegrationByID(id string) (*domain.Integration, error)
	GetIntegrationsByPosID(posID string) ([]*domain.Integration, error)
	UpdateIntegration(integration *domain.Integration) error
	DeleteIntegration(id string) error
	SaveIntegrationConfig(config *domain.IntegrationConfig) error
	GetIntegrationConfigs(integrationID string) ([]domain.IntegrationConfig, error)
	DeleteIntegrationConfigs(integrationID string) error
}

type dynamoDBIntegrationRepository struct {
	dynamoClient      *dynamodb.DynamoDB
	integrationsTable string
	configsTable      string
}

func NewDynamoDBIntegrationRepository() (IntegrationRepository, error) {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-2"),
	})
	if err != nil {
		return nil, fmt.Errorf("error creating AWS session: %w", err)
	}

	dynamoClient := dynamodb.New(sess)

	return &dynamoDBIntegrationRepository{
		dynamoClient:      dynamoClient,
		integrationsTable: "Integrations",
		configsTable:      "IntegrationConfig",
	}, nil
}

func (r *dynamoDBIntegrationRepository) SaveIntegration(integration *domain.Integration) error {
	if integration.IntegrationID == "" {
		return fmt.Errorf("integration ID cannot be empty")
	}

	if integration.CreatedAt.IsZero() {
		integration.CreatedAt = time.Now()
	}
	if integration.LastSync.IsZero() {
		integration.LastSync = time.Now()
	}

	item, err := dynamodbattribute.MarshalMap(integration)
	if err != nil {
		return fmt.Errorf("error marshalling integration: %w", err)
	}

	input := &dynamodb.PutItemInput{
		Item:      item,
		TableName: aws.String(r.integrationsTable),
	}

	_, err = r.dynamoClient.PutItem(input)
	if err != nil {
		return fmt.Errorf("error saving integration to DynamoDB: %w", err)
	}

	return nil
}

func (r *dynamoDBIntegrationRepository) GetIntegrationByID(id string) (*domain.Integration, error) {
	input := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"integrationId": {
				S: aws.String(id),
			},
		},
		TableName: aws.String(r.integrationsTable),
	}

	result, err := r.dynamoClient.GetItem(input)
	if err != nil {
		return nil, fmt.Errorf("error getting integration from DynamoDB: %w", err)
	}

	if result.Item == nil {
		return nil, fmt.Errorf("integration with ID %s not found", id)
	}

	var integration domain.Integration
	err = dynamodbattribute.UnmarshalMap(result.Item, &integration)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling integration: %w", err)
	}

	configs, err := r.GetIntegrationConfigs(id)
	if err != nil {
		return nil, fmt.Errorf("error getting integration configs: %w", err)
	}

	integration.Configs = configs
	return &integration, nil
}

func (r *dynamoDBIntegrationRepository) GetIntegrationsByPosID(posID string) ([]*domain.Integration, error) {
	input := &dynamodb.ScanInput{
		TableName:        aws.String(r.integrationsTable),
		FilterExpression: aws.String("posId = :posId"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":posId": {
				S: aws.String(posID),
			},
		},
	}

	result, err := r.dynamoClient.Scan(input)
	if err != nil {
		return nil, fmt.Errorf("error scanning integrations from DynamoDB: %w", err)
	}

	var integrations []*domain.Integration
	err = dynamodbattribute.UnmarshalListOfMaps(result.Items, &integrations)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling integrations: %w", err)
	}

	for _, integration := range integrations {
		configs, err := r.GetIntegrationConfigs(integration.IntegrationID)
		if err != nil {
			return nil, fmt.Errorf("error getting configs for integration %s: %w", integration.IntegrationID, err)
		}
		integration.Configs = configs
	}

	return integrations, nil
}

func (r *dynamoDBIntegrationRepository) UpdateIntegration(integration *domain.Integration) error {
	_, err := r.GetIntegrationByID(integration.IntegrationID)
	if err != nil {
		return fmt.Errorf("integration to update not found: %w", err)
	}

	integration.LastSync = time.Now()

	return r.SaveIntegration(integration)
}

func (r *dynamoDBIntegrationRepository) DeleteIntegration(id string) error {
	err := r.DeleteIntegrationConfigs(id)
	if err != nil {
		return fmt.Errorf("error deleting integration configs: %w", err)
	}

	input := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"integrationId": {
				S: aws.String(id),
			},
		},
		TableName: aws.String(r.integrationsTable),
	}

	_, err = r.dynamoClient.DeleteItem(input)
	if err != nil {
		return fmt.Errorf("error deleting integration from DynamoDB: %w", err)
	}

	return nil
}

func (r *dynamoDBIntegrationRepository) SaveIntegrationConfig(config *domain.IntegrationConfig) error {
	if config.IntegrationConfigID == "" {
		return fmt.Errorf("integration config ID cannot be empty")
	}

	if config.IntegrationID == "" {
		return fmt.Errorf("integration ID in config cannot be empty")
	}

	_, err := r.GetIntegrationByID(config.IntegrationID)
	if err != nil {
		return fmt.Errorf("integration with ID %s not found: %w", config.IntegrationID, err)
	}

	item, err := dynamodbattribute.MarshalMap(config)
	if err != nil {
		return fmt.Errorf("error marshalling config: %w", err)
	}

	input := &dynamodb.PutItemInput{
		Item:      item,
		TableName: aws.String(r.configsTable),
	}

	_, err = r.dynamoClient.PutItem(input)
	if err != nil {
		return fmt.Errorf("error saving config to DynamoDB: %w", err)
	}

	return nil
}

func (r *dynamoDBIntegrationRepository) GetIntegrationConfigs(integrationID string) ([]domain.IntegrationConfig, error) {
	input := &dynamodb.QueryInput{
		TableName:              aws.String(r.configsTable),
		IndexName:              aws.String("integrationId-index"),
		KeyConditionExpression: aws.String("integrationId = :integrationId"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":integrationId": {
				S: aws.String(integrationID),
			},
		},
	}

	result, err := r.dynamoClient.Query(input)
	if err != nil {
		return nil, fmt.Errorf("error querying configs from DynamoDB: %w", err)
	}

	var configs []domain.IntegrationConfig
	err = dynamodbattribute.UnmarshalListOfMaps(result.Items, &configs)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling configs: %w", err)
	}

	return configs, nil
}

func (r *dynamoDBIntegrationRepository) DeleteIntegrationConfigs(integrationID string) error {
	configs, err := r.GetIntegrationConfigs(integrationID)
	if err != nil {
		return fmt.Errorf("error getting configs to delete: %w", err)
	}

	for _, config := range configs {
		input := &dynamodb.DeleteItemInput{
			Key: map[string]*dynamodb.AttributeValue{
				"integrationConfigId": {
					S: aws.String(config.IntegrationConfigID),
				},
			},
			TableName: aws.String(r.configsTable),
		}

		_, err = r.dynamoClient.DeleteItem(input)
		if err != nil {
			return fmt.Errorf("error deleting config %s from DynamoDB: %w", config.IntegrationConfigID, err)
		}
	}

	return nil
}
