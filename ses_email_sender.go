package sharedinfrastructure

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	offer "github.com/Kivio-Product/Kivio.Product.Auctions.Domain.Shared"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/ses"
)

type IEmailSender interface {
	SendEmail(ctx context.Context, offer *offer.Offer, auctionURL string) error
}

type SESEmailSender struct {
	sesClient   *ses.SES
	s3Client    *s3.S3
	sender      string
	s3Bucket    string
	s3Key       string
	templateKey string
}

func NewSESEmailSender() (IEmailSender, error) {
	fmt.Println("SES Email Sender inicializado correctamente")
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-2"),
	})

	if err != nil {
		return nil, fmt.Errorf("error creando sesión de AWS: %v", err)
	}

	sesClient := ses.New(sess)
	s3Client := s3.New(sess)

	if sesClient == nil || s3Client == nil {
		return nil, fmt.Errorf("no se pudo crear el cliente SES o S3")
	}

	fmt.Println("SES Email Sender inicializado correctamente")

	return &SESEmailSender{
		sesClient:   sesClient,
		s3Client:    s3Client,
		sender:      os.Getenv("SES_SENDER_EMAIL"),
		s3Bucket:    os.Getenv("S3_BUCKET_NAME"),
		s3Key:       os.Getenv("S3_EMAILS_FILE"),
		templateKey: os.Getenv("S3_TEMPLATE_FILE"),
	}, nil
}

func (s *SESEmailSender) getCustomerEmails(ctx context.Context) ([]string, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.s3Bucket),
		Key:    aws.String(s.s3Key),
	}

	result, err := s.s3Client.GetObjectWithContext(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("error al obtener el archivo de S3: %w", err)
	}
	defer result.Body.Close()

	scanner := bufio.NewScanner(result.Body)
	var emails []string
	emailRegex := regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`)

	for scanner.Scan() {
		line := scanner.Text()

		possibleEmails := strings.FieldsFunc(line, func(r rune) bool {
			return r == ';' || r == ',' || r == ' ' || r == '\t'
		})

		for _, email := range possibleEmails {
			cleaned := strings.TrimSpace(email)
			if emailRegex.MatchString(cleaned) {
				emails = append(emails, cleaned)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error al leer el archivo de S3: %w", err)
	}

	return emails, nil
}

func (s *SESEmailSender) getTemplate(ctx context.Context) (string, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.s3Bucket),
		Key:    aws.String(s.templateKey),
	}

	result, err := s.s3Client.GetObjectWithContext(ctx, input)
	if err != nil {
		return "", fmt.Errorf("error al obtener la plantilla de S3: %w", err)
	}
	defer result.Body.Close()

	body, err := io.ReadAll(result.Body)
	if err != nil {
		return "", fmt.Errorf("error al leer la plantilla de S3: %w", err)
	}

	return string(body), nil
}

func (s *SESEmailSender) SendEmail(ctx context.Context, offer *offer.Offer, auctionURL string) error {
	if s.sesClient == nil {
		return fmt.Errorf("SES client is not initialized")
	}

	emails, err := s.getCustomerEmails(ctx)
	if err != nil {
		return err
	}

	cleanedEmails := make([]string, 0, len(emails))
	for _, email := range emails {
		cleaned := strings.TrimSpace(email)
		if cleaned != "" {
			cleanedEmails = append(cleanedEmails, cleaned)
		}
	}

	fmt.Println("Correos obtenidos correctamente", emails)
	fmt.Printf("%q", emails)
	fmt.Printf("%q", cleanedEmails)

	if len(cleanedEmails) == 0 {
		return fmt.Errorf("no hay correos en la lista")
	}

	template, err := s.getTemplate(ctx)
	if err != nil {
		return err
	}

	for _, email := range cleanedEmails {
		personalizedURL := auctionURL + "&email=" + email

		body := strings.ReplaceAll(template, "{{OFFER_NAME}}", offer.Name)
		body = strings.ReplaceAll(body, "{{OFFER_DESCRIPTION}}", offer.Description)
		body = strings.ReplaceAll(body, "{{DISCOUNT}}", "50%")
		body = strings.ReplaceAll(body, "{{EXPIRATION_DATE}}", offer.ExpireAt.Format("02/01/2006"))
		body = strings.ReplaceAll(body, "{{AUCTION_URL}}", personalizedURL)

		subject := fmt.Sprintf("🔥 Oferta Especial: %s", offer.Name)

		input := &ses.SendEmailInput{
			Source: aws.String(s.sender),
			Destination: &ses.Destination{
				ToAddresses: aws.StringSlice([]string{email}),
			},
			Message: &ses.Message{
				Subject: &ses.Content{Data: aws.String(subject)},
				Body:    &ses.Body{Html: &ses.Content{Data: aws.String(body)}},
			},
		}

		_, err := s.sesClient.SendEmailWithContext(ctx, input)
		if err != nil {
			fmt.Printf("Error al enviar correo a %s: %v\n", email, err)
		} else {
			fmt.Printf("Correo enviado correctamente a %s\n", email)
		}
	}

	return nil
}
