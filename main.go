package main

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/patrickmn/go-cache"
)

// Inisialisasi cache (TTL 8 jam, Cleanup setiap 10 menit)
var requestCache = cache.New(8*time.Hour, 10*time.Minute)

// Mutex untuk menghindari race condition
var mu sync.Mutex

// Struct untuk menyimpan request yang gagal
type FailedRequest struct {
	FailedURLs  []string          `json:"failed_urls"`
	Method      string            `json:"method"`
	Headers     map[string]string `json:"headers"`
	Body        []byte            `json:"body"`
	RetryCount  int               `json:"retry_count"`
	CreatedAt   time.Time         `json:"created_at"`
	MaxDuration time.Duration     `json:"max_duration"`
}

func main() {
	app := fiber.New()

	// Endpoint menerima request yang gagal
	app.Post("/retry", func(c *fiber.Ctx) error {
		var req FailedRequest

		// Parsing body JSON
		if err := c.BodyParser(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid request format"})
		}

		// Set waktu pertama kali request gagal
		req.CreatedAt = time.Now()
		req.MaxDuration = 8 * time.Hour

		// Simpan setiap URL dalam cache secara terpisah
		mu.Lock()
		for _, url := range req.FailedURLs {
			cacheKey := fmt.Sprintf("%s-%d", url, time.Now().Unix())
			cachedReq := FailedRequest{
				FailedURLs:  []string{url}, // Simpan satu URL per entry
				Method:      req.Method,
				Headers:     req.Headers,
				Body:        req.Body,
				RetryCount:  0,
				CreatedAt:   req.CreatedAt,
				MaxDuration: req.MaxDuration,
			}
			requestCache.Set(cacheKey, cachedReq, 8*time.Hour)
		}
		mu.Unlock()

		return c.JSON(fiber.Map{"message": "Requests stored for retry"})
	})

	app.Get("/retry/list", func(c *fiber.Ctx) error {
		mu.Lock()
		defer mu.Unlock()

		var retryList []FailedRequest

		for _, item := range requestCache.Items() {
			req, ok := item.Object.(FailedRequest)
			if ok {
				retryList = append(retryList, req)
			}
		}

		return c.JSON(fiber.Map{
			"retry_requests": retryList,
			"total":          len(retryList),
		})
	})

	app.Get("/", func(c *fiber.Ctx) error {
		return c.SendString("Hello from retry API")
	})

	// Start worker untuk retry setiap 5 menit
	go startRetryWorker()

	log.Fatal(app.Listen(":8080"))
}

// Worker untuk retry request setiap 5 menit
func startRetryWorker() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		<-ticker.C
		log.Println("Retry worker running...")

		// Ambil semua request yang gagal
		mu.Lock()
		for key, item := range requestCache.Items() {
			req, ok := item.Object.(FailedRequest)
			if !ok {
				continue
			}

			// Jika sudah lebih dari 8 jam, hapus dari cache
			if time.Since(req.CreatedAt) > req.MaxDuration {
				requestCache.Delete(key)
				log.Println("Request expired and removed:", req.FailedURLs[0])
				continue
			}

			// Kirim ulang request
			success := sendRequest(req)
			if success {
				requestCache.Delete(key) // Hapus jika berhasil
			} else {
				req.RetryCount++
				requestCache.Set(key, req, 8*time.Hour) // Update retry count
			}
		}
		mu.Unlock()
	}
}

// Fungsi untuk mengirim ulang request ke setiap URL dalam array
func sendRequest(req FailedRequest) bool {
	client := &http.Client{Timeout: 10 * time.Second}

	// Proses setiap URL yang gagal
	for _, url := range req.FailedURLs {
		log.Printf("Retrying request to: %s", url)

		// Buat request baru
		newReq, err := http.NewRequest(req.Method, url, bytes.NewBuffer(req.Body))
		if err != nil {
			log.Println("Error creating request:", err)
			return false
		}

		// Tambahkan headers
		for key, value := range req.Headers {
			newReq.Header.Set(key, value)
		}

		// log.Println("method: ", req.Method)
		// log.Println("url: ", url)
		// log.Println("body: ", string(req.Body))

		// Kirim request
		resp, err := client.Do(newReq)
		if err != nil || resp.StatusCode >= 400 {
			log.Printf("Retry failed for %s (Retry count: %d)\n", url, req.RetryCount)
			return false
		}

		log.Printf("Retry success for %s\n", url)
	}

	return true
}
