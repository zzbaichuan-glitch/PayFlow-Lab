package distributed

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type idempotencyCacheRecord struct {
	PaymentID   string `json:"payment_id"`
	Fingerprint string `json:"fingerprint"`
}

// Cache is an acceleration layer only. Every correctness decision is repeated
// against MySQL, whose unique key remains the final idempotency guard.
type Cache struct {
	client *redis.Client
	prefix string
	ttl    time.Duration
}

func OpenRedis(ctx context.Context, address, password, prefix string) (*Cache, error) {
	if strings.TrimSpace(address) == "" {
		return nil, nil
	}
	if prefix == "" {
		prefix = "payflow:"
	}
	client := redis.NewClient(&redis.Options{Addr: address, Password: password})
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		return nil, err
	}
	return &Cache{client: client, prefix: prefix, ttl: 24 * time.Hour}, nil
}

func (c *Cache) Close() error {
	if c == nil {
		return nil
	}
	return c.client.Close()
}

func (c *Cache) GetIdempotency(ctx context.Context, key string) (string, string, bool) {
	if c == nil {
		return "", "", false
	}
	raw, err := c.client.Get(ctx, c.idempotencyKey(key)).Bytes()
	if err != nil {
		return "", "", false
	}
	var record idempotencyCacheRecord
	if json.Unmarshal(raw, &record) != nil || record.PaymentID == "" || record.Fingerprint == "" {
		return "", "", false
	}
	return record.PaymentID, record.Fingerprint, true
}

func (c *Cache) SetIdempotency(ctx context.Context, key, paymentID, fingerprint string) {
	if c == nil {
		return
	}
	raw, err := json.Marshal(idempotencyCacheRecord{PaymentID: paymentID, Fingerprint: fingerprint})
	if err != nil {
		return
	}
	_ = c.client.Set(ctx, c.idempotencyKey(key), raw, c.ttl).Err()
}

func (c *Cache) Ping(ctx context.Context) error {
	if c == nil {
		return nil
	}
	return c.client.Ping(ctx).Err()
}

func (c *Cache) Clear(ctx context.Context) error {
	if c == nil {
		return nil
	}
	var cursor uint64
	for {
		keys, next, err := c.client.Scan(ctx, cursor, c.prefix+"*", 100).Result()
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			if err := c.client.Del(ctx, keys...).Err(); err != nil {
				return err
			}
		}
		cursor = next
		if cursor == 0 {
			return nil
		}
	}
}

func (c *Cache) idempotencyKey(key string) string {
	digest := sha256.Sum256([]byte(key))
	return c.prefix + "idempotency:" + hex.EncodeToString(digest[:])
}
