package tests

import (
	"context"
	"errors"
	"testing"

	"gorm.io/gorm"
)

func TestTransactionWithContextTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db := DB.WithContext(ctx)

	err := db.Transaction(func(tx1 *gorm.DB) error {
		user1 := User{Name: "outer-user"}
		if err := tx1.Create(&user1).Error; err != nil {
			return err
		}

		// Start nested transaction
		err := tx1.Transaction(func(tx2 *gorm.DB) error {
			user2 := User{Name: "inner-user"}
			if err := tx2.Create(&user2).Error; err != nil {
				return err
			}

			// Cancel context to simulate timeout/cancellation
			cancel()

			// Try to do another operation which should fail or be affected by cancellation
			user3 := User{Name: "inner-user-2"}
			return tx2.Create(&user3).Error
		})

		return err
	})

	if err == nil {
		t.Fatalf("expected error due to context cancellation, got nil")
	}

	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.Canceled or context.DeadlineExceeded, got %v", err)
	}

	// Verify that neither outer-user nor inner-user exists in the database
	var count int64
	if err := DB.Model(&User{}).Where("name IN ?", []string{"outer-user", "inner-user", "inner-user-2"}).Count(&count).Error; err != nil {
		t.Fatalf("failed to count users: %v", err)
	}

	if count != 0 {
		t.Fatalf("expected 0 users to be created, but found %d", count)
	}
}
