package service

import (
	"context"

	"entgo.io/ent/dialect"
	dbent "github.com/Wei-Shaw/sub2api/ent"
	dbuser "github.com/Wei-Shaw/sub2api/ent/user"
)

// lockBillingWallet serializes currency-sensitive wallet mutations on databases
// with row locking. SQLite serializes the surrounding write transaction and
// does not support SELECT FOR UPDATE syntax.
func lockBillingWallet(ctx context.Context, client *dbent.Client, userID int64) (*dbent.User, error) {
	if err := AcquireSQLiteBillingWalletWriteLock(ctx, client, userID); err != nil {
		return nil, err
	}
	query := client.User.Query().Where(dbuser.IDEQ(userID))
	if client.Driver().Dialect() != dialect.SQLite {
		query = query.ForUpdate()
	}
	return query.Only(ctx)
}

// AcquireSQLiteBillingWalletWriteLock obtains SQLite's transaction-level write
// lock before reading a wallet snapshot. A plain SELECT without FOR UPDATE does
// not serialize a later balance or currency mutation.
func AcquireSQLiteBillingWalletWriteLock(ctx context.Context, client *dbent.Client, userID int64) error {
	if client == nil || client.Driver().Dialect() != dialect.SQLite {
		return nil
	}
	result, err := client.ExecContext(ctx, `
		UPDATE users
		SET billing_currency = billing_currency
		WHERE id = ? AND deleted_at IS NULL
	`, userID)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrUserNotFound
	}
	return nil
}
